# client/client_sdk.py
import requests
import grpc
import os
import math
import logging
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'generated'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import generated.dfs_pb2 as dfs_pb2
import generated.dfs_pb2_grpc as dfs_pb2_grpc
from common import config

logger = logging.getLogger(__name__)
CHUNK_SIZE_CLIENT_GRPC = 1 * 1024 * 1024 # 1MB

class DFSClient:
    def __init__(self, namenode_url):
        self.namenode_url = namenode_url
        self.current_path = "/"

    def _make_namenode_request(self, method, endpoint, params=None, json_data=None): # Canal de Control REST [cite: 18]
        url = f"{self.namenode_url}{endpoint}"
        try:
            if method.upper() == 'GET': response = requests.get(url, params=params, timeout=10)
            elif method.upper() == 'POST': response = requests.post(url, json=json_data, timeout=10)
            else: raise ValueError(f"Método HTTP no soportado: {method}")
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            logger.error(f"Error HTTP para {url}: {e.response.status_code} - {e.response.text}")
            return e.response.json() if e.response.content else {"error": e.response.text}
        except requests.exceptions.RequestException as e:
            logger.error(f"Fallo de petición para {url}: {e}")
            return {"error": str(e)}

    def _resolve_path(self, path):
        if path.startswith('/'): return path
        return os.path.normpath(os.path.join(self.current_path, path))

    def mkdir(self, dir_path): return self._make_namenode_request('POST', '/mkdir', json_data={'path': self._resolve_path(dir_path)})
    def ls(self, path="."): return self._make_namenode_request('GET', '/ls', params={'path': self._resolve_path(path)})
    def rmdir(self, dir_path): return self._make_namenode_request('POST', '/rmdir', json_data={'path': self._resolve_path(dir_path)})
    def rm(self, item_path): return self._make_namenode_request('POST', '/rm', json_data={'path': self._resolve_path(item_path)})

    def _write_block_to_datanode(self, block_data, block_id, file_id, primary_dn_addr, secondary_dn_addr): # Canal de Datos gRPC [cite: 18]
        try:
            with grpc.insecure_channel(primary_dn_addr) as channel:
                stub = dfs_pb2_grpc.DataNodeServiceStub(channel)
                def generate_reqs():
                    yield dfs_pb2.WriteBlockRequest(block_info=dfs_pb2.BlockInfo(block_id=block_id, file_id=str(file_id), secondary_datanode_grpc_address=secondary_dn_addr or ""))
                    offset = 0
                    while offset < len(block_data):
                        chunk = block_data[offset : offset + CHUNK_SIZE_CLIENT_GRPC]
                        yield dfs_pb2.WriteBlockRequest(chunk_data=chunk)
                        offset += len(chunk)
                
                response = stub.WriteBlock(generate_reqs(), timeout=30)
                return response.success, response.message
        except Exception as e:
            logger.error(f"Error gRPC escribiendo bloque {block_id} a {primary_dn_addr}: {e}")
            return False, str(e)

    def put(self, local_file_path, dfs_file_path): # [cite: 28]
        abs_dfs_path = self._resolve_path(dfs_file_path)
        if not os.path.exists(local_file_path) or not os.path.isfile(local_file_path):
            return {"error": f"Archivo local {local_file_path} no encontrado o no es un archivo."}
        
        total_size = os.path.getsize(local_file_path)
        init_resp = self._make_namenode_request('POST', '/put/initiate', json_data={'path': abs_dfs_path, 'size': total_size}) # [cite: 26]
        if 'error' in init_resp or not init_resp.get('data'): return init_resp

        file_id = init_resp['data']['file_id']
        assignments = init_resp['data']['block_assignments'] # [cite: 25]
        
        with open(local_file_path, 'rb') as f:
            for assign in assignments: # Cada archivo es particionado en n bloques [cite: 20]
                block_data = f.read(config.BLOCK_SIZE_BYTES) # El tamaño real del bloque lo determina el NameNode
                if not block_data: break
                success, msg = self._write_block_to_datanode(block_data, assign['block_id'], file_id, assign['primary_datanode_grpc'], assign.get('secondary_datanode_grpc')) # [cite: 18, 27]
                if not success: return {"error": f"Fallo al escribir bloque {assign['block_id']}: {msg}"}
        
        return self._make_namenode_request('POST', '/put/complete', json_data={'path': abs_dfs_path, 'file_id': file_id})

    def _read_block_from_datanode(self, block_id, datanode_addrs): # Canal de Datos gRPC [cite: 18]
        for addr in datanode_addrs: # Intenta con réplicas si la primera falla [cite: 24]
            try:
                with grpc.insecure_channel(addr) as channel:
                    stub = dfs_pb2_grpc.DataNodeServiceStub(channel)
                    block_content = bytearray()
                    for resp_chunk in stub.ReadBlock(dfs_pb2.ReadBlockRequest(block_id=block_id), timeout=20):
                        block_content.extend(resp_chunk.chunk_data)
                    return bytes(block_content)
            except Exception as e:
                logger.warning(f"Fallo al leer bloque {block_id} de {addr}: {e}")
        return None

    def get(self, dfs_file_path, local_target_path): # [cite: 28]
        abs_dfs_path = self._resolve_path(dfs_file_path)
        info_resp = self._make_namenode_request('GET', '/get', params={'path': abs_dfs_path}) # [cite: 25]
        if 'error' in info_resp or not info_resp.get('data'): return info_resp
        
        blocks_meta = sorted(info_resp['data']['blocks'], key=lambda x: x['sequence'])
        try:
            with open(local_target_path, 'wb') as f_out:
                for meta in blocks_meta:
                    block_content = self._read_block_from_datanode(meta['block_id'], meta['datanode_grpc_addresses']) # [cite: 24]
                    if block_content is None:
                        try: os.remove(local_target_path) # Limpieza
                        except OSError: pass
                        return {"error": f"Fallo al leer bloque {meta['block_id']}. Descarga abortada."}
                    f_out.write(block_content)
            return {"message": f"Archivo '{dfs_file_path}' descargado a '{local_target_path}'."}
        except Exception as e:
            try: os.remove(local_target_path)
            except OSError: pass
            return {"error": f"Error durante descarga: {e}"}


    def cd(self, path): # [cite: 28]
        prospective_path = self._resolve_path(path)
        if prospective_path == "/" or prospective_path == ".":
            self.current_path = "/"
            return {"message": f"Directorio actual cambiado a {self.current_path}"}
        
        ls_result = self.ls(prospective_path) # Verifica si el directorio existe y es accesible
        if 'error' in ls_result:
            return {"error": f"No se puede cambiar a '{prospective_path}': {ls_result['error']}"}
        
        # Asegurarse que es un directorio (ls podría listar un archivo si se implementara así)
        # Esta verificación es indirecta. Una forma más directa sería que el NameNode devuelva el tipo.
        # Por ahora, si `ls` no da error, asumimos que es un directorio válido para `cd`.
        self.current_path = prospective_path
        return {"message": f"Directorio actual cambiado a {self.current_path}"}