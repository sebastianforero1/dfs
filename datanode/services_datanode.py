# datanode/services_datanode.py
import grpc
import logging
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'generated'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import generated.dfs_pb2 as dfs_pb2
import generated.dfs_pb2_grpc as dfs_pb2_grpc
import block_manager
from common import config

logger = logging.getLogger(__name__)

class DataNodeServiceImpl(dfs_pb2_grpc.DataNodeServiceServicer):
    def __init__(self, datanode_id, block_dir):
        self.datanode_id = datanode_id
        self.block_dir = block_dir
        block_manager.setup_block_storage(self.block_dir)
        logger.info(f"DataNodeService inicializado para {datanode_id} usando dir {self.block_dir}")

    def WriteBlock(self, request_iterator, context): # Escritura directa Cliente-DataNode [cite: 18]
        logger.info(f"[{self.datanode_id}] WriteBlock invocado.")
        block_info_msg = next(request_iterator).block_info
        block_id = block_info_msg.block_id
        secondary_dn_address = block_info_msg.secondary_datanode_grpc_address # El NameNode informa cuál es el secundario [cite: 27]

        is_first_chunk = True
        full_block_data_for_replication = bytearray()
        for req_chunk in request_iterator:
            chunk_data = req_chunk.chunk_data
            success, err = block_manager.write_block_chunk(block_id, chunk_data, is_first_chunk, self.block_dir)
            if not success:
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details(f"Fallo al escribir chunk: {err}")
                return dfs_pb2.WriteBlockResponse(block_id=block_id, success=False, message=err)
            is_first_chunk = False
            full_block_data_for_replication.extend(chunk_data)
        
        logger.info(f"[{self.datanode_id}] Bloque {block_id} escrito. Tamaño: {len(full_block_data_for_replication)}")
        
        # Replicar al DataNode secundario (Follower del bloque) [cite: 27]
        rep_success_msg = "Replicación no intentada (sin secundario)."
        if secondary_dn_address:
            logger.info(f"[{self.datanode_id}] Replicando bloque {block_id} a {secondary_dn_address}")
            try:
                with grpc.insecure_channel(secondary_dn_address) as channel:
                    stub = dfs_pb2_grpc.DataNodeServiceStub(channel)
                    rep_req = dfs_pb2.ReplicateBlockRequest(block_id=block_id, data=bytes(full_block_data_for_replication))
                    rep_resp = stub.ReplicateBlock(rep_req, timeout=10)
                    if rep_resp.success:
                        rep_success_msg = f"Replicado exitosamente a {secondary_dn_address}."
                    else:
                        rep_success_msg = f"Fallo al replicar a {secondary_dn_address}: {rep_resp.message}"
            except Exception as e_rep:
                rep_success_msg = f"Error replicando a {secondary_dn_address}: {str(e_rep)}"
                logger.error(f"[{self.datanode_id}] {rep_success_msg}")
        
        return dfs_pb2.WriteBlockResponse(block_id=block_id, success=True, message=f"Bloque escrito en primario. {rep_success_msg}")

    def ReadBlock(self, request, context): # Lectura directa Cliente-DataNode [cite: 18]
        block_id = request.block_id
        logger.info(f"[{self.datanode_id}] ReadBlock invocado para: {block_id}")
        try:
            for chunk_data in block_manager.read_block_chunks(block_id, self.block_dir):
                yield dfs_pb2.ReadBlockResponse(chunk_data=chunk_data)
        except FileNotFoundError:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"Bloque {block_id} no encontrado.")
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Error leyendo bloque: {str(e)}")
    
    def ReplicateBlock(self, request, context): # DataNode (Líder del bloque) a DataNode (Seguidor del bloque) [cite: 27]
        block_id = request.block_id
        data = request.data
        logger.info(f"[{self.datanode_id}] ReplicateBlock invocado para: {block_id}. Tamaño: {len(data)}")
        success, message = block_manager.store_block_data(block_id, data, self.block_dir)
        return dfs_pb2.ReplicateBlockResponse(block_id=block_id, success=success, message=message)

    def DeleteBlock(self, request, context):
        block_id = request.block_id
        logger.info(f"[{self.datanode_id}] DeleteBlock invocado para: {block_id}")
        success, message = block_manager.delete_block_data(block_id, self.block_dir)
        # Si el bloque no se encuentra, se considera una eliminación exitosa desde la perspectiva del NameNode.
        if not success and "no encontrado" in message.lower():
            success = True 
            message = "Bloque no encontrado, considerado eliminado."
        return dfs_pb2.DeleteBlockResponse(block_id=block_id, success=success, message=message)