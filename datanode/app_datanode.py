# datanode/app_datanode.py
import grpc
from concurrent import futures
import time
import logging
import argparse
import requests
import threading
import os
from flask import Flask, jsonify

import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'generated'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import generated.dfs_pb2_grpc as dfs_pb2_grpc
from services_datanode import DataNodeServiceImpl
from common import config
import block_manager

admin_app_dn = Flask(__name__) # Diferente de la app del NameNode

# --- Heartbeat ---
def send_heartbeat(datanode_id, datanode_grpc_addr, datanode_flask_addr, namenode_url):
    while True:
        try:
            payload = {"datanode_id": datanode_id} # Simplificado, podría incluir reporte de bloques, espacio, etc.
            # El endpoint de heartbeat en NameNode también puede manejar re-registro/actualización de IP
            reg_payload = {
                "datanode_id": datanode_id,
                "grpc_address": datanode_grpc_addr,
                "flask_address": datanode_flask_addr
            }
            # Primero intentar registrar (es idempotente en el NameNode)
            try:
                response = requests.post(f"{namenode_url}/datanode/register", json=reg_payload, timeout=5)
                response.raise_for_status()
                # logging.info(f"Heartbeat/Register to NameNode successful: {response.json().get('message')}")
            except requests.exceptions.RequestException as e_reg:
                 logging.warning(f"Failed to send register/heartbeat (via register endpoint) to NameNode: {e_reg}")


            # Enviar heartbeat regular
            hb_response = requests.post(f"{namenode_url}/datanode/heartbeat", json={"datanode_id": datanode_id})
            hb_response.raise_for_status()
            logging.info(f"Heartbeat to NameNode successful: {hb_response.json().get('message')}")
            # Aquí se podrían procesar tareas devueltas por el NameNode
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to send heartbeat to NameNode: {e}")
        except Exception as e_gen:
            logging.error(f"Unexpected error in heartbeat thread: {e_gen}")
        time.sleep(config.HEARTBEAT_INTERVAL_SEC)

def serve_grpc_dn(datanode_id, grpc_port, block_dir_instance):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    service_impl = DataNodeServiceImpl(datanode_id, block_dir_instance)
    dfs_pb2_grpc.add_DataNodeServiceServicer_to_server(service_impl, server)
    listen_addr = f"{config.DATANODE_HOST}:{grpc_port}"
    server.add_insecure_port(listen_addr)
    logging.info(f"DataNode gRPC server {datanode_id} iniciando en {listen_addr}")
    server.start()
    server.wait_for_termination()

def serve_flask_dn(datanode_id, flask_port):
    logging.info(f"DataNode Flask admin {datanode_id} iniciando en {config.DATANODE_HOST}:{flask_port}")
    # admin_app_dn.run(...) # Ejecutar en un hilo separado
    admin_app_dn.run(host=config.DATANODE_HOST, port=flask_port, debug=False, use_reloader=False)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Servidor DataNode para DFS.")
    parser.add_argument("--id", required=True, help="ID único para este DataNode.")
    parser.add_argument("--grpc_port", type=int, default=config.DATANODE_DEFAULT_GRPC_PORT, help="Puerto gRPC.")
    parser.add_argument("--flask_port", type=int, required=True, help="Puerto Flask para admin.")
    parser.add_argument("--namenode_url", default=config.NAMENODE_URL, help="URL del NameNode.")
    parser.add_argument("--blocks_dir", default=None, help="Directorio para bloques.")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format=f'%(asctime)s - {args.id} - %(levelname)s - %(message)s')
    
    instance_block_dir = args.blocks_dir or os.path.join(os.getcwd(), f"{config.BLOCKS_DIR_DEFAULT}_{args.id}")
    block_manager.setup_block_storage(instance_block_dir)

    # Para EC2, obtener la IP pública o privada según sea necesario para la comunicación.
    # DATANODE_PUBLIC_IP se usará para registrarse con el NameNode.
    # Los clientes y otros nodos deben poder alcanzar esta IP:puerto.
    datanode_public_ip_env = os.environ.get('DATANODE_PUBLIC_IP', 'localhost')
    datanode_grpc_public_address = f"{datanode_public_ip_env}:{args.grpc_port}"
    datanode_flask_public_address = f"http://{datanode_public_ip_env}:{args.flask_port}"

    # 1. Registrar con NameNode (mejorado en la función de heartbeat)
    # 2. Iniciar Hilo de Heartbeat
    hb_thread = threading.Thread(target=send_heartbeat, args=(args.id, datanode_grpc_public_address, datanode_flask_public_address, args.namenode_url), daemon=True)
    hb_thread.start()
    
    # 3. Iniciar Servidor Flask Admin (opcional, si se necesitan comandos REST del NameNode al DataNode)
    # flask_thread_dn = threading.Thread(target=serve_flask_dn, args=(args.id, args.flask_port), daemon=True)
    # flask_thread_dn.start() # Por ahora, la app de flask no tiene rutas, así que no es crítico

    # 4. Iniciar Servidor gRPC (bloquea el hilo principal aquí)
    serve_grpc_dn(args.id, args.grpc_port, instance_block_dir)