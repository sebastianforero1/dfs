# namenode/app_namenode.py
from flask import Flask, request, jsonify
import logging
import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))

import metadata_manager
from common import config
# Importar gRPC generado si el NameNode necesita hacer llamadas gRPC (ej. a DataNodes)
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'generated'))
import generated.dfs_pb2 as dfs_pb2
import generated.dfs_pb2_grpc as dfs_pb2_grpc
import grpc


app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Operaciones del Sistema de Archivos (API REST) ---
@app.route('/mkdir', methods=['POST']) # [cite: 28]
def mkdir():
    data = request.get_json()
    path = data.get('path')
    if not path:
        return jsonify({"error": "Se requiere la ruta"}), 400
    obj, message = metadata_manager.create_directory(path)
    if obj:
        return jsonify({"message": message, "path": path, "id": obj['id']}), 201
    return jsonify({"error": message}), 400

@app.route('/ls', methods=['GET']) # [cite: 28]
def ls():
    path = request.args.get('path', '/')
    items, message = metadata_manager.list_directory(path)
    if items is not None:
        return jsonify({"path": path, "contents": items}), 200
    return jsonify({"error": message}), 404

@app.route('/rm', methods=['POST']) # [cite: 28]
def rm():
    data = request.get_json()
    path = data.get('path')
    if not path:
        return jsonify({"error": "Se requiere la ruta"}), 400
    
    success, message, block_ids_to_delete = metadata_manager.remove_object(path)
    
    if success:
        if block_ids_to_delete: # Si era un archivo y tenía bloques
            logger.info(f"Archivo {path} eliminado. Bloques a limpiar en DataNodes: {block_ids_to_delete}")
            # Instruir a los DataNodes para que eliminen estos bloques físicamente
            for block_id in block_ids_to_delete:
                datanode_grpc_addresses = metadata_manager.get_block_locations_for_delete(block_id)
                for dn_addr in datanode_grpc_addresses:
                    try:
                        with grpc.insecure_channel(dn_addr) as channel:
                            stub = dfs_pb2_grpc.DataNodeServiceStub(channel)
                            delete_req = dfs_pb2.DeleteBlockRequest(block_id=block_id)
                            response = stub.DeleteBlock(delete_req, timeout=5)
                            if response.success:
                                logger.info(f"Bloque {block_id} eliminado de DataNode {dn_addr}")
                            else:
                                logger.warning(f"DataNode {dn_addr} falló al eliminar el bloque {block_id}: {response.message}")
                    except grpc.RpcError as e:
                        logger.error(f"Error gRPC al contactar a DataNode {dn_addr} para eliminar bloque {block_id}: {e.details()}")
                    except Exception as e_gen:
                        logger.error(f"Error inesperado al contactar a DataNode {dn_addr} para eliminar bloque {block_id}: {str(e_gen)}")
        return jsonify({"message": message}), 200
    return jsonify({"error": message}), 400


@app.route('/rmdir', methods=['POST']) # [cite: 28]
def rmdir():
    data = request.get_json()
    path = data.get('path')
    if not path: return jsonify({"error": "Se requiere la ruta"}), 400
    obj = metadata_manager._get_object_by_path(path) # Verificar que es un directorio
    if not obj: return jsonify({"error": "Directorio no encontrado"}), 404
    if not obj['is_directory']: return jsonify({"error": f"Ruta '{path}' no es un directorio."}), 400
    success, message, _ = metadata_manager.remove_object(path) # remove_object maneja si está vacío
    if success: return jsonify({"message": message}), 200
    return jsonify({"error": message}), 400

# --- Operaciones de Transferencia de Archivos (API REST) ---
@app.route('/put/initiate', methods=['POST']) # [cite: 26, 28]
def put_initiate():
    data = request.get_json()
    file_path = data.get('path')
    total_size = data.get('size')
    if not file_path or total_size is None:
        return jsonify({"error": "Se requieren ruta de archivo y tamaño total"}), 400
    if not isinstance(total_size, int) or total_size < 0:
        return jsonify({"error": "Tamaño total inválido"}), 400

    assignment_info, message = metadata_manager.initiate_file_put(file_path, total_size)
    if assignment_info:
        return jsonify({"message": message, "data": assignment_info}), 200 # [cite: 25]
    return jsonify({"error": message}), 400

@app.route('/put/complete', methods=['POST']) # [cite: 28]
def put_complete():
    data = request.get_json()
    file_path = data.get('path')
    if not file_path: return jsonify({"error": "Se requiere la ruta del archivo"}), 400
    logger.info(f"Cliente completó 'put' para: {file_path}")
    return jsonify({"message": f"Procesamiento de archivo {file_path} reconocido como completo por NameNode."}), 200

@app.route('/get', methods=['GET']) # [cite: 28]
def get_file():
    file_path = request.args.get('path')
    if not file_path: return jsonify({"error": "Se requiere la ruta del archivo"}), 400
    file_info, message = metadata_manager.get_file_info_for_read(file_path) # [cite: 25]
    if file_info: return jsonify({"message": message, "data": file_info}), 200
    return jsonify({"error": message}), 404

# --- Gestión de DataNode (API REST) ---
@app.route('/datanode/register', methods=['POST'])
def register_datanode_route():
    data = request.get_json()
    datanode_id = data.get('datanode_id')
    grpc_address = data.get('grpc_address')
    flask_address = data.get('flask_address')
    if not all([datanode_id, grpc_address, flask_address]):
        return jsonify({"error": "datanode_id, grpc_address, y flask_address son requeridos"}), 400
    result, message = metadata_manager.register_datanode(datanode_id, grpc_address, flask_address)
    if result: return jsonify({"message": message, "datanode_id_assigned": result['id']}), 201
    return jsonify({"error": message}), 400

@app.route('/datanode/heartbeat', methods=['POST'])
def datanode_heartbeat_route():
    data = request.get_json()
    datanode_id = data.get('datanode_id')
    if not datanode_id: return jsonify({"error": "datanode_id es requerido"}), 400
    success, message, tasks = metadata_manager.datanode_heartbeat(datanode_id)
    if success: return jsonify({"message": message, "tasks": tasks}), 200
    return jsonify({"error": message}), 400

@app.cli.command('init-db')
def init_db_command():
    metadata_manager.init_db()
    logger.info('Base de datos inicializada.')

if __name__ == '__main__':
    db_file_path = os.path.join(os.path.dirname(__file__), config.METADATA_DB_PATH)
    if not os.path.exists(db_file_path):
         logger.info("Archivo de BD no encontrado, inicializando...")
         metadata_manager.init_db()
    else:
        logger.info("Archivo de BD encontrado.")
    metadata_manager.get_active_datanodes() # Marcar DNs inactivos al inicio
    logger.info(f"NameNode iniciando en {config.NAMENODE_HOST}:{config.NAMENODE_PORT}")
    app.run(host=config.NAMENODE_HOST, port=config.NAMENODE_PORT, debug=True, use_reloader=False)