# namenode/metadata_manager.py
import sqlite3
import os
import uuid
from datetime import datetime, timedelta
import random
import logging

import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from common import config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

DB_PATH = os.path.join(os.path.dirname(__file__), config.METADATA_DB_PATH)

def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

def init_db():
    if os.path.exists(DB_PATH):
        logging.info("La base de datos ya existe.")
    conn = get_db_connection()
    schema_path = os.path.join(os.path.dirname(__file__), 'db_schema.sql')
    with open(schema_path) as f:
        conn.executescript(f.read())
    conn.commit()
    conn.close()
    logging.info("Base de datos inicializada.")

# --- Operaciones de Directorios y Archivos ---
def _get_object_by_path(path_str):
    conn = get_db_connection()
    parts = [p for p in path_str.strip('/').split('/') if p]
    current_parent_id = 1 # ID del directorio raíz

    if not parts: # La ruta es la raíz
        obj = conn.execute("SELECT * FROM fs_objects WHERE id = ?", (current_parent_id,)).fetchone()
        conn.close()
        return obj

    obj = None
    for i, part_name in enumerate(parts):
        is_last_part = (i == len(parts) - 1)
        query = "SELECT * FROM fs_objects WHERE parent_id = ? AND name = ?"
        if not is_last_part: # Debe ser un directorio si no es la última parte
            query += " AND is_directory = TRUE"

        obj = conn.execute(query, (current_parent_id, part_name)).fetchone()
        if not obj:
            conn.close()
            return None
        current_parent_id = obj['id']
    conn.close()
    return obj

def create_directory(path_str): # Para `mkdir` [cite: 28]
    conn = get_db_connection()
    if not path_str.startswith('/'):
        return None, "La ruta debe ser absoluta (comenzar con '/')."
    if path_str == '/':
        return _get_object_by_path('/'), "El directorio raíz ya existe."

    parts = [p for p in path_str.strip('/').split('/') if p]
    if not parts:
         return _get_object_by_path('/'), "No se puede crear la raíz."

    dir_name = parts[-1]
    parent_path = '/' + '/'.join(parts[:-1])

    parent_obj = _get_object_by_path(parent_path)
    if not parent_obj:
        return None, f"La ruta padre '{parent_path}' no existe."
    if not parent_obj['is_directory']:
        return None, f"La ruta padre '{parent_path}' no es un directorio."

    try:
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO fs_objects (parent_id, name, is_directory) VALUES (?, ?, TRUE)",
            (parent_obj['id'], dir_name)
        )
        new_dir_id = cursor.lastrowid
        conn.commit()
        new_dir_obj = conn.execute("SELECT * FROM fs_objects WHERE id = ?", (new_dir_id,)).fetchone()
        conn.close()
        return new_dir_obj, "Directorio creado exitosamente."
    except sqlite3.IntegrityError:
        conn.close()
        return None, f"El directorio o archivo '{dir_name}' ya existe en '{parent_path}'."

def list_directory(path_str): # Para `ls` [cite: 28]
    parent_obj = _get_object_by_path(path_str)
    if not parent_obj:
        return None, f"Ruta '{path_str}' no encontrada."
    if not parent_obj['is_directory']:
        return None, f"Ruta '{path_str}' no es un directorio."

    conn = get_db_connection()
    items = conn.execute(
        "SELECT name, is_directory, size, modification_time FROM fs_objects WHERE parent_id = ?",
        (parent_obj['id'],)
    ).fetchall()
    conn.close()
    return [{"name": item['name'], "is_directory": bool(item['is_directory']),
             "size": item['size'], "modified": item['modification_time']} for item in items], "Listado exitoso."

def remove_object(path_str): # Para `rm` y `rmdir` [cite: 28]
    conn = get_db_connection()
    obj = _get_object_by_path(path_str)
    if not obj:
        return False, "Objeto no encontrado.", []
    if obj['name'] == '/' and obj['parent_id'] is None: # Directorio Raíz
        return False, "No se puede eliminar el directorio raíz.", []

    if obj['is_directory']:
        children = conn.execute("SELECT 1 FROM fs_objects WHERE parent_id = ? LIMIT 1", (obj['id'],)).fetchone()
        if children:
            return False, "El directorio no está vacío.", []
    
    deleted_block_ids = []
    if not obj['is_directory']: # Si es un archivo, obtener sus bloques antes de la eliminación en cascada
        blocks_stmt = conn.execute("SELECT block_id FROM blocks WHERE file_id = ?", (obj['id'],))
        deleted_block_ids = [row['block_id'] for row in blocks_stmt.fetchall()]

    try:
        conn.execute("DELETE FROM fs_objects WHERE id = ?", (obj['id'],)) # CASCADE se encarga de blocks y block_locations
        conn.commit()
        conn.close()
        return True, "Objeto eliminado exitosamente.", deleted_block_ids
    except Exception as e:
        conn.rollback()
        conn.close()
        logging.error(f"Error eliminando objeto {path_str}: {e}")
        return False, f"Error eliminando objeto: {e}", []


# --- Operaciones de Bloques de Archivo ---
def initiate_file_put(file_path_str, total_size): # Para `put` [cite: 28]
    conn = get_db_connection()
    if not file_path_str.startswith('/'):
        return None, "La ruta debe ser absoluta."
    if file_path_str.endswith('/'):
        return None, "La ruta no puede ser un directorio para 'put'."

    parts = [p for p in file_path_str.strip('/').split('/') if p]
    file_name = parts[-1]
    parent_path = '/' + '/'.join(parts[:-1])

    parent_obj = _get_object_by_path(parent_path)
    if not parent_obj:
        return None, f"Ruta padre '{parent_path}' no existe."
    if not parent_obj['is_directory']:
        return None, f"Ruta padre '{parent_path}' no es un directorio."

    existing_obj = conn.execute("SELECT id FROM fs_objects WHERE parent_id = ? AND name = ?", (parent_obj['id'], file_name)).fetchone()
    if existing_obj: # WORM: no sobrescribir [cite: 10, 15]
        return None, f"Archivo '{file_name}' ya existe en '{parent_path}'. Elimínalo primero."

    active_datanodes = get_active_datanodes()
    if len(active_datanodes) < config.REPLICATION_FACTOR: # [cite: 23]
        return None, f"No hay suficientes DataNodes activos ({len(active_datanodes)}) para el factor de replicación {config.REPLICATION_FACTOR}."

    try:
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO fs_objects (parent_id, name, is_directory, size) VALUES (?, ?, FALSE, ?)",
            (parent_obj['id'], file_name, total_size)
        )
        file_id = cursor.lastrowid
        conn.commit() 

        num_blocks = (total_size + config.BLOCK_SIZE_BYTES - 1) // config.BLOCK_SIZE_BYTES # [cite: 20]
        block_assignments = []

        for i in range(num_blocks):
            block_id = f"{file_id}_{i}"
            actual_block_size = min(config.BLOCK_SIZE_BYTES, total_size - (i * config.BLOCK_SIZE_BYTES))

            # Selección de DataNodes para este bloque [cite: 19, 26]
            chosen_nodes_for_block = random.sample(active_datanodes, config.REPLICATION_FACTOR)
            primary_node = chosen_nodes_for_block[0] # El DataNode que recibe del Cliente es Líder del bloque [cite: 27]
            secondary_nodes = chosen_nodes_for_block[1:] # El Seguidor del bloque [cite: 27]

            cursor.execute(
                "INSERT INTO blocks (block_id, file_id, block_sequence, size) VALUES (?, ?, ?, ?)",
                (block_id, file_id, i, actual_block_size)
            )
            cursor.execute( # Primario
                "INSERT INTO block_locations (block_id, datanode_id, is_primary) VALUES (?, ?, TRUE)",
                (block_id, primary_node['id'])
            )
            for node in secondary_nodes: # Secundarios/Réplicas [cite: 22]
                 cursor.execute(
                    "INSERT INTO block_locations (block_id, datanode_id, is_primary) VALUES (?, ?, FALSE)",
                    (block_id, node['id'])
                )

            block_assignments.append({
                "block_id": block_id,
                "primary_datanode_grpc": primary_node['grpc_address'],
                "secondary_datanode_grpc": secondary_nodes[0]['grpc_address'] if secondary_nodes else None
            })
        conn.commit()
        conn.close()
        return {"file_id": file_id, "block_assignments": block_assignments, "block_size": config.BLOCK_SIZE_BYTES}, "Inicio de 'put' de archivo exitoso."
    except Exception as e:
        logging.error(f"Error en initiate_file_put: {e}")
        conn.rollback()
        conn.close()
        if 'file_id' in locals() and file_id: # Limpieza
            cleanup_conn = get_db_connection()
            cleanup_conn.execute("DELETE FROM fs_objects WHERE id = ?", (file_id,))
            cleanup_conn.commit()
            cleanup_conn.close()
        return None, f"Falló el inicio de 'put' de archivo: {str(e)}"

def get_file_info_for_read(file_path_str): # Para `get` [cite: 28]
    file_obj = _get_object_by_path(file_path_str)
    if not file_obj:
        return None, "Archivo no encontrado."
    if file_obj['is_directory']:
        return None, "La ruta es un directorio, no un archivo."

    conn = get_db_connection() # El NameNode entrega al cliente la lista y orden de bloques [cite: 25]
    blocks_query = """
        SELECT b.block_id, b.block_sequence, b.size, GROUP_CONCAT(dn.grpc_address) as datanode_grpc_addresses
        FROM blocks b
        JOIN block_locations bl ON b.block_id = bl.block_id
        JOIN datanodes dn ON bl.datanode_id = dn.id
        WHERE b.file_id = ? AND dn.is_active = TRUE
        GROUP BY b.block_id, b.block_sequence, b.size
        ORDER BY b.block_sequence
    """
    blocks_data = conn.execute(blocks_query, (file_obj['id'],)).fetchall()
    conn.close()

    if not blocks_data:
        return None, "Archivo encontrado, pero ningún DataNode activo contiene sus bloques."
    
    formatted_blocks = []
    for row in blocks_data:
        available_dns = row['datanode_grpc_addresses'].split(',')
        formatted_blocks.append({
            "block_id": row['block_id'],
            "sequence": row['block_sequence'],
            "size": row['size'],
            "datanode_grpc_addresses": available_dns # El cliente elige uno [cite: 24]
        })
    
    # Validar que todos los bloques estén presentes
    expected_num_blocks = (file_obj['size'] + config.BLOCK_SIZE_BYTES - 1) // config.BLOCK_SIZE_BYTES
    if len(formatted_blocks) != expected_num_blocks:
        logging.warning(f"Faltan bloques o DataNodes para el archivo {file_path_str}. Esperados: {expected_num_blocks}, Encontrados: {len(formatted_blocks)}")
        return None, "El archivo está incompleto o algunos bloques no están disponibles actualmente."

    return {
        "file_name": file_obj['name'],
        "total_size": file_obj['size'],
        "block_size": config.BLOCK_SIZE_BYTES,
        "blocks": formatted_blocks
    }, "Información de archivo recuperada para lectura."


# --- Gestión de DataNode ---
def register_datanode(datanode_id, grpc_address, flask_address):
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO datanodes (datanode_id, grpc_address, flask_address, last_heartbeat, is_active)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP, TRUE)
            ON CONFLICT(datanode_id) DO UPDATE SET
                grpc_address = excluded.grpc_address,
                flask_address = excluded.flask_address,
                last_heartbeat = CURRENT_TIMESTAMP,
                is_active = TRUE
            """, (datanode_id, grpc_address, flask_address)
        )
        conn.commit()
        dn_db_id = cursor.lastrowid or conn.execute("SELECT id FROM datanodes WHERE datanode_id = ?", (datanode_id,)).fetchone()['id']
        conn.close()
        logging.info(f"DataNode {datanode_id} registrado/actualizado. DB ID: {dn_db_id}")
        return {"id": dn_db_id, "datanode_id": datanode_id}, "DataNode registrado/actualizado."
    except sqlite3.IntegrityError as e:
        conn.close()
        logging.error(f"Fallo al registrar DataNode {datanode_id}: {e}")
        return None, f"Registro de DataNode fallido: {e}"

def datanode_heartbeat(datanode_id):
    conn = get_db_connection()
    now = datetime.utcnow()
    try:
        cursor = conn.execute("UPDATE datanodes SET last_heartbeat = ?, is_active = TRUE WHERE datanode_id = ?", (now, datanode_id))
        if cursor.rowcount == 0:
            conn.close()
            return False, "DataNode no encontrado para heartbeat.", []
        conn.commit()
        # Aquí se podrían añadir tareas de re-replicación/eliminación si se detectan inconsistencias
        replication_tasks = [] 
        deletion_tasks = [] 
        conn.close()
        logging.info(f"Heartbeat recibido de {datanode_id}")
        return True, "Heartbeat exitoso.", {"replication_tasks": replication_tasks, "deletion_tasks": deletion_tasks}
    except Exception as e:
        conn.close()
        logging.error(f"Error procesando heartbeat para {datanode_id}: {e}")
        return False, f"Heartbeat fallido: {e}", []

def get_active_datanodes():
    conn = get_db_connection()
    timeout_threshold = datetime.utcnow() - timedelta(seconds=config.HEARTBEAT_INTERVAL_SEC * config.HEARTBEAT_TIMEOUT_FACTOR)
    conn.execute("UPDATE datanodes SET is_active = FALSE WHERE last_heartbeat < ? AND is_active = TRUE", (timeout_threshold,))
    conn.commit()
    datanodes = conn.execute("SELECT id, datanode_id, grpc_address, flask_address FROM datanodes WHERE is_active = TRUE").fetchall()
    conn.close()
    return [dict(dn) for dn in datanodes]

def get_block_locations_for_delete(block_id):
    conn = get_db_connection()
    # Obtener todas las ubicaciones, incluso si el DN está inactivo, para intentar la eliminación
    locations = conn.execute("""
        SELECT dn.grpc_address
        FROM block_locations bl
        JOIN datanodes dn ON bl.datanode_id = dn.id
        WHERE bl.block_id = ?
    """, (block_id,)).fetchall()
    conn.close()
    return [loc['grpc_address'] for loc in locations]


if __name__ == '__main__':
    print("Inicializando base de datos del NameNode...")
    init_db()
    print("Inicialización de base de datos del NameNode completa.")