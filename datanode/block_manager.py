# datanode/block_manager.py
import os
import logging
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from common import config

logger = logging.getLogger(__name__)

def setup_block_storage(datanode_specific_block_dir):
    if not os.path.exists(datanode_specific_block_dir):
        os.makedirs(datanode_specific_block_dir)
        logger.info(f"Directorio de almacenamiento de bloques creado en {datanode_specific_block_dir}")

def get_block_path(block_id, block_dir_instance):
    return os.path.join(block_dir_instance, str(block_id))

def write_block_chunk(block_id, chunk_data, is_first_chunk, block_dir_instance):
    path = get_block_path(block_id, block_dir_instance)
    mode = 'wb' if is_first_chunk else 'ab'
    try:
        with open(path, mode) as f:
            f.write(chunk_data)
        return True, None
    except Exception as e:
        logger.error(f"Error escribiendo chunk al bloque {block_id}: {e}")
        return False, str(e)

def read_block_chunks(block_id, block_dir_instance, chunk_size=8192):
    path = get_block_path(block_id, block_dir_instance)
    if not os.path.exists(path):
        raise FileNotFoundError(f"Bloque {block_id} no encontrado en {path}")
    with open(path, 'rb') as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk: break
            yield chunk

def store_block_data(block_id, data, block_dir_instance): # Usado por ReplicateBlock
    path = get_block_path(block_id, block_dir_instance)
    try:
        with open(path, 'wb') as f:
            f.write(data)
        logger.info(f"Bloque {block_id} almacenado (replicado) en {path}, tamaño: {len(data)}")
        return True, "Bloque almacenado."
    except Exception as e:
        logger.error(f"Error almacenando bloque {block_id}: {e}")
        return False, str(e)

def delete_block_data(block_id, block_dir_instance):
    path = get_block_path(block_id, block_dir_instance)
    try:
        if os.path.exists(path):
            os.remove(path)
            logger.info(f"Bloque {block_id} eliminado de {path}.")
            return True, "Bloque eliminado."
        else:
            logger.warning(f"Bloque {block_id} no encontrado para eliminar en {path}.")
            return False, "Bloque no encontrado para eliminar." # NameNode puede tratar esto como éxito
    except Exception as e:
        logger.error(f"Error eliminando bloque {block_id}: {e}")
        return False, str(e)