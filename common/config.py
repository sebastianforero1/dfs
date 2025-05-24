# common/config.py
import os

# Configuración del NameNode
NAMENODE_HOST = os.environ.get('NAMENODE_HOST', '0.0.0.0') # Escucha en todas las interfaces
NAMENODE_PORT = 5000
NAMENODE_URL = os.environ.get('NAMENODE_PUBLIC_URL', f"http://localhost:{NAMENODE_PORT}") # URL accesible públicamente

# Configuración del DataNode
DATANODE_HOST = '0.0.0.0' # Escucha en todas las interfaces para gRPC y Flask interno
DATANODE_DEFAULT_GRPC_PORT = 50051
DATANODE_FLASK_PORT_START = 5001 # Para administración interna/comandos desde NameNode

# Configuración del Sistema de Archivos
BLOCK_SIZE_BYTES = int(os.environ.get('BLOCK_SIZE_BYTES', 1 * 1024 * 1024))  # Tamaño de bloque por defecto 1MB [cite: 21]
REPLICATION_FACTOR = int(os.environ.get('REPLICATION_FACTOR', 2)) # [cite: 23]

# BD de Metadatos del NameNode
METADATA_DB_PATH = 'namenode_metadata.db' # Relativo a donde se ejecuta la app del namenode

# Almacenamiento de Bloques del DataNode
BLOCKS_DIR_DEFAULT = 'datanode_blocks' # Relativo a donde se ejecuta la app del datanode

HEARTBEAT_INTERVAL_SEC = 10
HEARTBEAT_TIMEOUT_FACTOR = 3 # Si se pierden 3 heartbeats, el DN se considera caído