-- Objetos del Sistema de Archivos (Archivos y Directorios)
CREATE TABLE IF NOT EXISTS fs_objects (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    parent_id INTEGER, -- NULL para los hijos del directorio raíz
    name TEXT NOT NULL,
    is_directory BOOLEAN NOT NULL,
    size INTEGER DEFAULT 0, -- Para archivos, tamaño total
    creation_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    modification_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(parent_id, name) -- Asegura nombres únicos dentro de un directorio
);

-- Información de Bloques
CREATE TABLE IF NOT EXISTS blocks (
    block_id TEXT PRIMARY KEY, -- ej: fileid_numerosecuencia
    file_id INTEGER NOT NULL,
    block_sequence INTEGER NOT NULL, -- Orden del bloque en el archivo
    size INTEGER NOT NULL,
    FOREIGN KEY (file_id) REFERENCES fs_objects(id) ON DELETE CASCADE
);

-- Información de DataNodes
CREATE TABLE IF NOT EXISTS datanodes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    datanode_id TEXT UNIQUE NOT NULL, -- ej: "datanode-ip-puerto" o un UUID
    grpc_address TEXT NOT NULL UNIQUE, -- ej: "ip:puerto_grpc"
    flask_address TEXT NOT NULL UNIQUE, -- ej: "http://ip:puerto_flask"
    last_heartbeat TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE,
    total_space INTEGER DEFAULT 0, -- Opcional: Para asignación más inteligente
    used_space INTEGER DEFAULT 0   -- Opcional
);

-- Mapeo de Bloque a DataNode (réplicas)
CREATE TABLE IF NOT EXISTS block_locations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    block_id TEXT NOT NULL,
    datanode_id INTEGER NOT NULL, -- Referencia a datanodes.id
    is_primary BOOLEAN DEFAULT FALSE, -- Indica si es la copia primaria para la escritura inicial
    FOREIGN KEY (block_id) REFERENCES blocks(block_id) ON DELETE CASCADE,
    FOREIGN KEY (datanode_id) REFERENCES datanodes(id) ON DELETE CASCADE,
    UNIQUE (block_id, datanode_id)
);

-- Inicializar directorio raíz si no existe
INSERT OR IGNORE INTO fs_objects (id, parent_id, name, is_directory) VALUES (1, NULL, '/', TRUE);