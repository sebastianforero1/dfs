#!/bin/bash



echo "-----------------------------------------------------"
echo "INICIO DE PRUEBAS DEL SISTEMA DE ARCHIVOS DISTRIBUIDO"
echo "-----------------------------------------------------"
echo "Fecha: $(date)"
echo "NameNode URL: ${NAMENODE_URL:-(No configurado, se usará el default de config.py)}"
echo ""

# Variables
PYTHON_EXEC="python3"
CLI_SCRIPT="cli.py"
TEST_DIR_DFS="/autotest_dir"
TEST_FILE_LOCAL1="local_testfile1.txt"
TEST_FILE_LOCAL2="local_testfile2.txt"
TEST_FILE_DFS1="${TEST_DIR_DFS}/dfs_testfile1.txt"
TEST_FILE_DFS2="${TEST_DIR_DFS}/dfs_anotherfile.txt"
DOWNLOADED_FILE1="downloaded_file1.txt"

# Función para ejecutar y mostrar comandos
run_command() {
    echo ""
    echo "EJECUTANDO: $PYTHON_EXEC $CLI_SCRIPT $@"
    echo "-----------------------------------------------------"
    $PYTHON_EXEC $CLI_SCRIPT "$@"
    echo "-----------------------------------------------------"
    # Pausa breve para poder leer la salida si se ejecuta interactivamente
    # read -p "Presiona Enter para continuar..." -t 5
    sleep 1 # Pausa no interactiva
}

# Limpieza inicial (opcional, para empezar desde un estado conocido)
echo "Limpiando entorno de prueba previo (si existe)..."
$PYTHON_EXEC $CLI_SCRIPT rm ${TEST_FILE_DFS1} > /dev/null 2>&1
$PYTHON_EXEC $CLI_SCRIPT rm ${TEST_FILE_DFS2} > /dev/null 2>&1
$PYTHON_EXEC $CLI_SCRIPT rmdir ${TEST_DIR_DFS} > /dev/null 2>&1
rm -f $TEST_FILE_LOCAL1 $TEST_FILE_LOCAL2 $DOWNLOADED_FILE1 > /dev/null 2>&1
echo "Limpieza completada."

# 0. Crear archivos locales de prueba
echo ""
echo "Creando archivos locales para pruebas..."
echo "Este es el contenido del primer archivo de prueba para el DFS." > $TEST_FILE_LOCAL1
head -c 2M /dev/urandom > $TEST_FILE_LOCAL2 # Crear un archivo de 2MB para probar bloques múltiples
echo "Archivos locales creados:"
ls -lh $TEST_FILE_LOCAL1 $TEST_FILE_LOCAL2
echo ""

# 1. Probar 'mkdir': Crear un directorio en el DFS [cite: 60]
run_command mkdir $TEST_DIR_DFS

# 2. Probar 'ls': Listar el directorio raíz y el nuevo directorio [cite: 60]
run_command ls /
run_command ls $TEST_DIR_DFS

# 3. Probar 'put': Subir el primer archivo local al nuevo directorio en DFS [cite: 60]
run_command put $TEST_FILE_LOCAL1 $TEST_FILE_DFS1

# 4. Probar 'ls' de nuevo para ver el archivo subido
run_command ls $TEST_DIR_DFS

# 5. Probar 'put' con un archivo más grande (para asegurar múltiples bloques) [cite: 20, 52]
run_command put $TEST_FILE_LOCAL2 $TEST_FILE_DFS2

# 6. Probar 'ls' de nuevo para ver el segundo archivo
run_command ls $TEST_DIR_DFS

# 7. Probar 'get': Descargar el primer archivo del DFS [cite: 60]
run_command get $TEST_FILE_DFS1 $DOWNLOADED_FILE1

# 8. Verificar contenido del archivo descargado
echo ""
echo "Verificando contenido de $DOWNLOADED_FILE1..."
if cmp -s "$TEST_FILE_LOCAL1" "$DOWNLOADED_FILE1"; then
    echo "VERIFICACIÓN ÉXITOSA: $DOWNLOADED_FILE1 es idéntico a $TEST_FILE_LOCAL1."
else
    echo "ERROR DE VERIFICACIÓN: $DOWNLOADED_FILE1 difiere de $TEST_FILE_LOCAL1."
    echo "Diff:"
    diff "$TEST_FILE_LOCAL1" "$DOWNLOADED_FILE1"
fi
echo "-----------------------------------------------------"

# 9. Probar 'cd': Cambiar de directorio (estado del cliente) [cite: 60]
run_command cd $TEST_DIR_DFS
run_command pwd # Verificar el cambio

# 10. Probar 'ls' con ruta relativa después de 'cd'
run_command ls . # Debería listar el contenido de $TEST_DIR_DFS

# 11. Volver al directorio raíz
run_command cd /
run_command pwd

# 12. Probar 'rm': Eliminar el primer archivo del DFS [cite: 60]
run_command rm $TEST_FILE_DFS1

# 13. Probar 'ls' para verificar la eliminación
run_command ls $TEST_DIR_DFS

# 14. Probar 'rm' con el segundo archivo
run_command rm $TEST_FILE_DFS2

# 15. Probar 'rmdir': Eliminar el directorio (ahora debería estar vacío) [cite: 60]
run_command rmdir $TEST_DIR_DFS

# 16. Probar 'ls' en el directorio raíz para verificar la eliminación del directorio
run_command ls /

# Limpieza final de archivos locales
echo ""
echo "Limpiando archivos locales de prueba..."
rm -f $TEST_FILE_LOCAL1 $TEST_FILE_LOCAL2 $DOWNLOADED_FILE1
echo "Archivos locales eliminados."
echo ""

echo "-----------------------------------------------------"
echo "FIN DE PRUEBAS DEL SISTEMA DE ARCHIVOS DISTRIBUIDO"
echo "-----------------------------------------------------"