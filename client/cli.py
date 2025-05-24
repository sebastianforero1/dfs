# client/cli.py
import click
import os
import json
import sys

# Asegúrate de que estas importaciones también estén presentes y correctas
# Ajusta la ruta si es necesario para encontrar 'dfs_project'
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from client_sdk import DFSClient
from common import config as common_config

# Configuración de logging (opcional, pero útil para depurar el SDK)
import logging
# Descomenta la siguiente línea para ver más detalles de lo que hace el SDK:
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


# --- PRIMERO: Definición del GRUPO 'cli' ---
@click.group()
@click.option('--namenode_url', default=lambda: os.environ.get('NAMENODE_URL', common_config.NAMENODE_URL), help='URL del NameNode.')
@click.pass_context
def cli(ctx, namenode_url):
    """CLI para DFS Minimalista."""
    ctx.ensure_object(dict)
    client = DFSClient(namenode_url)
    ctx.obj['client'] = client
    # Lógica para cargar/guardar current_path para persistencia básica de `cd`
    ctx.obj['current_path_file'] = os.path.expanduser("~/.dfs_cli_path.txt")
    try:
        with open(ctx.obj['current_path_file'], 'r') as f:
            current_path = f.read().strip()
            # Asegurarse de que current_path siempre sea una ruta absoluta válida
            if not current_path or not current_path.startswith("/"):
                client.current_path = "/"
            else:
                client.current_path = current_path
    except FileNotFoundError:
        client.current_path = "/"

# --- LUEGO: Funciones de ayuda ---
def _save_current_path(ctx):
    with open(ctx.obj['current_path_file'], 'w') as f:
        f.write(ctx.obj['client'].current_path)

def _format_output(result):
    if isinstance(result, dict) or isinstance(result, list):
        return json.dumps(result, indent=2, ensure_ascii=False)
    return str(result)

# --- DESPUÉS: Definición de los COMANDOS que pertenecen al grupo 'cli' ---

@cli.command("ls")
@click.argument('path', default=".", required=False)
@click.pass_context
def ls(ctx, path):
    """List directory contents."""
    client = ctx.obj['client']
    result = client.ls(path)
    click.echo(_format_output(result))

@cli.command("mkdir")
@click.argument('dir_path')
@click.pass_context
def mkdir(ctx, dir_path):
    """Create a directory."""
    client = ctx.obj['client']
    result = client.mkdir(dir_path)
    click.echo(_format_output(result))

@cli.command("put")
@click.argument('local_file_path', type=click.Path(exists=True, dir_okay=False, resolve_path=True))
@click.argument('dfs_file_path')
@click.pass_context
def put(ctx, local_file_path, dfs_file_path):
    """Upload a file to DFS."""
    client = ctx.obj['client']
    # La función _resolve_path del SDK se encarga de normalizar dfs_file_path
    resolved_dfs_path = client._resolve_path(dfs_file_path)
    click.echo(f"Subiendo '{local_file_path}' a '{resolved_dfs_path}'...")
    result = client.put(local_file_path, dfs_file_path) # El SDK maneja la resolución de dfs_file_path
    click.echo(_format_output(result))

@cli.command("get")
@click.argument('dfs_file_path')
@click.argument('local_target_path', type=click.Path(dir_okay=False, resolve_path=True))
@click.pass_context
def get(ctx, dfs_file_path, local_target_path):
    """Download a file from DFS."""
    client = ctx.obj['client']
    resolved_dfs_path = client._resolve_path(dfs_file_path)
    click.echo(f"Descargando '{resolved_dfs_path}' a '{local_target_path}'...")
    result = client.get(dfs_file_path, local_target_path) # El SDK maneja la resolución de dfs_file_path
    click.echo(_format_output(result))

@cli.command("cd")
@click.argument('path')
@click.pass_context
def cd(ctx, path):
    """Change current directory (client-side state)."""
    client = ctx.obj['client']
    result = client.cd(path) # cd en el SDK actualiza client.current_path
    click.echo(_format_output(result))
    # Guardar el current_path solo si el cd fue exitoso
    if 'error' not in result and (isinstance(result, dict) and 'error' not in result.get('message', {})): # Ajustar según la respuesta de cd
        _save_current_path(ctx)


@cli.command("pwd")
@click.pass_context
def pwd(ctx):
    """Print working directory (client-side state)."""
    client = ctx.obj['client']
    click.echo(client.current_path)


@cli.command("rmdir")
@click.argument('dir_path')
@click.pass_context
def rmdir(ctx, dir_path):
    """Remove an empty directory."""
    client = ctx.obj['client']
    result = client.rmdir(dir_path)
    click.echo(_format_output(result))

@cli.command("rm")
@click.argument('item_path')
@click.pass_context
def rm(ctx, item_path):
    """Remove a file or an empty directory.""" # El NameNode decidirá si es archivo o dir
    client = ctx.obj['client']
    result = client.rm(item_path)
    click.echo(_format_output(result))


# --- AL FINAL: El bloque if __name__ == '__main__': ---
if __name__ == '__main__':
    cli(obj={}) # Esto ejecuta el grupo 'cli'