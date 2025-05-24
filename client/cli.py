# client/cli.py
import click
import os
import json
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from client_sdk import DFSClient
from common import config as common_config

@click.group()
@click.option('--namenode_url', default=lambda: os.environ.get('NAMENODE_URL', common_config.NAMENODE_URL), help='URL del NameNode.')
@click.pass_context
def cli(ctx, namenode_url):
    """CLI para DFS Minimalista.""" # [cite: 28]
    ctx.ensure_object(dict)
    client = DFSClient(namenode_url)
    ctx.obj['client'] = client
    # Cargar/guardar `current_path` para persistencia básica de `cd`
    ctx.obj['current_path_file'] = os.path.expanduser("~/.dfs_cli_path.txt")
    try:
        with open(ctx.obj['current_path_file'], 'r') as f: client.current_path = f.read().strip() or "/"
    except FileNotFoundError: client.current_path = "/"

def _save_current_path(ctx):
    with open(ctx.obj['current_path_file'], 'w') as f: f.write(ctx.obj['client'].current_path)
def _format_output(result): return json.dumps(result, indent=2, ensure_ascii=False)

@cli.command() @click.argument('path', default=".") @click.pass_context # ls [cite: 28]
def ls(ctx, path): click.echo(_format_output(ctx.obj['client'].ls(path)))
@cli.command() @click.argument('dir_path') @click.pass_context # mkdir [cite: 28]
def mkdir(ctx, dir_path): click.echo(_format_output(ctx.obj['client'].mkdir(dir_path)))
@cli.command() @click.argument('lfp', type=click.Path(exists=True,dir_okay=False)) @click.argument('dfp') @click.pass_context # put [cite: 28]
def put(ctx, lfp, dfp): click.echo(_format_output(ctx.obj['client'].put(lfp, dfp)))
@cli.command() @click.argument('dfp') @click.argument('ltp', type=click.Path(dir_okay=False)) @click.pass_context # get [cite: 28]
def get(ctx, dfp, ltp): click.echo(_format_output(ctx.obj['client'].get(dfp, ltp)))
@cli.command() @click.argument('path') @click.pass_context # cd [cite: 28]
def cd(ctx, path): 
    result = ctx.obj['client'].cd(path)
    click.echo(_format_output(result))
    if 'error' not in result: _save_current_path(ctx)
@cli.command() @click.pass_context # pwd (no está en la lista pero es útil con cd)
def pwd(ctx): click.echo(ctx.obj['client'].current_path)
@cli.command() @click.argument('dir_path') @click.pass_context # rmdir [cite: 28]
def rmdir(ctx, dir_path): click.echo(_format_output(ctx.obj['client'].rmdir(dir_path)))
@cli.command() @click.argument('item_path') @click.pass_context # rm [cite: 28]
def rm(ctx, item_path): click.echo(_format_output(ctx.obj['client'].rm(item_path)))

if __name__ == '__main__': cli(obj={})