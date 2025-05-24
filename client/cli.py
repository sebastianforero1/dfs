import requests, grpc, file_pb2_grpc, file_pb2

NAMENODE = "http://namenode-ip:5000"

def put(filename):
    with open(filename, 'rb') as f:
        content = f.read()
    blocks = [content[i:i+1024] for i in range(0, len(content), 1024)]
    block_data = [{'id': i} for i in range(len(blocks))]
    r = requests.post(f"{NAMENODE}/upload", json={'filename': filename, 'blocks': block_data})
    metadata = r.json()['locations']
    for i, b in enumerate(blocks):
        leader = metadata[i]['leader']
        ip, port = leader.split(':')
        channel = grpc.insecure_channel(f"{ip}:{port}")
        stub = file_pb2_grpc.BlockTransferStub(channel)
        stub.SendBlock(file_pb2.BlockData(filename=filename, block_id=i, content=b))

def get(filename):
    r = requests.get(f"{NAMENODE}/metadata/{filename}")
    metadata = r.json()
    content = b''
    for block in metadata:
        ip, port = block['leader'].split(':')
        channel = grpc.insecure_channel(f"{ip}:{port}")
        stub = file_pb2_grpc.BlockTransferStub(channel)
        res = stub.GetBlock(file_pb2.BlockRequest(filename=filename, block_id=block['id']))
        content += res.content
    with open("downloaded_" + filename, 'wb') as f:
        f.write(content)

# Puedes añadir comandos ls, cd, etc. de forma similar
