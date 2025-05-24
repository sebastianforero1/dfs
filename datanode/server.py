from concurrent import futures
import grpc
import os
from datanode_pb2 import BlockResponse, StatusResponse
from datanode_pb2_grpc import DataNodeServicer, add_DataNodeServicer_to_server

class DataNodeService(DataNodeServicer):
    def __init__(self):
        self.blocks_dir = "blocks"
        os.makedirs(self.blocks_dir, exist_ok=True)

    def WriteBlock(self, request, context):
        block_path = os.path.join(self.blocks_dir, request.block_id)
        with open(block_path, 'wb') as f:
            f.write(request.data)
        return StatusResponse(success=True)

    def ReadBlock(self, request, context):
        block_path = os.path.join(self.blocks_dir, request.block_id)
        if not os.path.exists(block_path):
            return BlockResponse(data=b'')
        with open(block_path, 'rb') as f:
            return BlockResponse(data=f.read())

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    add_DataNodeServicer_to_server(DataNodeService(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()