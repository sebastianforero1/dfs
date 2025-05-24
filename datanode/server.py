from concurrent import futures
import grpc, file_pb2_grpc, file_pb2

class DataNode(file_pb2_grpc.BlockTransferServicer):
    def __init__(self):
        self.storage = {}

    def SendBlock(self, request, context):
        key = f"{request.filename}_{request.block_id}"
        self.storage[key] = request.content
        return file_pb2.Ack(success=True)

    def GetBlock(self, request, context):
        key = f"{request.filename}_{request.block_id}"
        return file_pb2.BlockData(filename=request.filename, block_id=request.block_id, content=self.storage[key])

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    file_pb2_grpc.add_BlockTransferServicer_to_server(DataNode(), server)
    server.add_insecure_port('[::]:8000')
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()
