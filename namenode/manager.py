import random

class NameNodeManager:
    def __init__(self):
        self.metadata = {}
        self.datanodes = ['ip1:8000', 'ip2:8000']

    def register_file(self, filename, blocks):
        locations = []
        for i, block in enumerate(blocks):
            leader = random.choice(self.datanodes)
            follower = random.choice([d for d in self.datanodes if d != leader])
            locations.append({'id': i, 'leader': leader, 'follower': follower})
        self.metadata[filename] = locations
        return {'status': 'ok', 'locations': locations}

    def get_file_blocks(self, filename):
        return self.metadata.get(filename, {})
