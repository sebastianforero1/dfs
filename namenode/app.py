from flask import Flask, request, jsonify
from manager import NameNodeManager

app = Flask(__name__)
nn = NameNodeManager()

@app.route('/upload', methods=['POST'])
def upload_metadata():
    data = request.json
    return jsonify(nn.register_file(data['filename'], data['blocks']))

@app.route('/metadata/<filename>', methods=['GET'])
def get_metadata(filename):
    return jsonify(nn.get_file_blocks(filename))

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
