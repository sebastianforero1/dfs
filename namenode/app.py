from flask import Flask, jsonify
from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///metadata.db'
db = SQLAlchemy(app)

class Block(db.Model):
    id = db.Column(db.String, primary_key=True)
    file_path = db.Column(db.String)
    leader_node = db.Column(db.String)
    follower_nodes = db.Column(db.String)

@app.route('/blocks/<path:file_path>', methods=['GET'])
def get_blocks(file_path):
    blocks = Block.query.filter_by(file_path=file_path).all()
    return jsonify([{"id": b.id, "leader": b.leader_node} for b in blocks])

# Otros endpoints: /mkdir, /rm, /ls, etc.