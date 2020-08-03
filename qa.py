import os
import json
import qa_lib
from flask import Flask, request
from flask_cors import CORS
from waitress import serve
from dotenv import load_dotenv
from os.path import join, dirname

dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)

ready_path = os.getenv('READY_PATH')
ingest_path = os.getenv('INGEST_PATH')

app = Flask(__name__)
cors = CORS(app, resources={r"/api/*": {"origins": "*"}})

@app.route('/', methods=['GET'])
def index():
    return f'DigitalDU-QA v0.1.0'

@app.route('/api/v1/qa/list-ready', methods=['GET'])
def list_ready_folders():
    api_key = request.args.get('api_key')

    if api_key is None:
        return json.dumps(['Access denied.'])
    elif api_key != os.getenv('API_KEY'):
        return json.dumps(['Access denied.'])

    folders = [f for f in os.listdir(ready_path) if not f.startswith('.')]
    return json.dumps(folders)

@app.route('/api/v1/qa/ready', methods=['GET'])
def run_qa_on_ready():
    api_key = request.args.get('api_key')

    if api_key is None:
        return json.dumps(['Access denied.'])
    elif api_key != os.getenv('API_KEY'):
        return json.dumps(['Access denied.'])

    folder = request.args.get('folder')
    result = qa_lib.check_package_names(ready_path, folder)

    if result == -1:
        response = dict(missing_uris='empty', missing_files='empty')
        return json.dumps(response)

    missing_files = qa_lib.check_file_names(ready_path, folder)
    missing_uris = qa_lib.check_uri_txt(ready_path, folder)
    total_size = qa_lib.check_file_sizes(ready_path, folder)

    # TODO: figure out how to split up LARGE collections

    results = dict(missing_uris=missing_uris, missing_files=missing_files, total_size=total_size)

    return json.dumps(results)

@app.route('/api/v1/qa/move-to-ingest', methods=['GET'])
def move_to_ingest():
    api_key = request.args.get('api_key')

    if api_key is None:
        return json.dumps(['Access denied.'])
    elif api_key != os.getenv('API_KEY'):
        return json.dumps(['Access denied.'])

    pid = request.args.get('pid')
    folder = request.args.get('folder')
    qa_lib.move_to_ingest(ready_path, ingest_path, folder)
    result = qa_lib.move_to_sftp(ingest_path, folder, pid)

    if len(result) > 0:
        return json.dumps(dict(message='Package not uploaded to Archivematica sftp'))
    else:
        return json.dumps(dict(message='Package uploaded to Archivematica sftp'))

serve(app, host='0.0.0.0', port=8080)