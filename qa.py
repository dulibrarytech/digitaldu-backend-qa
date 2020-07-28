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
        return json.dumps([f'There are no packages in "{folder}".'])

    missing_files = qa_lib.check_file_names(ready_path, folder)
    missing_uris = qa_lib.check_uri_txt(ready_path, folder)

    errors = dict(missing_uris=missing_uris, missing_files=missing_files)

    return json.dumps(errors)

serve(app, host='0.0.0.0', port=8080)