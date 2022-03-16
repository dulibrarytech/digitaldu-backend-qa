import json
import os
from os.path import join, dirname
from dotenv import load_dotenv
from flask import Flask, request
from flask_cors import CORS
from waitress import serve

import qa_lib

dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)

ready_path = os.getenv('READY_PATH')
batch_size_limit = os.getenv('BATCH_SIZE_LIMIT')
app_version = os.getenv('APP_VERSION')

app = Flask(__name__)
cors = CORS(app, resources={r"/api/*": {"origins": "*"}})


@app.route('/', methods=['GET'])
def index():
    """
    Renders QA API Information
    @returns: String
    """

    return 'DigitalDU-QA ' + app_version


@app.route('/api/v1/qa/list-ready', methods=['GET'])
def list_ready_folders():
    """"
    Gets a list of ready folders
    @param: api_key
    @returns: Json
    """

    api_key = request.args.get('api_key')
    errors = []

    if api_key is None:
        errors.append('Access denied.')
    elif api_key != os.getenv('API_KEY'):
        errors.append('Access denied.')

    if len(errors) > 0:
        return json.dumps(errors), 403

    ready_list = qa_lib.get_ready_folders()

    return json.dumps(ready_list), 200


@app.route('/api/v1/qa/run-qa', methods=['GET'])
def run_qa_on_ready():
    """
    Runs QA on ready folder
    @param: api_key
    @param: folder
    @returns: Json
    """

    api_key = request.args.get('api_key')
    folder = request.args.get('folder')
    errors = []

    if api_key is None:
        errors.append('Access denied.')
    elif api_key != os.getenv('API_KEY'):
        errors.append('Access denied.')

    if len(errors) > 0:
        return json.dumps(errors), 403

    if folder is None:
        return json.dumps(['Bad Request: Missing folder param']), 400

    qa_lib.set_collection_folder_name(folder)
    folder_name_results = qa_lib.check_folder_name(folder)
    package_name_results = qa_lib.check_package_names(folder)
    file_count_results = qa_lib.check_file_names(folder)
    uri_results = qa_lib.check_uri_txt(folder)
    get_uri_results = qa_lib.get_uri_txt(folder)
    total_batch_size = qa_lib.get_total_batch_size(folder)

    results = dict(
        folder_name_results=folder_name_results,
        package_name_results=package_name_results,
        file_count_results=file_count_results,
        uri_results=uri_results,
        get_uri_results=get_uri_results,
        total_batch_size=total_batch_size
    )

    return json.dumps(results), 200


@app.route('/api/v1/qa/move-to-ingest', methods=['GET'])
def move_to_ingest():
    """
    Moves packages to ingest folder
    @param: api_key
    @param: folder
    @returns: Json
    """

    api_key = request.args.get('api_key')
    pid = request.args.get('pid')
    folder = request.args.get('folder')
    errors = []

    if api_key is None:
        errors.append('Access denied.')
    elif api_key != os.getenv('API_KEY'):
        errors.append('Access denied.')

    if len(errors) > 0:
        return json.dumps(errors), 403

    if pid is None:
        return json.dumps(['Bad Request: Missing pid param.']), 400

    if folder is None:
        return json.dumps(['Bad Request: Missing folder param.']), 400

    results = qa_lib.move_to_ingest(pid, folder)

    return json.dumps(results), 200


@app.route('/api/v1/qa/move-to-sftp', methods=['GET'])
def move_to_sftp():
    """
    Uploads packges to Archivematica server
    @param: api_key
    @param: pid
    @param: folder
    @returns: Json
    """

    api_key = request.args.get('api_key')
    pid = request.args.get('pid')
    errors = []

    if api_key is None:
        errors.append('Access denied.')
    elif api_key != os.getenv('API_KEY'):
        errors.append('Access denied.')

    if len(errors) > 0:
        return json.dumps(errors), 403

    if pid is None:
        return json.dumps(['Bad Request: Missing pid param.']), 400

    qa_lib.move_to_sftp(pid)

    return json.dumps(dict(message='Uploading packages to Archivematica sftp')), 200


@app.route('/api/v1/qa/upload-status', methods=['GET'])
def check_sftp():
    """
    Checks upload status of packages on Archivematica sftp
    @param: api_key
    @param: pid
    @returns: Json
    """

    api_key = request.args.get('api_key')
    pid = request.args.get('pid')
    total_batch_file_count = request.args.get('total_batch_file_count')
    errors = []

    if api_key is None:
        errors.append('Access denied.')
    elif api_key != os.getenv('API_KEY'):
        errors.append('Access denied.')

    if len(errors) > 0:
        return json.dumps(errors), 403

    if pid is None:
        return json.dumps(['Bad Request: Missing pid param.']), 400

    if total_batch_file_count == None:
        return json.dumps(dict(message='File count not found.', data=[])), 200

    results = qa_lib.check_sftp(pid, total_batch_file_count)
    return json.dumps(results), 200


@app.route('/api/v1/qa/move-to-ingested', methods=['GET'])
def move_to_ingested():
    """
    Move packages to ingested folder
    @param: api_key
    @param: folder
    @param: pid
    @returns: Json
    """

    api_key = request.args.get('api_key')
    folder = request.args.get('folder')
    pid = request.args.get('pid')

    if api_key is None:
        return json.dumps(['Access denied.']), 403
    elif api_key != os.getenv('API_KEY'):
        return json.dumps(['Access denied.']), 403

    if folder == 'collection':
        folder = qa_lib.get_collection_folder_name()

    results = qa_lib.move_to_ingested(pid, folder)

    return json.dumps(results), 200


@app.route('/api/v1/qa/cleanup', methods=['GET'])
def clean_up():
    """
    Test endpoint
    :return:
    """

    api_key = request.args.get('api_key')
    pid = request.args.get('pid')

    if api_key is None:
        return json.dumps(['Access denied.']), 403
    elif api_key != os.getenv('API_KEY'):
        return json.dumps(['Access denied.']), 403

    qa_lib.clean_up(pid)
    return json.dumps('collection folder removed'), 200


serve(app, host='0.0.0.0', port=os.getenv('APP_PORT'))
