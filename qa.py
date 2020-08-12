import os
import json
import qa_lib
# import asyncio
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

'''
Renders QA API Information
@returns: String
'''
@app.route('/', methods=['GET'])
def index():
    return f'DigitalDU-QA v0.1.0'


'''
Gets a list of ready folders
@param: api_key
@returns: Json
'''
@app.route('/api/v1/qa/list-ready', methods=['GET'])
def list_ready_folders():

    api_key = request.args.get('api_key')

    if api_key is None:
        return json.dumps(['Access denied.'])
    elif api_key != os.getenv('API_KEY'):
        return json.dumps(['Access denied.'])

    folders = [f for f in os.listdir(ready_path) if not f.startswith('.')]
    return json.dumps(folders)


'''
Runs QA on ready folder
@param: api_key
@param: folder
@returns: Json
'''
@app.route('/api/v1/qa/run-qa', methods=['GET'])
def run_qa_on_ready():

    api_key = request.args.get('api_key')

    if api_key is None:
        return json.dumps(['Access denied.'])
    elif api_key != os.getenv('API_KEY'):
        return json.dumps(['Access denied.'])

    folder = request.args.get('folder')
    errors = qa_lib.check_folder_name(folder)

    if len(errors) > 0:
        result = dict(message=f'"{folder}". Please review the ingest documentation for folder naming convention.', errors=errors)
        return json.dumps(result)

    result = qa_lib.check_package_names(ready_path, folder)

    if result == -1:
        response = dict(file_errors='empty', uri_errors='empty', message='Folder is empty')
        return json.dumps(response)

    file_errors = qa_lib.check_file_names(ready_path, folder)
    uri_errors = qa_lib.check_uri_txt(ready_path, folder)
    total_size = qa_lib.get_package_size(ready_path, folder)

    if total_size > 225000000000:
        # TODO: figure out how to split up LARGE (over 225GB) collections
        print('Split up packages')

    results = dict(file_errors=file_errors, uri_errors=uri_errors, total_size=total_size, message='Package results')

    return json.dumps(results)


'''
Moves packages to ingest folder
@param: api_key
@param: folder
@returns: Json
'''
@app.route('/api/v1/qa/move-to-ingest', methods=['GET'])
def move_to_ingest():

    api_key = request.args.get('api_key')

    if api_key is None:
        return json.dumps(['Access denied.'])
    elif api_key != os.getenv('API_KEY'):
        return json.dumps(['Access denied.'])

    pid = request.args.get('pid')
    folder = request.args.get('folder')
    errors = qa_lib.move_to_ingest(ready_path, ingest_path, pid, folder)

    if len(errors) > 0:
        return json.dumps(dict(message='QA process failed.', errors=errors))

    return json.dumps(dict(message='Packages moved to ingest folder', errors=errors))


'''
Uploads packges to Archivematica server
@param: api_key
@param: pid
@param: folder
@returns: Json
'''
@app.route('/api/v1/qa/move-to-sftp', methods=['GET'])
def move_to_sftp():

    api_key = request.args.get('api_key')

    if api_key is None:
        return json.dumps(['Access denied.'])
    elif api_key != os.getenv('API_KEY'):
        return json.dumps(['Access denied.'])

    pid = request.args.get('pid')
    # folder = request.args.get('folder')

    '''
    async def move_to_sftp_async():
        task = asyncio.create_task(qa_lib.move_to_sftp(ingest_path, folder, pid))
        await task

    asyncio.run(move_to_sftp_async())
    '''

    qa_lib.move_to_sftp(ingest_path, pid)

    return json.dumps(dict(message='Uploading packages to Archivematica sftp'))


'''
Checks upload status of packages on Archivematica sftp
@param: api_key
@param: pid
@returns: Json
'''
@app.route('/api/v1/qa/upload-status', methods=['GET'])
def check_sftp():

    api_key = request.args.get('api_key')

    if api_key is None:
        return json.dumps(['Access denied.'])
    elif api_key != os.getenv('API_KEY'):
        return json.dumps(['Access denied.'])

    pid = request.args.get('pid')
    results = qa_lib.check_sftp(ingest_path, pid)
    print(results)
    return json.dumps(dict(message='Checking packages on Archivematica sftp', data=results))

serve(app, host='0.0.0.0', port=8080)