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
export_aws_default_profile = 'export AWS_DEFAULT_PROFILE=' + os.getenv('AWS_DEFAULT_PROFILE')
export_aws_access_key_id = 'export AWS_ACCESS_KEY_ID=' + os.getenv('AWS_ACCESS_KEY_ID')
export_aws_secret_access_key = 'export AWS_SECRET_ACCESS_KEY=' + os.getenv('AWS_SECRET_ACCESS_KEY')
export_aws_default_region = 'export AWS_DEFAULT_REGION=' + os.getenv('AWS_DEFAULT_REGION')
os.system(export_aws_default_profile)
os.system(export_aws_access_key_id)
os.system(export_aws_secret_access_key)
os.system(export_aws_default_region)
os.system('printenv')

app = Flask(__name__)
cors = CORS(app, resources={r"/api/*": {"origins": "*"}})
prefix = '/api/'
version = 'v2'
endpoint = '/qa/'


@app.route('/', methods=['GET'])
def index():
    """
    Renders QA API Information
    @returns: String
    """

    return 'DigitalDU-QA ' + app_version


@app.route(prefix + version + endpoint + 'list-ready-folders', methods=['GET'])
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


@app.route(prefix + version + endpoint + 'set-collection-folder', methods=['GET'])
def set_collection_folder_name():
    """
    Runs QA process to set collection folder name (saves name to txt file)
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

    is_set = qa_lib.set_collection_folder_name(folder)

    return json.dumps(dict(is_set=is_set)), 200


@app.route(prefix + version + endpoint + 'check-collection-folder', methods=['GET'])
def check_collection_folder_name():
    """
    Runs QA process to check collection folder name (checks folder nomenclature)
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

    folder_name_results = qa_lib.check_folder_name(folder)

    results = dict(folder_name_results=folder_name_results)

    return json.dumps(results), 200


@app.route(prefix + version + endpoint + 'check-package-names', methods=['GET'])
def check_package_names():
    """
    Runs QA process to checks package names (checks package name nomenclature)
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

    package_name_results = qa_lib.check_package_names(folder)

    results = dict(package_name_results=package_name_results)

    return json.dumps(results), 200


@app.route(prefix + version + endpoint + 'check-file-names', methods=['GET'])
def check_file_names():
    """
    Runs QA process to check file names
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

    file_count_results = qa_lib.check_file_names(folder)

    results = dict(file_count_results=file_count_results)

    return json.dumps(results), 200


@app.route(prefix + version + endpoint + 'check-uri-txt', methods=['GET'])
def check_uri_txt():
    """
    Runs QA process to check uri.txt
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

    uri_results = qa_lib.check_uri_txt(folder)

    results = dict(uri_results=uri_results)

    return json.dumps(results), 200


@app.route(prefix + version + endpoint + 'get-uri-txt', methods=['GET'])
def get_uri_txt():
    """
    Runs QA process to get uri.txt
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

    get_uri_results = qa_lib.get_uri_txt(folder)

    results = dict(get_uri_results=get_uri_results)

    return json.dumps(results), 200


@app.route(prefix + version + endpoint + 'get-total-batch-size', methods=['GET'])
def get_total_batch_size():
    """
    Runs QA process to get batch size
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

    total_batch_size = qa_lib.get_total_batch_size(folder)

    results = dict(total_batch_size=total_batch_size)

    return json.dumps(results), 200


@app.route(prefix + version + endpoint + 'move-to-ingest', methods=['GET'])
def move_to_ingest():
    """
    Moves packages to ingest folder
    @param: api_key
    @param: folder
    @returns: Json
    """

    api_key = request.args.get('api_key')
    uuid = request.args.get('uuid')
    folder = request.args.get('folder')
    errors = []

    if api_key is None:
        errors.append('Access denied.')
    elif api_key != os.getenv('API_KEY'):
        errors.append('Access denied.')

    if len(errors) > 0:
        return json.dumps(errors), 403

    if uuid is None:
        return json.dumps(['Bad Request: Missing pid param.']), 400

    if folder is None:
        return json.dumps(['Bad Request: Missing folder param.']), 400

    results = qa_lib.move_to_ingest(uuid, folder)

    return json.dumps(results), 200


@app.route(prefix + version + endpoint + 'move-to-sftp', methods=['GET'])
def move_to_sftp():
    """
    Uploads packages to Archivematica server
    @param: api_key
    @param: pid
    @param: folder
    @returns: Json
    """

    api_key = request.args.get('api_key')
    uuid = request.args.get('uuid')
    errors = []

    if api_key is None:
        errors.append('Access denied.')
    elif api_key != os.getenv('API_KEY'):
        errors.append('Access denied.')

    if len(errors) > 0:
        return json.dumps(errors), 403

    if uuid is None:
        return json.dumps(['Bad Request: Missing pid param.']), 400

    qa_lib.move_to_sftp(uuid)

    return json.dumps(dict(message='Uploading packages to Archivematica sftp')), 200


@app.route(prefix + version + endpoint + 'upload-status', methods=['GET'])
def check_sftp():
    """
    Checks upload status of packages on Archivematica sftp
    @param: api_key
    @param: pid
    @returns: Json
    """

    api_key = request.args.get('api_key')
    uuid = request.args.get('uuid')
    total_batch_file_count = request.args.get('total_batch_file_count')
    errors = []

    if api_key is None:
        errors.append('Access denied.')
    elif api_key != os.getenv('API_KEY'):
        errors.append('Access denied.')

    if len(errors) > 0:
        return json.dumps(errors), 403

    if uuid is None:
        return json.dumps(['Bad Request: Missing pid param.']), 400

    if total_batch_file_count == None:
        return json.dumps(dict(message='File count not found.', data=[])), 200

    results = qa_lib.check_sftp(uuid, total_batch_file_count)
    return json.dumps(results), 200


@app.route(prefix + version + endpoint + 'move-to-ingested', methods=['GET'])
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
    uuid = request.args.get('uuid')

    if api_key is None:
        return json.dumps(['Access denied.']), 403
    elif api_key != os.getenv('API_KEY'):
        return json.dumps(['Access denied.']), 403

    if folder == 'collection':
        folder = qa_lib.get_collection_folder_name()

    results = qa_lib.move_to_ingested(uuid, folder)

    return json.dumps(results), 200


@app.route(prefix + version + endpoint + 'reset_permissions', methods=['GET'])
def reset_permissions():
    """
    Resets collection folder permissions
    :return: boolean
    """

    api_key = request.args.get('api_key')
    folder = request.args.get('folder')

    if api_key is None:
        return json.dumps(['Access denied.']), 403
    elif api_key != os.getenv('API_KEY'):
        return json.dumps(['Access denied.']), 403

    if folder is None:
        return json.dumps(['Bad Request.']), 400
    elif api_key != os.getenv('API_KEY'):
        return json.dumps(['Bad Request.']), 400

    is_reset = qa_lib.reset_permissions(folder)

    return json.dumps(is_reset), 200


@app.route(prefix + version + endpoint + 'cleanup_sftp', methods=['GET'])
def clean_up_sftp():
    """
    Removes collection folder from Archivematica SFTP server
    :return: string
    """

    api_key = request.args.get('api_key')
    uuid = request.args.get('uuid')

    if api_key is None:
        return json.dumps(['Access denied.']), 403
    elif api_key != os.getenv('API_KEY'):
        return json.dumps(['Access denied.']), 403

    qa_lib.clean_up_sftp(uuid)
    return json.dumps('collection folder removed'), 200


app.debug = True
serve(app, host='0.0.0.0', port=os.getenv('APP_PORT'))
