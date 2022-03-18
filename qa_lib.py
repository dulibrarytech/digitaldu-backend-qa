import os
import shutil
import threading
from os.path import join, dirname

import pysftp
from PIL import Image
from dotenv import load_dotenv

dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)

ready_path = os.getenv('READY_PATH')
ingest_path = os.getenv('INGEST_PATH')
ingested_path = os.getenv('INGESTED_PATH')
sftp_host = os.getenv('SFTP_HOST')
sftp_username = os.getenv('SFTP_ID')
sftp_password = os.getenv('SFTP_PWD')
sftp_path = os.getenv('SFTP_REMOTE_PATH')
wasabi_endpoint = os.getenv('WASABI_ENDPOINT')
wasabi_bucket = os.getenv('WASABI_BUCKET')
wasabi_profile = os.getenv('WASABI_PROFILE')
errors_file = os.getenv('ERRORS_FILE')


def get_ready_folders():
    """
    Gets ready folders
    @returns Dictionary
    """

    ready_list = {}
    folders = [f for f in os.listdir(ready_path) if not f.startswith('.')]

    for folder in folders:

        package_count = len([name for name in os.listdir(ready_path + folder) if
                             os.path.isdir(os.path.join(ready_path + folder, name))])

        if package_count > 0:
            ready_list[folder] = package_count

    return dict(result=ready_list, errors=[])


def set_collection_folder_name(folder):
    """
    Creates collection folder file
    @param: folder
    @returns: void
    """

    try:
        file = open('collection', 'w+')
        file.write(folder)
    except:
        print('ERROR: Unable to create collection folder file - ' + folder)


def get_collection_folder_name():
    """
    Gets collection folder file
    @param: folder
    @returns: string
    """

    try:
        with open('collection') as collection_file:
            folder = collection_file.read()
    except:
        print('ERROR: Unable to open error file - ' + errors_file)

    return folder


def check_folder_name(folder):
    """
    Checks if folder name conforms to naming standard
    @param: folder
    @returns: Dictionary
    """

    errors = []

    if folder.find('new_') == -1:
        errors.append('Collection folder name is missing "new_" part.')

    if folder.find('-resources') == -1:
        errors.append('Collection folder name is missing "-resources" part.')

    if folder.find('resources_') == -1:
        errors.append('Collection folder name is missing "resources_" part')

    tmp = folder.split('_')
    is_id = tmp[-1].isdigit()

    if is_id == False:
        errors.append('Collection folder is missing "URI" part')

    return dict(result='collection_folder_name_checked', errors=errors)


def check_package_names(folder):
    """
    Checks package names and fixes case issues and removes spaces
    @param: folder
    @returns: Dictionary
    """

    threads = []
    packages = [f for f in os.listdir(ready_path + folder) if not f.startswith('.')]
    [os.remove(ready_path + folder + '/' + f) for f in os.listdir(ready_path + folder) if f.startswith('.')]
    errors = []

    if len(packages) == 0:
        errors.append(['No packages found'])

    for i in packages:

        thread = threading.Thread(target=check_package_names_threads, args=(folder, i))
        threads.append(thread)
        thread.start()

        for thread in threads:
            thread.join()

    return dict(result='package_names_checked.', errors=errors)


def check_package_names_threads(folder, i):
    """
    Processes packages (thread function for check_package_names)
    @param: folder
    @param: i
    @returns: Dictionary
    """

    package = ready_path + folder + '/'

    if i.upper():
        call_number = i.find('.')

        if call_number == -1:
            os.rename(package + i, package + i.lower().replace(' ', ''))


def check_file_names(folder):
    """
    Checks file names and fixes case issues and removes spaces
    @param: folder
    @returns: Dictionary
    """

    packages = [f for f in os.listdir(ready_path + folder) if not f.startswith('.')]
    threads = []
    files_arr = []
    errors = []

    if os.path.exists(errors_file):
        os.remove(errors_file)

    for i in packages:

        thread = threading.Thread(target=check_file_names_threads, args=(folder, i))
        threads.append(thread)
        thread.start()

        # Get total file count from packages
        package = ready_path + folder + '/' + i + '/'
        files = [f for f in os.listdir(package) if not f.startswith('.')]
        [os.remove(package + f) for f in os.listdir(package) if f.startswith('.')]

        if len(files) < 2:
            errors.append(i)

        for j in files:
            files_arr.append(j)

        local_file_count = len(files_arr)

    for thread in threads:
        thread.join()

    try:
        with open(errors_file) as file_errors:
            errors = file_errors.readlines()
    except:
        print('ERROR: Unable to open error file - ' + errors_file)

    return dict(result=local_file_count, errors=errors)


def check_file_names_threads(folder, i):
    """
    Processes packages (thread function for check_file_names)
    @param: folder
    @param: i
    @returns: void
    """

    package = ready_path + folder + '/' + i + '/'
    files = [f for f in os.listdir(package) if not f.startswith('.')]
    [os.remove(package + f) for f in os.listdir(package) if f.startswith('.')]

    for j in files:

        if j.upper():

            call_number = j.find('.')

            if call_number == -1:
                os.rename(package + j, package + j.lower().replace(' ', ''))
            elif call_number != -1:
                os.rename(package + j, package + j.replace(' ', ''))

            # check images here
            file = package + j
            if file.endswith('.tiff') or file.endswith('.tif') or file.endswith('.jpg') or file.endswith('.png'):
                # validates image
                check_image_file(file, j)

            # check pdfs here
            if file.endswith('.pdf'):
                check_pdf_file(file, j)


def check_image_file(full_path, file_name):
    """
    Checks image files to determine if they are broken/corrupt
    @param: full_path
    @param: file_name
    @returns: Dictionary
    """

    try:
        img = Image.open(full_path)
        img.verify()  # confirm that file is an image
        img.close()
        img = Image.open(full_path)
        img.transpose(Image.FLIP_LEFT_RIGHT)  # attempt to manipulate file to determine if it's broken
        img.close()
    except OSError as error:
        try:
            errors = open(errors_file, 'a+')
            errors.write(file_name + ' - ' + str(error) + '\n')
        except:
            print('ERROR: Unable to create error file - ' + errors_file)


def check_pdf_file(full_path, file_name):
    """
    Checks pdf files to determine file size. Rejects files larger than 900mb
    @param: full_path
    @param: file_name
    @returns: Dictionary
    """
    # TODO:...
    # try:
    # print(full_path)
    # print(file_name)
    # TODO: reject anything over 900mb
    # https://stackoverflow.com/questions/2104080/how-can-i-check-file-size-in-python
    # i.e. Path('somefile.txt').stat().st_size

    # return dict(error=False, file='')

    # except OSError as error:
    # return dict(error=str(error), file=file_name)


def check_uri_txt(folder):
    """
    Checks for missing uri.txt files
    @param: ready_path
    @param: folder
    @returns: Dictionary
    """

    errors = []
    packages = [f for f in os.listdir(ready_path + folder) if not f.startswith('.')]

    if len(packages) == 0:
        return errors.append(-1)

    for i in packages:

        package = ready_path + folder + '/' + i + '/'
        files = [f for f in os.listdir(package) if not f.startswith('.')]

        if 'uri.txt' not in files:
            errors.append(i)

    return dict(result='URI txt files checked', errors=errors)


def get_uri_txt(folder):
    """
    Gets ArchivesSpace URIs
    @param: ready_path
    @param: folder
    @returns: Dictionary
    """

    uris = []
    errors = []
    packages = [f for f in os.listdir(ready_path + folder) if not f.startswith('.')]

    if len(packages) == 0:
        return errors.append(-1)

    for i in packages:

        package = ready_path + folder + '/' + i + '/'
        files = [f for f in os.listdir(package) if not f.startswith('.')]

        if 'uri.txt' in files:
            # TODO: apply threads?
            uri_txt = ready_path + folder + '/' + i + '/uri.txt'
            with open(f'{uri_txt}', 'r') as uri:
                uri_text = uri.read()
                uris.append(uri_text)

    return dict(result=uris, errors=errors)


def get_total_batch_size(folder):
    """
    Checks package file size (bytes)
    @param: folder
    @returns: Dictionary
    https://stackoverflow.com/questions/1392413/calculating-a-directorys-size-using-python
    """

    package = ready_path + folder
    total_size = 0
    errors = []

    try:
        for dirpath, dirnames, filenames in os.walk(package):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                # skip if it is symbolic link
                if not os.path.islink(fp):
                    total_size += os.path.getsize(fp)
    except:
        errors.append('Unable to get total batch size')

    return dict(result=total_size, errors=errors)


def move_to_ingest(pid, folder):
    '''
    Moves folder from ready to ingest folder and renames it using pid
    @param: pid
    @param: folder
    @returns: Dictionary
    '''

    errors = []
    mode = 0o777

    try:
        shutil.move(ready_path + folder, ingest_path + folder)
    except:
        errors.append('ERROR: Unable to move folder (move_to_ingest)')

    try:
        os.rename(ingest_path + folder, ingest_path + pid)
    except:
        errors.append('ERROR: Unable to rename folder (move_to_ingest)')

    try:
        os.mkdir(ready_path + folder, mode)
    except:
        errors.append('ERROR: Unable to create folder (move_to_ingest)')

    if len(errors) == 0:
        result = 'packages_moved_to_ingested_folder.'
    else:
        result = 'packages_not_moved_to_ingested_folder.'

    return dict(result=result, errors=errors)


def move_to_sftp(pid):
    """"
    Moves folder to Archivematica sftp via ssh
    @param: pid
    @returns: void
    """

    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
    errors = []

    with pysftp.Connection(host=sftp_host, username=sftp_username, password=sftp_password, cnopts=cnopts) as sftp:
        sftp.put_r(ingest_path, sftp_path, preserve_mtime=True)
        packages = sftp.listdir()

        if pid not in packages:
            errors.append(-1)


def check_sftp(pid, local_file_count):
    """
    checks upload status on archivematica sftp
    @param: pid
    @param: local_file_count
    @returns: Dictionary
    """

    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
    file_names = []
    dir_names = []
    un_name = []

    def store_files_name(fname):
        file_names.append(fname)

    def store_dir_name(dirname):
        dir_names.append(dirname)

    def store_other_file_types(name):
        un_name.append(name)

    with pysftp.Connection(host=sftp_host, username=sftp_username, password=sftp_password, cnopts=cnopts) as sftp:
        remote_package = sftp_path + '/' + pid + '/'
        sftp.cwd(remote_package)
        sftp.walktree(remote_package, store_files_name, store_dir_name, store_other_file_types, recurse=True)
        remote_file_count = len(file_names)

        with sftp.cd(remote_package):
            remote_package_size = sftp.execute('du -h -s')

        if int(local_file_count) == remote_file_count:
            return dict(message='upload_complete', data=[file_names, remote_file_count])

        return dict(message='in_progress', file_names=file_names, remote_file_count=remote_file_count,
                    local_file_count=local_file_count,
                    remote_package_size=remote_package_size[0].decode().strip().replace('\t', ''))


def move_to_ingested(pid, folder):
    """
    Moves packages to ingested folder and Wasabi S3 bucket
    @param: pid
    @param: folder
    @returns: Dictionary
    """

    errors = []
    ingested = ingested_path + folder.replace('new_', '')
    exists = os.path.isdir(ingested)
    result = 'packages_not_moved_to_ingested_folder'

    if exists:

        try:  # move only files because collection folder already exists
            file_names = [f for f in os.listdir(ingest_path + pid) if not f.startswith('.')]

            for file_name in file_names:
                os.system('cp -R ' + os.path.join(ingest_path + pid, file_name) + ' ' + ingested)

            source = ingest_path + pid + '/'
            move_result = move_to_s3(source, folder.replace('new_', ''))
            if move_result == 1:
                errors.append('ERROR: Unable to move packages to wasabi s3')
            else:
                shutil.rmtree(source)
        except:
            return errors.append('ERROR: Unable to move files to ingested folder (move_to_ingested)')

    else:  # move entire folder

        try:
            shutil.move(ingest_path + pid, ingest_path + folder.replace('new_', ''))
            os.system('cp -R ' + ingest_path + folder.replace('new_', '') + ' ' + ingested)
            source = ingest_path
            move_result = move_to_s3(source, '')
            if move_result == 1:
                errors.append('ERROR: Unable to move packages to wasabi s3')
            else:
                shutil.rmtree(ingest_path + folder.replace('new_', ''))

        except:
            return errors.append('ERROR: Unable to move folder (move_to_ingested)')

    if len(errors) == 0:
        try:
            os.remove('collection')
        except:
            print('collection file not found')

        try:
            clean_up_sftp(pid)
            result = 'packages_moved_to_ingested_folder'
        except:
            print('unable to run clean up sftp funtion')

    return dict(result=result, errors=errors)


def move_to_s3(source, folder):
    """
    Moves packages to Wasabi S3 bucket
    @param: source
    @param: folder
    @returns: void
    """

    errors = []
    aws_exec = '/usr/local/bin/aws s3 cp'
    aws_endpoint = '--endpoint-url=' + wasabi_endpoint
    aws_bucket = wasabi_bucket
    aws_args = '--recursive --profile ' + wasabi_profile
    result = 1

    if folder != '':
        try:
            aws_cmd = aws_exec + ' ' + source + ' ' + aws_endpoint + ' ' + aws_bucket + folder + ' ' + aws_args
            result = os.system(aws_cmd)
            # if result == 0:
                # shutil.rmtree(source)
        except:
            errors.append('error')
    else:
        try:
            aws_cmd = aws_exec + ' ' + source + ' ' + aws_endpoint + ' ' + aws_bucket + ' ' + aws_args
            result = os.system(aws_cmd)
            #if result == 0:
                # shutil.rmtree(source)
        except:
            errors.append('error')

    return result


def clean_up_sftp(pid):
    """
    Deletes collection folder from ingest folder and sftp server
    :param pid
    :param folder
    :return void
    """

    host = os.getenv('SFTP_HOST')
    username = os.getenv('SFTP_ID')
    password = os.getenv('SFTP_PWD')
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
    sftp_path = os.getenv('SFTP_REMOTE_PATH')

    with pysftp.Connection(host=host, username=username, password=password, cnopts=cnopts) as sftp:
        sftp.cwd(sftp_path)
        sftp.execute('rm -R ' + pid)
