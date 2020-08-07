import os
import shutil
import pysftp
import time
from PIL import Image

'''
Checks if folder name conforms to naming standard
@param: folder
@returns: Array
'''
def check_folder_name(folder):

    errors = []

    if folder.find('new_') == -1:
        errors.append('Folder name is missing "new_" part.')

    if folder.find('-resources') == -1:
        errors.append('Folder name is missing "-resources" part.')

    if folder.find('resources_') == -1:
        errors.append('Folder name is missing "resources_" part')

    tmp = folder.split('_')
    is_id = tmp[-1].isdigit()

    if is_id == False:
        errors.append('Folder is missing "URI" part')

    if len(errors) > 0:
        return errors
    else:
        return []


'''
Checks package names and fixes case issues and removes spaces
@param: ready_path
@param: folder
@returns: void
'''
def check_package_names(ready_path, folder):

    packages = [f for f in os.listdir(ready_path + folder) if not f.startswith('.')]
    [os.remove(ready_path + folder + '/' + f) for f in os.listdir(ready_path + folder) if f.startswith('.')]

    if len(packages) == 0:
        return -1

    for i in packages:
        package = ready_path + folder + '/'

        if i.upper():
            os.rename(package + i, package + i.lower().replace(' ', ''))


'''
Checks file names and fixes case issues and removes spaces
@param: ready_path
@param: folder
@returns: Array
'''
def check_file_names(ready_path, folder):

    errors = []
    packages = [f for f in os.listdir(ready_path + folder) if not f.startswith('.')]

    for i in packages:
        package = ready_path + folder + '/' + i + '/'
        files = [f for f in os.listdir(package) if not f.startswith('.')]
        [os.remove(package + f) for f in os.listdir(package) if f.startswith('.')]

        if len(files) < 2:
            errors.append(i)

        for j in files:
            if j.upper():
                os.rename(package + j, package + j.lower().replace(' ', ''))
                # check images here
                file = package + j
                if file.endswith('.tiff') or file.endswith('.tif') or file.endswith('.jpg') or file.endswith('.png'):
                    result = check_image_file(file, j)

                    if result.get('error') != False:
                        errors.append(result)

    return errors


'''
Checks image files to determine if they are broken/corrupt
@param: full_path
@param: file_name
@returns: Object
'''
def check_image_file(full_path, file_name):

    try:
        img = Image.open(full_path)
        img.verify() # confirm that file is an image
        img.close()
        img = Image.open(full_path)
        img.transpose(Image.FLIP_LEFT_RIGHT) # attempt to manipulate file to determine if it's broken
        img.close()
        return dict(error=False, file='')
    except OSError as error:
        return dict(error=str(error), file=file_name)


'''
Checks for missing uri.txt files
@param: ready_path
@param: folder
@returns: Array
'''
def check_uri_txt(ready_path, folder):

    errors = []
    packages = [f for f in os.listdir(ready_path + folder) if not f.startswith('.')]

    if len(packages) == 0:
        return errors.append(-1)

    for i in packages:
        package = ready_path + folder + '/' + i + '/'
        files = [f for f in os.listdir(package) if not f.startswith('.')]

        if 'uri.txt' not in files:
            errors.append(i)

    return errors


'''
Checks package file size (bytes)
@param: ready_path
@param: folder
@returns: Integer
https://stackoverflow.com/questions/1392413/calculating-a-directorys-size-using-python
'''
def get_package_size(ready_path, folder):

    package = ready_path + folder
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(package):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            # skip if it is symbolic link
            if not os.path.islink(fp):
                total_size += os.path.getsize(fp)

    return total_size


'''
Moves folder from ready to ingest folder and renames it using pid
@param: ready_path
@param: ingest_path
@param: folder
@returns: String (if there is an error)
'''
def move_to_ingest(ready_path, ingest_path, folder):

    errors = []
    mode = 0o666

    try:
        shutil.move(ready_path + folder, ingest_path + folder)
    except:
        return errors.append('ERROR: Unable to move folder (move_to_ingest)')

    try:
        os.mkdir(ready_path + folder, mode)
    except:
        return errors.append('ERROR: Unable to create folder (move_to_ingest)')


'''
Moves folder to archivematica sftp
@param: host
@param: username
@param: password
@param: sftp_path
@returns: Array
'''
def move_to_sftp(ingest_path, folder, pid):

    host = os.getenv('SFTP_HOST')
    username = os.getenv('SFTP_ID')
    password = os.getenv('SFTP_PWD')
    sftp_path = os.getenv('SFTP_REMOTE_PATH')
    cnopts = pysftp.CnOpts()
    errors = []

    time.sleep(10.0)

    try:
        os.rename(ingest_path + folder, ingest_path + pid)
    except:
        return errors.append('ERROR: Unable to rename folder (move_to_sftp)')

    '''
    with pysftp.Connection(host=host, username=username, password=password, cnopts=cnopts) as sftp:

        # TODO: try catch
        sftp.put_r(ingest_path, sftp_path, preserve_mtime=True)
        packages = sftp.listdir()

        if pid not in packages:
            errors.append(-1)

        sftp.close()
    '''
    return errors