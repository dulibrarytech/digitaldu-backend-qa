import os
import shutil
import pysftp
import time

# Checks packages
def check_package_names(ready_path, folder):

    packages = [f for f in os.listdir(ready_path + folder) if not f.startswith('.')]
    [os.remove(ready_path + folder + '/' + f) for f in os.listdir(ready_path + folder) if f.startswith('.')]

    if len(packages) == 0:
        return -1

    for i in packages:
        package = ready_path + folder + '/'

        if i.upper():
            os.rename(package + i, package + i.lower().replace(' ', ''))

# Checks package files
def check_file_names(ready_path, folder):

    missing = []
    packages = [f for f in os.listdir(ready_path + folder) if not f.startswith('.')]

    for i in packages:
        package = ready_path + folder + '/' + i + '/'
        files = [f for f in os.listdir(package) if not f.startswith('.')]
        [os.remove(package + f) for f in os.listdir(package) if f.startswith('.')]

        if len(files) < 2:
            missing.append(i)

        for j in files:
            if j.upper():
                os.rename(package + j, package + j.lower().replace(' ', ''))

    return missing

# Checks for missing uri.txt files
def check_uri_txt(ready_path, folder):

    missing = []
    packages = [f for f in os.listdir(ready_path + folder) if not f.startswith('.')]

    if len(packages) == 0:
        return -1

    for i in packages:
        package = ready_path + folder + '/' + i + '/'
        files = [f for f in os.listdir(package) if not f.startswith('.')]

        if 'uri.txt' not in files:
            missing.append(i)

    return missing

# Get package file size
# https://stackoverflow.com/questions/1392413/calculating-a-directorys-size-using-python
def check_file_sizes(ready_path, folder):
    package = ready_path + folder
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(package):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            # skip if it is symbolic link
            if not os.path.islink(fp):
                total_size += os.path.getsize(fp)

    return total_size

# Moves folder from ready to ingest folder and renames to pid
def move_to_ingest(ready_path, ingest_path, folder):

    mode = 0o666
    # TODO: add try catch
    # TODO: get total size of ingest packages
    shutil.move(ready_path + folder, ingest_path + folder)
    os.mkdir(ready_path + folder, mode)

# Moves folder to archivematica sftp
def move_to_sftp(ingest_path, folder, pid):

    host = os.getenv('SFTP_HOST')
    username = os.getenv('SFTP_ID')
    password = os.getenv('SFTP_PWD')
    sftp_path = os.getenv('SFTP_REMOTE_PATH')
    cnopts = pysftp.CnOpts()
    missing = []

    # TODO: add try catch
    os.rename(ingest_path + folder, ingest_path + pid)

    with pysftp.Connection(host=host, username=username, password=password, cnopts=cnopts) as sftp:

        # TODO: try catch
        sftp.put_r(ingest_path, sftp_path, preserve_mtime=True)
        packages = sftp.listdir()

        if pid not in packages:
            missing.append(-1)

        sftp.close()
        return missing