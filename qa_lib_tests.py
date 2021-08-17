import os
import pysftp

def move_to_sftp(ingest_path, pid):
    host = os.getenv('SFTP_HOST')
    username = os.getenv('SFTP_ID')
    password = os.getenv('SFTP_PWD')
    sftp_path = os.getenv('SFTP_REMOTE_PATH')
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
    errors = []

    with pysftp.Connection(host=host, username=username, password=password, cnopts=cnopts) as sftp:
        sftp.put_r(ingest_path, sftp_path, preserve_mtime=True)
        packages = sftp.listdir()

        # TODO: get package count and compare after upload
        # TODO: log
        if pid not in packages:
            errors.append(-1)


'''
checks upload on archivematica sftp
@param: host
@param: username
@param: password
@param: sftp_path
@returns: Array
'''


def check_sftp(pid, local_file_count):
    host = os.getenv('SFTP_HOST')
    username = os.getenv('SFTP_ID')
    password = os.getenv('SFTP_PWD')
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
    sftp_path = os.getenv('SFTP_REMOTE_PATH')
    file_names = []
    dir_names = []
    un_name = []

    def store_files_name(fname):
        file_names.append(fname)

    def store_dir_name(dirname):
        dir_names.append(dirname)

    def store_other_file_types(name):
        un_name.append(name)

    with pysftp.Connection(host=host, username=username, password=password, cnopts=cnopts) as sftp:
        remote_package = sftp_path + '/' + pid + '/'
        sftp.cwd(remote_package)
        sftp.walktree(remote_package, store_files_name, store_dir_name, store_other_file_types, recurse=True)
        remote_file_count = len(file_names)

        with sftp.cd(remote_package):
            remote_package_size = sftp.execute('du -h -s')
            # TODO: compare local and remote file sizes
            print('remote package size: ')
            print(remote_package_size)

        if int(local_file_count) == remote_file_count:
            return dict(message='upload_complete', data=[file_names, remote_file_count])

        return dict(message='in_progress', data=[file_names, remote_file_count])

# check_sftp('a5efb5d1-0484-429c-95a5-15c12ff40ca0', 2)

import os, os.path

# get package count
# path joining version for other paths
DIR = './tmp'
test = len([name for name in os.listdir(DIR) if os.path.isdir(os.path.join(DIR, name))])
print(test)
