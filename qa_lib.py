import os

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