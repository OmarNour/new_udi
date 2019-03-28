import os


def create_folder(path):
    try:
        os.makedirs(path)
    except OSError:
        print("Creation of the directory %s failed" % path)
    else:
        print("Successfully created the directory %s " % path)


def remove_folder(path):
    try:
        os.rmdir(path)
    except OSError:
        print("Deletion  of the directory %s failed" % path)
    else:
        print("Successfully deleted the directory %s " % path)


def get_files_in_dir(path, ext=""):
    files = [name for name in os.listdir(path) if "."+ext in name
                                                and "~$" not in name
                                                and os.path.isfile(os.path.join(path, name))]

    # print(files)
    return files
