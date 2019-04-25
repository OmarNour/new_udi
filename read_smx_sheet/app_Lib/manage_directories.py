import os
import shutil


def create_folder(path):
    try:
        os.makedirs(path)
    except OSError:
        pass
        # print("Creation of the directory %s failed" % path)
    else:
        pass
        # print("Successfully created the directory %s " % path)


def remove_folder(path):
    try:
        shutil.rmtree(path)
    except OSError:
        pass
        # print("Deletion  of the directory %s failed" % path)
    else:
        pass
        # print("Successfully deleted the directory %s " % path)


def get_files_in_dir(path, ext=""):
    files = [name for name in os.listdir(path) if "."+ext in name
                                                and "~$" not in name
                                                and os.path.isfile(os.path.join(path, name))]

    # print(files)
    return files
