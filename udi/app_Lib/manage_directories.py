import os
import shutil
import sys, os


def get_dirs():
    if getattr(sys, 'frozen', False):
        bundle_dir = sys._MEIPASS
    else:
        # we are running in a normal Python environment
        bundle_dir = os.path.dirname(os.path.abspath(__file__))
    cwd = os.getcwd()
    return bundle_dir, cwd

def create_folder(path):
    try:
        os.makedirs(path)
    except OSError:
        # TODO
        pass
    else:
        # TODO
        pass


def remove_folder(path):
    try:
        shutil.rmtree(path)
    except OSError:
        # TODO
        pass

    else:
        # TODO
        pass



def get_files_in_dir(path, ext=""):
    files = [name for name in os.listdir(path) if "."+ext in name and "~$" not in name and os.path.isfile(os.path.join(path, name))]
    return files
