import functools
from app_Lib import functions as funcs
import traceback


def Logging_decorator(function):
    @functools.wraps(function)
    def wrapper(*args, **kwargs):
        cf = args[0]
        source_output_path = args[1]
        file_name = funcs.get_file_name(__file__)
        try:
            function(*args, **kwargs)
        except:
            funcs.TemplateLogError(cf.output_path, source_output_path, file_name, traceback.format_exc()).log_error()

    return wrapper
