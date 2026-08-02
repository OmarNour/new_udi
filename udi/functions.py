import logging
import os
import datetime as dt
import shutil
import sys
import traceback
import functools
from datetime import datetime
import configparser
import concurrent.futures
import multiprocessing
import re
import textwrap
import threading
from collections import Counter

import psutil

try:
    import cPickle as pickle
except:
    import pickle


class WriteFile:
    def __init__(self, file_path, file_name, ext, f_mode="w", new_line=False):
        self.new_line = new_line
        self.f = open(os.path.join(file_path, file_name + "." + ext), f_mode, encoding="utf-8")

    def write(self, txt, new_line=None):
        self.f.write(txt)
        new_line = self.new_line if new_line is None else None
        self.f.write("\n") if new_line else None

    def close(self):
        self.f.close()

ROW_SHEET_NAMES = {
    "SystemRow": "System",
    "StagingTableRow": "Stg tables",
    "StgSrciColumnRow": "Stg tables",
    "SourceViewMappingRow": "Stg tables",
    "SrciViewMappingRow": "Stg tables",
    "BKeyViewRow": "Stg tables",
    "BMapViewRow": "Stg tables",
    "CoreTableRow": "Core tables",
    "CoreColumnRow": "Core tables",
    "BKeyTableRow": "BKEY",
    "BKeyDataSetRow": "BKEY",
    "BKeyDomainRow": "BKEY",
    "BMapTableRow": "BMAP",
    "BMapDataSetRow": "BMAP",
    "BMapDomainRow": "BMAP",
    "BMapValueRow": "BMAP values",
    "TableMappingRow": "Table mapping",
    "ColumnMappingRow": "Column mapping",
}

_LOG_STATS_LOCK = threading.Lock()
_LOG_STATS = {
    "ERROR": Counter(),
    "WARNING": Counter(),
    "CRITICAL": Counter(),
}


def reset_validation_log_stats():
    with _LOG_STATS_LOCK:
        for counter in _LOG_STATS.values():
            counter.clear()


def get_validation_log_stats():
    with _LOG_STATS_LOCK:
        return {
            level: dict(counter)
            for level, counter in _LOG_STATS.items()
        }


def _row_to_dict(row):
    if row is None:
        return {}
    if hasattr(row, "_asdict"):
        return dict(row._asdict())
    if hasattr(row, "__dict__"):
        return {
            key: value
            for key, value in vars(row).items()
            if not key.startswith("_")
        }
    return {}


def infer_sheet_name(row):
    if row is None:
        return None
    return ROW_SHEET_NAMES.get(type(row).__name__)


def _format_value(value, indent="    ", width=118):
    if value is None:
        return ""
    text = str(value).replace("\r\n", "\n").replace("\r", "\n")
    if not text:
        return ""
    lines = []
    for original_line in text.split("\n"):
        wrapped = textwrap.wrap(
            original_line,
            width=max(20, width - len(indent)),
            replace_whitespace=False,
            drop_whitespace=False,
        ) or [""]
        lines.extend(wrapped)
    return ("\n" + indent).join(lines)


def format_processing_row(row, row_index=None, include_empty=True):
    data = _row_to_dict(row)
    if not data:
        return str(row)

    index = (
        row_index
        if row_index is not None
        else data.get("source_row_number")
    )
    header = f"Excel row {index}" if index not in (None, "") else "Row context"
    lines = [header]
    for key, value in data.items():
        if key == "source_row_number":
            continue
        if not include_empty and value in (None, ""):
            continue
        value_text = _format_value(value, indent=" " * 36)
        lines.append(f"    {key:<30} {value_text}")
    return "\n".join(lines)


def _record_log_issue(level_name, title):
    counter = _LOG_STATS.get(level_name)
    if counter is None:
        return
    with _LOG_STATS_LOCK:
        counter[title] += 1


def format_log_issue(
    title,
    *,
    row=None,
    sheet=None,
    problem=None,
    action=None,
    impact=None,
    details=None,
    original_error=None,
):
    row_data = _row_to_dict(row)
    sheet = sheet or infer_sheet_name(row)
    excel_row = row_data.get("source_row_number")

    lines = [str(title).strip()]

    if sheet or excel_row not in (None, ""):
        lines.extend(["", "Location:"])
        if sheet:
            lines.append(f"  Sheet: {sheet}")
        if excel_row not in (None, ""):
            lines.append(f"  Excel row: {excel_row}")

    if problem:
        lines.extend(["", "Problem:", f"  {_format_value(problem, indent='  ')}"])

    if original_error:
        lines.extend([
            "",
            "Original error:",
            f"  {_format_value(original_error, indent='  ')}",
        ])

    if action:
        lines.extend([
            "",
            "Program action:",
            f"  {_format_value(action, indent='  ')}",
        ])

    if impact:
        lines.extend([
            "",
            "Impact:",
            f"  {_format_value(impact, indent='  ')}",
        ])

    if details:
        lines.extend([
            "",
            "Details:",
            f"  {_format_value(details, indent='  ')}",
        ])

    if row_data:
        lines.extend([
            "",
            "Workbook row context:",
            format_processing_row(row, include_empty=True),
        ])

    return "\n".join(lines)


def log_validation_issue(
    title,
    *,
    row=None,
    sheet=None,
    problem=None,
    action=None,
    impact=None,
    details=None,
    original_error=None,
    level=logging.ERROR,
):
    level_name = logging.getLevelName(level)
    _record_log_issue(level_name, title)
    logging.log(
        level,
        format_log_issue(
            title,
            row=row,
            sheet=sheet,
            problem=problem,
            action=action,
            impact=impact,
            details=details,
            original_error=original_error,
        ),
    )


def log_validation_error(title, **kwargs):
    log_validation_issue(title, level=logging.ERROR, **kwargs)


def log_validation_warning(title, **kwargs):
    log_validation_issue(title, level=logging.WARNING, **kwargs)


def get_dirs():
    if getattr(sys, 'frozen', False):
        bundle_dir = sys._MEIPASS
    else:
        # we are running in a normal Python environment
        bundle_dir = os.path.dirname(os.path.abspath(__file__))
    cwd = os.getcwd()
    return bundle_dir, cwd

def string_to_dict(sting_dict, separator=' '):
    if sting_dict:
        # ex: Firstname="Sita" Lastname="Sharma" Age=22 Phone=1234567890
        return eval("dict(%s)" % ','.join(sting_dict.split(separator)))


def get_config_file_path():
    config_file_path = get_dirs()[1]
    return config_file_path


def get_config_file_values(config_file_path=None):
    separator = "$$$"
    parameters = ""
    if config_file_path is None:
        try:
            config_file_path = get_config_file_path()
            config_file = open(config_file_path + "/" + default_config_file_name, "r")
        except:
            config_file_path = input("Enter config.txt path please:")
            config_file = open(config_file_path + "/" + default_config_file_name, "r")
    else:
        try:
            config_file = open(config_file_path, "r")
        except:
            config_file = None

    if config_file:
        for i in config_file.readlines():
            line = i.strip()
            if line != "":
                if line[0] != '#':
                    parameters = parameters + line + separator

        param_dic = string_to_dict(parameters, separator)

        source_names = param_dic['source_names'].split(',')
        source_names = None if source_names[0] == "" and len(source_names) > 0 else source_names
        param_dic['source_names'] = source_names

        ################################################################################################
        dt_now = dt.datetime.now()
        dt_folder = dt_now.strftime("%Y") + "_" + \
                    dt_now.strftime("%b").upper() + "_" + \
                    dt_now.strftime("%d") + "_" + \
                    dt_now.strftime("%H") + "_" + \
                    dt_now.strftime("%M") + "_" + \
                    dt_now.strftime("%S")
        param_dic['output_path'] = param_dic["home_output_folder"] + "/" + dt_folder

        db_prefix = param_dic['db_prefix']

        param_dic['T_STG'] = db_prefix + "T_STG"
        param_dic['t_WRK'] = db_prefix + "T_WRK"
        param_dic['v_stg'] = db_prefix + "V_STG"
        param_dic['v_base'] = db_prefix + "V_BASE"
        param_dic['INPUT_VIEW_DB'] = db_prefix + "V_INP"

        param_dic['MACRO_DB'] = db_prefix + "M_GCFR"
        param_dic['UT_DB'] = db_prefix + "P_UT"
        param_dic['UTLFW_v'] = db_prefix + "V_UTLFW"
        param_dic['UTLFW_t'] = db_prefix + "T_UTLFW"

        param_dic['TMP_DB'] = db_prefix + "T_TMP"
        param_dic['APPLY_DB'] = db_prefix + "P_PP"
        param_dic['base_DB'] = db_prefix + "T_BASE"

        param_dic['SI_DB'] = db_prefix + "T_SRCI"
        param_dic['SI_VIEW'] = db_prefix + "V_SRCI"

        param_dic['GCFR_t'] = db_prefix + "t_GCFR"
        param_dic['GCFR_V'] = db_prefix + "V_GCFR"
        param_dic['keycol_override_base'] = db_prefix + "T_GCFR.GCFR_TRANSFORM_KEYCOL_OVERRIDE"
        param_dic['M_GCFR'] = db_prefix + "M_GCFR"
        param_dic['P_UT'] = db_prefix + "P_UT"

        param_dic['core_table'] = db_prefix + "T_BASE"
        param_dic['core_view'] = db_prefix + "V_BASE"

        try:
            staging_view_db = param_dic['staging_view_db']
        except:
            staging_view_db = ''

        if staging_view_db is not None and staging_view_db != "":
            staging_view_db = db_prefix + "V_" + staging_view_db
        else:
            staging_view_db = ''
        param_dic['staging_view_db'] = staging_view_db
    else:
        param_dic = {}
    return param_dic

def get_server_info():
    cpu_per = psutil.cpu_percent(interval=0.5, percpu=False)
    mem_per = psutil.virtual_memory()[2]

    return (cpu_per, mem_per)





def upper_string_in_list(_list: list) -> list:
    return [x.upper() if isinstance(x, str) else x for x in _list]


def generate_run_id():
    return str(dt.datetime.now().strftime("%Y%m%d%H%M%S"))


def open_folder(path):
    if sys.platform == "win32":
        os.startfile(path)
    else:
        opener = "open" if sys.platform == "darwin" else "xdg-open"
        subprocess.call([opener, path])


def create_folder(path):
    os.makedirs(path, exist_ok=True)


def remove_folder(path):
    try:
        shutil.rmtree(path)
    except FileNotFoundError:
        pass


def string_to_list(string, separator=None) -> list:
    if separator is None:
        return string.split()
    else:
        return string.split(separator)


def list_to_string(_list, separator=None, quotes=0):
    if separator is None:
        prefix = ""
    else:
        prefix = separator

    to_string = prefix.join((single_quotes(str(x)) if quotes == 1 else str(x)) if x is not None else "" for x in _list)
    return to_string


def single_quotes(string):
    return "'%s'" % string


def time_elapsed_decorator(function):
    @functools.wraps(function)
    def wrapper(*args, **kwargs):
        start_time = datetime.now()
        x = function(*args, **kwargs)
        print(f'Time elapsed for {function.__name__}: {datetime.now() - start_time} \n')
        return x

    return wrapper


class TemplateLogError(WriteFile):
    def __init__(self, log_error_path, file_name_path, error_file_name, error):
        self.log_error_path = log_error_path
        self.log_file_name = "log"
        self.ext = "txt"
        super().__init__(self.log_error_path, self.log_file_name, self.ext, "a+", True)
        self.file_name_path = file_name_path
        self.error_file_name = error_file_name
        self.error = error

    def log_error(self):
        error_separator = "##############################################################################"
        self.write(str(dt.datetime.now()))
        self.write(self.file_name_path)
        self.write(self.error_file_name)
        self.write(self.error)
        self.write(error_separator)


def _find_processing_row(args, kwargs):
    for value in list(args[1:]) + list(kwargs.values()):
        if hasattr(value, "_asdict") or hasattr(value, "source_row_number"):
            return value
    return None


def _humanize_function_name(name):
    return str(name).replace("_", " ").strip().capitalize()


def log_error_decorator(sheet=None):
    def decorator(function):
        @functools.wraps(function)
        def wrapper(*args, **kwargs):
            try:
                return function(*args, **kwargs)
            except Exception as exc:
                row = _find_processing_row(args, kwargs)
                column_name = getattr(row, "column_name", "") if row else ""
                table_name = getattr(row, "table_name", "") if row else ""
                error_text = str(exc)

                if "Already Exists" in error_text:
                    shown_value = column_name or "the same value"
                    title = f"Duplicate definition '{shown_value}'"
                    problem = (
                        f"The value '{shown_value}' was received more than once "
                        f"for table '{table_name or 'unknown'}'. "
                        "The duplicate definition cannot be added to the model."
                    )
                    if str(shown_value).upper() == "#REF!":
                        problem = (
                            "Excel returned '#REF!' as the column name. The same "
                            f"'#REF!' value already exists for table "
                            f"'{table_name or 'unknown'}'."
                        )
                    log_validation_error(
                        title,
                        row=row,
                        sheet=sheet,
                        problem=problem,
                        original_error=error_text,
                        action=(
                            "The duplicate definition was not added. Processing "
                            "continued with the remaining workbook rows."
                        ),
                        impact=(
                            "The affected table or mapping may be incomplete. "
                            "The original workbook value is shown exactly as read."
                        ),
                        details=(
                            f"Processing step: {_humanize_function_name(function.__name__)}"
                        ),
                    )
                else:
                    log_validation_issue(
                        "Unexpected processing failure",
                        row=row,
                        sheet=sheet,
                        problem=error_text or exc.__class__.__name__,
                        action=(
                            "The affected item was not added. Processing continued "
                            "where it was safe to do so."
                        ),
                        impact=(
                            "Review the workbook row context below. The generated "
                            "output may not include this item."
                        ),
                        details=(
                            f"Processing step: {_humanize_function_name(function.__name__)}"
                        ),
                        level=logging.CRITICAL,
                    )
                return None

        return wrapper

    return decorator


def get_file_name(file):
    return os.path.splitext(os.path.basename(file))[0]


def processes(target_func, iterator, max_workers=multiprocessing.cpu_count()):
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        return executor.map(target_func, iterator)


def threads(target_func, iterator, max_workers=multiprocessing.cpu_count()):
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        return executor.map(target_func, iterator)


def read_config_file(file):
    parser = configparser.ConfigParser()
    parser.read(file)
    return parser


def remove_all_from_list(lst: [], word: str):
    return [item for item in lst if item != word]


def merge_multiple_spaces(txt: str):
    return re.sub(r'\s+', ' ', txt.strip())


def split_text(text, sep, maxsplit=0):
    return re.split(sep, text, flags=re.IGNORECASE, maxsplit=maxsplit)


def parse_data_type(data_type_precision: str):
    data_type_lst = data_type_precision.split(sep='(')
    data_type = data_type_lst[0]
    precision = data_type_lst[1].split(sep=')')[0] if len(data_type_lst) > 1 else None
    return data_type, precision

if __name__ == '__main__':
    _ = """ dasda's """
    x = single_quotes(_)
    print(x)
    print(generate_run_id())

