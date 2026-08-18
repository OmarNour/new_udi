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
_LOG_SHEET_STATS = Counter()
_LOG_AFFECTED_OBJECTS = {}
_LOG_ISSUE_SEQUENCE = Counter()
_LOG_ISSUE_REFERENCES = {}
_NORMALIZATION_STATS = Counter()
_NORMALIZATION_EXAMPLES = []


def reset_validation_log_stats():
    with _LOG_STATS_LOCK:
        for counter in _LOG_STATS.values():
            counter.clear()
        _LOG_SHEET_STATS.clear()
        _LOG_AFFECTED_OBJECTS.clear()
        _LOG_ISSUE_SEQUENCE.clear()
        _LOG_ISSUE_REFERENCES.clear()
        _NORMALIZATION_STATS.clear()
        _NORMALIZATION_EXAMPLES.clear()


def get_validation_log_stats():
    with _LOG_STATS_LOCK:
        return {
            "levels": {
                level: dict(counter)
                for level, counter in _LOG_STATS.items()
            },
            "sheets": dict(_LOG_SHEET_STATS),
            "affected_objects": {
                category: dict(counter)
                for category, counter in _LOG_AFFECTED_OBJECTS.items()
            },
            "normalization": {
                "counts": dict(_NORMALIZATION_STATS),
                "examples": list(_NORMALIZATION_EXAMPLES),
            },
        }


def register_issue_reference(reference_type, reference_key, issue_id):
    if not issue_id or reference_key in (None, ""):
        return
    key = (str(reference_type).lower(), str(reference_key).strip().lower())
    with _LOG_STATS_LOCK:
        _LOG_ISSUE_REFERENCES[key] = issue_id


def get_issue_reference(reference_type, reference_key):
    if reference_key in (None, ""):
        return None
    key = (str(reference_type).lower(), str(reference_key).strip().lower())
    with _LOG_STATS_LOCK:
        return _LOG_ISSUE_REFERENCES.get(key)


def record_input_normalization(field_name, original_value, normalized_value):
    if original_value == normalized_value:
        return

    field = str(field_name or "value")
    example = (field, str(original_value), str(normalized_value))
    with _LOG_STATS_LOCK:
        _NORMALIZATION_STATS[field] += 1
        if example not in _NORMALIZATION_EXAMPLES and len(_NORMALIZATION_EXAMPLES) < 8:
            _NORMALIZATION_EXAMPLES.append(example)


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


def _next_issue_id(level_name):
    prefix = {
        "ERROR": "E",
        "WARNING": "W",
        "CRITICAL": "C",
    }.get(level_name, "I")
    with _LOG_STATS_LOCK:
        _LOG_ISSUE_SEQUENCE[prefix] += 1
        return f"{prefix}{_LOG_ISSUE_SEQUENCE[prefix]:04d}"


def _extract_quoted_value(problem, label):
    match = re.search(rf"{re.escape(label)}\s+'([^']*)'", str(problem or ""), re.IGNORECASE)
    return match.group(1) if match else None


def _infer_affected_object(title, row, problem):
    data = _row_to_dict(row)
    title_lower = str(title).lower()

    if "main source table" in title_lower:
        return str(data.get("main_source", "") or "<blank>")
    if "join table" in title_lower:
        return _extract_quoted_value(problem, "JOIN table") or "<unknown>"
    if "source-table alias" in title_lower:
        return str(data.get("mapped_to_table", "") or "<unknown>")
    if "source column" in title_lower:
        table_name = data.get("mapped_to_table") or data.get("table_name") or "<unknown>"
        column_name = data.get("mapped_to_column") or data.get("column_name") or "<unknown>"
        return f"{table_name}.{column_name}"
    if "transformation type" in title_lower:
        return str(data.get("mapping_name", "") or data.get("column_name", "") or "<unknown>")
    if "history table" in title_lower:
        return str(data.get("target_table_name", "") or "<unknown>")
    if "#ref!" in title_lower:
        return str(data.get("table_name", "") or "<unknown>")
    if "bmap" in title_lower or "code set" in title_lower:
        return str(data.get("code_set_name", "") or data.get("physical_table", "") or "<unknown>")
    if "domain" in title_lower:
        table_name = data.get("table_name")
        column_name = data.get("column_name")
        if table_name or column_name:
            return f"{table_name or '<unknown>'}.{column_name or '<unknown>'}"
        return str(data.get("code_domain_name", "") or data.get("code_domain_id", "") or "<unknown>")
    if "primary key" in title_lower:
        return str(data.get("table_name", "") or "<unknown>")
    return None


def _record_log_issue(level_name, title, sheet_label, affected_object):
    counter = _LOG_STATS.get(level_name)
    if counter is None:
        return
    with _LOG_STATS_LOCK:
        counter[title] += 1
        if level_name in {"ERROR", "CRITICAL"}:
            _LOG_SHEET_STATS[sheet_label or "General validation"] += 1
        if affected_object:
            category_counter = _LOG_AFFECTED_OBJECTS.setdefault(title, Counter())
            category_counter[str(affected_object)] += 1


def format_log_issue(
    title,
    *,
    issue_id=None,
    row=None,
    sheet=None,
    validation_stage=None,
    affected_object=None,
    problem=None,
    action=None,
    impact=None,
    details=None,
    related_issue_id=None,
    related_cause=None,
    original_error=None,
):
    row_data = _row_to_dict(row)
    sheet = sheet or infer_sheet_name(row)
    excel_row = row_data.get("source_row_number")

    heading = f"[{issue_id}] {str(title).strip()}" if issue_id else str(title).strip()
    lines = [heading, "", "Location:"]

    if sheet:
        lines.append(f"  Sheet: {sheet}")
    elif validation_stage:
        lines.append(f"  Validation stage: {validation_stage}")
    else:
        lines.append("  Validation stage: General validation")

    if excel_row not in (None, ""):
        lines.append(f"  Excel row: {excel_row}")
    if affected_object:
        lines.append(f"  Affected object: {affected_object}")

    if problem:
        lines.extend(["", "Problem:", f"  {_format_value(problem, indent='  ')}"])

    if related_issue_id or related_cause:
        lines.extend(["", "Related cause:"])
        if related_issue_id and related_cause:
            lines.append(f"  {related_issue_id} — {_format_value(related_cause, indent='  ')}")
        elif related_issue_id:
            lines.append(f"  {related_issue_id}")
        else:
            lines.append(f"  {_format_value(related_cause, indent='  ')}")

    # original_error remains accepted for backward compatibility, but ordinary
    # validation reports intentionally do not expose internal Python/model text.

    if action:
        lines.extend(["", "Program action:", f"  {_format_value(action, indent='  ')}"])

    if impact:
        lines.extend(["", "Impact:", f"  {_format_value(impact, indent='  ')}"])

    if details:
        lines.extend(["", "Details:", f"  {_format_value(details, indent='  ')}"])

    if row_data:
        lines.extend(["", "Workbook row context:", format_processing_row(row, include_empty=True)])

    return "\n".join(lines)


def log_validation_issue(
    title,
    *,
    row=None,
    sheet=None,
    validation_stage=None,
    affected_object=None,
    problem=None,
    action=None,
    impact=None,
    details=None,
    related_issue_id=None,
    related_cause=None,
    original_error=None,
    level=logging.ERROR,
):
    level_name = logging.getLevelName(level)
    issue_id = _next_issue_id(level_name)
    resolved_sheet = sheet or infer_sheet_name(row) or validation_stage or "General validation"
    affected_object = affected_object or _infer_affected_object(title, row, problem)
    _record_log_issue(level_name, title, resolved_sheet, affected_object)
    logging.log(
        level,
        format_log_issue(
            title,
            issue_id=issue_id,
            row=row,
            sheet=sheet,
            validation_stage=validation_stage,
            affected_object=affected_object,
            problem=problem,
            action=action,
            impact=impact,
            details=details,
            related_issue_id=related_issue_id,
            related_cause=related_cause,
            original_error=original_error,
        ),
    )
    return issue_id


def log_validation_error(title, **kwargs):
    return log_validation_issue(title, level=logging.ERROR, **kwargs)


def log_validation_warning(title, **kwargs):
    return log_validation_issue(title, level=logging.WARNING, **kwargs)


def _count_level(level_name):
    return sum(_LOG_STATS[level_name].values())


def _build_category_lines():
    combined = Counter(_LOG_STATS["ERROR"])
    combined.update(_LOG_STATS["CRITICAL"])
    if not combined:
        return ["  None"]
    width = max(len(name) for name in combined)
    return [
        f"  {name:<{width}}  {count:>5}"
        for name, count in combined.most_common()
    ]


def _build_sheet_lines():
    if not _LOG_SHEET_STATS:
        return ["  None"]
    width = max(len(name) for name in _LOG_SHEET_STATS)
    return [
        f"  {name:<{width}}  {count:>5}"
        for name, count in _LOG_SHEET_STATS.most_common()
    ]


def _build_affected_object_lines():
    groups = []
    for category, counter in _LOG_AFFECTED_OBJECTS.items():
        repeated = [(name, count) for name, count in counter.most_common(5) if count > 1]
        if repeated:
            groups.append((sum(counter.values()), category, repeated))

    if not groups:
        return ["  No repeated affected objects."]

    lines = []
    for _, category, repeated in sorted(groups, reverse=True)[:8]:
        lines.append(f"  {category}:")
        for name, count in repeated:
            lines.append(f"    {name}: {count}")
    return lines


def _build_normalization_lines():
    total = sum(_NORMALIZATION_STATS.values())
    lines = [f"  Values normalized: {total}"]

    if not total:
        return lines

    top_field_limit = 10
    example_limit = 6

    top_fields = _NORMALIZATION_STATS.most_common(top_field_limit)
    other_fields_count = max(
        len(_NORMALIZATION_STATS) - len(top_fields),
        0,
    )

    lines.append("")
    lines.append("  Most frequently normalized fields:")

    for field, count in top_fields:
        lines.append(f"    {field}: {count}")

    if other_fields_count:
        lines.append("")
        lines.append(f"  Other normalized fields: {other_fields_count}")

    examples = _NORMALIZATION_EXAMPLES[:example_limit]

    if examples:
        lines.append("")
        lines.append("  Examples:")

        for field, original, normalized in examples:
            lines.append(
                f"    {field}: {original!r} -> {normalized!r}"
            )

    return lines


def build_validation_summary_report():
    with _LOG_STATS_LOCK:
        error_count = _count_level("ERROR")
        warning_count = _count_level("WARNING")
        critical_count = _count_level("CRITICAL")

        if critical_count:
            status = "COMPLETED WITH CRITICAL ERRORS"
            output_condition = "Generated output may be incomplete or unreliable"
        elif error_count:
            status = "COMPLETED WITH ERRORS"
            output_condition = "Generated with omissions"
        elif warning_count:
            status = "SUCCESS WITH WARNINGS"
            output_condition = "Generated successfully; review warnings"
        else:
            status = "SUCCESS"
            output_condition = "Generated successfully"

        lines = [
            "Run Result:",
            "######################",
            "",
            f"Status: {status}",
            "",
            f"Errors:   {error_count}",
            f"Warnings: {warning_count}",
            f"Critical: {critical_count}",
            "",
            "Scripts generated: Yes",
            f"Output condition: {output_condition}",
            "",
            "",
            "Errors by category:",
            "######################",
            "",
            *_build_category_lines(),
            "",
            "",
            "Errors by sheet:",
            "######################",
            "",
            *_build_sheet_lines(),
            "",
            "",
            "Most affected objects:",
            "######################",
            "",
            *_build_affected_object_lines(),
            "",
            "",
            "Input normalization:",
            "######################",
            "",
            *_build_normalization_lines(),
        ]
        return "\n".join(lines)


class ValidationSummaryFilter(logging.Filter):
    """Prepend the enhanced report immediately before the legacy Summary."""

    def filter(self, record):
        try:
            message = record.getMessage()
            if (
                record.levelno == logging.INFO
                and "Summary:" in message
                and "######################" in message
                and not getattr(record, "_udi_summary_augmented", False)
            ):
                record.msg = f"{build_validation_summary_report()}\n\n{message}"
                record.args = ()
                record._udi_summary_augmented = True
        except Exception:
            # Logging must never break script generation.
            pass
        return True

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
                row_type = type(row).__name__ if row is not None else ""
                column_name = getattr(row, "column_name", "") if row else ""
                table_name = getattr(row, "table_name", "") if row else ""
                error_text = str(exc)
                processing_step = _humanize_function_name(function.__name__)

                if "Already Exists" in error_text:
                    if row_type == "BMapValueRow":
                        code_set_name = getattr(row, "code_set_name", "")
                        code_domain_id = getattr(row, "code_domain_id", "")
                        source_code = getattr(row, "source_code", "")
                        log_validation_error(
                            "Duplicate domain value",
                            row=row,
                            sheet=sheet,
                            affected_object=code_set_name or "<unknown>",
                            problem=(
                                f"Source code '{source_code}' is already defined in "
                                f"domain ID '{code_domain_id}' for code set "
                                f"'{code_set_name}'."
                            ),
                            action=(
                                "This duplicate domain value was not added. The "
                                "previously defined value remains unchanged."
                            ),
                            impact=(
                                f"No additional value was created for source code "
                                f"'{source_code}'."
                            ),
                            details=f"Processing step: {processing_step}",
                        )
                    else:
                        shown_value = column_name or "the same value"
                        title = f"Duplicate definition '{shown_value}'"
                        problem = (
                            f"The value '{shown_value}' was received more than once "
                            f"for table '{table_name or 'unknown'}'. The duplicate "
                            "definition cannot be added to the model."
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
                            action=(
                                "The duplicate definition was not added. Processing "
                                "continued with the remaining workbook rows."
                            ),
                            impact=(
                                "The affected table or mapping may be incomplete. "
                                "The original workbook value is shown exactly as read."
                            ),
                            details=f"Processing step: {processing_step}",
                        )
                else:
                    log_validation_issue(
                        "Unexpected processing failure",
                        row=row,
                        sheet=sheet,
                        validation_stage=processing_step,
                        problem=error_text or exc.__class__.__name__,
                        action=(
                            "The affected item was not added. Processing continued "
                            "where it was safe to do so."
                        ),
                        impact=(
                            "Review the workbook row context below. The generated "
                            "output may not include this item."
                        ),
                        details=f"Processing step: {processing_step}",
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
