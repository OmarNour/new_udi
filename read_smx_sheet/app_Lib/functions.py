import os
import sys

sys.path.append(os.getcwd())
import numpy as np
import pandas as pd
# import pyarrow.parquet as pq
# import pyarrow as pa
# from pyarrow.formatting import *
import dask.dataframe as dd
from read_smx_sheet.app_Lib import manage_directories as md
from read_smx_sheet.parameters import parameters as pm
import datetime as dt
import psutil


def read_excel(file_path, sheet_name, filter=None, reserved_words_validation=None, nan_to_empty=True):
    try:
        df = pd.read_excel(file_path, sheet_name)
        df_cols = list(df.columns.values)
        df = df.applymap(lambda x: x.strip() if type(x) is str else x)

        if filter:
            df = df_filter(df, filter, False)

        if nan_to_empty:
            if isinstance(df, pd.DataFrame):
                df = replace_nan(df, '')
                df = df.applymap(lambda x: int(x) if type(x) is float else x)
            else:
                df = pd.DataFrame(columns=df_cols)

        if reserved_words_validation is not None:
            df = rename_sheet_reserved_word(df, reserved_words_validation[0], reserved_words_validation[1],
                                            reserved_words_validation[2])

    except:
        df = pd.DataFrame()
    return df


def df_filter(df, filter=None, filter_index=True):
    df_cols = list(df.columns.values)
    if filter:
        for i in filter:
            if filter_index:
                df = df[df.index.isin(i[1])]
            else:
                df = df[df[i[0]].isin(i[1])]

    if df.empty:
        df = pd.DataFrame(columns=df_cols)

    return df


def replace_nan(df, replace_with):
    return df.replace(np.nan, replace_with, regex=True)


def is_Reserved_word(Supplements, Reserved_words_source, word):
    Reserved_words = Supplements[Supplements['Reserved words source'] == Reserved_words_source][['Reserved words']]
    is_Reserved_word = True if Reserved_words[Reserved_words['Reserved words'] == word][
                                   'Reserved words'].any() == word else False
    return is_Reserved_word


def rename_sheet_reserved_word(sheet_df, Supplements_df, Reserved_words_source, columns):
    if not sheet_df.empty:
        for col in columns:
            sheet_df[col] = sheet_df.apply(
                lambda row: rename_reserved_word(Supplements_df, Reserved_words_source, row[col]), axis=1)
    return sheet_df


def rename_reserved_word(Supplements, Reserved_words_source, word):
    return word + '_' if is_Reserved_word(Supplements, Reserved_words_source, word) else word


def get_file_name(file):
    return os.path.splitext(os.path.basename(file))[0]


def get_core_table_columns(Core_tables, Table_name):
    Core_tables_df = Core_tables.loc[(Core_tables['Layer'] == 'CORE')
                                     & (Core_tables['Table name'] == Table_name)
                                     ].reset_index()
    return Core_tables_df


def get_core_tables(Core_tables):
    return Core_tables.loc[Core_tables['Layer'] == 'CORE'][['Table name', 'Fallback']].drop_duplicates()


def get_stg_tables(STG_tables, source_name=None):
    if source_name:
        stg_table_names = STG_tables.loc[STG_tables['Source system name'] == source_name][
            ['Table name', 'Fallback']].drop_duplicates()
    else:
        stg_table_names = STG_tables[['Table name', 'Fallback']].drop_duplicates()
    return stg_table_names


def get_src_code_set_names(STG_tables, source_name):
    code_set_names = list()
    for stg_tables_index, stg_tables_row in STG_tables.iterrows():
        if stg_tables_row['Source system name'] == source_name and str(
                stg_tables_row['Column name']).upper().startswith('BM_') and stg_tables_row['Code set name'] != '':
            code_set_names.append(str(stg_tables_row['Code set name']))
    return pd.unique(code_set_names)


def get_stg_table_nonNK_columns(STG_tables, source_name, Table_name, with_sk_columns=False):
    STG_tables_df = STG_tables.loc[(STG_tables['Source system name'] == source_name)
                                   & (STG_tables['Table name'] == Table_name)
                                   & (STG_tables['Natural key'].isnull())
                                   ].reset_index()
    return STG_tables_df


def get_stg_table_columns(STG_tables, source_name, Table_name, with_sk_columns=False):
    if source_name:
        STG_tables_df = STG_tables.loc[(STG_tables['Source system name'] == source_name)
                                       & (STG_tables['Table name'].str.upper() == Table_name.upper())
                                       ].reset_index()
    else:
        STG_tables_df = STG_tables.loc[STG_tables['Table name'].str.upper() == Table_name.upper()].reset_index()

    if not with_sk_columns:
        STG_tables_df = STG_tables_df.loc[(STG_tables_df['Key set name'] == '')
                                          & (STG_tables_df['Code set name'] == '')
                                          ].reset_index()

    return STG_tables_df


def single_quotes(string):
    return "'%s'" % string


def assertions(table_maping_row, Core_tables_list):
    assert (table_maping_row['Main source'] != None), 'Missing Main Source  for Table Mapping:{}'.format(
        str(table_maping_row['Mapping name']))
    assert (table_maping_row[
                'Target table name'] in Core_tables_list), 'TARGET TABLE NAME not found in Core Tables Sheet for Table Mapping:{}'.format(
        str(table_maping_row['Mapping name']))


def list_to_string(list, separator=None, between_single_quotes=0):
    if separator is None:
        prefix = ""
    else:
        prefix = separator
    to_string = prefix.join(
        (single_quotes(str(x)) if between_single_quotes == 1 else str(x)) if x is not None else "" for x in list)

    return to_string


def string_to_dict(sting_dict, separator=' '):
    if sting_dict:
        # ex: Firstname="Sita" Lastname="Sharma" Age=22 Phone=1234567890
        return eval("dict(%s)" % ','.join(sting_dict.split(separator)))


def wait_for_processes_to_finish(processes_numbers, processes_run_status, processes_names):
    count_finished_processes = 0
    no_of_subprocess = len(processes_numbers)

    while processes_numbers:
        for p_no in range(no_of_subprocess):
            if processes_run_status[p_no].poll() is not None:
                try:
                    processes_numbers.remove(p_no)
                    count_finished_processes += 1
                    # print('-----------------------------------------------------------')
                    # print('\nProcess no.', p_no, 'finished, total finished', count_finished_processes, 'out of', no_of_subprocess)
                    print(count_finished_processes, 'out of', no_of_subprocess, 'finished.\t', processes_names[p_no])
                except:
                    pass


def xstr(s):
    if s is None:
        return ''
    return str(s)


def save_to_parquet(pq_df, dataset_root_path, partition_cols=None, string_columns=None):
    if not pq_df.empty:

        # all_object_columns = df.select_dtypes(include='object').columns
        # print(all_object_columns)

        if string_columns is None:
            # string_columns = df.columns
            string_columns = pq_df.select_dtypes(include='object').columns

        for i in string_columns:
            pq_df[i] = pq_df[i].apply(xstr)

        partial_results_table = pa.Table.from_pandas(df=pq_df, nthreads=None)

        pq.write_to_dataset(partial_results_table, root_path=dataset_root_path, partition_cols=partition_cols,
                            use_dictionary=False
                            )
        # flavor = 'spark'
        # print("{:,}".format(len(df.index)), 'records inserted into', dataset_root_path, 'in', datetime.datetime.now() - start_time)


def read_all_from_parquet(dataset, columns, use_threads, filter=None):
    try:
        df = pq.read_table(dataset,
                           columns=columns,
                           use_threads=use_threads,
                           use_pandas_metadata=True).to_pandas()

        if filter:
            df = df_filter(df, filter, False)
    except:
        df = pd.DataFrame()

    return df


def read_all_from_parquet_delayed(dataset, columns=None, filter=None):
    df = dd.read_parquet(path=dataset, columns=columns, engine='pyarrow')
    if filter:
        for i in filter:
            df = df[df[i[0]].isin(i[1])]
    return df


def get_sheet_path(parquet_db_name, smx_file_path, output_path, sheet_name):
    file_name = get_file_name(smx_file_path)
    parquet_path = output_path + "/" + file_name + "/" + parquet_db_name + "/" + sheet_name
    return parquet_path


def save_sheet_data(parquet_db_name, df, smx_file_path, output_path, sheet_name):
    parquet_path = get_sheet_path(parquet_db_name, smx_file_path, output_path, sheet_name)
    save_to_parquet(df, parquet_path, partition_cols=None, string_columns=None)


def get_sheet_data(parquet_db_name, smx_file_path, output_path, sheet_name, df_filter=None):
    parquet_path = get_sheet_path(parquet_db_name, smx_file_path, output_path, sheet_name)
    df_sheet = read_all_from_parquet(parquet_path, None, True, filter=df_filter)
    if not isinstance(df_sheet, pd.DataFrame):
        df_sheet = pd.DataFrame()
    return df_sheet


def is_smx_file(file, sheets):
    file_sheets = pd.ExcelFile(file).sheet_names
    required_sheets = list(sheets)
    for required_sheet in sheets:
        for file_sheet in file_sheets:
            if file_sheet == required_sheet:
                required_sheets.remove(required_sheet)

    return True if len(required_sheets) == 0 else False


def get_smx_files(smx_path, smx_ext):
    smx_files = []
    all_files = md.get_files_in_dir(smx_path, smx_ext)
    for i in all_files:
        smx_files.append(i)
    return smx_files


def get_config_file_path():
    config_file_path = md.get_dirs()[1]
    return config_file_path


def get_config_file_values(config_file_path=None):
    separator = "$$$"
    parameters = ""
    # config_file_path = os.path.dirname(sys.modules['__main__'].__file__)
    if config_file_path is None:
        try:
            config_file_path = get_config_file_path()
            config_file = open(config_file_path + "/" + pm.default_config_file_name, "r")
        except:
            config_file_path = input("Enter config.txt path please:")
            config_file = open(config_file_path + "/" + pm.default_config_file_name, "r")
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

    else:
        param_dic = {}
    return param_dic


def server_info():
    cpu_per = psutil.cpu_percent(interval=0.5, percpu=False)
    # cpu_ghz = psutil.cpu_freq()
    # io = psutil.disk_io_counters()
    mem_per = psutil.virtual_memory()[2]

    return (cpu_per, mem_per)


def get_model_col(df, table_name):
    core_tables_IDS = df[df['Column name'].str.endswith(str('_ID'))]
    for core_tables_index, core_tables_row in core_tables_IDS.iterrows():
        if core_tables_row['Table name'] == table_name:
            return core_tables_row['Column name']


def table_has_modification_type_column(stg_tables, table_name):
    flag = False
    for stg_table_index, stg_table_row in stg_tables.iterrows():
        column_table_name = stg_table_row['Table name']
        if column_table_name.upper() == table_name.upper():
            Column_name = stg_table_row['Column name'].upper()
            if Column_name == "MODIFICATION_TYPE":
                flag = True
    return flag


class WriteFile:
    def __init__(self, file_path, file_name, ext, f_mode="w+", new_line=False):
        self.new_line = new_line
        self.f = open(os.path.join(file_path, file_name + "." + ext), f_mode, encoding="utf-8")

    def write(self, txt, new_line=None):
        self.f.write(txt)
        new_line = self.new_line if new_line is None else None
        self.f.write("\n") if new_line else None

    def close(self):
        self.f.close()


class SMXFilesLogError(WriteFile):
    def __init__(self, log_error_path, smx_file_name, system_row, error):
        self.log_error_path = log_error_path
        self.log_file_name = "log"
        self.ext = "txt"
        super().__init__(self.log_error_path, self.log_file_name, self.ext, "a+", True)
        self.smx_file_name = smx_file_name
        self.system_row = system_row
        self.error = error

    def log_error(self):
        error_separator = "##############################################################################"
        self.write(str(dt.datetime.now()))
        self.write(self.smx_file_name) if self.smx_file_name else None
        self.write(self.system_row) if self.system_row else None
        self.write(self.error)
        self.write(error_separator)


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
