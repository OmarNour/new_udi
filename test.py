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
from read_smx_sheet.app_Lib import functions as fn


def read_excel(file_path, sheet_name, filter=None, reserved_words_validation=None, nan_to_empty=True):
    try:
        df = pd.read_excel(file_path, sheet_name)
        df_cols = list(df.columns.values)
        df = df.applymap(lambda x: x.strip() if type(x) is str else x)

        if filter:
            df = fn.df_filter(df, filter, False)

        if nan_to_empty:
            
            if isinstance(df, pd.DataFrame):
                df = fn.replace_nan(df, '')
                df = df.applymap(lambda x: int(x) if type(x) is float else x)
            else:
                
                df = pd.DataFrame(columns=df_cols)

        if reserved_words_validation is not None:
            df = rename_sheet_reserved_word(df, reserved_words_validation[0], reserved_words_validation[1],
                                            reserved_words_validation[2])

        # print(df)

    except:
        df = pd.DataFrame()
    return df


def d000(cf, source_output_path, source_name, df):
    file_name = fn.get_file_name(__file__)
    f = fn.WriteFile(source_output_path, file_name, "sql")
    # config_file_values = fn.get_config_file_values("C:/Users/oh255011/Documents/Teradata/new_udi/read_smx_sheet/config.txt")

    # target table
    target_table  = cf['db_prefix'] + "T_STG.UNIFIED_GOV" 

    delete_query = f"DELETE FROM {target_table} WHERE SOURCE = '{source_name}';\n"
    f.write(delete_query)
    print(delete_query)
    insert_prefix = f"INSERT INTO {target_table}(SOURCE_TABLE_NAME, DWH_GOVERNORATE_ID, GOVERNORATE_DESCRIPTION_AR, GOVERNORATE_DESCRIPTION_EN, SOURCE_GOVERNORATE_ID, SOURCE_GOVERNORATE_DESC_AR, SOURCE_GOVERNORATE_DESC_EN, DWH_TYPE, SOURCE, MODIFICATION_TYPE, INS_DTTM) VALUES("
    for index, row in df.iterrows():
        script = insert_prefix + f"'{str(row['SOURCE_TABLE_NAME'])}', '{str(row['DWH_GOVERNORATE_ID'])}', '{str(row['GOVERNORATE_DESCRIPTION_AR'])}', '{str(row['GOVERNORATE_DESCRIPTION_EN'])}', '{str(row['SOURCE_GOVERNORATE_ID'])}', '{str(row['SOURCE_GOVERNORATE_DESC_AR'])}', '{str(row['SOURCE_GOVERNORATE_DESC_EN'])}', '{str(row['DWH_TYPE'])}', '{str(row['SOURCE'])}', 'I', current_timestamp);\n"
        f.write(script)
        print(script)

    call_procedure_query = f"CALL {cf['db_prefix']}P_PP.SRCI_LOADING('UNIFIED_GOV', 'UNIFIED_GOV', NULL, NULL, NULL, X, Y, Z);\n"
    f.write(call_procedure_query)
    print(call_procedure_query)



# # # print(fn.WriteFile("C:/Users/oh255011/Documents/Teradata/SMX/UNIFIED", fn.get_file_name(__file__), "sql"))
# df = read_excel("C:/Users/oh255011/Documents/Teradata/SMX/UNIFIED/UNIFIED.xlsx", "Unified Gov", [['SOURCE', ['AZHAR']]])

# config_file_values = fn.get_config_file_values("C:/Users/oh255011/Documents/Teradata/new_udi/read_smx_sheet/config.txt")
# d000(config_file_values, "C:/Users/oh255011/Documents/Teradata/SMX/UNIFIED", "AZHAR", df)

print(sys.platform)

# {'smx_path': '/Users/oh255011/Documents/Teradata/SMX', 'home_output_folder': '/Users/oh255011/Documents/Teradata/SMX/UDI_OUTPUT', 'templates_folder_path': '/Users/oh255011/Documents/Teradata/new_udi/Templates_format', 'source_names': ['CSO'], 'online_source_t': 'stg_online', 'offline_source_t': 
# 'stg_layer', 'staging_view_db': 'GDEV1V_DEV_STG_ONLINE', 'etl_process_table': 'ETL_PROCESS', 'SOURCE_TABLES_LKP_table': 'SOURCE_TABLES_LKP', 'SOURCE_NAME_LKP_table': 'SOURCE_NAME_LKP', 'history_tbl': 'HISTORY', 'db_prefix': 'GDEV1', 'scripts_flag': 'All', 'gcfr_ctl_Id': 1, 'gcfr_stream_key': 1, 
# 'gcfr_system_name': 'Economic', 'gcfr_stream_name': 'Economic stream', 'gcfr_bkey_process_type': 21, 'gcfr_snapshot_txf_process_type': 24, 'gcfr_insert_txf_process_type': 25, 'gcfr_others_txf_process_type': 29, 'read_sheets_parallel': 1, 'Data_mover_flag': 0, 'output_path': '/Users/oh255011/Documents/Teradata/SMX/UDI_OUTPUT/2023_FEB_06_12_38_49', 'T_STG': 'GDEV1T_STG', 't_WRK': 'GDEV1T_WRK', 'v_stg': 'GDEV1V_STG', 'v_base': 'GDEV1V_BASE', 'INPUT_VIEW_DB': 'GDEV1V_INP', 'MACRO_DB': 'GDEV1M_GCFR', 'UT_DB': 'GDEV1P_UT', 'UTLFW_v': 'GDEV1V_UTLFW', 'UTLFW_t': 'GDEV1T_UTLFW', 'TMP_DB': 'GDEV1T_TMP', 'APPLY_DB': 'GDEV1P_PP', 'base_DB': 'GDEV1T_BASE', 'SI_DB': 'GDEV1T_SRCI', 'SI_VIEW': 'GDEV1V_SRCI', 'GCFR_t': 'GDEV1t_GCFR', 'GCFR_V': 'GDEV1V_GCFR', 'keycol_override_base': 'GDEV1T_GCFR.GCFR_TRANSFORM_KEYCOL_OVERRIDE', 'M_GCFR': 'GDEV1M_GCFR', 'P_UT': 'GDEV1P_UT', 'core_table': 'GDEV1T_BASE', 'core_view': 'GDEV1V_BASE', 'online_source_v': 'GDEV1V_stg_online', 'offline_source_v': 'GDEV1V_stg_layer'}


# print(config_file_values['db_prefix'])