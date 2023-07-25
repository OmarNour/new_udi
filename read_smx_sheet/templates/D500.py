# import os
# import sys

# sys.path.append(os.getcwd())
# import numpy as np
# import pandas as pd
# # import pyarrow.parquet as pq
# # import pyarrow as pa
# # from pyarrow.formatting import *
# import dask.dataframe as dd
# from read_smx_sheet.app_Lib import manage_directories as md
# from read_smx_sheet.parameters import parameters as pm
# import datetime as dt
# import psutil
# from read_smx_sheet.app_Lib import functions as fn
from app_Lib import functions as funcs
from app_Lib import TransformDDL
from Logging_Decorator import Logging_decorator

@Logging_decorator
def d500(cf, source_output_path, source_name, df):

    file_name = funcs.get_file_name(__file__)
    f = funcs.WriteFile(source_output_path, file_name, "sql")
    if df.empty:
        f.write(f"--no UNIFIED_GOV to generate for {source_name} source")
    else:
        target_table  = cf.db_prefix + "T_STG.UNIFIED_GOV" 
        delete_query = f"DELETE FROM {target_table} WHERE SOURCE = '{source_name}';\n\n"
        f.write(f"--Delete unified governorates records for {source_name}\n")
        f.write(delete_query)
        insert_prefix = f"INSERT INTO {target_table}(SOURCE_TABLE_NAME, DWH_GOVERNORATE_ID, GOVERNORATE_DESCRIPTION_AR, GOVERNORATE_DESCRIPTION_EN, SOURCE_GOVERNORATE_ID, SOURCE_GOVERNORATE_DESC_AR, SOURCE_GOVERNORATE_DESC_EN, DWH_TYPE, SOURCE, MODIFICATION_TYPE, INS_DTTM) VALUES("
        f.write(f"--Insert into unified governorates for {source_name}\n")
        for index, row in df.iterrows():
            script = insert_prefix + f"'{str(row['SOURCE_TABLE_NAME'])}', '{str(row['DWH_GOVERNORATE_ID'])}', '{str(row['GOVERNORATE_DESCRIPTION_AR'])}', '{str(row['GOVERNORATE_DESCRIPTION_EN'])}', '{str(row['SOURCE_GOVERNORATE_ID'])}', '{str(row['SOURCE_GOVERNORATE_DESC_AR'])}', '{str(row['SOURCE_GOVERNORATE_DESC_EN'])}', '{str(row['DWH_TYPE'])}', '{str(row['SOURCE'])}', 'I', current_timestamp);\n"
            f.write(script)

        f.write("\n")
        f.write("--Call SRCI_LOADING SP to move data from STG layer to SRCI layer\n")
        call_procedure_query = f"CALL {cf.db_prefix}P_PP.SRCI_LOADING('UNIFIED_GOV', 'UNIFIED_GOV', NULL, NULL, NULL, X, Y, Z);\n"
        f.write(call_procedure_query)
        