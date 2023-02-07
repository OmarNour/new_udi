from read_smx_sheet.app_Lib import functions as funcs
from read_smx_sheet.app_Lib import TransformDDL
from read_smx_sheet.Logging_Decorator import Logging_decorator
@Logging_decorator
def d502(cf, source_output_path, source_name, df):

    file_name = funcs.get_file_name(__file__)
    f = funcs.WriteFile(source_output_path, file_name, "sql")

    target_table  = cf.db_prefix + "T_STG.UNIFIED_CITY" 

    delete_query = f"DELETE FROM {target_table} WHERE SOURCE = '{source_name}';\n"
    f.write(delete_query)
    insert_prefix = f"INSERT INTO {target_table}(SOURCE_TABLE_NAME, DWH_CITY_ID, SOURCE_CITY_ID, SOURCE_GOVERNORATE_ID, CITY_DESCRIPTION, CSO_GOVERNORATE_DESCRIPTION, CSO_GOVERNORATE_ID, SOURCE, MODIFICATION_TYPE, INS_DTTM) VALUES("
    for index, row in df.iterrows():
        script = insert_prefix + f"'{str(row['SOURCE_TABLE_NAME'])}', '{str(row['DWH_CITY_ID'])}', '{str(row['SOURCE_CITY_ID'])}', '{str(row['SOURCE_GOVERNORATE_ID'])}', '{str(row['CITY_DESCRIPTION'])}', '{str(row['CSO_GOVERNORATE_DESCRIPTION'])}', '{str(row['CSO_GOVERNORATE_ID'])}', '{str(row['SOURCE'])}', 'I', current_timestamp);\n"
        f.write(script)

    call_procedure_query = f"CALL {cf.db_prefix}P_PP.SRCI_LOADING('UNIFIED_CITY', 'UNIFIED_CITY', NULL, NULL, NULL, X, Y, Z);\n"
    f.write(call_procedure_query)