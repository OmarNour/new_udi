from read_smx_sheet.app_Lib import functions as funcs
from read_smx_sheet.app_Lib import TransformDDL
from read_smx_sheet.Logging_Decorator import Logging_decorator
@Logging_decorator
def d501(cf, source_output_path, source_name, df):

    file_name = funcs.get_file_name(__file__)
    f = funcs.WriteFile(source_output_path, file_name, "sql")

    target_table  = cf.db_prefix + "T_STG.UNIFIED_COUNTRY" 

    delete_query = f"DELETE FROM {target_table} WHERE SOURCE = '{source_name}';\n"
    f.write(delete_query)
    insert_prefix = f"INSERT INTO {target_table}(SOURCE_TABLE_NAME, DWH_COUNTRY_ID, COUNTRY_DESCRIPTION, SOURCE_COUNTRY_ID, SOURCE_COUNTRY_DESCRIPTION, COUNTRY_ENGLISH_DESCRIPTION,  DWH_TYPE, SOURCE, MODIFICATION_TYPE, INS_DTTM) VALUES("
    for index, row in df.iterrows():
        script = insert_prefix + f"'{str(row['SOURCE_TABLE_NAME'])}', '{str(row['DWH_COUNTRY_ID'])}', '{str(row['COUNTRY_DESCRIPTION'])}', '{str(row['SOURCE_COUNTRY_ID'])}', '{str(row['SOURCE_COUNTRY_DESCRIPTION'])}', '{str(row['COUNTRY_ENGLISH_DESCRIPTION'])}', '{str(row['DWH_TYPE'])}', '{str(row['SOURCE'])}', 'I', current_timestamp);\n"
        f.write(script)

    call_procedure_query = f"CALL {cf.db_prefix}P_PP.SRCI_LOADING('UNIFIED_COUNTRY', 'UNIFIED_COUNTRY', NULL, NULL, NULL, X, Y, Z);\n"
    f.write(call_procedure_query)