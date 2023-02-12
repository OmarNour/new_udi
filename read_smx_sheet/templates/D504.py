from read_smx_sheet.app_Lib import functions as funcs
from read_smx_sheet.app_Lib import TransformDDL
from read_smx_sheet.Logging_Decorator import Logging_decorator
@Logging_decorator
def d504(cf, source_output_path, source_name, df):

    file_name = funcs.get_file_name(__file__)
    f = funcs.WriteFile(source_output_path, file_name, "sql")


    if df.empty:
        f.write(f"--no UNIFIED_CURRENCY to generate for {source_name} source")
    else:
        target_table  = cf.db_prefix + "T_STG.UNIFIED_CURRENCY"
        delete_query = f"DELETE FROM {target_table} WHERE SOURCE = '{source_name}';\n\n"
        f.write(f"--Delete UNIFIED_CURRENCY records for {source_name}\n")
        f.write(delete_query)
        insert_prefix = f"INSERT INTO {target_table}(SOURCE_TABLE_NAME, DWH_CURRENCY_ID, SOURCE_CURRENCY_ID, CURRENCY_DESCRIPTION_EN, CURRENCY_DESCRIPTION_AR, INTERNATIONAL_CURRENCY_CODE, SOURCE, MODIFICATION_TYPE, INS_DTTM) VALUES("
        f.write(f"--Insert into UNIFIED_CURRENCY for {source_name}\n")
        for index, row in df.iterrows():
            script = insert_prefix + f"'{str(row['SOURCE_TABLE_NAME'])}', '{str(row['DWH_CURRENCY_ID'])}', '{str(row['SOURCE_CURRENCY_ID'])}', '{str(row['CURRENCY_DESCRIPTION_EN'])}', '{str(row['CURRENCY_DESCRIPTION_AR'])}', '{str(row['INTERNATIONAL_CURRENCY_CODE'])}', '{str(row['SOURCE'])}', 'I', current_timestamp);\n"
            f.write(script)

        f.write("\n")
        f.write("--Call SRCI_LOADING SP to move data from STG layer to SRCI layer\n")
        call_procedure_query = f"CALL {cf.db_prefix}P_PP.SRCI_LOADING('UNIFIED_CURRENCY', 'UNIFIED_CURRENCY', NULL, NULL, NULL, X, Y, Z);\n"
        f.write(call_procedure_query)