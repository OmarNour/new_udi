from read_smx_sheet.app_Lib import functions as funcs
from read_smx_sheet.app_Lib import TransformDDL
from read_smx_sheet.Logging_Decorator import Logging_decorator

@Logging_decorator
def d503(cf, source_output_path, source_name, df):

    file_name = funcs.get_file_name(__file__)
    f = funcs.WriteFile(source_output_path, file_name, "sql")

    if df.empty:
        f.write(f"--no UNIFIED_DISTRICT to generate for {source_name} source")
    else:
        target_table  = cf.db_prefix + "T_STG.UNIFIED_DISTRICT"
        delete_query = f"DELETE FROM {target_table} WHERE SOURCE = '{source_name}';\n\n"
        f.write(f"--Delete UNIFIED_DISTRICT records for {source_name}\n")
        f.write(delete_query)
        insert_prefix = f"INSERT INTO {target_table}(SOURCE_TABLE_NAME, DWH_DISTRICT_ID, DISTRICT_DESCRIPTION, DWH_GOVERNORATE_ID, DWH_POLICE_STATION_ID, CSO_GOVERNORATE_DESCRIPTION, POLICE_STATION_DESCRIPTION, SOURCE_DISTRICT_ID, SOURCE_DISTRICT_DESCRIPTION, SOURCE_GOVERNORATE_ID, SOURCE_POLICE_STATION_ID, SOURCE, MODIFICATION_TYPE, INS_DTTM) VALUES("
        f.write(f"--Insert into UNIFIED_DISTRICT for {source_name}\n")
        for index, row in df.iterrows():
            script = insert_prefix + f"'{str(row['SOURCE_TABLE_NAME'])}', '{str(row['DWH_DISTRICT_ID'])}', '{str(row['DISTRICT_DESCRIPTION'])}', '{str(row['DWH_GOVERNORATE_ID'])}', '{str(row['DWH_POLICE_STATION_ID'])}', '{str(row['CSO_GOVERNORATE_DESCRIPTION'])}', '{str(row['POLICE_STATION_DESCRIPTION'])}', '{str(row['SOURCE_DISTRICT_ID'])}', '{str(row['SOURCE_DISTRICT_DESCRIPTION'])}', '{str(row['SOURCE_GOVERNORATE_ID'])}', '{str(row['SOURCE_POLICE_STATION_ID'])}', '{str(row['SOURCE'])}', 'I', current_timestamp);\n"
            f.write(script)

        f.write("\n")
        f.write("--Call SRCI_LOADING SP to move data from STG layer to SRCI layer\n")
        call_procedure_query = f"CALL {cf.db_prefix}P_PP.SRCI_LOADING('UNIFIED_DISTRICT', 'UNIFIED_DISTRICT', NULL, NULL, NULL, X, Y, Z);\n"
        f.write(call_procedure_query)