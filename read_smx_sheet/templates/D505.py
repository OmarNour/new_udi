from read_smx_sheet.app_Lib import functions as funcs
from read_smx_sheet.app_Lib import TransformDDL
from read_smx_sheet.Logging_Decorator import Logging_decorator

@Logging_decorator
def d505(cf, source_output_path, source_name, df):

    file_name = funcs.get_file_name(__file__)
    f = funcs.WriteFile(source_output_path, file_name, "sql")

    if df.empty:
        f.write(f"--no UNIFIED_POLICE_STATION to generate for {source_name} source")
    else:
        target_table  = cf.db_prefix + "T_STG.UNIFIED_POLICE_STATION" 
        delete_query = f"DELETE FROM {target_table} WHERE SOURCE = '{source_name}';\n\n"
        f.write(f"--Delete UNIFIED_POLICE_STATION records for {source_name}\n")
        f.write(delete_query)
        insert_prefix = f"INSERT INTO {target_table}(SOURCE_TABLE_NAME, DWH_POLICE_STATION_ID, POLICE_STATION_DESCRIPTION, DWH_GOVERNORATE_ID, CSO_GOVERNORATE_DESCRIPTION, SOURCE_POLICE_STATION_ID, SOURCE_POLICE_STATION_DESCRIPTION, SOURCE, MODIFICATION_TYPE, INS_DTTM) VALUES("
        f.write(f"--Insert into UNIFIED_POLICE_STATION for {source_name}\n")
        for index, row in df.iterrows():
            script = insert_prefix + f"'{str(row['SOURCE_TABLE_NAME'])}', '{str(row['DWH_POLICE_STATION_ID'])}', '{str(row['POLICE_STATION_DESCRIPTION'])}', '{str(row['DWH_GOVERNORATE_ID'])}', '{str(row['CSO_GOVERNORATE_DESCRIPTION'])}', '{str(row['SOURCE_POLICE_STATION_ID'])}', '{str(row['SOURCE_POLICE_STATION_DESCRIPTION'])}', '{str(row['SOURCE'])}', 'I', current_timestamp);\n"
            f.write(script)
        
        f.write("\n")
        f.write("--Call SRCI_LOADING SP to move data from STG layer to SRCI layer\n")
        call_procedure_query = f"CALL {cf.db_prefix}P_PP.SRCI_LOADING('UNIFIED_POLICE_STATION', 'UNIFIED_POLICE_STATION', NULL, NULL, NULL, X, Y, Z);\n"
        f.write(call_procedure_query)