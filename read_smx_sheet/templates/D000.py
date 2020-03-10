from read_smx_sheet.app_Lib import functions as funcs
from read_smx_sheet.Logging_Decorator import Logging_decorator


@Logging_decorator
def d000(cf, source_output_path, source_name, Table_mapping, STG_tables, BKEY):
    file_name = funcs.get_file_name(__file__)
    f = funcs.WriteFile(source_output_path, file_name, "sql")
    f.write("delete from " + cf.GCFR_t + "." + cf.etl_process_table + " where SOURCE_NAME = '" + source_name + "';\n\n")
    for table_maping_index, table_maping_row in Table_mapping.iterrows():
        prcess_type = "TXF"
        layer = str(table_maping_row['Layer'])
        matching_flag = funcs.xstr(table_maping_row['Matching Included'])
        process_name = prcess_type + "_" + layer + "_" + str(table_maping_row['Mapping name'])
        target_table = str(table_maping_row['Target table name'])
        Historization_algorithm = str(table_maping_row['Historization algorithm'])
        active_flag = "1"
        f.write(
            "insert into " + cf.GCFR_t + "." + cf.etl_process_table + "(SOURCE_NAME, PROCESS_TYPE, PROCESS_NAME, BASE_TABLE, APPLY_TYPE, RECORD_ID, active)\n")
        f.write(
            "VALUES ('" + source_name + "', '" + prcess_type + "', '" + process_name + "', '" + target_table + "', '" + Historization_algorithm + "', NULL," + active_flag + ")" + ";\n")
        f.write("\n")

        if str(matching_flag) == "1":
            process_name = prcess_type + "_" + layer + "_" + str(table_maping_row['Mapping name']) + "_Matching"
            active_flag = "0"
            f.write(
                "insert into " + cf.GCFR_t + "." + cf.etl_process_table + "(SOURCE_NAME, PROCESS_TYPE, PROCESS_NAME, BASE_TABLE, APPLY_TYPE, RECORD_ID, active)\n")
            f.write(
                "VALUES ('" + source_name + "', '" + prcess_type + "', '" + process_name + "', '" + target_table + "', '" + Historization_algorithm + "', NULL," + active_flag + ")" + ";\n")
            f.write("\n")

    for STG_tables_index, STG_tables_row in STG_tables.loc[STG_tables['Key set name'] != ""].iterrows():
        generation_flag = STG_tables_row['Bkey generation flag']
        Key_set_name = STG_tables_row['Key set name']
        Key_domain_name = STG_tables_row['Key domain name']
        Table_name = STG_tables_row['Table name']
        Column_name = STG_tables_row['Column name']
        prcess_type = "BKEY"
        target_table = ""
        Historization_algorithm = "INSERT"


        for BKEY_index, BKEY_row in BKEY.loc[(BKEY['Key set name'] == Key_set_name) & (BKEY['Key domain name'] == Key_domain_name) & (generation_flag != 0)].iterrows():
            Key_set_id = int(BKEY_row['Key set ID'])
            Key_domain_ID = int(BKEY_row['Key domain ID'])

            process_name = "BK_" + str(Key_set_id) + "_" + Table_name + "_" + Column_name + "_" + str(Key_domain_ID)
            f.write(
                "insert into " + cf.GCFR_t + "." + cf.etl_process_table + "(SOURCE_NAME, PROCESS_TYPE, PROCESS_NAME, BASE_TABLE, APPLY_TYPE, RECORD_ID)\n")
            f.write(
                "VALUES ('" + source_name + "', '" + prcess_type + "', '" + process_name + "', '" + target_table + "', '" + Historization_algorithm + "', NULL)" + ";\n")
            f.write("\n")
    f.close()