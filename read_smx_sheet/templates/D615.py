from app_Lib import functions as funcs
from Logging_Decorator import Logging_decorator


@Logging_decorator
def d615(cf, source_output_path, Core_tables,STG_tables):
    
    # initialize variables
    file_name = funcs.get_file_name(__file__)
    f = funcs.WriteFile(source_output_path, file_name, "sql")
    core_tables_df = funcs.get_core_tables(Core_tables)
    staging_tables_df = funcs.get_stg_tables(STG_tables)
    insert_statement = f"INSERT INTO {cf.GCFR_t}.GCFR_Transform_KeyCol(OUT_DB_NAME, OUT_OBJECT_NAME, KEY_COLUMN, UPDATE_DATE, UPDATE_USER, UPDATE_TS)\n"
    with_statement = "WITH\n"
    key_tables_core = ""
    all_keys_core = ""
    key_tables_stg = ""
    all_keys_stg = ""
    

    # core scripts
    counter = 0
    for core_tables_df_index, core_tables_df_row in core_tables_df.iterrows():
        core_table_name = core_tables_df_row['Table name']
        core_table_columns = funcs.get_core_table_columns(Core_tables, core_table_name )
        for core_table_columns_index, core_table_columns_row in core_table_columns.iterrows():
            if core_table_columns_row['PK'].upper() == 'Y':
                Column_name = core_table_columns_row['Column name']
                

                key_tables_core += f"NEW_KEY{counter}\nAS(SELECT '{cf.base_DB}' OUT_DB_NAME\n,'{core_table_name}' (VARCHAR(130)) OUT_OBJECT_NAME\n,'{Column_name}' (VARCHAR(130)) KEY_COLUMN, CURRENT_DATE UPDATE_DATE, CURRENT_USER UPDATE_USER, CURRENT_TIMESTAMP UPDATE_TS\n)\n,"
                all_keys_core += f"SELECT * FROM NEW_KEY{counter} K{counter} WHERE NOT EXISTS(SELECT * FROM {cf.GCFR_t}.GCFR_TRANSFORM_KEYCOL K WHERE K.OUT_DB_NAME=K{counter}.OUT_DB_NAME AND K.OUT_OBJECT_NAME=K{counter}.OUT_OBJECT_NAME) \nUNION\n"
                counter+=1
                
    counter = 0
    for stage_table_index, stage_table_row in staging_tables_df.iterrows():
        stage_table = stage_table_row['Table name']
        stage_columns = funcs.get_Staging_Key_Columns(STG_tables,stage_table)

        for column in stage_columns:
            

            key_tables_stg += f"NEW_KEY{counter}\nAS(SELECT '{cf.T_STG}' OUT_DB_NAME\n,'{stage_table}' (VARCHAR(130)) OUT_OBJECT_NAME\n,'{column}' (VARCHAR(130)) KEY_COLUMN, CURRENT_DATE UPDATE_DATE, CURRENT_USER UPDATE_USER, CURRENT_TIMESTAMP UPDATE_TS\n)\n,"
            all_keys_stg += f"SELECT * FROM NEW_KEY{counter} K{counter} WHERE NOT EXISTS(SELECT * FROM {cf.GCFR_t}.GCFR_TRANSFORM_KEYCOL K WHERE K.OUT_DB_NAME=K{counter}.OUT_DB_NAME AND K.OUT_OBJECT_NAME=K{counter}.OUT_OBJECT_NAME) \nUNION\n"
            counter+=1

            # stage_script += "INSERT INTO " + cf.GCFR_t + ".GCFR_Transform_KeyCol \n"
            # stage_script += "SELECT '" + cf.T_STG + "' , '" + stage_table + "' , '" + column + "' , " + "CURRENT_DATE , CURRENT_USER , CURRENT_TIMESTAMP    \n"
            # stage_script += "WHERE  NOT EXISTS (SELECT   OUT_OBJECT_NAME  FROM " + cf.GCFR_t + ".GCFR_Transform_KeyCol  ); \n \n \n"

    # remove the last union in all_keys
    all_keys_core = ' '.join(all_keys_core.split(' ')[:-1])
    all_keys_stg = ' '.join(all_keys_stg.split(' ')[:-1])


    # build scripts
    core_script = insert_statement + with_statement + key_tables_core + "ALL_KEYS AS\n(" + all_keys_core + ")\n\nSELECT OUT_DB_NAME,OUT_OBJECT_NAME, KEY_COLUMN, UPDATE_DATE, UPDATE_USER, UPDATE_TS FROM ALL_KEYS;\n\n" 
    stage_script = insert_statement + with_statement + key_tables_stg + "ALL_KEYS AS\n(" + all_keys_stg + ")\n\nSELECT OUT_DB_NAME,OUT_OBJECT_NAME, KEY_COLUMN, UPDATE_DATE, UPDATE_USER, UPDATE_TS FROM ALL_KEYS;" 
    
    # write to file
    f.write(core_script)
    f.write(stage_script)
    
    # close file
    f.close()
