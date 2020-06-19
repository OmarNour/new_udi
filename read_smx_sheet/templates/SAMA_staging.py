from read_smx_sheet.app_Lib import functions as funcs
from read_smx_sheet.Logging_Decorator import Logging_decorator


@Logging_decorator
def stg_tables_DDL(cf, source_output_path, STG_tables, Data_types):
    file_name = funcs.get_file_name(__file__)
    f = funcs.WriteFile(source_output_path, file_name, "sql")
    stg_tables_df = funcs.get_sama_stg_tables(STG_tables, None)
    table_technical_columns = ',batch_id INTEGER' + "\n" + ',filename VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC' + "\n" + ',dump_dttm TIMESTAMP(3)' + "\n"
    dup_table_technical_columns = ',b_id INTEGER TITLE ' + "'B_Id'" + "\n" + ',file_name VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC' + "\n" + ',insrt_dttm TIMESTAMP(6) TITLE ' + "'Insert_Dttm'"  + "\n" + 'updt_dttm TIMESTAMP(6) TITLE ' + "'Update_Dttm'" + "\n"

    for stg_tables_df_index, stg_tables_df_row in stg_tables_df.iterrows():
        Table_name = stg_tables_df_row['Table_Name']
        create_stg_table_oi = "create multiset table " + cf.oi_prefix+stg_tables_df_row['Schema_Name'] + "." + Table_name + "\n" + "(\n"
        create_stg_table_stg = "create multiset table " + cf.stg_prefix+stg_tables_df_row['Schema_Name'] + "." + Table_name + "\n" + "(\n"
        create_stg_table_stg_dup = "create multiset table " + cf.stg_prefix+stg_tables_df_row['Schema_Name'] + "." + Table_name + cf.duplicate_table_suffix + "\n" + "(\n"

        STG_table_columns = funcs.get_sama_stg_table_columns(STG_tables, Table_name)
        pi_columns = ""

        for STG_table_columns_index, STG_table_columns_row in STG_table_columns.iterrows():
            Column_name = STG_table_columns_row['Column_Name']
            comma = ',' if STG_table_columns_index > 0 else ' '
            comma_Column_name = comma + Column_name

            source_data_type = STG_table_columns_row['Data_Type']
            if str(STG_table_columns_row['Data_Length']) != '' and str(STG_table_columns_row['Data_Precision']) == '':
                source_data_type = source_data_type+"("+str(STG_table_columns_row['Data_Length'])+")"
                Data_type = source_data_type
            elif str(STG_table_columns_row['Data_Length']) != '' and str(STG_table_columns_row['Data_Precision']) != '':
                source_data_type = source_data_type + "(" + str(STG_table_columns_row['Data_Length']) + ',' + str(STG_table_columns_row['Data_Precision']) + ")"
                Data_type = source_data_type.replace("NUMBER", "DECIMAL")

            for data_type_index, data_type_row in Data_types.iterrows():
                if data_type_row['Source Data Type'] == source_data_type:
                    Data_type = str(data_type_row['Teradata Data Type'])

            if source_data_type == 'VARCHAR2':
                if STG_table_columns_row['Data_Type'] == 'Y':
                    character_set = " CHARACTER SET UNICODE NOT CASESPECIFIC "
                else:
                    character_set = " CHARACTER SET LATIN NOT CASESPECIFIC "
            else:
                character_set = ""

            if STG_table_columns_row['Primary_Key_Flag'].upper() == 'Y' or STG_table_columns_row['Nullability_Flag'].upper() == 'N':
                not_null = " not null "
            else:
                not_null = ""

            create_stg_table_oi = create_stg_table_oi + comma_Column_name + " " + Data_type + character_set + not_null + "\n"
            create_stg_table_stg = create_stg_table_stg + comma_Column_name + " " + Data_type + character_set + not_null + "\n"
            create_stg_table_stg_dup = create_stg_table_stg_dup + comma_Column_name + " " + Data_type + character_set + not_null + "\n"

            create_stg_table_oi = create_stg_table_oi + comma_Column_name + " " + Data_type + character_set + not_null + "\n"
            create_stg_table_stg = create_stg_table_stg + comma_Column_name + " " + Data_type + character_set + not_null + "\n"
            create_stg_table_stg_dup = create_stg_table_stg_dup + comma_Column_name + " " + Data_type + character_set + not_null + "\n"

            if STG_table_columns_row['Primary_Key_Flag'].upper() == 'Y':
                pi_columns = pi_columns + ',' + Column_name if pi_columns != "" else Column_name

        if pi_columns != "":
            Primary_Index = ")UNIQUE PRIMARY INDEX PI_" + Table_name + "(" + pi_columns + ")\n"
        else:
            Primary_Index = ")"

        create_stg_table_oi_ddl = create_stg_table_oi + table_technical_columns + Primary_Index
        create_stg_table_oi_ddl = create_stg_table_oi_ddl + ";\n\n"
        f.write(create_stg_table_oi_ddl)

        create_stg_table_stg_ddl = create_stg_table_stg + table_technical_columns + Primary_Index
        create_stg_table_stg_ddl = create_stg_table_stg_ddl + ";\n\n"
        f.write(create_stg_table_stg_ddl)

        create_stg_table_stg_dup_ddl = create_stg_table_stg_dup + dup_table_technical_columns + Primary_Index
        create_stg_table_stg_dup_ddl = create_stg_table_stg_dup_ddl + ";\n\n"
        f.write(create_stg_table_stg_dup_ddl)


    f.close()
