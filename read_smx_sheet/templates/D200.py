from read_smx_sheet.app_Lib import functions as funcs
from read_smx_sheet.Logging_Decorator import Logging_decorator


@Logging_decorator
def d200(cf, source_output_path, STG_tables, Loading_Type):
    file_name = funcs.get_file_name(__file__)
    f = funcs.WriteFile(source_output_path, file_name, "sql")
    MODIFICATION_TYPE_found = 0
    Data_mover_flag = cf.Data_mover_flag
    INS_DTTM = ",INS_DTTM  TIMESTAMP(6) NOT NULL \n"
    stg_tables_df = funcs.get_stg_tables(STG_tables, None)
    for stg_tables_df_index, stg_tables_df_row in stg_tables_df.iterrows():
        Table_name = stg_tables_df_row['Table name']

        Fallback = ', Fallback' if stg_tables_df_row['Fallback'].upper() == 'Y' else ''

        create_stg_table = "create multiset table " + cf.T_STG + "." + Table_name + Fallback + "\n" + "(\n"
        create_wrk_table = "create multiset table " + cf.t_WRK + "." + Table_name + Fallback + "\n" + "(\n"

        STG_table_columns = funcs.get_stg_table_columns(STG_tables, None, Table_name, False)
        pi_columns = ""
        MODIFICATION_TYPE_found = funcs.table_has_modification_type_column(STG_tables, Table_name)
        for STG_table_columns_index, STG_table_columns_row in STG_table_columns.iterrows():
            Column_name = STG_table_columns_row['Column name']
            comma = ',' if STG_table_columns_index > 0 else ' '
            comma_Column_name = comma + Column_name

            Data_type = str(STG_table_columns_row['Data type'])
            character_set = " CHARACTER SET UNICODE NOT CASESPECIFIC " if "CHAR" in Data_type.upper() or "VARCHAR" in Data_type.upper() else ""
            not_null = " not null " if STG_table_columns_row['Mandatory'].upper() == 'Y' or STG_table_columns_row['PK'].upper() == 'Y' else " "

            create_stg_table = create_stg_table + comma_Column_name + " " + Data_type + character_set + not_null + "\n"
            create_wrk_table = create_wrk_table + comma_Column_name + " " + Data_type + character_set + "\n"

            if STG_table_columns_row['PK'].upper() == 'Y':
                pi_columns = pi_columns + ',' + Column_name if pi_columns != "" else Column_name

        wrk_extra_columns = ",REJECTED INTEGER\n" + ",BATCH_LOADED INTEGER\n" + ",NEW_ROW INTEGER\n"

        if pi_columns == "":
            pi_columns = "SEQ_NO"
            seq_column = ",SEQ_NO DECIMAL(10,0) NOT NULL GENERATED ALWAYS AS IDENTITY\n\t (START WITH 1 INCREMENT BY 1  MINVALUE 1  MAXVALUE 2147483647  NO CYCLE)\n"
        else:
            seq_column = ""

        Primary_Index = ") UNIQUE PRIMARY INDEX (" + pi_columns + ")\n"

        if MODIFICATION_TYPE_found == 0:
            MODIFICATION_TYPE = ",MODIFICATION_TYPE char(1) CHARACTER SET UNICODE NOT CASESPECIFIC  not null\n"
        else:
            MODIFICATION_TYPE = ""

        if Data_mover_flag == 1:
            Run_date_column = ", RUN_DATE TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP\n"
            partition_statement = "PARTITION BY RANGE_N(RUN_DATE BETWEEN TIMESTAMP '2020-03-03 00:00:00.000000+00:00' AND TIMESTAMP '2100-03-03 00:00:00.000000+00:00' EACH INTERVAL'1'DAY)\n"
        else:
            Run_date_column=""
            partition_statement=""

        create_stg_table = create_stg_table + MODIFICATION_TYPE + INS_DTTM + seq_column + Run_date_column + Primary_Index + partition_statement
        create_wrk_table = create_wrk_table + MODIFICATION_TYPE + INS_DTTM + wrk_extra_columns + seq_column + Run_date_column + Primary_Index + partition_statement

        create_stg_table = create_stg_table + ";\n\n"
        create_wrk_table = create_wrk_table + ";\n\n"

        f.write(create_stg_table)
        f.write(create_wrk_table)
    f.close()
