from read_smx_sheet.app_Lib import functions as funcs
import traceback


def d200(cf, source_output_path, STG_tables):
    file_name = funcs.get_file_name(__file__)
    f = funcs.WriteFile(source_output_path, file_name, "sql")
    MODIFICATION_TYPE_found = 0
    INS_DTTM = ",INS_DTTM  TIMESTAMP(6) NOT NULL \n"
    try:
        stg_tables_df = funcs.get_stg_tables(STG_tables, None)
        for stg_tables_df_index, stg_tables_df_row in stg_tables_df.iterrows():
            Table_name = stg_tables_df_row['Table name']

            Fallback = ', Fallback' if stg_tables_df_row['Fallback'].upper() == 'Y' else ''

            create_stg_table = "create multiset table " + cf.T_STG + "." + Table_name + Fallback + "\n" + "(\n"
            create_wrk_table = "create multiset table " + cf.t_WRK + "." + Table_name + Fallback + "\n" + "(\n"

            STG_table_columns = funcs.get_stg_table_columns(STG_tables, None, Table_name, False)
            pi_columns = ""
            for STG_table_columns_index, STG_table_columns_row in STG_table_columns.iterrows():
                Column_name = STG_table_columns_row['Column name']
                if Column_name == "MODIFICATION_TYPE":
                    MODIFICATION_TYPE_found = 1
                comma = ',' if STG_table_columns_index > 0 else ' '
                comma_Column_name = comma + Column_name

                Data_type = str(STG_table_columns_row['Data type'])
                character_set = " CHARACTER SET UNICODE NOT CASESPECIFIC " if "CHAR" in Data_type.upper() or "VARCHAR" in Data_type.upper() else ""
                not_null = " not null " if STG_table_columns_row['Mandatory'].upper() == 'Y' or STG_table_columns_row['PK'].upper() == 'Y' else " "

                create_stg_table = create_stg_table + comma_Column_name + " " + Data_type + character_set + not_null + "\n"
                create_wrk_table = create_wrk_table + comma_Column_name + " " + Data_type + character_set + not_null + "\n"

                if STG_table_columns_row['PK'].upper() == 'Y':
                    pi_columns = pi_columns + ',' + Column_name if pi_columns != "" else Column_name

            wrk_extra_columns = ",REJECTED INTEGER\n" + ",BATCH_LOADED INTEGER\n" + ",NEW_ROW INTEGER\n"

            if pi_columns == "":
                pi_columns = "SEQ_NO"
                seq_column = ",SEQ_NO DECIMAL(10,0) NOT NULL GENERATED ALWAYS AS IDENTITY\n\t (START WITH 1 INCREMENT BY 1  MINVALUE 1  MAXVALUE 2147483647  NO CYCLE)\n"
            else:
                seq_column = ""

            Primary_Index = ")Primary Index (" + pi_columns + ")"

            if MODIFICATION_TYPE_found == 0:
                MODIFICATION_TYPE = ",MODIFICATION_TYPE char(1) CHARACTER SET UNICODE NOT CASESPECIFIC  not null\n"
            else:
                MODIFICATION_TYPE = ""
            create_stg_table = create_stg_table + MODIFICATION_TYPE + INS_DTTM + seq_column + Primary_Index
            create_wrk_table = create_wrk_table + MODIFICATION_TYPE + INS_DTTM + wrk_extra_columns + seq_column + Primary_Index

            create_stg_table = create_stg_table + ";\n\n"
            create_wrk_table = create_wrk_table + ";\n\n"

            f.write(create_stg_table)
            f.write(create_wrk_table)
    except:
        funcs.TemplateLogError(cf.output_path, source_output_path, file_name, traceback.format_exc()).log_error()

    f.close()
