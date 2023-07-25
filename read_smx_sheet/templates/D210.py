from app_Lib import functions as funcs
from Logging_Decorator import Logging_decorator


@Logging_decorator
def d210(cf, source_output_path, STG_tables, Loading_Type):
    file_name = funcs.get_file_name(__file__)
    f = funcs.WriteFile(source_output_path, file_name, "sql")
    INS_DTTM = ",CURRENT_TIMESTAMP AS INS_DTTM \n"
    MODIFICATION_TYPE_found = 0
    stg_tables_df = funcs.get_stg_tables(STG_tables, None)
    for stg_tables_df_index, stg_tables_df_row in stg_tables_df.iterrows():
        Table_name = stg_tables_df_row['Table name']

        create_stg_view = "CREATE VIEW " + cf.v_stg + "." + Table_name + " AS LOCK ROW FOR ACCESS \n"
        create_stg_view = create_stg_view + "SELECT\n"

        STG_table_columns = funcs.get_stg_table_columns(STG_tables, None, Table_name)

        for STG_table_columns_index, STG_table_columns_row in STG_table_columns.iterrows():
            Column_name = STG_table_columns_row['Column name']
            if Column_name == "MODIFICATION_TYPE":
                MODIFICATION_TYPE_found = 1

            comma = ',' if STG_table_columns_index > 0 else ' '
            comma_Column_name = comma + Column_name

            create_stg_view = create_stg_view + comma_Column_name + "\n"

        if MODIFICATION_TYPE_found == 0:
            MODIFICATION_TYPE = ",MODIFICATION_TYPE\n"
        else:
            MODIFICATION_TYPE = ""

        create_stg_view = create_stg_view + MODIFICATION_TYPE + INS_DTTM
        create_stg_view = create_stg_view + "from " + cf.T_STG + "." + Table_name + ";\n\n"
        f.write(create_stg_view)
    f.close()
