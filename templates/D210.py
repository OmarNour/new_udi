import pandas as pd
import app_Lib.manage_directories as md
import parameters.parameters as pm
import app_Lib.functions as funcs


def d210(source_output_path, source_name, STG_tables):
    file_name = funcs.get_file_name(__file__)
    f = open(source_output_path + "/" + file_name + ".sql", "w+")

    INS_DTTM = ",CURRENT_TIMESTAMP AS INS_DTTM \n"
    stg_tables_df = funcs.get_stg_tables(STG_tables, source_name)
    for stg_tables_df_index, stg_tables_df_row in stg_tables_df.iterrows():
        Table_name = stg_tables_df_row['Table name']
        # Table_name = Table_name + '_' if funcs.is_Reserved_word(Supplements, 'TERADATA', Table_name) else Table_name

        create_stg_view = "REPLACE VIEW " + pm.gdev1v_stg + "." + Table_name + " AS LOCK ROW FOR ACCESS \n"
        create_stg_view = create_stg_view + "SELECT\n"

        STG_table_columns = funcs.get_stg_table_columns(STG_tables, source_name, Table_name)

        for STG_table_columns_index, STG_table_columns_row in STG_table_columns.iterrows():
            # print(STG_tables_index)
            Column_name = STG_table_columns_row['Column name']
            # Column_name = Column_name + '_' if funcs.is_Reserved_word(Supplements, 'TERADATA', Column_name) else Column_name
            comma = ',' if STG_table_columns_index > 0 else ' '
            comma_Column_name = comma + Column_name

            create_stg_view = create_stg_view + comma_Column_name + "\n"

        create_stg_view = create_stg_view + INS_DTTM
        create_stg_view = create_stg_view + "from " + pm.gdev1t_stg + "." + Table_name + ";\n\n"
        f.write(create_stg_view)

    f.close()
