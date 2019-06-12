from read_smx_sheet.app_Lib import functions as funcs
import traceback


def d110(cf, source_output_path, STG_tables, Loading_Type):
    file_name = funcs.get_file_name(__file__)
    f = funcs.WriteFile(source_output_path, file_name, "sql")

    source_t = cf.online_source_t if Loading_Type == "ONLINE" else cf.offline_source_t
    source_v = cf.online_source_v if Loading_Type == "ONLINE" else cf.offline_source_v

    try:
        stg_tables_df = funcs.get_stg_tables(STG_tables, None)
        for stg_tables_df_index, stg_tables_df_row in stg_tables_df.iterrows():
            Table_name = stg_tables_df_row['Table name']

            create_stg_view = "REPLACE VIEW " + source_v + "." + Table_name + " AS LOCK ROW FOR ACCESS \n"
            create_stg_view = create_stg_view + "SELECT\n"

            STG_table_columns = funcs.get_stg_table_columns(STG_tables, None, Table_name)

            for STG_table_columns_index, STG_table_columns_row in STG_table_columns.iterrows():
                Column_name = STG_table_columns_row['Column name in source']
                column_transformation_rule = str(STG_table_columns_row['Column Transformation Rule'])
                Column_name = (column_transformation_rule + " AS " + Column_name) if column_transformation_rule != "" else Column_name

                comma = ',' if STG_table_columns_index > 0 else ' '
                comma_Column_name = comma + Column_name

                create_stg_view = create_stg_view + comma_Column_name + "\n"

            create_stg_view = create_stg_view + "from " + source_t + "." + Table_name + ";\n\n"
            f.write(create_stg_view)
    except:
        funcs.TemplateLogError(cf.output_path, source_output_path, file_name, traceback.format_exc()).log_error()
    f.close()
