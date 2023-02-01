from read_smx_sheet.app_Lib import functions as funcs
from read_smx_sheet.Logging_Decorator import Logging_decorator


@Logging_decorator
def d110(cf, source_output_path, stg_Table_mapping, STG_tables, Loading_Type):
    file_name = funcs.get_file_name(__file__)
    f = funcs.WriteFile(source_output_path, file_name, "sql")
    source_t = cf.online_source_t if Loading_Type == "ONLINE" else cf.offline_source_t
    if cf.staging_view_db == '':
        source_v = cf.online_source_v if Loading_Type == "ONLINE" else cf.offline_source_v
    else:
        source_v = cf.staging_view_db
    stg_tables_df = funcs.get_stg_tables(STG_tables, None)
    for stg_tables_df_index, stg_tables_df_row in stg_tables_df.iterrows():
        Table_name = stg_tables_df_row['Table name']
        try:
            where_clause = "\nWhere "
            where_clause = where_clause + str(stg_Table_mapping.loc[(stg_Table_mapping['Mapping name'] == Table_name)]['Filter criterion'].values[0])
            where_clause = where_clause.replace("#SRC#", source_t)
        except:
            where_clause = ""
        for stg_Table_mapping_index,stg_Table_mapping_row in stg_Table_mapping.iterrows():
            if stg_Table_mapping_row['Mapping name'] == Table_name:
                if stg_Table_mapping_row['Source layer'] == 'MATCHING':
                    source_t = cf.db_prefix + 'V_ANALYTICS'

        create_stg_view = "CREATE VIEW " + source_v + "." + Table_name + " AS LOCK ROW FOR ACCESS \n"
        create_stg_view = create_stg_view + "SELECT\n"

        STG_table_columns = funcs.get_stg_table_columns(STG_tables, None, Table_name)

        for STG_table_columns_index, STG_table_columns_row in STG_table_columns.iterrows():
            Column_name_as_src = '"' + STG_table_columns_row['Column name in source'] + '"'
            Column_name = '"' + STG_table_columns_row['Column name'] + '"'
            column_transformation_rule = str(STG_table_columns_row['Column Transformation Rule'])
            # Column_name = (column_transformation_rule + " AS " + Column_name) if column_transformation_rule != "" else  Column_name

            if column_transformation_rule != "":
                Column_name = (column_transformation_rule + " AS " + Column_name)
            else:
                Column_name = ("TRIM(" + Column_name_as_src + ") AS " + Column_name)

            comma = ',' if STG_table_columns_index > 0 else ' '
            comma_Column_name = comma + Column_name

            create_stg_view = create_stg_view + comma_Column_name + "\n"

        create_stg_view = create_stg_view + "from " + source_t + "." + Table_name + " t " + where_clause + ";\n\n"
        f.write(create_stg_view)
    f.close()
