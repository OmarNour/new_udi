import parameters.parameters as pm
import app_Lib.functions as funcs


def d001(source_output_path, source_name, STG_tables):
    file_name = funcs.get_file_name(__file__)
    f = open(source_output_path + "/" + file_name + ".sql", "w+")

    stg_tables_df = STG_tables.loc[STG_tables['Source system name'] == source_name][['Table name']].drop_duplicates()
    for STG_tables_index, STG_tables_row in stg_tables_df.iterrows():
        Table_name = STG_tables_row['Table name']
        # print(Table_name)

        f.write("delete from " + pm.automation_db + "." + pm.SOURCE_TABLES_LKP_table + " where SOURCE_NAME = '" + source_name + "';\n")
        f.write("insert into " + pm.automation_db + "." + pm.SOURCE_TABLES_LKP_table + "(SOURCE_NAME, TABLE_NAME)\n")
        f.write("VALUES ('" + source_name + "', '" + Table_name + "')" + ";\n")
        f.write("\n")

    f.close()
