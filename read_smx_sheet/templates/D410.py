from read_smx_sheet.app_Lib import functions as funcs


def d410(cf, source_output_path, STG_tables):
    file_name = funcs.get_file_name(__file__)
    f = open(source_output_path + "/" + file_name + ".sql", "w+")

    try:
        stg_tables_df = funcs.get_stg_tables(STG_tables)
        for stg_tables_df_index, stg_tables_df_row in stg_tables_df.iterrows():
            stg_table_name = stg_tables_df_row['Table name']

            script = "REPLACE VIEW " + cf.SI_VIEW + "." + stg_table_name + " AS\n"
            script = script + "SELECT * FROM " + cf.SI_DB + "." + stg_table_name + ";\n\n"

            f.write(script)
    except:
        pass
    f.close()
