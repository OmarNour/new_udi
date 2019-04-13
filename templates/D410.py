import pandas as pd
import app_Lib.manage_directories as md
import parameters.parameters as pm
import app_Lib.functions as funcs


def d410(source_output_path, STG_tables):
    file_name = funcs.get_file_name(__file__)
    f = open(source_output_path + "/" + file_name + ".sql", "w+")

    stg_tables_df = funcs.get_stg_tables(STG_tables)
    for stg_tables_df_index, stg_tables_df_row in stg_tables_df.iterrows():
        stg_table_name = stg_tables_df_row['Table name']

        script = "REPLACE VIEW " + pm.SI_VIEW + "." + stg_table_name + " AS\n"
        script = script + "SELECT * FROM " + pm.SI_DB + "." + stg_table_name + ";\n\n"

        f.write(script)

    f.close()
