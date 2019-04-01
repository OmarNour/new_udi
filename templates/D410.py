import pandas as pd
import app_Lib.manage_directories as md
import parameters.parameters as pm
import app_Lib.functions as funcs


def d410(source_output_path, source_name, STG_tables, Supplements):
    file_name = funcs.get_file_name(__file__)
    f = open(source_output_path + "/" + file_name + ".sql", "w+")

    stg_tables_df = funcs.get_stg_tables(STG_tables, source_name)
    for stg_tables_df_index, stg_tables_df_row in stg_tables_df.iterrows():
        Table_name = stg_tables_df_row['Table name']
        stg_table_name = Table_name + '_' if funcs.is_Reserved_word(Supplements, 'TERADATA', Table_name) else Table_name

        script = "REPLACE VIEW " + pm.SI_VIEW + "." + stg_table_name + " AS\n"
        script = script + "SELECT * FROM " + pm.SI_DB + "." + stg_table_name + ";\n\n"

        f.write(script)

    f.close()
