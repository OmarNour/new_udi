import pandas as pd
import app_Lib.manage_directories as md
import parameters.parameters as pm
import app_Lib.functions as funcs
import numpy as np

def d340(source_output_path, source_name, STG_tables, BKEY):
    file_name = funcs.get_file_name(__file__)
    f = open(source_output_path + "/" + file_name + ".sql", "w+")

    stg_tables_df = STG_tables.loc[(STG_tables['Source system name'] == source_name)
                                   & ~(STG_tables['Key domain name'].isnull())
                                   & ~(STG_tables['Natural key'].isnull())]

    for stg_tables_df_index, stg_tables_df_row in stg_tables_df.replace(np.nan, "", regex=True).iterrows():
        key_domain_name = stg_tables_df_row['Key domain name']
        stg_table_name = stg_tables_df_row['Table name']
        stg_Column_name = stg_tables_df_row['Column name']

        try:
            bkey_df = BKEY.loc[(BKEY['Key domain name'] == key_domain_name)]
            key_set_name = bkey_df['Key set name'].values[0]
            Key_set_ID = str(int(bkey_df['Key set ID'].values[0]))
            Key_domain_ID = str(int(bkey_df['Key domain ID'].values[0]))
            Physical_table = bkey_df['Physical table'].values[0]

            script = "CALL " + pm.APPLY_DB + ".GCFR_PP_BKEY("
            script = script + "'BK_" + Key_set_ID + "_" + stg_table_name + "_" + stg_Column_name + "_" + Key_domain_ID + "'"
            script = script + ",6, oRC, oRM);"

            f.write(script + '\n')
        except:
            pass
    f.close()


