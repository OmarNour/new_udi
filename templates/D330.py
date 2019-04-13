import parameters.parameters as pm
import app_Lib.functions as funcs
import numpy as np

def d330(source_output_path, STG_tables, BKEY):
    file_name = funcs.get_file_name(__file__)
    f = open(source_output_path + "/" + file_name + ".sql", "w+")

    stg_tables_df = STG_tables.loc[(STG_tables['Key domain name'] != "")
                                   & (STG_tables['Natural key'] != "")]

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

            script = "EXEC " + pm.MACRO_DB + ".GCFR_Register_Process("
            script = script + "'BK_" + Key_set_ID + "_" + stg_table_name + "_" + stg_Column_name + "_" + Key_domain_ID + "',"
            script = script + "'define bkey for the table " + key_set_name + " and the domain " + key_domain_name + "',"
            script = script + str(pm.gcfr_bkey_process_type) + ","
            script = script + str(pm.gcfr_ctl_Id) + ","
            script = script + str(pm.gcfr_stream_key) + ","
            script = script + "'" + pm.INPUT_VIEW_DB + "',"
            script = script + "'BK_" + Key_set_ID + "_" + stg_table_name + "_" + stg_Column_name + "_" + Key_domain_ID + "_IN',"
            script = script + "'" + pm.G_BKEY_V + "',"
            script = script + "'" + Physical_table + "',"
            script = script + "'" + pm.G_BKEY_T + "',"
            script = script + "'" + Physical_table + "',"
            script = script + "'" + pm.TMP_DB + "',"
            script = script + "'" + Key_set_ID + "',"
            script = script + "'" + Key_domain_ID + "',"
            script = script + "'',0,0,0,0);"

            f.write(script + '\n')
        except:
            pass
    f.close()
