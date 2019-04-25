from read_smx_sheet.parameters import parameters as pm
from read_smx_sheet.app_Lib import functions as funcs


def d300(source_output_path, STG_tables, BKEY):
    file_name = funcs.get_file_name(__file__)
    f = open(source_output_path + "/" + file_name + ".sql", "w+")

    try:
        Key_domain_names_df = STG_tables.loc[STG_tables['Key domain name'] != ''][['Key domain name']].drop_duplicates()

        for Key_domain_names_df_index, Key_domain_names_df_row in Key_domain_names_df.iterrows():
            key_domain_name = Key_domain_names_df_row['Key domain name']

            bkey_df = BKEY.loc[(BKEY['Key domain name'] == key_domain_name)]
            key_set_name = bkey_df['Key set name'].values[0]
            Key_set_ID = str(int(bkey_df['Key set ID'].values[0]))
            Key_domain_ID = str(int(bkey_df['Key domain ID'].values[0]))
            Physical_table = bkey_df['Physical table'].values[0]

            script1 = "EXEC " + pm.MACRO_DB + ".GCFR_Register_Bkey_Key_Set(" + Key_set_ID + ", '" + key_set_name + "', '" + Physical_table + "', '" + pm.UTLFW_v + "');"
            script2 = "CALL " + pm.UT_DB + ".GCFR_UT_BKEY_St_Key_CT('" + pm.UTLFW_t + "', '" + Physical_table + "', '1', :OMessage);"
            script3 = "CALL " + pm.UT_DB + ".GCFR_UT_BKEY_St_Key_CV('" + pm.UTLFW_t + "', '" + Physical_table + "', '" + pm.UTLFW_v + "', :OMessage);"
            script4 = "CALL " + pm.UT_DB + ".GCFR_UT_BKEY_Key_Set_RI_Check(" + Key_set_ID + ", :OMessage);"
            script5 = "CALL " + pm.UT_DB + ".GCFR_UT_BKEY_St_Key_NextId_CT('" + Physical_table + "', '1', :OMessage);"
            script6 = "CALL " + pm.UT_DB + ".GCFR_UT_BKEY_St_Key_NextId_CV('" + Physical_table + "', :OMessage);"
            script7 = "CALL " + pm.UT_DB + ".GCFR_UT_BKEY_S_K_NextId_Log_CT('" + Physical_table + "', '1', :OMessage);"
            script8 = "CALL " + pm.UT_DB + ".GCFR_UT_BKEY_S_K_NextId_Log_CV('" + Physical_table + "', :OMessage);"
            script9 = "CALL " + pm.UT_DB + ".GCFR_UT_BKEYStandKeyNextId_Gen('" + pm.UTLFW_t + "', '" + Physical_table + "', " + Key_set_ID + ", :OMessage);"
            script10 = "EXEC " + pm.MACRO_DB + ".GCFR_Register_Bkey_Domain(" + Key_set_ID + ", " + Key_domain_ID + ", '" + key_domain_name + "');"

            f.write(script1 + '\n')
            f.write(script2 + '\n')
            f.write(script3 + '\n')
            f.write(script4 + '\n')
            f.write(script5 + '\n')
            f.write(script6 + '\n')
            f.write(script7 + '\n')
            f.write(script8 + '\n')
            f.write(script9 + '\n')
            f.write(script10 + '\n')

            f.write('\n')
    except:
        pass
    f.close()
