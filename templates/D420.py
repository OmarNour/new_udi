import parameters.parameters as pm
import app_Lib.functions as funcs


def d420(source_output_path, STG_tables, BKEY):
    file_name = funcs.get_file_name(__file__)
    f = open(source_output_path + "/" + file_name + ".sql", "w+")

    separator = pm.separator
    stg_tables_df = funcs.get_stg_tables(STG_tables)

    for stg_tables_df_index, stg_tables_df_row in stg_tables_df.iterrows():
        stg_table_name = stg_tables_df_row['Table name']

        stg_Natural_key_df = STG_tables.loc[(STG_tables['Table name'] == stg_table_name)
                                            & (STG_tables['Natural key'] != "")]
        bkey_Natural_key_list = []
        for stg_Natural_key_df_index, stg_Natural_key_df_row in stg_Natural_key_df.iterrows():
            # bkey_Natural_key_list = stg_Natural_key_df_row['Natural key'].split(separator)
            bkey_Natural_key_list.append(stg_Natural_key_df_row['Natural key'])

        bkey_Natural_key_list_str = funcs.list_to_string(bkey_Natural_key_list, ',').upper()

        stg_table_has_pk = True if len(STG_tables.loc[(STG_tables['Table name'] == stg_table_name)
                                                      & (STG_tables['PK'].str.upper() == 'Y')].index) > 0 else False

        if not stg_table_has_pk:
            seq_pk_col = " SEQ_NO\n,"
        else:
            seq_pk_col = " "

        create_view_script = "REPLACE VIEW " + pm.SI_VIEW + "." + stg_table_name + " AS\nSELECT \n"
        from_clause = "FROM " + pm.v_stg + "." + stg_table_name + " t"
        STG_table_columns = funcs.get_stg_table_columns(STG_tables, None, stg_table_name, True)

        bkeys_left_join = ""
        bkeys_left_join_count = 0
        for STG_table_columns_index, STG_table_columns_row in STG_table_columns.iterrows():
            Column_name = STG_table_columns_row['Column name'].upper()
            alias = Column_name
            if Column_name in bkey_Natural_key_list_str:
                if "COALESCE" in bkey_Natural_key_list_str:
                    Column_name = "COALESCE( " + Column_name + ",'')"
                Column_name = "TRIM(Trailing '.' from TRIM(" + Column_name + ")) " + alias

            comma = ',' if STG_table_columns_index > 0 else seq_pk_col
            comma_Column_name = comma + Column_name

            create_view_script = create_view_script + comma_Column_name + "\n"

            try:
                Key_domain_name = STG_table_columns_row['Key domain name']
                bkey_physical_table = BKEY.loc[(BKEY['Key domain name'].str.upper() == Key_domain_name)]['Physical table'].values[0]
                bkeys_left_join_count = bkeys_left_join_count + 1
                bk_alias = " bk" + str(bkeys_left_join_count)
                bkeys_left_join = bkeys_left_join + "LEFT JOIN " + pm.G_BKEY_V + "." + bkey_physical_table + bk_alias + "\n"

                Natural_key = STG_table_columns_row['Natural key']
                split_Natural_key = Natural_key.replace(" ","").split(separator)
                trim_Natural_key = []
                for i in split_Natural_key:
                    trim_Natural_key.append("TRIM(Trailing '.' from TRIM(" + i.strip() + "))")
                Natural_key = funcs.list_to_string(trim_Natural_key, separator)
                bkeys_left_join = bkeys_left_join + "\tON " + bk_alias + ".Source_Key = " + Natural_key + "\n"
            except:
                pass

        create_view_script = create_view_script + from_clause + "\n" + bkeys_left_join
        f.write(create_view_script+"\n")
        # f.write(Natural_key_list_str + "\n")

    f.close()
