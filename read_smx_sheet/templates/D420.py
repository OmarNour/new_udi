from read_smx_sheet.parameters import parameters as pm
from read_smx_sheet.app_Lib import functions as funcs
from read_smx_sheet.Logging_Decorator import Logging_decorator


@Logging_decorator
def d420(cf, source_output_path, STG_tables, BKEY, BMAP, Loading_Type, source_name):
    file_name = funcs.get_file_name(__file__)
    f = funcs.WriteFile(source_output_path, file_name, "sql")

    MODIFICATION_TYPE_found = 0
    Call_srci = ''
    separator = pm.stg_cols_separator
    stg_tables_df = funcs.get_stg_tables(STG_tables)

    bmap_physical_table = "BMAP_STANDARD_MAP"

    for stg_tables_df_index, stg_tables_df_row in stg_tables_df.iterrows():
        stg_table_name = stg_tables_df_row['Table name'].upper()
   #     print(stg_tables_df_row, ">>>>>>>>>>>>>>>>>>>>>>>>>")
   #     Column_name = stg_tables_df_row['Column name']

    #    if Column_name == "MODIFICATION_TYPE":
    #        MODIFICATION_TYPE_found = 1
        stg_Natural_key_df = STG_tables.loc[(STG_tables['Table name'].str.upper() == stg_table_name)
                                            & (STG_tables['Natural key'] != "")]
        Natural_key_list = []
        for stg_Natural_key_df_index, stg_Natural_key_df_row in stg_Natural_key_df.iterrows():
            Natural_key_split = str(stg_Natural_key_df_row['Natural key']).split(separator)
            for i in Natural_key_split:
                Natural_key_list.append(i.upper())

        # Natural_key_list_str = funcs.list_to_string(Natural_key_list, ',').upper()

        stg_table_has_pk = True if len(STG_tables.loc[(STG_tables['Table name'].str.upper() == stg_table_name)
                                                          & (STG_tables['PK'].str.upper() == 'Y')].index) > 0 else False

        if not stg_table_has_pk:
            seq_pk_col = " SEQ_NO\n,"
        else:
            seq_pk_col = " "

        create_view = "CREATE VIEW " + cf.SI_VIEW + "." + stg_table_name + " AS LOCK ROW FOR ACCESS\nSELECT \n"
        from_clause = "FROM " + cf.v_stg + "." + stg_table_name + " t"
        STG_table_columns = funcs.get_stg_table_columns(STG_tables, None, stg_table_name, True)

        bkeys_query = ""
        bkeys_left_join_count = 0
        bmap_query = ""
        bmap_left_join_count = 0
        normal_columns = ""
        bkey_columns = ""
        bmap_columns = ""
        bkey_join_statement = ''
        bkey_join_type = ''
        for STG_table_columns_index, STG_table_columns_row in STG_table_columns.iterrows():
            comma = ',' if STG_table_columns_index > 0 else seq_pk_col

            Column_name = STG_table_columns_row['Column name'].upper()
            Natural_key = str(STG_table_columns_row['Natural key']).upper()

            if Column_name == "MODIFICATION_TYPE":
                 MODIFICATION_TYPE_found = 1

            alias = Column_name
            Column_name = "t." + Column_name
            if STG_table_columns_row['Bkey Join'] != '':
                Bkey_join = str(STG_table_columns_row['Bkey Join']).upper()
                Bkey_join_splitted = Bkey_join.split("JOIN ")
                bkey_join_type = Bkey_join_splitted[0]
                bkey_join_statement = bkey_join_statement + "JOIN " + cf.v_stg + '.' + Bkey_join_splitted[1]

            for i in list(set(Natural_key_list)):
                i = i.replace(" ", "")
                if alias == i or "COALESCE(" + alias + ",'')" == i:
                    if "COALESCE" in i:
                        Column_name = "COALESCE(" + Column_name + ",'')"
                    Column_name = "TRIM(Trailing '.' from TRIM(" + Column_name + ")) " + alias

            if Natural_key == "":
                comma_Column_name = comma + Column_name
                normal_columns = normal_columns + comma_Column_name + "\n"

            else:
                trim_Natural_key = []
                split_Natural_key = Natural_key.split(separator)
                for i in split_Natural_key:
                    trim_Natural_key.append("TRIM(Trailing '.' from TRIM(" + i + "))")
                trimed_Natural_key = funcs.list_to_string(trim_Natural_key, separator)

                Key_domain_name = STG_table_columns_row['Key domain name'].upper()
                if Key_domain_name != "":
                    BKEY_row = BKEY.loc[(BKEY['Key domain name'].str.upper() == Key_domain_name)]
                    if len(BKEY_row.index) > 0:
                        bkey_physical_table = BKEY_row['Physical table'].values[0]
                        bkey_domain_id = str(int(BKEY_row['Key domain ID'].values[0]))
                        bkeys_left_join_count = bkeys_left_join_count + 1
                        bk_alias = " bk" + str(bkeys_left_join_count)

                        bkeys_query = "( Select " + bk_alias + ".EDW_Key\n"
                        bkeys_query = bkeys_query + "\tFrom " + cf.UTLFW_v + "." + bkey_physical_table + bk_alias + "\n"
                        bkeys_query = bkeys_query + "\tWhere " + bk_alias + ".Source_Key = " + trimed_Natural_key + "\n"
                        bkeys_query = bkeys_query + "\tand " + bk_alias + ".Domain_ID = " + bkey_domain_id + ")"

                        comma_Column_name = comma + bkeys_query + " AS " + alias
                        bkey_columns = bkey_columns + comma_Column_name + "\n"

                Code_domain_name = STG_table_columns_row["Code domain name"].upper()
                if Code_domain_name != "":
                    BMAP_row = BMAP.loc[(BMAP["Code domain name"].str.upper() == Code_domain_name)]
                    if len(BMAP_row.index) > 0:
                        Code_set_ID = str(int(BMAP_row["Code set ID"].values[0]))
                        Code_domain_ID = str(int(BMAP_row["Code domain ID"].values[0]))
                        bmap_left_join_count = bmap_left_join_count + 1
                        bmap_alias = " bm" + str(bmap_left_join_count)

                        bmap_query = "( Select " + bmap_alias + ".EDW_Code\n"
                        bmap_query = bmap_query + "\tFrom " + cf.UTLFW_v + "." + bmap_physical_table + bmap_alias + "\n"
                        bmap_query = bmap_query + "\tWhere " + bmap_alias + ".Source_Code = " + trimed_Natural_key + "\n"
                        bmap_query = bmap_query + "\tand " + bmap_alias + ".Code_Set_id = " + Code_set_ID + "\n"
                        bmap_query = bmap_query + "\tand " + bmap_alias + ".Domain_ID = " + Code_domain_ID + ")"

                        comma_Column_name = comma + bmap_query + " AS " + alias
                        bmap_columns = bmap_columns + comma_Column_name + "\n"

        if MODIFICATION_TYPE_found == 0:
            modification_type = ",t.modification_type\n"
        else:
            modification_type = ""

        normal_columns = normal_columns + modification_type
        if bkey_join_statement != '':
            bkey_join_statement = bkey_join_type + bkey_join_statement
            from_clause = from_clause + '/n' + bkey_join_statement
        create_view_script = create_view + normal_columns + bkey_columns + bmap_columns + from_clause + ";\n"
        f.write(create_view_script+"\n")
    Call_srci = 'CALL ' + cf.db_prefix + "P_PP.SRCI_LOADING('" + source_name + "',NULL,NULL,NULL,1,X,Y,Z);"
    f.write(Call_srci+"\n")
    f.close()
