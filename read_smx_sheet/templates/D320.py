from read_smx_sheet.parameters import parameters as pm
from read_smx_sheet.app_Lib import functions as funcs
from read_smx_sheet.Logging_Decorator import Logging_decorator


@Logging_decorator
def d320(cf, source_output_path, STG_tables, BKEY):
    file_name = funcs.get_file_name(__file__)
    f = funcs.WriteFile(source_output_path, file_name, "sql")
    separator = pm.stg_cols_separator
    stg_tables_df = STG_tables.loc[(STG_tables['Key domain name'] != "")
                                    & (STG_tables['Natural key'] != "")]
    trimmed_Natural_key = []
    normal_columns = ""
    join_type = ""
    join_statement = ""
    for stg_tables_df_index, stg_tables_df_row in stg_tables_df.iterrows():
        stg_table_name = stg_tables_df_row['Table name'].upper()
        Column_name = stg_tables_df_row['Column name'].upper()
        stg_Natural_key_df = STG_tables.loc[(STG_tables['Table name'].str.upper() == stg_table_name)
                                            & (STG_tables['Natural key'] != "")]
        Natural_key_list = []
        for stg_Natural_key_df_index, stg_Natural_key_df_row in stg_Natural_key_df.iterrows():
            Natural_key_split = str(stg_Natural_key_df_row['Natural key']).split(separator)
            for i in Natural_key_split:
                Natural_key_list.append(i.upper())

            Natural_key = str(stg_tables_df_row['Natural key']).upper()

            for i in list(set(Natural_key_list)):
                i = i.replace(" ", "")
                if "COALESCE" in i:
                    Column_name = "COALESCE(" + Column_name + ",'')"
                Column_name = "TRIM(Trailing '.' from TRIM(" + Column_name + ")) "

            if Natural_key == "":
                comma_Column_name = ',' + Column_name
                normal_columns = normal_columns + comma_Column_name + "\n"
            else:
                trim_Natural_key = []
                split_Natural_key = Natural_key.split(separator)
                for i in split_Natural_key:
                    trim_Natural_key.append("TRIM(Trailing '.' from TRIM(" + i + "))")
                trimmed_Natural_key = funcs.list_to_string(trim_Natural_key, separator)

        key_domain_name = stg_tables_df_row['Key domain name']
        stg_table_name = stg_tables_df_row['Table name']
        stg_Column_name = stg_tables_df_row['Column name']
        generation_flag = stg_tables_df_row['Bkey generation flag']

        Bkey_filter = str(stg_tables_df_row['Bkey filter']).upper()
        Bkey_filter = "WHERE " + Bkey_filter if Bkey_filter != "" and "JOIN" not in Bkey_filter else Bkey_filter
        Bkey_filter = Bkey_filter + "\n" if Bkey_filter != "" else Bkey_filter
        Bkey_join = str(stg_tables_df_row['Bkey Join']).upper()
        if Bkey_join != "":
            Bkey_join_splitted = Bkey_join.split("JOIN ")
            join_type = Bkey_join_splitted[0]
            join_statement = "JOIN " + cf.v_stg + '.' + Bkey_join_splitted[1]
        Natural_key_list = stg_tables_df_row['Natural key'].split(separator)
        trim_Trailing_Natural_key_list = []

        for i in Natural_key_list:
            trim_Trailing_Natural_key_list.append("TRIM(Trailing '.' from TRIM(" + i.strip() + "))")

        Source_Key = funcs.list_to_string(trim_Trailing_Natural_key_list, separator)
        coalesce_count = Source_Key.upper().count("COALESCE")
        separator_count = Source_Key.count(separator)

        compare_string = funcs.single_quotes("_" * separator_count) if coalesce_count > separator_count else "''"

        Source_Key_cond = "WHERE " if "WHERE" not in Bkey_filter else " AND "
        Source_Key_cond = Source_Key_cond + "COALESCE(Source_Key,"+compare_string+") <> "+compare_string+" "

        bkey_df = BKEY.loc[(BKEY['Key domain name'] == key_domain_name)]
        Key_set_ID = str(int(bkey_df['Key set ID'].values[0]))
        Key_domain_ID = str(int(bkey_df['Key domain ID'].values[0]))

        if generation_flag != 0:
            script = "CREATE VIEW " + cf.INPUT_VIEW_DB + ".BK_" + Key_set_ID + "_" + stg_table_name + "_" + stg_Column_name + "_" + Key_domain_ID + "_IN AS LOCK ROW FOR ACCESS\n"
            script = script + "SELECT " + trimmed_Natural_key + " AS Source_Key\n"
            script = script + "FROM " + cf.v_stg + "." + stg_table_name + "\n"
            if Bkey_join != "":
                script = script + join_type + join_statement + "\n"
            script = script + Bkey_filter + Source_Key_cond + "\n"
            script = script + "GROUP BY 1;" + "\n"

            f.write(script)
            f.write('\n')
    f.close()
