import parameters.parameters as pm
import app_Lib.functions as funcs


def d420(source_output_path, source_name, STG_tables, Supplements):
    file_name = funcs.get_file_name(__file__)
    f = open(source_output_path + "/" + file_name + ".sql", "w+")

    separator = pm.separator
    stg_tables_df = funcs.get_stg_tables(STG_tables, source_name)
    stg_Natural_key_df = STG_tables.loc[(STG_tables['Source system name'] == source_name)
                                        & ~(STG_tables['Natural key'].isnull())]

    trim_Trailing_Natural_key_list = []
    for stg_Natural_key_df_index, stg_Natural_key_df_row in stg_Natural_key_df.iterrows():
        Natural_key_list = stg_Natural_key_df_row['Natural key'].split(separator)
        for i in Natural_key_list:
            trim_Trailing_Natural_key_list.append("TRIM(Trailing '.' from TRIM(" + i.strip() + ")) " + i.strip())

    for stg_tables_df_index, stg_tables_df_row in stg_tables_df.iterrows():

        stg_table_name = stg_tables_df_row['Table name']
        stg_table_has_pk = True if len(STG_tables.loc[(STG_tables['Table name'] == stg_table_name)
                                                      & (STG_tables['PK'].str.upper() == 'Y')].index) > 0 else False

        if not stg_table_has_pk:
            seq_pk_col = " SEQ_NO\n,"
        else:
            seq_pk_col = " "

        stg_table_name = stg_table_name + '_' if funcs.is_Reserved_word(Supplements, 'TERADATA', stg_table_name) else stg_table_name

        create_view_script = "REPLACE VIEW " + pm.SI_VIEW + "." + stg_table_name + " AS\nSELECT \n"
        from_clause = "FROM " + pm.gdev1v_stg + "." + stg_table_name + " t"
        STG_table_columns = funcs.get_stg_table_columns(STG_tables, source_name, stg_table_name, True)

        for STG_table_columns_index, STG_table_columns_row in STG_table_columns.iterrows():
            Column_name = STG_table_columns_row['Column name']
            # Column_name = Column_name + '_' if funcs.is_Reserved_word(Supplements, 'TERADATA', Column_name) else Column_name

            comma = ',' if STG_table_columns_index > 0 else seq_pk_col
            comma_Column_name = comma + Column_name

            create_view_script = create_view_script + comma_Column_name + "\n"


        left_join = "LEFT JOIN"
        create_view_script = create_view_script + from_clause + "\n"
        f.write(create_view_script+"\n")

    f.close()
