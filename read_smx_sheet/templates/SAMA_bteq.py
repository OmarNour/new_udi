from read_smx_sheet.app_Lib import functions as funcs
from read_smx_sheet.Logging_Decorator import Logging_decorator


@Logging_decorator
def bteq_script(cf, source_output_path, STG_tables):
    stg_tables_df = funcs.get_sama_stg_tables(STG_tables, None)
    update_statement_bteq = ''
    on_statement_script = ''
    where_statement_script = ''
    where_statement_bteq = ''
    set_statement_script = ''
    where_statement_script_pk = ''
    column_statement_bteq = ""
    column_statement_bteq_stg = ""
    where_statement_script_pk_null = ""

    for stg_tables_df_index, stg_tables_df_row in stg_tables_df.iterrows():
        Table_name = stg_tables_df_row['Table_Name']
        f = funcs.WriteFile(source_output_path, Table_name, "bteq")

        run_bteq = '.run FILE=' + cf.bteq_run_file + ';'+"\n"+"BT;" + "\n" + "\n"
        close_bteq = 'ET;' + "\n" + "\n" + ".LOGOFF;" + "\n" + ".QUIT;" + "\n"

        update_statement_head = 'UPDATE ' + cf.stg_prefix + stg_tables_df_row[
            'Schema_Name'] + '.' + Table_name + "\n" + 'FROM (' + "\n" + "\t" + "\t" + 'SELECT' + "\n"
        insert_statement_head = 'INSERT INTO ' + cf.dm_prefix + stg_tables_df_row[
            'Schema_Name'] + '.' + Table_name + "\n" + "("
        insert_statement_head_dup = 'INSERT INTO ' + cf.dm_prefix + stg_tables_df_row[
            'Schema_Name'] + '.' + Table_name + cf.duplicate_table_suffix + "\n" + "("

        STG_table_columns = funcs.get_sama_stg_table_columns_minus_pk(STG_tables, Table_name)
        STG_table_pk_columns = funcs.get_sama_stg_table_columns_pk(STG_tables, Table_name)
        STG_table_columns_all = funcs.get_sama_stg_table_columns(STG_tables, Table_name)
        from_statement = "\t" + 'FROM ' + "\n" + "\t" + "\t" + cf.stg_prefix + stg_tables_df_row[
            'Schema_Name'] + '.' + Table_name + ' as stg' + "\n"
        join_statement = 'JOIN ' + cf.dm_prefix + stg_tables_df_row['Schema_Name'] + '.' + Table_name + ' as datamart'
        for column_name_index, column_name_row in STG_table_columns_all.iterrows():
            Column_name = column_name_row['Column_Name']
            comma = '\t' + ',' if column_name_index > 0 else ''
            comma_stg = '\t' + '\t' + ',' if column_name_index > 0 else ''
            comma_Column_name = comma + Column_name + "\n"
            column_statement_bteq = column_statement_bteq + comma_Column_name
            comma_Column_name_stg = comma_stg + 'stg.' + Column_name + "\n"
            column_statement_bteq_stg = column_statement_bteq_stg + comma_Column_name_stg

        column_select_statement = "\t" + column_statement_bteq + '\t' + ',b_id' + "\n" + '\t' + ',insrt_dttm' + "\n" + '\t' + ',updt_dttm' + "\n" + ')' + "\n"
        insert_select_statement = "\t" + " SELECT" + "\n" + "\t" + "\t" + column_statement_bteq_stg + '\t' + '\t' + ',stg.batch_id b_id' + "\n" + '\t' + '\t' + ',current_timestamp(0) insrt_dttm' + "\n" + '\t' + '\t' + ',current_timestamp(0) updt_dttm' + "\n"

        for column_name_index, column_name_row in STG_table_pk_columns.iterrows():
            Column_name = column_name_row['Column_Name']
            on_statement = 'stg.' + Column_name + '= datamart.' + Column_name
            and_statement = ' and ' if column_name_index > 0 else ' '
            and_Column_name = and_statement + on_statement + "\n"
            on_statement_script = on_statement_script + and_Column_name
            join_statement = join_statement + "\t" + "\t" + "\n" + "\t" + "\t" + 'ON ' + on_statement_script

            where_statement_pk = cf.stg_prefix + stg_tables_df_row[
                'Schema_Name'] + '.' + Table_name + '= updt.' + Column_name + '\n'
            and_statement = ' and ' if column_name_index > 0 else ' '
            and_Column_name = and_statement + where_statement_pk
            where_statement_script_pk = where_statement_script_pk + and_Column_name

            where_statement_pk_null = 'datamart.' + Column_name + ' is null '
            and_statement = ' and ' if column_name_index > 0 else ' '
            and_Column_name = and_statement + where_statement_pk_null
            where_statement_script_pk_null = where_statement_script_pk_null + and_Column_name

        for column_name_index, column_name_row in STG_table_columns.iterrows():
            Column_name = column_name_row['Column_Name']
            comma = ',' if column_name_index > 0 else ' '
            comma_Column_name = "\t" + "\t" + comma + Column_name + "\n"
            update_statement_bteq = update_statement_bteq + comma_Column_name

            where_statement = 'stg.' + Column_name + '<> datamart.' + Column_name + '\n' + "\t" + "\t"
            and_statement = ' and ' if column_name_index > 0 else ' '
            and_Column_name = and_statement + where_statement
            where_statement_script = where_statement_script + and_Column_name
            where_statement_bteq = "\t" + "\t" + 'where(' + where_statement_script + "\t" + "\t" + ')' + "\n" + "\t" + ')updt' + "\n"

            set_statement = Column_name + '= updt.' + Column_name + '\n'
            and_statement = ' and ' if column_name_index > 0 else ' '
            and_Column_name = and_statement + set_statement
            set_statement_script = set_statement_script + and_Column_name

        update_statement_bteq_script = update_statement_head + update_statement_bteq + "\t" + from_statement + "\t" + "\t" + join_statement + '\n' + where_statement_bteq + 'SET ' + set_statement_script + 'WHERE ' + where_statement_script_pk + ';' + "\n" + "\n" + "\n"
        insert_statement_bteq_script = insert_statement_head + column_select_statement + insert_select_statement + from_statement + '\t' + 'LEFT ' + join_statement + '\n' + "\t" + "\t" + 'WHERE' + where_statement_script_pk_null + ';' + "\n" + "\n" + "\n"
        insert_statement_bteq_script_dup = insert_statement_head_dup + column_select_statement + insert_select_statement + from_statement + '\t' + join_statement + "\t" + "\t" ';' + "\n" + "\n" + "\n"

        f.write(run_bteq)
        f.write(update_statement_bteq_script)
        f.write(insert_statement_bteq_script)
        f.write(insert_statement_bteq_script_dup)
        f.write(close_bteq)

        update_statement_bteq = ''
        on_statement_script = ''
        where_statement_script = ''
        where_statement_bteq = ''
        set_statement_script = ''
        where_statement_script_pk = ''
        column_statement_bteq = ""
        column_statement_bteq_stg = ""
        where_statement_script_pk_null = ""

        f.close()
