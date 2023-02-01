from read_smx_sheet.app_Lib import functions as funcs
from read_smx_sheet.Logging_Decorator import Logging_decorator


@Logging_decorator
def d415(cf, source_output_path, STG_tables):
    file_name = funcs.get_file_name(__file__)
    f = funcs.WriteFile(source_output_path, file_name, "sql")
    stg_tables_df = funcs.get_stg_tables(STG_tables)
    for stg_tables_df_index, stg_tables_df_row in stg_tables_df.iterrows():
        stg_table_name = stg_tables_df_row['Table name']

        del_script = "DEL FROM " + cf.GCFR_V + ".GCFR_Transform_KeyCol "
        del_script = del_script + " WHERE OUT_DB_NAME = '" + cf.SI_VIEW + "' AND OUT_OBJECT_NAME = '" + stg_table_name + "';\n"

        STG_table_columns = funcs.get_stg_table_columns(STG_tables, None, stg_table_name, True)

        exe_ = "EXEC " + cf.MACRO_DB + ".GCFR_Register_Tfm_KeyCol('" + cf.SI_VIEW + "'"
        _p = ",'" + stg_table_name + "'"
        _p = _p + ",'SEQ_NO' );\n\n"
        exe_p = exe_ + _p
        exe_p_ = ""
        for STG_table_columns_index, STG_table_columns_row in STG_table_columns.iterrows():
            if STG_table_columns_row['PK'].upper() == 'Y':
                Column_name = STG_table_columns_row['Column name']

                _p = ",'" + stg_table_name + "'"
                _p = _p + ",'" + Column_name + "' );\n"

                exe_p_ = exe_p_ + exe_ + _p

        exe_p = exe_p_ + "\n" if exe_p_ != "" else exe_p

        f.write(del_script)
        f.write(exe_p)
    f.close()
