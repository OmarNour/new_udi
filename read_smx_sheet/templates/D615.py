from read_smx_sheet.app_Lib import functions as funcs


def d615(cf, source_output_path, Core_tables):
    file_name = funcs.get_file_name(__file__)
    f = funcs.WriteFile(source_output_path, file_name, "sql")
    try:
        core_tables_df = funcs.get_core_tables(Core_tables)
        for core_tables_df_index, core_tables_df_row in core_tables_df.iterrows():
            core_table_name = core_tables_df_row['Table name']

            del_script = "DEL FROM " + cf.GCFR_V + ".GCFR_Transform_KeyCol "
            del_script = del_script + " WHERE OUT_DB_NAME = '" + cf.core_view + "' AND OUT_OBJECT_NAME = '" + core_table_name + "';\n"

            core_table_columns = funcs.get_core_table_columns(Core_tables, core_table_name )

            exe_ = "EXEC " + cf.MACRO_DB + ".GCFR_Register_Tfm_KeyCol('" + cf.core_view + "'"
            _p = ",'" + core_table_name + "'"
            _p = _p + ",'SEQ_NO' );\n\n"
            exe_p = exe_ + _p
            exe_p_ = ""
            for core_table_columns_index, core_table_columns_row in core_table_columns.iterrows():
                if core_table_columns_row['PK'].upper() == 'Y':
                    Column_name = core_table_columns_row['Column name']

                    _p = ",'" + core_table_name + "'"
                    _p = _p + ",'" + Column_name + "' );\n"

                    exe_p_ = exe_p_ + exe_ + _p

            exe_p = exe_p_ + "\n" if exe_p_ != "" else exe_p

            f.write(del_script)
            f.write(exe_p)
    except:
        pass
    f.close()
