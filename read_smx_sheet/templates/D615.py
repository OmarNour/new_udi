from read_smx_sheet.app_Lib import functions as funcs
from read_smx_sheet.Logging_Decorator import Logging_decorator


@Logging_decorator
def d615(cf, source_output_path, Core_tables,STG_tables):
    file_name = funcs.get_file_name(__file__)
    f = funcs.WriteFile(source_output_path, file_name, "sql")
    core_tables_df = funcs.get_core_tables(Core_tables)
    staging_tables_df = funcs.get_stg_tables(STG_tables)
    Script = ""
    stage_script = ""
    for core_tables_df_index, core_tables_df_row in core_tables_df.iterrows():
        core_table_name = core_tables_df_row['Table name']


        # del_script = "DEL FROM " + cf.GCFR_V + ".GCFR_Transform_KeyCol "
        # del_script_view = del_script + " WHERE OUT_DB_NAME = '" + cf.core_view + "' AND OUT_OBJECT_NAME = '" + core_table_name + "';\n"
        # del_script_table = del_script + " WHERE OUT_DB_NAME = '" + cf.core_table + "' AND OUT_OBJECT_NAME = '" + core_table_name + "';\n"
        core_table_columns = funcs.get_core_table_columns(Core_tables, core_table_name )

        # exe_ = "EXEC " + cf.MACRO_DB + ".GCFR_Register_Tfm_KeyCol('" + cf.core_view + "'"
        # exe_table = "EXEC " + cf.MACRO_DB + ".GCFR_Register_Tfm_KeyCol('" + cf.core_table + "'"
        # _p = ",'" + core_table_name + "'"
        # _p = _p + ",'SEQ_NO' );\n\n"
        # exe_p = exe_ + _p
        # exe_p_t = exe_table + _p
        # exe_p_ = ""
        # exe_p_table = ""
        for core_table_columns_index, core_table_columns_row in core_table_columns.iterrows():
            if core_table_columns_row['PK'].upper() == 'Y':
                Column_name = core_table_columns_row['Column name']
                Script += "INSERT INTO " + cf.GCFR_t + ".GCFR_Transform_KeyCol \n"
                Script += "SELECT '" + cf.core_table + "' , '" + core_table_name + "' , '" + Column_name + "' , " + "CURRENT_DATE , CURRENT_USER , CURRENT_TIMESTAMP    \n"
                Script += "WHERE NOT EXISTS (SELECT   OUT_OBJECT_NAME  FROM " +cf.GCFR_t + ".GCFR_Transform_KeyCol  ); \n \n \n"



                # _p = ",'" + core_table_name + "'"
                # _p = _p + ",'" + Column_name + "' );\n"
                #
                # exe_p_ = exe_p_ + exe_ + _p
                # exe_p_table = exe_p_ + exe_table + _p

        # exe_p = exe_p_ + "\n" if exe_p_ != "" else exe_p
        # exe_table = exe_p_table + "\n" if exe_p_table != "" else exe_p_t

        # f.write(del_script_view)
        # f.write(del_script_table)
        # f.write(exe_p)
        # f.write(exe_table)

    for stage_table_index, stage_table_row in staging_tables_df.iterrows():
        stage_table = stage_table_row['Table name']
        stage_columns = funcs.get_Staging_Key_Columns(STG_tables,stage_table)

        for column in stage_columns:
            stage_script += "INSERT INTO " + cf.GCFR_t + ".GCFR_Transform_KeyCol \n"
            stage_script += "SELECT '" + cf.T_STG + "' , '" + stage_table + "' , '" + column + "' , " + "CURRENT_DATE , CURRENT_USER , CURRENT_TIMESTAMP    \n"
            stage_script += "WHERE  NOT EXISTS (SELECT   OUT_OBJECT_NAME  FROM " + cf.GCFR_t + ".GCFR_Transform_KeyCol  ); \n \n \n"

    f.write(Script)
    f.write(stage_script)
    f.close()
