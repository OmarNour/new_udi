import pandas as pd
import app_Lib.manage_directories as md
import parameters.parameters as pm
import app_Lib.functions as funcs


def d415(source_output_path, source_name, STG_tables):
    file_name = funcs.get_file_name(__file__)
    f = open(source_output_path + "/" + file_name + ".sql", "w+")

    stg_tables_df = funcs.get_stg_tables(STG_tables, source_name)
    for stg_tables_df_index, stg_tables_df_row in stg_tables_df.iterrows():
        stg_table_name = stg_tables_df_row['Table name']
        # stg_table_name = Table_name + '_' if funcs.is_Reserved_word(Supplements, 'TERADATA', Table_name) else Table_name

        del_script = "DEL FROM " + pm.GCFR_V + ".GCFR_Transform_KeyCol "
        del_script = del_script + " WHERE OUT_OBJECT_NAME = '" + stg_table_name + "';\n"

        STG_table_columns = funcs.get_stg_table_columns(STG_tables, source_name, stg_table_name, True)

        exe_ = "EXEC " + pm.MACRO_DB + ".GCFR_Register_Tfm_KeyCol('" + pm.SI_VIEW + "'"
        _p = ",'" + stg_table_name + "'"
        _p = _p + ",'SEQ_NO' );\n\n"
        exe_p = exe_ + _p
        exe_p_ = ""
        for STG_table_columns_index, STG_table_columns_row in STG_table_columns.iterrows():
            if STG_table_columns_row['PK'].upper() == 'Y':
                Column_name = STG_table_columns_row['Column name']
                # Column_name = Column_name + '_' if funcs.is_Reserved_word(Supplements, 'TERADATA', Column_name) else Column_name

                _p = ",'" + stg_table_name + "'"
                _p = _p + ",'" + Column_name + "' );\n"

                exe_p_ = exe_p_ + exe_ + _p

        exe_p = exe_p_ + "\n" if exe_p_ != "" else exe_p

        f.write(del_script)
        f.write(exe_p)

    f.close()
