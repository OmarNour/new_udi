import parameters.parameters as pm
import app_Lib.functions as funcs
import app_Lib.TransformDDL as TransformDDL


def D610(source_output_path, source_name, Table_mapping, Core_tables):
    file_name = funcs.get_file_name(__file__)
    f = open(source_output_path + "/" + file_name + ".sql", "w+", encoding="utf-8")
    core_tables_list = TransformDDL.get_src_core_tbls(source_name, Core_tables, Table_mapping)

    for tbl_name in core_tables_list:
        core_view = 'REPLACE VIEW '+pm.core_view+'.'+tbl_name+' AS SELECT * FROM ' +pm.core_table+'.'+tbl_name+'; \n'
        f.write(core_view)

    f.close()