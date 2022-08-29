from read_smx_sheet.app_Lib import functions as funcs
from read_smx_sheet.app_Lib import TransformDDL
from read_smx_sheet.Logging_Decorator import Logging_decorator


@Logging_decorator
def d610(cf, source_output_path, Table_mapping,STG_tables,source_name):
    file_name = funcs.get_file_name(__file__)
    f = funcs.WriteFile(source_output_path, file_name, "sql")
    core_tables_list = TransformDDL.get_src_core_tbls(Table_mapping)
    src_look_up_tables = funcs.get_src_code_set_names(STG_tables,source_name)

    for tbl_name in core_tables_list:
        core_view = 'CREATE VIEW ' + cf.core_view + '.' + tbl_name + ' AS LOCK ROW FOR ACCESS SELECT * FROM ' + cf.core_table + '.' + tbl_name + '; \n'
        f.write(core_view)
    for src_look_up_table in src_look_up_tables:
        core_view = 'CREATE VIEW ' + cf.core_view + '.' + src_look_up_table + ' AS LOCK ROW FOR ACCESS SELECT * FROM ' + cf.core_table + '.' + src_look_up_table + '; \n'
        f.write(core_view)
    f.close()
