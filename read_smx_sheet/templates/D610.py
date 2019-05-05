from read_smx_sheet.app_Lib import functions as funcs
from read_smx_sheet.app_Lib import TransformDDL
import traceback


def d610(cf, source_output_path, Table_mapping):
    file_name = funcs.get_file_name(__file__)
    f = funcs.WriteFile(source_output_path, file_name, "sql")
    try:
        core_tables_list = TransformDDL.get_src_core_tbls(Table_mapping)

        for tbl_name in core_tables_list:
            core_view = 'REPLACE VIEW '+cf.core_view+'.'+tbl_name+' AS SELECT * FROM ' +cf.core_table+'.'+tbl_name+'; \n'
            f.write(core_view)
    except:
        funcs.TemplateLogError(cf.output_path, source_output_path, file_name, traceback.format_exc()).log_error()
    f.close()