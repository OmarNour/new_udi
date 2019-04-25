from read_smx_sheet.parameters import parameters as pm
from read_smx_sheet.app_Lib import functions as funcs


def d610(source_output_path, Table_mapping):
    file_name = funcs.get_file_name(__file__)
    f = open(source_output_path + "/" + file_name + ".sql", "w+", encoding="utf-8")

    try:
        core_tables_list = TransformDDL.get_src_core_tbls(Table_mapping)

        for tbl_name in core_tables_list:
            core_view = 'REPLACE VIEW '+pm.core_view+'.'+tbl_name+' AS SELECT * FROM ' +pm.core_table+'.'+tbl_name+'; \n'
            f.write(core_view)
    except:
        pass
    f.close()