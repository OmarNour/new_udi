from read_smx_sheet.app_Lib import functions as funcs
from read_smx_sheet.app_Lib import TransformDDL
import traceback


def duplicates_check(cf, source_output_path, table_mapping, core_tables):
    file_name = funcs.get_file_name(__file__)
    f = funcs.WriteFile(source_output_path, file_name, "sql")
    count=0
    try:
        core_tables_list= TransformDDL.get_src_core_tbls(table_mapping)
        for table_name in core_tables_list:
            count = count+1
            core_table_pks = TransformDDL.get_trgt_pk(core_tables, table_name)
            dup_line = "---DUP_Test_Case_" + str(count) + "---"+'\n'
            dup_test_case_exp = 'SEL ' + core_table_pks + ' FROM ' + cf.base_DB + '.' + table_name + ' GROUP BY '+ core_table_pks + ' HAVING COUNT(*)>1;'+'\n'+'\n'
            f.write(dup_line+dup_test_case_exp)

    except:
        funcs.TemplateLogError(cf.output_path, source_output_path, file_name, traceback.format_exc()).log_error()

    f.close()