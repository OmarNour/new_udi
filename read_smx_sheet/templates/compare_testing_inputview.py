from read_smx_sheet.app_Lib import functions as funcs
import calendar
import time
import traceback
from read_smx_sheet.app_Lib import TransformDDL


def compare_views_check(cf, source_output_path,core_Table_mapping,comparison_flag):
    count = 1
    file_name = comparison_flag
    f = funcs.WriteFile(source_output_path, file_name, "sql")
    try:
        test_exp = ''
        for table_mapping_index,table_mapping_row in core_Table_mapping.iterrows():
            if comparison_flag == 'FROM_TESTING_TO_UDI':
                test_exp='SEL * FROM '+cf.INPUT_VIEW_DB+'.TXF_CORE_'+table_mapping_row['Mapping name']+'_IN_TESTING'+'\n'
                test_exp = test_exp + 'MINUS \n'+ 'SEL * FROM '+cf.INPUT_VIEW_DB+'.TXF_CORE_'+table_mapping_row['Mapping name']+'_IN'
            elif comparison_flag == 'FROM_UDI_TO_TESTING':
                test_exp = 'SEL * FROM ' + cf.INPUT_VIEW_DB + '.TXF_CORE_' + table_mapping_row['Mapping name'] + '_IN' + '\n'
                test_exp = test_exp + 'MINUS \n' + 'SEL * FROM ' + cf.INPUT_VIEW_DB + '.TXF_CORE_' + table_mapping_row['Mapping name'] + '_IN_TESTING'
            input_view_check_line = "---Input_view_check_test_Case_" + str(count) + "---"
            call_exp=input_view_check_line+'\n'+test_exp+'\n\n\n'
            f.write(call_exp)
            count = count + 1

    except:
        funcs.TemplateLogError(cf.output_path, source_output_path, file_name, traceback.format_exc()).log_error()
    f.close()
