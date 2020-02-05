from read_smx_sheet.app_Lib import functions as funcs
import traceback
from read_smx_sheet.app_Lib import TransformDDL


def hist_timegap_check(cf, source_output_path, table_mapping, core_tables):
    file_name = funcs.get_file_name(__file__)
    f = funcs.WriteFile(source_output_path, file_name, "sql")
    try:
        count = 1
        for table_mapping_index, table_mapping_row in table_mapping.iterrows():
            hist_check_name_line = "---hist_timegap_Test_Case_" + str(count) + "---"
            if table_mapping_row['Historization algorithm'] == 'HISTORY':
                target_table = table_mapping_row['Target table name']
                process_name = table_mapping_row['Mapping name']
                start_date = TransformDDL.get_core_tbl_sart_date_column(core_tables, target_table)
                end_date = TransformDDL.get_core_tbl_end_date_column(core_tables, target_table)
                hist_keys = TransformDDL.get_trgt_hist_keys(core_tables, target_table)
                call_line1 = "SELECT "+hist_keys+','+start_date+',end_'
                call_line2 = "FROM ( sel "+hist_keys+','+start_date+',MAX('+end_date+')over(partition by '
                call_line3 = hist_keys+' order by '+start_date+' rows between 1 preceding and 1 preceding)as end_'
                call_line4 = 'FROM '+cf.base_DB+'.'+target_table
                call_line5 = "WHERE PROCESS_NAME = 'TXF_CORE_"+process_name+"')tst"
                call_line6 = "WHERE CAST(CAST(tst.end_ AS DATE)AS TIMESTAMP(0))+ INTERVAL'1'SECOND<>tst."+start_date+';'+'\n\n\n'
                hist_test_case_exp = hist_check_name_line + '\n' + call_line1 + '\n' + call_line2 + '\n' + call_line3 + '\n' \
                                     + call_line4 + '\n' + call_line5 + '\n' + call_line6
                f.write(hist_test_case_exp)
                count = count + 1
    except:
        funcs.TemplateLogError(cf.output_path, source_output_path, file_name, traceback.format_exc()).log_error()

    f.close()
