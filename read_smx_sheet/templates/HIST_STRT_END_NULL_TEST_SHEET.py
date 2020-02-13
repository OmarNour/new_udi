from read_smx_sheet.app_Lib import functions as funcs
from read_smx_sheet.app_Lib import TransformDDL
from read_smx_sheet.Logging_Decorator import Logging_decorator


@Logging_decorator
def hist_start_end_null_check(cf, source_output_path, table_mapping, core_tables):
    file_name = funcs.get_file_name(__file__)
    f = funcs.WriteFile(source_output_path, file_name, "sql")
    count = 1
    for table_mapping_index, table_mapping_row in table_mapping.iterrows():
        hist_check_name_line = "---hist_start_end_null_Test_Case_" + str(count) + "---"
        if table_mapping_row['Historization algorithm'] == 'HISTORY':
            target_table = table_mapping_row['Target table name']
            process_name = table_mapping_row['Mapping name']
            hist_keys = TransformDDL.get_trgt_hist_keys(core_tables, target_table)
            start_date = TransformDDL.get_core_tbl_sart_date_column(core_tables, target_table)
            end_date = TransformDDL.get_core_tbl_end_date_column(core_tables, target_table)
            call_line1 = "SELECT "+hist_keys+" FROM "+cf.base_DB+'.'+target_table+" WHERE "+start_date+" IS NULL "
            call_line2 = "AND "+end_date+" IS NULL AND PROCESS_NAME = 'TXF_CORE_"+process_name+"'"
            call_line3 = "GROUP BY " + hist_keys
            call_line4 = "HAVING COUNT(*)>1;"+'\n\n\n'
            hist_test_case_exp = hist_check_name_line + '\n' + call_line1 + '\n' + call_line2 + '\n' \
                                 + call_line3 + '\n' + call_line4
            f.write(hist_test_case_exp)
            count = count + 1
    f.close()
