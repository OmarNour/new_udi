from read_smx_sheet.app_Lib import functions as funcs
from read_smx_sheet.app_Lib import TransformDDL
from read_smx_sheet.Logging_Decorator import Logging_decorator


@Logging_decorator
def hist_start_null_check(cf, source_output_path, table_mapping, core_tables):
    file_name = funcs.get_file_name(__file__)
    f = funcs.WriteFile(source_output_path, file_name, "sql")
    count = 1
    for table_mapping_index, table_mapping_row in table_mapping.iterrows():
        hist_check_name_line = "---hist_start_null_Test_Case_" + str(count) + "---"
        if table_mapping_row['Historization algorithm'] == 'HISTORY':
            target_table = table_mapping_row['Target table name']
            process_name = table_mapping_row['Mapping name']
            start_date = TransformDDL.get_core_tbl_sart_date_column(core_tables, target_table)
            call_line1 = "SELECT * FROM "+cf.base_DB+'.'+target_table+" WHERE "+start_date+" IS NULL AND"
            call_line2 = "PROCESS_NAME = 'TXF_CORE_"+process_name+"';"+'\n\n\n'
            hist_test_case_exp = hist_check_name_line + '\n' + call_line1 + '\n' + call_line2
            f.write(hist_test_case_exp)
    f.close()
