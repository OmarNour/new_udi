from read_smx_sheet.app_Lib import functions as funcs
import calendar
import time
import traceback


def process_check(cf, source_output_path, source_name, Table_mapping):
    file_name = funcs.get_file_name(__file__)
    f = funcs.WriteFile(source_output_path, file_name, "sql")
    count = 1
    try:
        for table_maping_index, table_maping_row in Table_mapping.iterrows():
            process_name_line = "---PROCESS_CHECK_Test_Case_" + str(count) + "---"
            layer = str(table_maping_row['Layer'])
            process_name = str(table_maping_row['Mapping name'])

            target_table_name = str(table_maping_row['Target table name'])
            tbl_name = table_maping_row['Target table name']

            process_check_test_case_exp = "SELECT * FROM " + cf.process_check_DB + "." + tbl_name + " WHERE PROCESS_NAME = 'TXF_" + layer + "_" + process_name + "' ;\n \n"
            process_check_test_case_exp = process_name_line + "\n" + process_check_test_case_exp
            f.write(process_check_test_case_exp)
            count = count + 1

    except:
        funcs.TemplateLogError(cf.output_path, source_output_path, file_name, traceback.format_exc()).log_error()
    f.close()
