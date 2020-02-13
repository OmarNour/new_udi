from read_smx_sheet.app_Lib import functions as funcs
from read_smx_sheet.app_Lib import TransformDDL
from read_smx_sheet.Logging_Decorator import Logging_decorator


@Logging_decorator
def process_check(cf, source_output_path, source_name, Table_mapping,Core_tables):
    file_name = funcs.get_file_name(__file__)
    f = funcs.WriteFile(source_output_path, file_name, "sql")
    count = 1
    for table_maping_index, table_maping_row in Table_mapping.iterrows():
        process_name_line = "---PROCESS_CHECK_Test_Case_" + str(count) + "---"
        process_name = str(table_maping_row['Mapping name'])
        tbl_name = table_maping_row['Target table name']
        table_pks=TransformDDL.get_trgt_pk(Core_tables,tbl_name)
        table_pks_splitted = table_pks.split(',')
        call_line1 = "SEL * FROM " + cf.INPUT_VIEW_DB + ".TXF_CORE_" + process_name + "_IN INP_VIEW"
        call_line2 = " WHERE NOT EXISTS ( SEL 1 FROM " + cf.base_view + "." + tbl_name + " BASE_VIEW"
        call_line3 = " WHERE INP_VIEW." + table_pks_splitted[0] + " = BASE_VIEW." + table_pks_splitted[0]
        process_check_test_case_exp = call_line1 + '\n' + call_line2 + '\n' + call_line3 + ');\n\n\n'
        process_check_test_case_exp = process_name_line + "\n" + process_check_test_case_exp
        f.write(process_check_test_case_exp)
        count = count + 1
    f.close()
