from read_smx_sheet.app_Lib import functions as funcs
import traceback


def nulls_check(cf, source_output_path, table_mapping, core_tables):
    file_name = funcs.get_file_name(__file__)
    f = funcs.WriteFile(source_output_path, file_name, "sql")
    try:
        nulls_test_case_exp=''
        count=1
        for table_mapping_index, table_mapping_row in table_mapping.iterrows():
            for core_table_index, core_table_row in core_tables.iterrows():
                if core_table_row['Table name'] == table_mapping_row['Target table name'] and core_table_row['Mandatory'] == 'Y':
                    nulls_test_case_exp += "---Null_Test_Case_" + str(count) + "---"+'\n'+"SEL * FROM " +cf.base_DB +"."+core_table_row['Table name']+" WHERE " + core_table_row['Column name'] + " IS NULL AND PROCESS_NAME='TXF_CORE_"+table_mapping_row['Mapping name']+"';"+'\n'+'\n'
                    count = count+1
        f.write(nulls_test_case_exp)
    except:
        funcs.TemplateLogError(cf.output_path, source_output_path, file_name, traceback.format_exc()).log_error()

    f.close()