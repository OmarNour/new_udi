from read_smx_sheet.app_Lib import functions as funcs
from read_smx_sheet.app_Lib import TransformDDL
from read_smx_sheet.Logging_Decorator import Logging_decorator


@Logging_decorator
def bmap_null_check(cf, source_output_path, table_mapping, core_tables,BMAP_values):
    file_name = funcs.get_file_name(__file__)
    f = funcs.WriteFile(source_output_path, file_name, "sql")
    core_tables_look_ups = core_tables[core_tables['Is lookup'] == 'Y']
    count = 1
    lookup_tables_list = TransformDDL.get_src_lkp_tbls(table_mapping, core_tables)
    code_set_names = TransformDDL.get_code_set_names(BMAP_values)
    for code_set_name in code_set_names:
        for table_name in lookup_tables_list:
            if table_name==code_set_name:
                CD_column = ''
                DESC_column = ''
                for core_table_index, core_table_row in core_tables_look_ups.iterrows():
                    if core_table_row['Table name'] == table_name:
                        if str(core_table_row['Column name']).endswith(str('_CD')) and core_table_row['PK'] == 'Y':
                            CD_column = core_table_row['Column name']
                        if str(core_table_row['Column name']).endswith(str('_DESC')):
                            DESC_column = core_table_row['Column name']
                bmap_check_name_line = "---bmap_null_check_Test_Case_" + str(count) + "---"
                call_line1 = "SEL * FROM " + cf.base_DB + "." + table_name
                call_line2 = " WHERE " + CD_column + " IS NULL" + " OR " + DESC_column + " IS NULL;\n\n\n"
                call_exp = bmap_check_name_line + "\n" + call_line1 + call_line2
                f.write(call_exp)
                count = count + 1
    f.close()
