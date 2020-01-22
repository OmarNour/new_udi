from read_smx_sheet.app_Lib import functions as funcs
from read_smx_sheet.app_Lib import TransformDDL
import traceback


def bmap_null_check(cf, source_output_path, table_mapping, core_tables):
    file_name = funcs.get_file_name(__file__)
    f = funcs.WriteFile(source_output_path, file_name, "sql")
    core_tables_look_ups = core_tables[core_tables['Is lookup'] == 'Y']
    count = 1

    try:
        lookup_tables_list = TransformDDL.get_src_lkp_tbls(table_mapping, core_tables)
        for table_name in lookup_tables_list:
            CD_column = ''
            DESC_column = ''
            for core_table_index, core_table_row in core_tables_look_ups.iterrows():
                if core_table_row['Table name'] == table_name:
                    if str(core_table_row['Column name']).endswith(str('_CD')):
                        CD_column = core_table_row['Column name']
                    if str(core_table_row['Column name']).endswith(str('_DESC')):
                        DESC_column = core_table_row['Column name']
            bmap_check_name_line = "---bmap_null_check_Test_Case_" + str(count) + "---"
            call_line1 = "SEL * FROM " + cf.base_DB + "." + table_name
            call_line2 = " WHERE " + CD_column + " IS NULL" + " OR " + DESC_column + " IS NULL;\n\n\n"
            call_exp = bmap_check_name_line + "\n" + call_line1 + call_line2
            f.write(call_exp)
            count = count + 1
    except:
        funcs.TemplateLogError(cf.output_path, source_output_path, file_name, traceback.format_exc()).log_error()

    f.close()
