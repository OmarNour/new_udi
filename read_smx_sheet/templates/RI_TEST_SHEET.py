from read_smx_sheet.app_Lib import functions as funcs
from read_smx_sheet.app_Lib import TransformDDL
import calendar
import time
import traceback


def ri_check(cf, source_output_path, table_mapping, RI_relations):
    file_name = funcs.get_file_name(__file__)
    f = funcs.WriteFile(source_output_path, file_name, "sql")
    count = 1
    try:
        core_tables_list = TransformDDL.get_src_core_tbls(table_mapping)
        for table_name in core_tables_list:
            for ri_table_index,ri_table_row in RI_relations.iterrows():
                RI_line = "---RI_Test_Case_" + str(count) + "---"
                if ri_table_row['CHILD TABLE'] == table_name :
                    call_line1 = "SELECT " + cf.base_DB + '.' + ri_table_row['CHILD TABLE'] + '.' + ri_table_row['CHILD COLUMN']
                    call_line2 = " FROM " + ri_table_row['CHILD TABLE'] + " LEFT JOIN " + cf.base_DB + '.' + ri_table_row['PARENT TABLE']
                    call_line3 = " ON " + ri_table_row['CHILD TABLE'] + '.' + ri_table_row['CHILD COLUMN']
                    call_line4 = " = " + ri_table_row['PARENT TABLE'] + '.' + ri_table_row['PARENT COLUMN']
                    call_line5 = " WHERE " + cf.base_DB + '.' + ri_table_row['PARENT TABLE'] + '.' + ri_table_row['PARENT COLUMN'] + " IS NULL"
                    call_line6 = " AND " + cf.base_DB + '.' + ri_table_row['CHILD TABLE'] + '.' + ri_table_row['CHILD COLUMN'] + " IS NOT NULL"

                    call_exp = RI_line+"\n"+call_line1+'\n'+call_line2 +'\n'+ call_line3+call_line4+'\n'+call_line5+call_line6+'\n\n\n'
                    f.write(call_exp)
                    count = count + 1

    except:
        funcs.TemplateLogError(cf.output_path, source_output_path, file_name, traceback.format_exc()).log_error()
    f.close()
