from read_smx_sheet.app_Lib import functions as funcs
from read_smx_sheet.app_Lib import TransformDDL
import traceback


def bmap_unmatched_values_check(cf, source_output_path, table_mapping, core_tables,BMAP):
    file_name = funcs.get_file_name(__file__)
    f = funcs.WriteFile(source_output_path, file_name, "sql")
    core_tables_look_ups = core_tables[core_tables['Is lookup'] == 'Y']
    count = 1

    try:
        lookup_tables_list = TransformDDL.get_src_lkp_tbls(table_mapping, core_tables)
        for table_name in lookup_tables_list:
            CD_column = ''
            for core_table_index, core_table_row in core_tables_look_ups.iterrows():
                if core_table_row['Table name'] == table_name:
                    if str(core_table_row['Column name']).endswith(str('_CD')):
                        CD_column = core_table_row['Column name']
                for bmap_table_index,bmap_table_row in BMAP.iterrows():
                    if bmap_table_row['Code set name'] == table_name:
                        CD_SET_ID_val = str(bmap_table_row['Code set ID'])
            bmap_check_name_line = "---bmap_null_check_Test_Case_" + str(count) + "---"
            call_line1 = "SEL COALESCE(EDW_CODE,'NOT IN BMAP TABLE BUT IN BASE TABLE')AS EDW_CODE,\n"
            call_line2 = "COALESCE(" + CD_column +",'NOT IN BASE TABLE BUT IN BMAP TABLE')AS BASE_CODE\n"
            call_line3 = " FROM GPROD1V_UTLFW.BMAP_STANDARD_MAP FULL OUTER JOIN "+cf.base_DB+'.'+table_name+'\n'
            call_line4 = "ON GPROD1V_UTLFW.BMAP_STANDARD_MAP.EDW_CODE = "+cf.base_DB+'.'+table_name+'.'+CD_column+'\n'
            call_line5 = "WHERE EDW_CODE IS NULL OR "+CD_column+" IS NULL AND CODE_SET_ID = "+CD_SET_ID_val+'\n\n\n'
            call_exp = bmap_check_name_line + "\n" + call_line1 + call_line2+call_line3+call_line4+call_line5
            f.write(call_exp)
            count = count + 1
    except:
        funcs.TemplateLogError(cf.output_path, source_output_path, file_name, traceback.format_exc()).log_error()

    f.close()
