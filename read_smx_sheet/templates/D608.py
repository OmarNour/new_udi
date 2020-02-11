from read_smx_sheet.app_Lib import functions as funcs
from read_smx_sheet.app_Lib import TransformDDL
import traceback

# def d608(source_output_path, Core_tables, BMAP_values):
#     file_name = funcs.get_file_name(__file__)
#     f = open(source_output_path + "/" + file_name + ".sql", "w+", encoding="utf-8")
#     lkp_tables = TransformDDL.get_lkp_tbls(Core_tables)
#     lkp_tables_names = TransformDDL.get_lkp_tbls_names(Core_tables)
#     for tbl_name in lkp_tables_names:
#         tbl_pk = TransformDDL.get_trgt_pk(Core_tables, tbl_name)
#         # f.write(tbl_name)
#         # f.write(": ")
#         # f.write(str(tbl_pk))
#         # f.write("\n")
#         del_st = ""
#         for bmap_values_indx,  bmap_values_row in BMAP_values[(BMAP_values['Code set name'] == tbl_name)].iterrows():
#
#             del_st = "DELETE FROM "+pm.core_table+"."+tbl_name+" WHERE "+tbl_pk+" = '" + str(bmap_values_row['EDW code']) + "';\n"
#             insert_into_st = "INSERT INTO "+pm.core_table+"."+tbl_name+"("+ TransformDDL.get_lkp_tbl_Cols(Core_tables,tbl_name)+") VALUES \n"
#             insert_values = "(" + str(bmap_values_row["EDW code"]) + ", '" + str(bmap_values_row["Description"]) + "'); \n"
#             insert_st=insert_into_st+insert_values
#             f.write(del_st)
#             f.write(insert_st)
#     f.close()


def d608(cf, source_output_path,source_name,STG_tables, Core_tables, BMAP_values):
    file_name = funcs.get_file_name(__file__)
    f = funcs.WriteFile(source_output_path, file_name, "sql")
    try:
        src_code_set_names = funcs.get_src_code_set_names(STG_tables,source_name)
        code_set_names= TransformDDL.get_code_set_names(BMAP_values)
        for code_set_name in src_code_set_names:
            for code_set in code_set_names:
                if code_set_name == code_set:
                    tbl_pk = TransformDDL.get_trgt_pk(Core_tables, code_set)
                    columns = TransformDDL.get_lkp_tbl_Cols(Core_tables, code_set)
                    flag_look_up = TransformDDL.get_is_look_up_flag(Core_tables,code_set)
                    print(flag_look_up)
                    if flag_look_up:
                        for bmap_values_indx, bmap_values_row in BMAP_values[(BMAP_values['Code set name'] == code_set) & (BMAP_values['Layer'] == 'CORE')][['EDW code','Description']].drop_duplicates().iterrows():
                            del_st = "DELETE FROM " + cf.core_table + "." + code_set + " WHERE " + tbl_pk + " = '" + str(bmap_values_row['EDW code']) + "';\n"
                            insert_into_st = "INSERT INTO " + cf.core_table + "." + code_set + "(" + columns + ")\nVALUES "
                            insert_values = ''
                            if columns.count(',') == 1:
                                insert_values = "(" + str(bmap_values_row["EDW code"]) + ", '" + str(bmap_values_row["Description"]) + "');\n\n"
                            elif columns.count(',') == 2:
                                insert_values = "(" + str(bmap_values_row["EDW code"]) + ", '" + str(bmap_values_row["Description"]) + ", '" + str(bmap_values_row["Description"])  + "');\n\n"
                            insert_st = insert_into_st + insert_values
                            f.write(del_st)
                            f.write(insert_st)
    except:
        funcs.TemplateLogError(cf.output_path, source_output_path, file_name, traceback.format_exc()).log_error()

    f.close()