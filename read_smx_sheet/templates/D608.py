from read_smx_sheet.parameters import parameters as pm
from read_smx_sheet.app_Lib import functions as funcs
from read_smx_sheet.app_Lib import TransformDDL


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


def d608(source_output_path, Core_tables, BMAP_values):
    file_name = funcs.get_file_name(__file__)
    f = open(source_output_path + "/" + file_name + ".sql", "w+", encoding="utf-8")

    try:
        code_set_names= TransformDDL.get_code_set_names(BMAP_values)
        for code_set in code_set_names:
            del_st = ""
            insert_into_st = ""
            insert_values = ""
            insert_st = ""
            tbl_pk = TransformDDL.get_trgt_pk(Core_tables, code_set)
            # Bmap_Vals=TransformDDL.get_bmap_values_for_codeset(BMAP_values,code_set)
            for bmap_values_indx, bmap_values_row in BMAP_values[(BMAP_values['Code set name'] == code_set) & (BMAP_values['Layer'] == 'CORE')][['EDW code','Description']].drop_duplicates().iterrows():
                del_st = "DELETE FROM " + pm.core_table + "." + code_set + " WHERE " + tbl_pk + " = '" + str(bmap_values_row['EDW code']) + "';\n"
                insert_into_st = "INSERT INTO " + pm.core_table + "." + code_set + "(" + TransformDDL.get_lkp_tbl_Cols(Core_tables, code_set) + ")\nVALUES "
                insert_values = "(" + str(bmap_values_row["EDW code"]) + ", '" + str(bmap_values_row["Description"]) + "');\n\n"
                insert_st = insert_into_st + insert_values
                f.write(del_st)
                f.write(insert_st)
    except:
        pass
    f.close()