from read_smx_sheet.app_Lib import functions as funcs
from read_smx_sheet.app_Lib import TransformDDL
from read_smx_sheet.Logging_Decorator import Logging_decorator


@Logging_decorator
def d608(cf, source_output_path,source_name,STG_tables, Core_tables, BMAP_values):
    file_name = funcs.get_file_name(__file__)
    f = funcs.WriteFile(source_output_path, file_name, "sql")
    src_code_set_names = funcs.get_src_code_set_names(STG_tables,source_name)
    code_set_names= TransformDDL.get_code_set_names(BMAP_values)
    for code_set_name in src_code_set_names:
        for code_set in code_set_names:
            if code_set_name == code_set:
                tbl_pk = TransformDDL.get_trgt_pk(Core_tables, code_set)
                columns = TransformDDL.get_lkp_tbl_Cols(Core_tables, code_set)
                flag_look_up = TransformDDL.get_is_look_up_flag(Core_tables,code_set)
                if flag_look_up:
                    for bmap_values_indx, bmap_values_row in BMAP_values[(BMAP_values['Code set name'] == code_set) & (BMAP_values['Layer'] == 'CORE')][['EDW code','Description']].drop_duplicates().iterrows():
                        del_st = "DELETE FROM " + cf.core_table + "." + code_set + " WHERE " + tbl_pk + " = '" + str(bmap_values_row['EDW code']) + "';\n"
                        insert_into_st = "INSERT INTO " + cf.core_table + "." + code_set + "(" + columns + ")\nVALUES "
                        insert_values = ''
                        if columns.count(',') == 1:
                            insert_values = "(" + str(bmap_values_row["EDW code"]) + ", '" + str(bmap_values_row["Description"]) + "');\n\n"
                        elif columns.count(',') == 2:
                            insert_values = "(" + str(bmap_values_row["EDW code"]) + ", '" + str(bmap_values_row["Description"]) + "','" + str(bmap_values_row["Description"]) + "');\n\n"
                        insert_st = insert_into_st + insert_values
                        f.write(del_st)
                        f.write(insert_st)
    f.close()