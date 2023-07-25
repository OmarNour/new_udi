from app_Lib import functions as funcs
from app_Lib import TransformDDL
from Logging_Decorator import Logging_decorator


@Logging_decorator
def d005(cf, source_output_path,STG_tables, Core_tables, BMAP_values,BMAP):
    file_name = funcs.get_file_name(__file__)
    f = funcs.WriteFile(source_output_path, file_name, "sql")
    stg_set_names = funcs.get_src_code_domain_names(STG_tables,BMAP)[0]
    stg_domain_ids = funcs.get_src_code_domain_names(STG_tables, BMAP)[1]


    for i in range(0,len(stg_set_names)):
        bmaps_sliced = BMAP_values[(BMAP_values['Code set name'] == stg_set_names[i]) & (BMAP_values['Code domain ID'] == stg_domain_ids[i]) & (BMAP_values['Layer'] == 'CORE')][['EDW code', 'Description']].drop_duplicates()
        code_set = stg_set_names[i]
        tbl_pk = TransformDDL.get_trgt_pk(Core_tables, code_set)
        columns = TransformDDL.get_lkp_tbl_Cols(Core_tables, code_set)
        for bmap_values_indx, bmap_values_row in bmaps_sliced.iterrows():
            del_st = "--DELETE FROM " + cf.core_table + "." + code_set + " WHERE " + tbl_pk + " = '" + str(
                bmap_values_row['EDW code']) + "';\n"
            insert_into_st = "INSERT INTO " + cf.core_table + "." + code_set + "(" + columns + ")\n "
            insert_values = ''
            if columns.count(',') == 1:
                # insert_values = "(" + str(bmap_values_row["EDW code"]) + ", '" + str(bmap_values_row["Description"]) + "');\n\n"
                insert_values = "SELECT " + str(bmap_values_row["EDW code"]) + ", '" + str(
                    bmap_values_row["Description"]) + "'"
                insert_values += " WHERE '" + str(bmap_values_row["EDW code"]) + "_" + str(
                    bmap_values_row["Description"]) + "' NOT IN (SELECT " + columns.replace(",",
                                                                                            "||' _ ' || ") + " FROM " + cf.core_table + "." + code_set + ");\n\n"
            elif columns.count(',') == 2:
                # insert_values = "(" + str(bmap_values_row["EDW code"]) + ", '" + str(bmap_values_row["Description"]) + "','" + str(bmap_values_row["Description"]) + "');\n\n"
                insert_values = "SELECT " + str(bmap_values_row["EDW code"]) + ", '" + str(
                    bmap_values_row["Description"]) + "','" + str(bmap_values_row["Description"]) + "'"
                insert_values += " WHERE '" + str(bmap_values_row["EDW code"]) + "_" + str(
                    bmap_values_row["Description"]) + "_" + str(
                    bmap_values_row["Description"]) + "' NOT IN (SELECT " + columns.replace(
                    ",", "|| '_' || ") + " FROM " + cf.core_table + "." + code_set + ");\n\n"
            insert_st = insert_into_st + insert_values
            f.write(del_st)
            f.write(insert_st)
    f.close()