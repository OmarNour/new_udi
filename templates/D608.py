import pandas as pd
import app_Lib.manage_directories as md
import parameters.parameters as pm
import os
import app_Lib.functions as funcs
import templates as tmp

def t608(source_output_path, Core_tables, BMAP_values):
    file_name = funcs.get_file_name(__file__)
    f = open(source_output_path + "/" + file_name + ".sql", "w+", encoding="utf-8")
    lkp_tables = tmp.TransformDDL.get_lkp_tbls(Core_tables)
    lkp_tables_names=tmp.TransformDDL.get_lkp_tbls_names(Core_tables)

    for tbl_name in lkp_tables_names:
        for bmap_values_indx,  bmap_values_row in BMAP_values[(BMAP_values['Code set name'] == tbl_name)].iterrows():
            tbl_pk = tmp.TransformDDL.get_trgt_pk(Core_tables,tbl_name)
            print(tbl_name, ":",BMAP_values['EDW code'])
            del_st = "DELETE FROM "+pm.gdev1t_base+"."+tbl_name+" WHERE "+tbl_pk+" = '" + str(bmap_values_row['EDW code']) + "';\n"
            insert_st = "INSERT INTO "+pm.gdev1t_base+"."+tbl_name+"("

        f.write(del_st)
    f.close()

