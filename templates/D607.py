import pandas as pd
import app_Lib.manage_directories as md
import parameters.parameters as pm
import os
import app_Lib.functions as funcs
import templates as tmp
import app_Lib.TransformDDL as TransformDDL

def d607(source_output_path, Core_tables):
    file_name = funcs.get_file_name(__file__)
    f = open(source_output_path + "/" + file_name + ".sql", "w+", encoding="utf-8")

    lkp_tables = TransformDDL.get_lkp_tbls(Core_tables)
    lkp_tables_names=TransformDDL.get_lkp_tbls_names(Core_tables)

    for tbl_name in lkp_tables_names:
        lkp_ddl = ''
        lkp_tbl_header = 'CREATE SET TABLE ' + pm.core_view + '.' + tbl_name + ', FALLBACK (\n'
        for lkp_tbl_indx, lkp_tbl_row in lkp_tables[(lkp_tables['Table name'] == tbl_name)].iterrows():
            lkp_ddl += lkp_tbl_row['Column name'] + ' ' + lkp_tbl_row['Data type'] + ' '
            if (lkp_tbl_row['Data type'].find('VARCHAR') != -1):
                lkp_ddl += 'CHARACTER SET UNICODE NOT CASESPECIFIC' + ' '
            if (lkp_tbl_row['Mandatory'] == 'Y'):
                lkp_ddl += 'NOT NULL '

            lkp_ddl += ',\n'
        lkp_ddl = lkp_ddl[0:len(lkp_ddl) - 2]
        lkp_tbl_pk = ') UNIQUE PRIMARY INDEX (' + TransformDDL.get_trgt_pk(lkp_tables, tbl_name) + '); \n  \n'
        lkp_tbl_ddl = lkp_tbl_header + lkp_ddl +"\n"+ lkp_tbl_pk
        f.write(lkp_tbl_ddl)



    f.close()