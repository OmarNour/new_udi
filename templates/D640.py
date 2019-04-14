import pandas as pd
import app_Lib.manage_directories as md
import parameters.parameters as pm
import os
import app_Lib.functions as funcs
import templates as tmp
import calendar;
import time;



def d640(source_output_path, source_name, Table_mapping):
    file_name = funcs.get_file_name(__file__)
    f = open(source_output_path + "/" + file_name + ".sql", "w+", encoding="utf-8")
    for table_maping_index, table_maping_row in Table_mapping[(Table_mapping['Source'] == source_name)].iterrows():
        process_type = table_maping_row['Historization algorithm']
        layer = str(table_maping_row['Layer'])
        table_maping_name = str(table_maping_row['Mapping name'])
        tbl_name = table_maping_row['Target table name']
        run_id=calendar.timegm(time.gmtime())
        load_id = calendar.timegm(time.gmtime())
        process_name = "TXF_" + layer + "_" + table_maping_name
        call_exp="CALL "+pm.APPLY_DB+".APP_APPLY('"+process_name+"','"+tbl_name+"','"+process_type+"',"
        call_exp+=str(run_id)+",'"+source_name+"',"+str(load_id)+",Y,X);\n"
        f.write(call_exp)
    f.close()
