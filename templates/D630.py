import pandas as pd
import app_Lib.manage_directories as md
import parameters.parameters as pm
import os
import app_Lib.functions as funcs
import templates as tmp


def d630(source_output_path, source_name, Table_mapping):

    file_name = funcs.get_file_name(__file__)
    f = open(source_output_path + "/" + file_name + ".sql", "w+", encoding="utf-8")
    for table_maping_index, table_maping_row in Table_mapping[(Table_mapping['Source'] == source_name)].iterrows(): #& (source_name=='CRA')& (Table_mapping['Mapping name'] == 'L1_PRTY_RLTD_L0_CRA_COMPANY_PERSON')].iterrows():
        process_type = table_maping_row['Historization algorithm']
        layer = str(table_maping_row['Layer'])
        table_maping_name = str(table_maping_row['Mapping name'])
        tbl_name=table_maping_row['Target table name']
        ctl_id = funcs.single_quotes(pm.gcfr_ctl_Id)
        stream_key = funcs.single_quotes(pm.gcfr_stream_key)
        process_name = "TXF_" + layer + "_" + table_maping_name
        reg_exp = "EXEC " + pm.MACRO_DB+".GCFR_Register_Process'"+process_name+"','',"
        if process_type == "SNAPSHOT":
            process_type_cd = pm.gcfr_snapshot_txf_process_type
        else:
            if process_type == 'INSERT':
                process_type_cd = pm.gcfr_insert_txf_process_type
            else:
                process_type_cd = pm.gcfr_others_txf_process_type

        process_type_cd = funcs.single_quotes(process_type_cd)
        # print(process_type_cd)
        reg_exp+=process_type_cd+','+ctl_id+','+stream_key+",'"+pm.INPUT_VIEW_DB+"','"+process_name+"_IN',"
        reg_exp+="'"+pm.core_view+"','"+tbl_name+"','"+pm.core_table+"','"+tbl_name+"','"+pm.TMP_DB+"',,,,1,0,1,0);\n"
        f.write(reg_exp)
    f.close()

