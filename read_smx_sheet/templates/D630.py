from read_smx_sheet.app_Lib import functions as funcs
from read_smx_sheet.Logging_Decorator import Logging_decorator


@Logging_decorator
def d630(cf, source_output_path, Table_mapping):
    file_name = funcs.get_file_name(__file__)
    f = funcs.WriteFile(source_output_path, file_name, "sql")
    for table_maping_index, table_maping_row in Table_mapping.iterrows(): #& (source_name=='CRA')& (Table_mapping['Mapping name'] == 'L1_PRTY_RLTD_L0_CRA_COMPANY_PERSON')].iterrows():
        process_type = table_maping_row['Historization algorithm']
        layer = str(table_maping_row['Layer'])
        table_maping_name = str(table_maping_row['Mapping name'])
        tbl_name=table_maping_row['Target table name']
        ctl_id = funcs.single_quotes(cf.gcfr_ctl_Id)
        stream_key = funcs.single_quotes(cf.gcfr_stream_key)
        process_name = "TXF_" + layer + "_" + table_maping_name
        reg_exp = "EXEC " + cf.MACRO_DB+".GCFR_Register_Process('"+process_name+"','',"
        if process_type == "SNAPSHOT":
            process_type_cd = cf.gcfr_snapshot_txf_process_type
        else:
            if process_type == 'INSERT':
                process_type_cd = cf.gcfr_insert_txf_process_type
            else:
                process_type_cd = cf.gcfr_others_txf_process_type

        process_type_cd = funcs.single_quotes(process_type_cd)
        # print(process_type_cd)
        reg_exp+=process_type_cd+','+ctl_id+','+stream_key+",'"+cf.INPUT_VIEW_DB+"','"+process_name+"_IN',"
        reg_exp+="'"+cf.core_view+"','"+tbl_name+"','"+cf.core_table+"','"+tbl_name+"','"+cf.TMP_DB+"',,,,1,0,1,0);\n"
        f.write(reg_exp)
    f.close()

