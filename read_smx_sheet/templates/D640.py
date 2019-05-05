from read_smx_sheet.app_Lib import functions as funcs
import calendar
import time
import traceback


def d640(cf, source_output_path, source_name, Table_mapping):
    file_name = funcs.get_file_name(__file__)
    f = funcs.WriteFile(source_output_path, file_name, "sql")
    try:
        for table_maping_index, table_maping_row in Table_mapping.iterrows():
            process_type = table_maping_row['Historization algorithm']
            layer = str(table_maping_row['Layer'])
            table_maping_name = str(table_maping_row['Mapping name'])
            tbl_name = table_maping_row['Target table name']
            run_id=calendar.timegm(time.gmtime())
            load_id = calendar.timegm(time.gmtime())
            process_name = "TXF_" + layer + "_" + table_maping_name
            call_exp="CALL "+cf.APPLY_DB+".APP_APPLY('"+process_name+"','"+tbl_name+"','"+process_type+"',"
            call_exp+=str(run_id)+",'"+source_name+"',"+str(load_id)+",Y,X);\n"
            f.write(call_exp)

    except:
        funcs.TemplateLogError(cf.output_path, source_output_path, file_name, traceback.format_exc()).log_error()
    f.close()
