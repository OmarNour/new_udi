from read_smx_sheet.app_Lib import functions as funcs
import calendar
import time
from read_smx_sheet.Logging_Decorator import Logging_decorator


@Logging_decorator
def d640(cf, source_output_path, source_name, Table_mapping):
    file_name = funcs.get_file_name(__file__)
    f = funcs.WriteFile(source_output_path, file_name, "sql")
    for table_maping_index, table_maping_row in Table_mapping.iterrows():
        process_type = table_maping_row['Historization algorithm']
        layer = str(table_maping_row['Layer'])
        table_maping_name = str(table_maping_row['Mapping name'])
        tbl_name = table_maping_row['Target table name']
        process_name = "TXF_" + layer + "_" + table_maping_name
        call_exp = "CALL "+cf.APPLY_DB+".APP_APPLY('"+process_name+"','"+tbl_name+"','"+process_type+"',"
        if cf.db_prefix == 'GDEVP1':
            call_exp += "NULL,'"+source_name+"',NULL,Y,X,Z);\n"
        else :
            call_exp += "NULL,'"+source_name+"',NULL,Y,X);\n"
        f.write(call_exp)
    f.close()
