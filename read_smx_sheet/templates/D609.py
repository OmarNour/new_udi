from read_smx_sheet.app_Lib import functions as funcs
from read_smx_sheet.app_Lib import TransformDDL
from read_smx_sheet.Logging_Decorator import Logging_decorator


@Logging_decorator
def d609(cf, source_output_path, Table_mapping, Core_tables):
    file_name = funcs.get_file_name(__file__)
    f = funcs.WriteFile(source_output_path, file_name, "sql")
    for table_mapping_index, table_mapping_row in Table_mapping[Table_mapping['Excluded keys'] != ''].iterrows():
        table_pks = TransformDDL.get_trgt_pk_list(Core_tables, table_mapping_row['Target table name'])
        excluded_keys = str(table_mapping_row['Excluded keys']).upper()
        excluded_keys = excluded_keys.split(',')
        filtered_keys = [x for x in table_pks if x not in excluded_keys]
        if len(filtered_keys) == 0:
            funcs.TemplateLogError(cf.output_path, source_output_path, file_name, "All the table's ["+table_mapping_row['Target table name']+"] keys are excluded and base tables must have at least one key in it.").log_error()
        for table_pk in filtered_keys:
            insert_statment = "INSERT INTO " + cf.keycol_override_base + "\n(Out_DB_Name\n,Out_Object_Name\n,Key_Column\n,Update_date"
            insert_statment = insert_statment + "\n,Update_User\n,Update_Ts\n,Process_Name) \n"
            insert_statment = insert_statment + "SELECT '" + cf.base_view + "',\n'" + table_mapping_row['Target table name']
            insert_statment = insert_statment + "',\n'" + table_pk + "',\n"+"CURRENT_DATE,\nCURRENT_USER,\nCURRENT_TIMESTAMP,\n"
            insert_statment = insert_statment + "'TXF_"+table_mapping_row['Layer']+'_'+table_mapping_row['Mapping name']+"'\n "
            insert_statment = insert_statment + " WHERE '" + table_pk + "' NOT IN (SELECT Key_Column FROM " + cf.keycol_override_base + " WHERE PROCESS_NAME = " + "'TXF_"+table_mapping_row['Layer']+'_'+table_mapping_row['Mapping name']+"');\n"
            f.write(insert_statment+'\n\n')
    f.close()
