from app_Lib import functions as funcs
from Logging_Decorator import Logging_decorator


@Logging_decorator
def data_src_check(cf, source_output_path,source_name,table_mapping, column_mapping):
    file_name = funcs.get_file_name(__file__)
    f = funcs.WriteFile(source_output_path, file_name, "sql")
    Column_mapping_Source_data_src = column_mapping[column_mapping['Column name'].str.endswith(str('_CD'))]
    Column_mapping_Source_data_src = Column_mapping_Source_data_src[Column_mapping_Source_data_src['Transformation rule'].astype(str).str.isdigit()]

    count = 1
    for table_mapping_index,table_mapping_row in table_mapping.iterrows():
        for column_mapping_index, column_mapping_row in Column_mapping_Source_data_src.iterrows():
            if table_mapping_row['Mapping name'] == column_mapping_row['Mapping name']:
                target_table_name = str(table_mapping_row['Target table name'])
                target_column_name = str(column_mapping_row['Column name'])
                target_column_value = str(column_mapping_row['Transformation rule'])
                target_column_process = str(column_mapping_row['Mapping name'])

                call_line1 = "SEL * FROM " + cf.base_DB + "." + target_table_name
                call_line2 = " WHERE " + target_column_name + "<>" + target_column_value + ' and process_name= '
                call_line3 = "'TXF_CORE_" + target_column_process + "';\n\n\n"

                data_src_name_line = "---data_src_Test_Case_" + str(count) + "---"
                call_exp = data_src_name_line + "\n" + call_line1 + call_line2 + call_line3
                f.write(call_exp)
                count = count + 1
    f.close()
