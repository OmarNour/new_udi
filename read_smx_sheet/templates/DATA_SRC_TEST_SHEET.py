from read_smx_sheet.app_Lib import functions as funcs
from read_smx_sheet.app_Lib import TransformDDL
import traceback


def data_src_check(cf, source_output_path,source_name, column_mapping):
    file_name = funcs.get_file_name(__file__)
    f = funcs.WriteFile(source_output_path, file_name, "sql")
    Column_mapping_Source=column_mapping[column_mapping['Mapping name'].str.contains(str(source_name))]
    Column_mapping_Source['target_tablename'] = Column_mapping_Source['Mapping name'].apply(lambda st: st[st.find("L1_") + 3:st.find("_L0")])
    Column_mapping_Source_data_src = Column_mapping_Source[Column_mapping_Source['Column name'].str.endswith(str('_CD'))]
    Column_mapping_Source_data_src = Column_mapping_Source_data_src[Column_mapping_Source_data_src['Transformation rule'].astype(str).str.isdigit()]

    count = 1
    try:
        for column_mapping_index, column_mapping_row in Column_mapping_Source_data_src.iterrows():
            target_table_name = str(column_mapping_row['target_tablename'])
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

    except:
        funcs.TemplateLogError(cf.output_path, source_output_path, file_name, traceback.format_exc()).log_error()

    f.close()