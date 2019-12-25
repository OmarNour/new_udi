from read_smx_sheet.app_Lib import functions as funcs
import traceback


def cso_check(cf, source_output_path, source_name, Column_mapping):
    file_name = funcs.get_file_name(__file__)
    f = funcs.WriteFile(source_output_path, file_name, "sql")
    Column_mapping_Source=Column_mapping[Column_mapping['Mapping name'].str.contains(str(source_name))]
    Column_mapping_NID = Column_mapping_Source[Column_mapping_Source['Mapped to column'] == 'SK_NATIONAL_ID']
    Column_mapping_NID['target_tablename'] = Column_mapping_NID['Mapping name'].apply(lambda st: st[st.find("L1_") + 3:st.find("_L0")])
    Column_mapping_drop_duplicate=Column_mapping_NID.drop_duplicates(['target_tablename', 'Column name'])

    count = 1
    try:
        for table_maping_index, column_mapping_row in Column_mapping_drop_duplicate.iterrows():

            target_table_name = str(column_mapping_row['target_tablename'])
            target_coloumn_name= str(column_mapping_row['Column name'])

            call_line1 = "SEL alias."+target_coloumn_name+" from "
            call_line2 = cf.base_DB+"."+target_table_name+" alias left join GPROD1V_UTLFW.BKEY_1_PRTY "
            call_line3 = "on alias." + target_coloumn_name + "= GPROD1V_UTLFW.BKEY_1_PRTY.EDW_KEY" +" left join STG_LAYER.CSO_NEW_PERSON B "
            call_line4 = "on trim(cast B.national_id) as varchar(100))) = GPROD1V_UTLFW.BKEY_1_PRTY.source_key "
            call_line5 = "where trim(cast B.national_id) as varchar(100))) is null "
            call_line6 = "AND GPROD1V_UTLFW.BKEY_1_PRTY.DOMAIN_ID=1 "
            call_line7 = "AND alias.PROCESS_NAME LIKE '%" + source_name +"%';\n\n\n"

            process_name_line = "---CSO_CHECK_Test_Case_" + str(count) + "---"
            call_exp = process_name_line+ "\n" +call_line1+call_line2+call_line3+call_line4+call_line5+call_line6+call_line7
            f.write(call_exp)
            count = count + 1
    except:
        funcs.TemplateLogError(cf.output_path, source_output_path, file_name, traceback.format_exc()).log_error()
    f.close()
