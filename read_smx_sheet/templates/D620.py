from read_smx_sheet.app_Lib import functions as funcs
from read_smx_sheet.app_Lib import TransformDDL
import traceback

def d620(cf, source_output_path, Table_mapping,Column_mapping,Core_tables, Loading_Type):
    file_name = funcs.get_file_name(__file__)
    f = funcs.WriteFile(source_output_path, file_name, "sql")
    try:
        notes= list()
        for table_maping_index, table_maping_row in Table_mapping.iterrows(): #& (source_name=='CRA')& (Table_mapping['Mapping name'] == 'L1_PRTY_RLTD_L0_CRA_COMPANY_PERSON')].iterrows():
        # for table_maping_index, table_maping_row in Table_mapping[(Table_mapping['Source'] == source_name) & (source_name=='TADAMON')& (Table_mapping['Mapping name'] == 'L1_PRTY_DEMOG_L0_TADAMON_CARD_SPONSOR_MOTHER')].iterrows():
            inp_view_select_clause = ''
            inp_view_from_clause = ''
            inp_view_left_join_clause = ''
            inp_view_where_clause =''
            sub_query_flag=0

            prcess_type = 'TXF'
            layer = str(table_maping_row['Layer'])
            table_maping_name=str(table_maping_row['Mapping name'])
            src_layer=str(table_maping_row['Source layer'])
            process_name = prcess_type + "_" + layer + "_" + table_maping_name

            inp_view_header = 'REPLACE VIEW ' + cf.INPUT_VIEW_DB + '.' + process_name + '_IN AS'
            target_table = str(table_maping_row['Target table name'])
            main_src=table_maping_row['Main source']
            # core_tables_list= pd.unique(list(Core_tables['Table name']))
            core_tables_list= TransformDDL.get_core_tables_list(Core_tables)
            msg = ''


            if main_src == None:
                msg='Missing Main Source  for Table Mapping:{}'.format(str(table_maping_row['Mapping name']))

                notes+=msg
                #raise Exception('Missing Main Source  for Table Mapping:{}'.format(str(table_maping_row['Mapping name'])))
                continue

            if target_table not in core_tables_list:
                msg='TARGET TABLE NAME not found in Core Tables Sheet for Table Mapping:{}'.format(str(table_maping_row['Mapping name']))

                notes += msg
              #  raise Exception('TARGET TABLE NAME not found in Core Tables Sheet for Table Mapping:{}'.format(str(table_maping_row['Mapping name'])))
                continue


           # tgt_pk=tmp.TransformDDL.get_trgt_pk(Core_tables, target_table)


            sub="/* Target table:	"+target_table+"*/"+'\n'+"/* Table mapping:	"+table_maping_name +"*/"+'\n'+"/* Mapping group:	"+table_maping_row['Mapping group'] +"*/"
            inp_view_select_clause='SELECT ' +'\n' + sub + TransformDDL.get_select_clause(target_table, Core_tables, table_maping_name, Column_mapping)
            map_grp = ' CAST(' +funcs.single_quotes(table_maping_row['Mapping group'])+' AS VARCHAR(100)) AS  MAP_GROUP ,'
            start_date = '(SELECT Business_Date FROM ' + cf.GCFR_V + '.GCFR_Process_Id'+'\n'+'   WHERE Process_Name = ' + "'" + process_name + "'"+'\n'+') AS Start_Date,'
            end_date='DATE '+"'9999-12-31'"+' AS End_Date,'
            modification_type=''
            if (Loading_Type == 'ONLINE'):
                modification_type=main_src+'.MODIFICATION_TYPE'
            else:
                modification_type = "'U' AS MODIFICATION_TYPE"

            inp_view_select_clause=inp_view_select_clause+'\n'+ map_grp+'\n'+start_date+ '\n'+end_date+ '\n'+modification_type+'\n'

            if table_maping_row['Join'] == "":
                inp_view_from_clause = 'FROM ' + cf.SI_VIEW + '.' + table_maping_row['Main source'] + ' ' + table_maping_row['Main source']
            elif table_maping_row['Join'] != "":
                if (table_maping_row['Join'].find("FROM".strip()) == -1): #no subquery in join clause
                    inp_view_from_clause = 'FROM ' + cf.SI_VIEW + '.' + table_maping_row['Main source'] + ' ' +table_maping_row['Main source']
                    inp_view_from_clause = inp_view_from_clause+'\n'+table_maping_row['Join']
                    join = 'JOIN '+cf.SI_VIEW+'.'
                    inp_view_from_clause = inp_view_from_clause.replace('JOIN',join)
                else:
                    sub_query_flag=1
                    join_clause=table_maping_row['Join']
                    subquery_clause= TransformDDL.get_sub_query(cf, join_clause, src_layer, main_src)
                    inp_view_from_clause = ' FROM \n'+ subquery_clause

            inp_view_where_clause=';'
            if table_maping_row['Filter criterion']!="":
                # if (sub_query_flag == 0):
                inp_view_where_clause = 'Where '+table_maping_row['Filter criterion']+';'
                # else:
                #     inp_view_where_clause = 'Where '+table_maping_row['Filter criterion']+');'


            f.write(inp_view_header)
            f.write("\n")
            f.write(inp_view_select_clause)
            f.write("\n")
            f.write( inp_view_from_clause)
            f.write("\n")
            f.write(inp_view_where_clause)
            f.write("\n")
            f.write("\n")
            f.write("\n")

    except Exception as error:
        # print(str(error))
        lf = funcs.WriteFile(cf.output_path, "log", "txt", "a+", True)
        lf.write(source_output_path)
        lf.write(file_name)
        lf.write(traceback.format_exc())

    f.close()









