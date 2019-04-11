import parameters.parameters as pm
import app_Lib.functions as funcs
import app_Lib.TransformDDL as TransformDDL
import pandas as pd



def d620(source_output_path, source_name, Table_mapping,Column_mapping,Core_tables, Loading_Type):
    file_name = funcs.get_file_name(__file__)
    f = open(source_output_path + "/" + file_name + ".sql", "w+", encoding="utf-8")
    notes= list()
    for table_maping_index, table_maping_row in Table_mapping[(Table_mapping['Source'] == source_name)].iterrows(): #& (source_name=='CRA')& (Table_mapping['Mapping name'] == 'L1_PRTY_RLTD_L0_CRA_COMPANY_PERSON')].iterrows():
        inp_view_select_clause = ''
        inp_view_from_clause = ''
        inp_view_left_join_clause = ''
        inp_view_where_clause =''

        prcess_type = 'TXF'
        layer = str(table_maping_row['Layer'])
        table_maping_name=str(table_maping_row['Mapping name'])
        process_name = prcess_type + "_" + layer + "_" + table_maping_name

        inp_view_header = 'REPLACE VIEW ' + pm.INPUT_VIEW_DB + '.' + process_name + '_IN AS'
        target_table = str(table_maping_row['Target table name'])
        main_src=table_maping_row['Main source']
        core_tables_list= pd.unique(list(Core_tables['Table name']))

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
        inp_view_select_clause='SELECT '+'\n'+sub+TransformDDL.get_select_clause(target_table, Core_tables, table_maping_name, Column_mapping)
        map_grp=' CAST(' +table_maping_row['Mapping group']+' AS VARCHAR(100)) AS  MAP_GROUP ,'
        start_date='(SELECT Business_Date FROM GDEV1V_GCFR.GCFR_Process_Id'+'\n'+'   WHERE Process_Name = '+ "'"+process_name+ "'"+'\n'+') AS Start_Date,'
        end_date='DATE '+"'9999-12-31'"+' AS End_Date,'
        modification_type=''
        if (Loading_Type == 'ONLINE'):
            modification_type=main_src+'.MODIFICATION_TYPE'
        else:
            modification_type = "'I' AS MODIFICATION_TYPE"

        inp_view_select_clause=inp_view_select_clause+'\n'+ map_grp+'\n'+start_date+ '\n'+end_date+ '\n'+modification_type+'\n'



        inp_view_from_clause='FROM '+pm.SI_VIEW+'.'+table_maping_row['Main source']+' ' +table_maping_row['Main source']

        if table_maping_row['Join'] != "":
            inp_view_from_clause=inp_view_from_clause+'\n'+table_maping_row['Join']
            join='JOIN '+pm.SI_VIEW+'.'
            inp_view_from_clause = inp_view_from_clause.replace('JOIN',join)

        inp_view_where_clause=';'
        if table_maping_row['Filter criterion']!="":
            inp_view_where_clause='Where '+table_maping_row['Filter criterion']+';'

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


    f.close()









