import pandas as pd
import app_Lib.manage_directories as md
import parameters.parameters as pm
import os
import app_Lib.functions as funcs
import numpy as np
import templates as tmp


def get_trgt_pk(Core_tables,target_table ):
    trgt_pk = ''
    for core_tbl_indx, core_tbl_row in Core_tables[(Core_tables['Table name'] == target_table) & (Core_tables['PK'] == 'Y')].iterrows():
        trgt_pk+= core_tbl_row['Column name']+','
    trgt_pk=trgt_pk[0:len(trgt_pk)-1]
    return trgt_pk


def get_lkp_tbls (Core_tables):
    lkp_tbls = Core_tables[Core_tables['Is lookup'] == 'Y']
    return lkp_tbls


def get_lkp_tbls_names (Core_tables):
    lkp_tbls= Core_tables[Core_tables['Is lookup'] =='Y']
    lkp_tbls_names=pd.unique(list(lkp_tbls['Table name']))
    return lkp_tbls_names

def get_column_data_type(Core_tables, column_name, table_name):
    trgt_col_data_type = ""
    #print("column_name ", column_name,"table_name ", table_name )
    curr_rec =Core_tables[(Core_tables['Table name'].str.strip()== table_name.strip()) & (Core_tables['Column name'].str.strip()== column_name.strip())]
    # print("curr_rec ", curr_rec)
    try:
        trgt_col_data_type=curr_rec['Data type'].item()
    except Exception as ERROR_msg:
        pass
        # print(ERROR_msg)
        # print("tbl:", table_name,"trgt_col_data_type ", trgt_col_data_type)
    return trgt_col_data_type

def get_select_clause(target_table,Core_tables,table_maping_name, Column_mapping):
    sel_clause='\n'
    for col_map_indx, col_map_row in Column_mapping[(Column_mapping['Mapping name'] == table_maping_name)].iterrows():
        sel_ready = ""
        src_tbl = col_map_row['Mapped to table']
        src_col = col_map_row['Mapped to column']

        sql_const=col_map_row['Transformation rule']
        # print("get select clause", col_map_row['Column name'])
        trgt_col_data_type = get_column_data_type(Core_tables, col_map_row['Column name'], target_table)
        src=""
        if src_tbl != "":
            src = 'Cast (' + src_tbl + '.' + src_col + ' AS ' + trgt_col_data_type + ')'
        if sql_const != "":
            src = 'Cast (' + str(sql_const)+ ' AS ' + trgt_col_data_type + ')'

        trgt_col = ' AS '+col_map_row['Column name']
        sel_ready=str(src)+str(trgt_col)+',\n'
        sel_clause=sel_clause+sel_ready


    return  sel_clause

def get_src_core_tbls (source_name, Core_tables, Table_mapping):
    src_mappings_df=Table_mapping[Table_mapping['Source']  == source_name]

    core_tables_list=src_mappings_df['Target table name']
 #   core_tables_list = [Table_mapping['Target table name'] for Table_mapping['Target table name'] in Table_mapping if Table_mapping['Source']  == source_name]
    core_tables_list = pd.unique(core_tables_list)
    return core_tables_list







