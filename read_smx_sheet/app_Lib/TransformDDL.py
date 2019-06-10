import pandas as pd
import numpy as np
from read_smx_sheet.app_Lib import functions as funcs

def get_sub_query(cf, subquery,src_layer,main_src):
    if src_layer == "SEM":
        join_ = 'JOIN '
        from_ = 'FROM '
    else:
        join_ = 'JOIN ' + cf.SI_VIEW + '.'
        from_ = 'FROM ' + cf.SI_VIEW + '.'
    if subquery.find("UNION") != -1:
        subquery = "("+subquery + ")" + main_src
    out = subquery.replace("FROM ", from_)
    out_2 = out.replace(from_ + " (", "FROM (")
    out_3 = out_2.replace("JOIN ", join_)
    out_4 = out_3.replace(join_+"(","JOIN (")
    out_5 = out_4.replace(join_+" (","JOIN (")
    out_5 = out_5.replace(join_ + "\n (", "JOIN (")
    out_6 = out_5.replace(join_+"CORE.", "JOIN " + cf.core_view+".")
    out_6 = out_6.replace(join_+" CORE.", "JOIN " + cf.core_view+".")
    return out_6


def get_unique_code_set_name_id(BMAP):
    bmap_unique = BMAP[["Code set name", "Code set ID", "Physical table"]].drop_duplicates()
    # print(" bmaps: ", bmap_unique)
    return bmap_unique


def get_bmap_physical_tbl_name (BMAP, code_set_name):
    bmap_unique = get_unique_code_set_name_id(BMAP)
    curr_rec = bmap_unique[bmap_unique["Code set name"] == code_set_name.strip()]
    try:
        tbl_name=curr_rec["Physical table"].item()
    except Exception as error:
        tbl_name = "undefined"
    return str(tbl_name)

def get_bmap_code_set_id(BMAP, code_set_name):
    bmap_unique = get_unique_code_set_name_id(BMAP)
    # print("bmap_unique: ",bmap_unique )
    curr_rec = bmap_unique[bmap_unique["Code set name"] == code_set_name.strip()]
    try:
        code_set_id = curr_rec["Code set ID"].item()
        # print("code_set_id ", code_set_id)
    except Exception as error:
        code_set_id = "undefined"
        # print("code_set_id ", code_set_id)
    return str(code_set_id)


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
    lkp_tbls= get_lkp_tbls(Core_tables)
    lkp_tbls_names=pd.unique(list(lkp_tbls['Table name']))
    return lkp_tbls_names

def get_lkp_tbl_Cols(Core_tables, tbl_name):
    Lkp_tbls = get_lkp_tbls(Core_tables)
    lkp_tbl_cols = ""
    for lkp_tbl_index, lkp_tbl_row in Lkp_tbls[(Lkp_tbls["Table name"] == tbl_name)].iterrows():
            lkp_tbl_cols += lkp_tbl_row["Column name"] + ","
    lkp_tbl_cols = lkp_tbl_cols[0:len(lkp_tbl_cols) - 1]
    return lkp_tbl_cols

def get_column_data_type(Core_tables, column_name, table_name):
    trgt_col_data_type = ""
    curr_rec =Core_tables[(Core_tables['Table name'].str.strip()== table_name.strip()) & (Core_tables['Column name'].str.strip()== column_name.strip())]

    try:
        trgt_col_data_type=curr_rec['Data type'].item()
    except Exception as ERROR_msg:
        # print(ERROR_msg)
        # print("tbl:", table_name,"Col_name: ", column_name, "trgt_col_data_type ", trgt_col_data_type)
        pass
    return trgt_col_data_type


def get_code_set_names(Bmap_values):
    code_set_names = pd.unique(list(Bmap_values['Code set name']))
    return code_set_names

def get_tbl_cols(Core_tables, tbl_name):
    tbl_cols = ""
    for lkp_tbl_index, lkp_tbl_row in Core_tables[(Core_tables["Table name"] == tbl_name)].iterrows():
        tbl_cols += lkp_tbl_row["Column name"] + ","
    lkp_tbl_cols = tbl_cols[0:len(tbl_cols) - 1]
    return lkp_tbl_cols


def get_core_tables_list(Core_tables):
    core_tables_list= pd.unique(list(Core_tables['Table name']))
    return core_tables_list

def get_bmap_values_for_codeset(Bmap_values, code_set_name):
    values=Bmap_values[Bmap_values['Code set name']==code_set_name]
    return values


def get_select_clause(target_table,Core_tables,table_maping_name, Column_mapping):
    sel_clause='\n'
    for col_map_indx, col_map_row in Column_mapping[(Column_mapping['Mapping name'] == table_maping_name)].iterrows():
        sel_ready = ""
        src_tbl = col_map_row['Mapped to table']
        src_col = col_map_row['Mapped to column']

        sql_const = str(col_map_row['Transformation rule'])
        if sql_const.upper() == funcs.single_quotes("NULL"):
            sql_const = "NULL"

        trgt_col_data_type = get_column_data_type(Core_tables, col_map_row['Column name'], target_table)
        src=""
        if src_tbl != "":
            src = 'Cast (' + src_tbl + '.' + src_col + ' AS ' + trgt_col_data_type + ')'
        if sql_const != "":
            src = 'Cast (' + sql_const+ ' AS ' + trgt_col_data_type + ')'

        trgt_col = ' AS '+col_map_row['Column name']
        sel_ready=str(src)+str(trgt_col)+',\n'
        sel_clause=sel_clause+sel_ready
    return sel_clause


def get_src_core_tbls(Table_mapping):
    # src_mappings_df=Table_mapping[Table_mapping['Source']  == source_name]
    core_tables_list=Table_mapping['Target table name']
 #   core_tables_list = [Table_mapping['Target table name'] for Table_mapping['Target table name'] in Table_mapping if Table_mapping['Source']  == source_name]
    core_tables_list = pd.unique(core_tables_list)
    return core_tables_list

def get_src_tbl_mappings (source_name, Table_mapping):
    src_mappings_df=Table_mapping[Table_mapping['Source']  == source_name]
    return src_mappings_df

def get_core_tbl_sart_date_column (Core_tables, tbl_name):

    hist_key="S"
    curr_rec = Core_tables[(Core_tables['Table name'].str.strip() == tbl_name.strip()) & (Core_tables['Historization key'].str.strip() == hist_key.strip())]
    try:
        start_date_col=curr_rec['Column name'].item()
    except Exception as error:
        start_date_col = "undefined"
        # print(error)
        # print ("start_date_col for tbl ", tbl_name, " is undefined")
        return start_date_col
    return  start_date_col

def get_core_tbl_end_date_column (Core_tables, tbl_name):
    hist_key="E"
    curr_rec = Core_tables[(Core_tables['Table name'].str.strip() == tbl_name.strip()) & (Core_tables['Historization key'].str.strip() == hist_key.strip())]
    try:
        end_date_col=curr_rec['Column name'].item()
    except Exception as error:
        end_date_col = "undefined"
        # print(error)
        # print("end_date_col for tbl ", tbl_name, " is undefined")
        return end_date_col

    return end_date_col


# def get_core_tbl_hist_keys_list (Core_tables, tbl_name):
#     try:
#         hist_keys = Core_tables[ (Core_tables['Table name'] == tbl_name) & (Core_tables['Historization key'] == "Y")]
#         hist_keys_list=pd.unique(list(hist_keys['Column name']))
#         if (len(hist_keys_list)==0):
#             hist_keys_list={"undefined"}
#
#     except Exception as error:
#         hist_keys_list ={"undefined"}
#         # print(error)
#         # print("hist_keys for tbl ", tbl_name, " is undefined")
#         return hist_keys_list
#     return hist_keys_list


def get_core_tbl_hist_keys_list (Core_tables, tbl_name , history_column_list):
    # try:
        # primary_keys = Core_tables[(Core_tables['Table name'] == tbl_name) & (Core_tables['PK'] == "Y")]
        # hist_keys = Core_tables[ (Core_tables['Table name'] == tbl_name) & (Core_tables['Historization key'] == "Y")]
    #
        hist_keys = Core_tables[(Core_tables['Table name'] == tbl_name) & (Core_tables['PK'] == "Y") & (Core_tables['Historization key']!= "S") & (Core_tables['Historization key']!= "E")]
        hist_keys_list = pd.unique(list(hist_keys['Column name']))
        # print("tbl_name ", tbl_name, " hist keys : ", hist_keys, " hist_keys_list no diff: ", hist_keys_list)
        hist_keys_list = np.setdiff1d(hist_keys_list, history_column_list )
        # print("tbl_name ", tbl_name, " hist keys : ", hist_keys, " hist_keys_list  diff: ", hist_keys_list)
        if len(hist_keys_list)== 0:
            hist_keys_list={"undefined"}

    # except Exception as error:
    #     hist_keys_list ={"undefinedxx"}
    #     # print(error)
    #     # print("hist_keys for tbl ", tbl_name, " is undefined")
    #     return hist_keys_list
        return hist_keys_list