import os
import sys
sys.path.append(os.getcwd())
import datetime as dt
from read_smx_sheet.app_Lib import functions as funcs

parameters = ""
param_dic = {}
module_path = os.path.dirname(sys.modules['__main__'].__file__)
config_file = open(module_path+"/config.txt","r")
for i in config_file.readlines():
    line = i.strip()
    if line != "":
        if line[0] != '#':
            parameters = parameters + line + "$$$"

param_dic = funcs.string_to_dict(parameters, "$$$")

smx_path = param_dic['smx_path']
home_output_folder = param_dic['home_output_folder']

source_names = param_dic['source_names'].split(',')
source_names = None if source_names[0] == "" and len(source_names) > 0 else source_names
# source_names = ["TAMWEEN", "TAX", "EDU", "CUSTOMS"]

etl_process_table = param_dic['etl_process_table']
SOURCE_TABLES_LKP_table = param_dic['SOURCE_TABLES_LKP_table']
SOURCE_NAME_LKP_table = param_dic['SOURCE_NAME_LKP_table']
history_tbl = param_dic['history_tbl']

db_prefix = param_dic['db_prefix']

gcfr_ctl_Id = param_dic['gcfr_ctl_Id']
gcfr_stream_key = param_dic['gcfr_stream_key']
gcfr_system_name = param_dic['gcfr_system_name']
gcfr_stream_name = param_dic['gcfr_stream_name']

gcfr_bkey_process_type = param_dic['gcfr_bkey_process_type']
gcfr_snapshot_txf_process_type = param_dic['gcfr_snapshot_txf_process_type']
gcfr_insert_txf_process_type = param_dic['gcfr_insert_txf_process_type']
gcfr_others_txf_process_type = param_dic['gcfr_others_txf_process_type']

read_sheets_parallel = True if param_dic['read_sheets_parallel'] == 1 else False
################################################################################################
dt_now = dt.datetime.now()
dt_folder = dt_now.strftime("%Y") + "_" + dt_now.strftime("%b").upper() + "_" + dt_now.strftime("%d") + "_" + dt_now.strftime("%H") + "_" + dt_now.strftime("%M")
output_path = home_output_folder + "/" + dt_folder
smx_ext = "xlsx"
System_sht = "System"
Supplements_sht = "Supplements"
Column_mapping_sht = "Column mapping"
BMAP_values_sht = "BMAP values"
BMAP_sht = "BMAP"
BKEY_sht = "BKEY"
STG_tables_sht = "STG tables"
Table_mapping_sht = "Table mapping"
Core_tables_sht = "Core tables"
sheets = [System_sht, Supplements_sht, Column_mapping_sht, BMAP_values_sht, BMAP_sht, BKEY_sht, STG_tables_sht, Table_mapping_sht, Core_tables_sht]

T_STG = db_prefix + "T_STG"
t_WRK = db_prefix + "T_WRK"
v_stg = db_prefix + "V_STG"
INPUT_VIEW_DB = db_prefix + "V_INP"

MACRO_DB = db_prefix + "M_GCFR"
UT_DB = db_prefix + "P_UT"
UTLFW_v = db_prefix + "V_UTLFW"
UTLFW_t = db_prefix + "T_UTLFW"

TMP_DB = db_prefix + "T_TMP"
APPLY_DB = db_prefix + "P_PP"

SI_DB = db_prefix + "T_SRCI"
SI_VIEW = db_prefix + "V_SRCI"

GCFR_t = db_prefix + "t_GCFR"
GCFR_V = db_prefix + "V_GCFR"

M_GCFR = db_prefix + "M_GCFR"
P_UT = db_prefix + "P_UT"

core_table = db_prefix + "T_BASE"
core_view = db_prefix + "V_BASE"

parquet_db_name = "smx_data"
sys_argv_separator = "|#|"
stg_cols_separator = "||'_'||"

################################################################################################