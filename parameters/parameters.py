import datetime as dt

smx_path = "C:/smx_sheets"
home_output_folder = "C:/smx_scripts"

etl_process_table = 'ETL_PROCESS'
SOURCE_TABLES_LKP_table = 'SOURCE_TABLES_LKP'
SOURCE_NAME_LKP_table = 'SOURCE_NAME_LKP'
history_tbl = 'HISTORY'

db_prefix = "GPROD1"

gcfr_ctl_Id = 1
gcfr_stream_key = 1
gcfr_system_name = "Economic"
gcfr_stream_name = "Economic stream"

gcfr_bkey_process_type = 21
gcfr_snapshot_txf_process_type = 24
gcfr_insert_txf_process_type = 25
gcfr_others_txf_process_type = 29

############################
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
################################

parquet_db_name = "smx_data"
sys_argv_separator = "|#|"
stg_cols_separator = "||'_'||"
read_sheets_parallel = False


