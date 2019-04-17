# exec GPROD1M_GCFR.GCFR_Register_System(1, 'Economic', '', 'Economic')
# call GPROD1P_UT.GCFR_UT_Register_Stream(1, 1, 'Economic stream', cast('2019-01-01' as date))

smx_ext = "xlsx"
# smx_path = 'C:/Users/AA250090/Downloads/New folder/input/'
# smx_path = "//10.10.10.250/TeraData/share/Salama/0Mpsn/Lastest SMX/"
# project_path = "C:/Users/omar_nour/PycharmProjects/new_udi"
smx_path = "C:/smx_sheets/"
output_path =  smx_path + "udi_outputs/"

etl_process_table = 'ETL_PROCESS'
SOURCE_TABLES_LKP_table = 'SOURCE_TABLES_LKP'
SOURCE_NAME_LKP_table = 'SOURCE_NAME_LKP'

db_prefix = "GPROD1"
history_tbl = 'HISTORY'
gcfr_bkey_process_type = 21
gcfr_snapshot_txf_process_type = 24
gcfr_insert_txf_process_type = 25
gcfr_others_txf_process_type = 29
gcfr_ctl_Id = 1
gcfr_stream_key = 1

gcfr_system_name = "Economic"
gcfr_stream_name = "Economic stream"

separator = "||'_'||"

############################
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

