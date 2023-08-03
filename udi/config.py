from collections import namedtuple
smx_path = r"/Users/oh255011/Documents/Teradata/SMX/[ACA] SMX_Economic_Units_03-01-2023.xlsx"
pickle_path = "pickled_objs"
scripts_path = "smx_scripts"
DB_NAME = 'teradata'
HOST = 'localhost'
USER = 'power_user'
PASSWORD = 'power_user'
default_config_file_name = "config.txt"

cls_keys = {
    'server': 'server_name'
    , 'Ip': ('server_id', 'ip')
    , 'DataBaseEngine': ('server_id', 'name')
    , 'Credential': ('db_engine_id', 'user_name')
    , 'datasource': 'source_name'
    , 'schema': ('db_id', 'schema_name')
    , 'table': ('schema_id', 'table_name')
    , 'DataSetType': 'name'
    , 'DataSet': ('set_type_id', 'set_code')
    , 'Domain': ('data_set_id', 'domain_code')
    , 'DomainValue': ('domain_id', 'source_key')
    , 'Column': ('table_id', 'column_name')
    , 'DataType': ('db_id', 'dt_name')
    , 'LayerType': 'type_name'
    , 'Layer': 'layer_name'
    , 'LayerTable': ('layer_id', 'table_id')
    , 'Pipeline': 'lyr_view_id'
    , 'ColumnMapping': ('pipeline_id', 'col_seq', 'tgt_col_id')
    , 'Filter': ('pipeline_id', 'filter_seq')
    , 'GroupBy': ('pipeline_id', 'col_id')
    , 'JoinType': 'code'
    , 'JoinWith': ('pipeline_id', 'with_lyr_table_id', 'with_alias')
    , 'joinOn': 'join_with_id'
}
TechColumn = namedtuple("TechColumn"
                        , "column_name data_type "
                          "is_created_at is_created_by "
                          "is_updated_at is_updated_by is_modification_type "
                          "is_load_id is_batch_id is_row_identity "
                          "is_delete_flag mandatory"
                        )
CORE_TECHNICAL_COLS = [TechColumn('PROCESS_NAME', 'VARCHAR(150)', 0, 1, 0, 0, 0, 0, 0, 0, 0, 1),
                       TechColumn('UPDATE_PROCESS_NAME', 'VARCHAR(150)', 0, 0, 0, 1, 0, 0, 0, 0, 0, 0),
                       TechColumn('START_TS', 'TIMESTAMP(6)', 1, 0, 0, 0, 0, 0, 0, 0, 0, 1),
                       TechColumn('UPDATE_TS', 'TIMESTAMP(6)', 0, 0, 1, 0, 0, 0, 0, 0, 0, 0),
                       TechColumn('END_TS', 'TIMESTAMP(6)', 0, 0, 0, 0, 0, 0, 0, 0, 1, 0),
                       TechColumn('BATCH_ID', 'INTEGER', 0, 0, 0, 0, 0, 0, 1, 0, 0, 1),
                       ]
STG_TECHNICAL_COLS = [TechColumn('MODIFICATION_TYPE', 'CHAR(1)', 0, 0, 0, 0, 1, 0, 0, 0, 0, 1),
                      TechColumn('LOAD_ID', 'VARCHAR(60)', 0, 0, 0, 0, 0, 1, 0, 0, 0, 1),
                      TechColumn('BATCH_ID', 'INT', 0, 0, 0, 0, 0, 0, 1, 0, 0, 1),
                      TechColumn('REF_KEY', 'BIGINT', 0, 0, 0, 0, 0, 0, 0, 1, 0, 1),
                      TechColumn('INS_DTTM', 'TIMESTAMP(6)', 1, 0, 0, 0, 0, 0, 0, 0, 0, 1),
                      TechColumn('UPD_DTTM', 'TIMESTAMP(6)', 0, 0, 3, 0, 0, 0, 0, 0, 0, 0),
                      ]

SHEETS = ['stg_tables', 'system', 'data_type', 'bkey', 'bmap'
    , 'bmap_values', 'core_tables', 'column_mapping', 'table_mapping'
    , 'supplements']
UNIFIED_SOURCE_SYSTEMS = ['UNIFIED_GOVERNORATE',
                          'UNIFIED_CITY',
                          'UNIFIED_POLICE_STATION',
                          'UNIFIED_DISTRICT',
                          'UNIFIED_COUNTRY', 
                          'UNIFIED_CURRENCY']
DS_BKEY = 'BKEY'
DS_BMAP = 'BMAP'
# LayerDtl = namedtuple("LayerDetail", "type level v_db t_db")
JoinTypes = namedtuple("JoinTypes", "code name")
JOIN_TYPES = [JoinTypes(code='ij', name='inner join'), JoinTypes(code='lj', name='left join'),
              JoinTypes(code='rj', name='right join'), JoinTypes(code='fj', name='full outer join')]

NUMBERS = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
ALPHABETS = ['a', 'b', 'c', 'd', 'e', 'f', 'g'
    , 'h', 'i', 'j', 'k', 'l', 'm', 'n'
    , 'o', 'p', 'q', 'r', 's', 't', 'u'
    , 'v', 'w', 'x', 'y', 'z']
SPECIAL_CHARACTERS = [
    '!', '"', '#', '$', '%', '&', "'", '(', ')', '*', '+', ','
    , '-', '.', '/', ':', ';', '<', '=', '>', '?', '@', '['
    , '\\', ']', '^', '_', '`', '{', '|', '}', '~'
]
SRC_SYSTEMS_FOLDER_NAME = "SOURCES"
CORE_MODEL_FOLDER_NAME = "CORE_MODEL"
CAST_DTYPE_TEMPLATE = """({dtype_name} {precise})"""
COL_MAPPING_TEMPLATE = """{comma}{col_name} {cast_dtype} {alias}"""
FROM_TEMPLATE = """from {schema_name}.{table_name} {alias}"""
WHERE_TEMPLATE = """where {conditions}"""
GROUP_BY_TEMPLATE = """group by {columns}"""
PI_TEMPLATE = """{unique_pi} PRIMARY INDEX ( {pi_cols} )"""
COL_DTYPE_TEMPLATE = """\t{comma}{col_name}  {data_type}{precision} {latin_unicode} {case_sensitive} {not_null}\n """
DROP_BEFORE_CREATE = True
DROP_TABLE_TEMPLATE = """DROP TABLE {schema_name}.{table_name};"""
DDL_TABLE_TEMPLATE = """ 
CREATE {set_multiset} TABLE {schema_name}.{table_name}
    ,FALLBACK
    ,NO BEFORE JOURNAL
    ,NO AFTER JOURNAL
    ,CHECKSUM = DEFAULT
    ,DEFAULT MERGEBLOCKRATIO
(\n{col_dtype})
{pi_index}
{si_index}
 """
CREATE_REPLACE = 'REPLACE'
DDL_VIEW_TEMPLATE = """{create_replace} VIEW /*VER.1*/  {schema_name}.{view_name} AS LOCK ROW FOR ACCESS {query_txt}"""
QUERY_TEMPLATE = """ {with_clause}\nselect {distinct}\n{col_mapping}\n{from_clause} {join_clause}\n{where_clause}\n{group_by_clause}\n{having_clause}"""
JOIN_CLAUSE_TEMPLATE = "\n\t{join_type} {with_table} {with_alias}\n\ton {on_clause}"
SRCI_V_BKEY_TEMPLATE_QUERY = """(select EDW_KEY\n from {bkey_db}.{bkey_table_name}\n where SOURCE_KEY = {src_key} {cast}\n and DOMAIN_ID={domain_id})"""
SRCI_V_BMAP_TEMPLATE_QUERY = """(select EDW_Code\n from {bmap_db}.{bmap_table_name}\n where SOURCE_CODE = {source_code} {cast}\n and CODE_SET_ID = {code_set_id}\n and DOMAIN_ID={domain_id})"""

BK_PROCESS_NAME_TEMPLATE = "BK_{set_id}_{src_table_name}_{column_name}_{domain_id}"
BK_VIEW_NAME_TEMPLATE = "{view_name}_IN"

BMAP_PROCESS_NAME_TEMPLATE = "BMAP_{set_id}_{src_table_name}_{column_name}_{domain_id}"
BMAP_VIEW_NAME_TEMPLATE = "{view_name}_IN"

CORE_PROCESS_NAME_TEMPLATE = "TXF_CORE_{mapping_name}"
CORE_VIEW_NAME_TEMPLATE = "{view_name}_IN"

LOADING_MODE = 'ONLINE'

DELETE_DATABASE_TEMPLATE = """DELETE DATABASE {db_name} ALL;"""
DROP_DATABASE_TEMPLATE = """DROP DATABASE {db_name};"""

DATABASE_TEMPLATE = """
CREATE DATABASE {db_name} from {main_db_name}
AS TEST = 60e6, -- 60MB
    SPOOL = 120e6; -- 120MB
"""
INSERT_INTO_SOURCE_SYSTEMS = """
INSERT INTO {meta_db}.SOURCE_SYSTEMS (
    SOURCE_NAME,LOADING_MODE, REJECTION_TABLE_NAME, BUSINESS_RULES_TABLE_NAME
     , STG_ACTIVE, BASE_ACTIVE, IS_SCHEDULED, SOURCE_LAYER, DATA_SRC_CD, ACTIVE
) VALUES (
    {SOURCE_NAME},{LOADING_MODE}, {REJECTION_TABLE_NAME}, {BUSINESS_RULES_TABLE_NAME}
     , 1, 1, 1, {SOURCE_LAYER}, {DATA_SRC_CD}, 1
    );
"""
INSERT_INTO_SOURCE_SYSTEM_TABLES = """
INSERT INTO {meta_db}.SOURCE_SYSTEM_TABLES (TABLE_NAME, SOURCE_NAME, IS_TARANSACTIOANL, ACTIVE)
VALUES ({TABLE_NAME}, {SOURCE_NAME}, {IS_TARANSACTIOANL}, 1);
"""

INSERT_INTO_EDW_TABLES = """
INSERT INTO {meta_db}.EDW_TABLES (LAYER_NAME, TABLE_NAME, IS_LOOKUP, IS_HISTORY,START_DATE_COLUMN ,END_DATE_COLUMN, ACTIVE)
VALUES ({LAYER_NAME}, {TABLE_NAME}, {IS_LOOKUP}, {IS_HISTORY},{START_DATE_COLUMN} ,{END_DATE_COLUMN}, 1);
"""
INSERT_INTO_TRANSFORM_KEYCOL = """
INSERT INTO {meta_db}.TRANSFORM_KEYCOL (LAYER_NAME, TABLE_NAME, KEY_COLUMN)
VALUES ({LAYER_NAME}, {TABLE_NAME}, {KEY_COLUMN});
"""
INSERT_INTO_PROCESS = """
INSERT INTO {meta_db}.PROCESS 
    (PROCESS_NAME, SOURCE_NAME, TGT_LAYER, TGT_TABLE, SRC_TABLE
    , APPLY_TYPE, MAIN_TABLE_NAME, KEY_SET_ID, CODE_SET_ID, DOMAIN_ID, ACTIVE) 
VALUES ({PROCESS_NAME}, {SOURCE_NAME}, {TGT_LAYER}, {TGT_TABLE}, {SRC_TABLE}
    , {APPLY_TYPE}, {MAIN_TABLE_NAME}, {KEY_SET_ID}, {CODE_SET_ID}, {DOMAIN_ID}, 1);
"""
INSERT_INTO_HISTORY = """
INSERT INTO {meta_db}.HISTORY
    (PROCESS_NAME, HISTORY_COLUMN) 
VALUES 
    ({PROCESS_NAME}, {HISTORY_COLUMN});
"""

# AppName_<Major>.<Minor>.<BuildNo>
ver_no = "| Build #3.5.1"
# ################################################################################################
# What is new : #3.5.1:
# changing layer 0 physical table schema to be the exact user input in source_layer0 parameter in the
# configuration file
# ################################################################################################
# What is new : #3.5.0 :
# removing logging handlers at the end of the program execution to run smoothly afterwards
# ################################################################################################
# What is new : #3.4.0 :
# deleting MyID class instances after the first program run
# ################################################################################################
# What is new : #3.3.0 :
# Adding threading to avoid locking the UDI while using it
# ################################################################################################
# What is new : #3.2.0 :
# Run on all sources when no source is given by the user and generate summary report in the log file
# ################################################################################################
# What is new : #3.1.0 :
# Capturing the database prefix from the configuration file
# ################################################################################################
# What is new : #3.0.0 :
# Changing the execution method and generating new folder structure for the output
# ################################################################################################
# What is new : #2.15.7 :
# Making sure to Generate PK for statging tables in key column transfer whether the 'Y' is capital or small
# ################################################################################################
# What is new : #2.15.6 :
# Editing get_lkp_tbl_Cols function in TransformDDL to make sure it only pulls the accurate key in the base lookup tables
# This has a direct impact when inserting lookup values in script D005
# ################################################################################################
# What is new : #2.15.5 :
# Optimizing the CTE Statements in D615 to only consider the base tables for the input source
# ################################################################################################
# What is new : #2.15.4 :
# in script D615 while inserting to TRANSFORM_KEY_COL GCFR table, we made sure to insert only the primary keys of the newly added
# base tables and the new STG tables for the source. Also fix script D001 for source_table_lkp to insert more than 1 record
# ################################################################################################
# What is new : #2.15.3 :
# Avoid trimming Datetime and Timestamp columns in D110
# ################################################################################################
# What is new : #2.15.2 :
# unified scripts to only be generated if the unified sheet contatins data for the source
# What is new : #2.15.1 :
# Editing D000 script to reflect change column name in ETL_PROCESS table in GCFR from STG_TABLE to STG_TABLE_NAME
# ################################################################################################
# What is new : #2.15.0 :
# Adding D500, D501, D502, D503, D504 and D505 scripts to insert unified_country, unified_gov,
# unified_district, unified_currency and unified_police_station to the STG layer and running the SRCI_LOADING
# SP to copy them to the SRCI layer
# ################################################################################################
# What is new : #2.14.7 :
# Adding D005 script to insert constants into base tables directly
# ################################################################################################
# What is new : #2.14.6 :
# converting not in to not exists in (000,001,615) scriptS
# ################################################################################################
# What is new : #2.14.5 :
# Adding alias to the joins
# ################################################################################################
# What is new : #2.14.4 :
# New Script is added to insert the constant bmaps into Bmap_Standard_Maps
#  Scripts : D004.PY
# ################################################################################################
# What is new : #2.14.3 :
# Trimming all Columns in D110
# Adding Staging Table in the etl process insert statement
# Removing from statement from base tables selecting constant values only
# Scripts : D000.py & D620.py
# ################################################################################################


if __name__ == '__main__':
    pass
