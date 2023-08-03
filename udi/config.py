# [Test]
from collections import namedtuple
# from front_end import FrontEnd
# from app_Lib.functions import get_config_file_path, get_config_file_values
# from smx.views import test
# import app_Lib.functions as funcs
# from parameters import parameters as pm
# import os

# config_file_path = os.path.join(funcs.get_config_file_path(), pm.default_config_file_name)


# source_name = "NEW_COLLECTIONS"
# smx_path = "/Users/omarnour/Downloads/ACA- Production Citizen SMX.xlsx"
# smx_path = "/Users/omarnour/Downloads/SALES_SMX18May2023.xlsx"
# smx_path = "/Users/oh255011/Downloads/SALES_SMX21May2023.xlsx"
smx_path = r"/Users/oh255011/Documents/Teradata/SMX/[ACA] SMX_Economic_Units_03-01-2023.xlsx"
# smx_path = "/Users/omarnour/Downloads/SALES_SMX02APRL2023.xlsx"
# smx_path = "/Users/omarnour/Downloads/Production_Citizen_SMX.xlsx"
# smx_path = "/Users/omarnour/Downloads/[ACA] SMX_Economic_Units_03-01-2023.xlsx"
# smx_path = "/Users/omarnour/Downloads/Wasl Schema v1.7.xlsx"

pickle_path = "pickled_objs"
# scripts_path = "/Users/oh255011/Downloads/smx_scripts"
scripts_path = "smx_scripts"
DB_NAME = 'teradata'
HOST = 'localhost'
USER = 'power_user'
PASSWORD = 'power_user'

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
# LAYER_TYPES = ['META', 'SRC', 'STG', 'SK', 'SRCI', 'CORE']  # Important: DO NOT CHANGE THE ORDER OF THIS LIST!
# PREFIX = 'GDEV1'
# PREFIX = FrontEnd.db_prefix
# LAYERS = {
#     'META': LayerDtl(LAYER_TYPES[0], 0, f'{PREFIX}_ETL', f'{PREFIX}_ETL')
#     , 'SRC': LayerDtl(LAYER_TYPES[1], 0, f'{PREFIX}V_STG_ONLINE', 'STG_ONLINE')
#     , 'STG': LayerDtl(LAYER_TYPES[2], 1, f'{PREFIX}V_STG', f'{PREFIX}T_STG')
#     , 'TXF_BKEY': LayerDtl(LAYER_TYPES[3], 2, f'{PREFIX}V_INP', '')
#     , 'TXF_BMAP': LayerDtl(LAYER_TYPES[3], 2, f'{PREFIX}V_INP', '')
#     , 'BKEY': LayerDtl(LAYER_TYPES[3], 3, f'{PREFIX}V_UTLFW', f'{PREFIX}T_UTLFW')
#     , 'BMAP': LayerDtl(LAYER_TYPES[3], 3, f'{PREFIX}V_UTLFW', f'{PREFIX}T_UTLFW')
#     , 'SRCI': LayerDtl(LAYER_TYPES[4], 4, f'{PREFIX}V_SRCI', f'{PREFIX}T_SRCI')
#     , 'TXF_CORE': LayerDtl(LAYER_TYPES[4], 5, f'{PREFIX}V_INP', '')
#     , 'CORE': LayerDtl(LAYER_TYPES[5], 6, f'{PREFIX}V_BASE', f'{PREFIX}T_BASE')
# }


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

# MAIN_DB_NAME = f"{PREFIX}_EDWH"

# MAIN_DATABASE_TEMPLATE = f"""
# CREATE DATABASE {MAIN_DB_NAME}
# AS PERMANENT = 5000e6, -- 5GB
#     SPOOL = 1000e6; -- 1GB
# """

# DATABASE_TEMPLATE = """
# CREATE DATABASE {db_name} from {main_db_name}
# AS PERMANENT = 60e6, -- 60MB
#     SPOOL = 120e6; -- 120MB
# """
DATABASE_TEMPLATE = """
CREATE DATABASE {db_name} from {main_db_name}
AS TEST = 60e6, -- 60MB
    SPOOL = 120e6; -- 120MB
"""
# OTHER_SCHEMAS = [f"{PREFIX}_ETL"]
# OTHER_SCHEMAS = [DATABASE_TEMPLATE.format(db_name=f"{PREFIX}P_FF")
#     , DATABASE_TEMPLATE.format(db_name=f"{PREFIX}P_PP")]
##################### Metadata ####################
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

# GRANTS = f"""
# GRANT CREATE PROCEDURE ON {PREFIX}_ETL TO DBC WITH GRANT OPTION;
# GRANT CREATE FUNCTION ON {PREFIX}_ETL TO DBC WITH GRANT OPTION;
# GRANT EXECUTE PROCEDURE ON {PREFIX}_ETL TO DBC WITH GRANT OPTION;
# GRANT EXECUTE FUNCTION ON {PREFIX}_ETL TO DBC WITH GRANT OPTION;

# GRANT SELECT ON STG_ONLINE TO {PREFIX}V_STG_ONLINE WITH GRANT OPTION;
# GRANT SELECT ON {PREFIX}T_STG TO {PREFIX}V_STG WITH GRANT OPTION;
# GRANT SELECT ON {PREFIX}T_UTLFW TO {PREFIX}V_UTLFW WITH GRANT OPTION;
# GRANT SELECT ON {PREFIX}V_UTLFW TO {PREFIX}V_SRCI WITH GRANT OPTION;
# GRANT SELECT ON {PREFIX}T_UTLFW TO {PREFIX}V_SRCI WITH GRANT OPTION;
# GRANT SELECT ON {PREFIX}V_STG TO {PREFIX}V_SRCI WITH GRANT OPTION;
# GRANT SELECT ON {PREFIX}T_STG TO {PREFIX}V_SRCI WITH GRANT OPTION;
# GRANT SELECT ON {PREFIX}T_STG TO {PREFIX}V_INP WITH GRANT OPTION;
# GRANT SELECT ON {PREFIX}T_SRCI TO {PREFIX}V_INP WITH GRANT OPTION;
# GRANT SELECT ON {PREFIX}T_SRCI TO {PREFIX}_ETL WITH GRANT OPTION;
# GRANT SELECT ON {PREFIX}V_SRCI TO {PREFIX}_ETL WITH GRANT OPTION;
# GRANT CREATE TABLE ON {PREFIX}T_SRCI TO {PREFIX}_ETL WITH GRANT OPTION;
# GRANT DROP TABLE ON {PREFIX}T_SRCI TO {PREFIX}_ETL WITH GRANT OPTION;
# GRANT SELECT ON {PREFIX}T_BASE TO {PREFIX}V_BASE WITH GRANT OPTION;

# GRANT SELECT ON DBC TO {PREFIX}_ETL WITH GRANT OPTION;
# GRANT SELECT ON {PREFIX}V_INP TO {PREFIX}_ETL WITH GRANT OPTION;
# GRANT SELECT ON {PREFIX}T_UTLFW TO {PREFIX}_ETL WITH GRANT OPTION;
# GRANT INSERT ON {PREFIX}T_UTLFW TO {PREFIX}_ETL WITH GRANT OPTION;
# GRANT SELECT ON {PREFIX}T_BASE TO {PREFIX}_ETL WITH GRANT OPTION;
# GRANT DELETE ON {PREFIX}T_BASE TO {PREFIX}_ETL WITH GRANT OPTION;
# GRANT UPDATE ON {PREFIX}T_BASE TO {PREFIX}_ETL WITH GRANT OPTION;
# GRANT INSERT ON {PREFIX}T_BASE TO {PREFIX}_ETL WITH GRANT OPTION;
# GRANT SELECT ON {PREFIX}V_BASE TO {PREFIX}_ETL WITH GRANT OPTION;
# GRANT SELECT ON {PREFIX}V_STG_ONLINE TO {PREFIX}_ETL WITH GRANT OPTION;
# GRANT SELECT ON {PREFIX}T_STG TO {PREFIX}_ETL WITH GRANT OPTION;
# GRANT DELETE ON {PREFIX}T_STG TO {PREFIX}_ETL WITH GRANT OPTION;
# GRANT UPDATE ON {PREFIX}T_STG TO {PREFIX}_ETL WITH GRANT OPTION;
# GRANT INSERT ON {PREFIX}T_STG TO {PREFIX}_ETL WITH GRANT OPTION;
# GRANT EXECUTE PROCEDURE ON {PREFIX}_ETL TO {PREFIX}_ETL WITH GRANT OPTION;
# GRANT EXECUTE FUNCTION ON {PREFIX}_ETL TO {PREFIX}_ETL WITH GRANT OPTION;
# """

if __name__ == '__main__':
    print(OTHER_SCHEMAS)
