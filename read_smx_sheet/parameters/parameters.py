default_config_file_name = "config.txt"
parquet_db_name = "smx_data"
sys_argv_separator = "|#|"
stg_cols_separator = "||'_'||"

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
RI_relations_sht = "RI relations"
Data_types_sht = "Data type"
D215_template_filename = "D215.txt"
compareSTGcounts_template_filename = "compareSTGcountsALL.txt"
compareSTGacccounts_template_filename = "compareSTGcountsAccepted.txt"
dataValidation_template_filename = "dataValidationAccepted.txt"
dataValidationAll_template_filename = "dataValidationALL.txt"
sheets = [System_sht, Supplements_sht, Column_mapping_sht, BMAP_values_sht, BMAP_sht, BKEY_sht, STG_tables_sht, Table_mapping_sht, Core_tables_sht]
unified_gov_sheet = "Unified Gov"
unified_country_sheet = "Unified Country"
unified_city_sheet = "Unified City"
unified_district_sheet = "Unified District"
unified_currency_sheet = "Unified Currency"
unified_police_station_sheet = "Unified Police Station"

# AppName_<Major>.<Minor>.<BuildNo>
ver_no = "| Build #3.5.0"
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
# What is new : #3.1.0 :
# Run on all sources when no source is given by the user and generate summary report in the log file
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

