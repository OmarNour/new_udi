select * from GPROD1_ETL.LAYERS ORDER BY LAYER_LEVEL;
select * from GPROD1_ETL.V_SOURCE_SYSTEMS;
select * from GPROD1_ETL.V_SOURCE_SYSTEM_TABLES;
select * from GPROD1_ETL.V_EDW_TABLES;
select * from GPROD1_ETL.V_TRANSFORM_KEYCOL;
select * from GPROD1_ETL.V_PROCESS;
select * from GPROD1_ETL.HISTORY;
-------------------------------------------------------------------------------------------
select
SOURCE_NAME
,PROCESS_NAME
,APPLY_TYPE
,TGT_DB
,TGT_TABLE
,KEY_COLUMNS
,START_DATE_COLUMN
,END_DATE_COLUMN
,HISTORY_COLUMNS
,DOMAIN_ID
,KEY_SET_ID
,CODE_SET_ID
,ACTIVE
from GPROD1_ETL.V_EDW_SCRIPTS_COMPOSER order by SOURCE_NAME, PROCESS_NAME, APPLY_TYPE;
-------------------------------------------------------------------------------------------
select * from GPROD1_ETL.CDC_AUDIT 
where 1=1
and source_name = 'CSO'
order by batch_id, table_name;
-------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------
select * from GPROD1_ETL.TGT_TABLES_QUEUE ORDER BY layer_Level, TGT_TABLE_STATE

select * from GPROD1_ETL.V_EXEC_SOURCE_LOAD_LOGS order by run_id, load_id, layer_level;

select * from GPROD1_ETL.V_EXEC_SOURCE_LOGS order by START_TIMESTAMP desc, END_TIMESTAMP desc;
select * from GPROD1_ETL.V_EXEC_PROCESS_LOGS  where RUN_ID = 1694592500387 order by START_TIMESTAMP desc, END_TIMESTAMP desc;
select * from GPROD1_ETL.V_EXEC_SCRIPT_LOGS  where RUN_ID = 1694592500387  order by START_TIMESTAMP desc, END_TIMESTAMP desc;





	
	


    
    
