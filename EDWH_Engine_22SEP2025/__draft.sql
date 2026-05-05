select SOURCE_NAME, LOAD_ID, RUN_ID, TGT_LAYER
, COUNT(1) ALL_TGTs
, sum(CASE when TGT_TABLE_STATE = 3 /* processed */ then 1 else 0 end )ALL_TGTs_processed
from GDEV1_ETL.TGT_TABLES_QUEUE
group by SOURCE_NAME, LOAD_ID, RUN_ID, TGT_LAYER
order by SOURCE_NAME, LOAD_ID, RUN_ID, TGT_LAYER;


select 
SOURCE_NAME, LOAD_ID, RUN_ID, 'STG' TGT_LAYER
, sum(CASE when ERROR_CODE = 0 /* successfull */ then 1 else 0 end )ALL_processed_successfully
from GDEV1_ETL.EXEC_PROCESS_LOGS
where PROCESS_NAME like 'STG_%'
group by SOURCE_NAME, LOAD_ID, RUN_ID;
