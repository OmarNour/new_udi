REPLACE VIEW /*VER.01*/ GPROD1_ETL.V_CDC_AUDIT AS LOCK ROW FOR ACCESS
select * from GPROD1_ETL.CDC_AUDIT;

REPLACE VIEW /*VER.01*/ GPROD1_ETL.V_SOURCE_SYSTEMS AS LOCK ROW FOR ACCESS
select DS.*, l.lyr_db,l.src_db,l.layer_level
from GPROD1_ETL.SOURCE_SYSTEMS DS,GPROD1_ETL.LAYERS l
where l.layer_name = DS.SOURCE_LAYER;
--------------------------------------------------------------------------------------------------------
REPLACE VIEW /*VER.01*/ GPROD1_ETL.V_SOURCE_SYSTEM_TABLES AS LOCK ROW FOR ACCESS
select  t.*
		,stg_l.src_db stg_src_db		
		,stg_l.lyr_db stg_db  
		,stg_l.layer_level stg_layer_level
		,stg_l.layer_name stg_layer_name
		
		,srci_l.src_db srci_src_db
		,srci_l.lyr_db srci_db  
		,srci_l.layer_level srci_layer_level
		,srci_l.layer_name srci_layer_name
		
from GPROD1_ETL.SOURCE_SYSTEM_TABLES t,GPROD1_ETL.LAYERS stg_l ,GPROD1_ETL.LAYERS srci_l
where stg_l.layer_name = 'STG'
and srci_l.layer_name = 'SRCI';
--------------------------------------------------------------------------------------------------------
REPLACE VIEW /*VER.01*/ GPROD1_ETL.V_EDW_TABLES AS LOCK ROW FOR ACCESS
select t.*, l.lyr_db,l.src_db,l.layer_level
from GPROD1_ETL.EDW_TABLES t,GPROD1_ETL.LAYERS l
where l.layer_name = T.layer_name;
--------------------------------------------------------------------------------------------------------
REPLACE VIEW /*VER.01*/ GPROD1_ETL.V_TRANSFORM_KEYCOL AS LOCK ROW FOR ACCESS
select KC.*, l.lyr_db
from GPROD1_ETL.TRANSFORM_KEYCOL KC,GPROD1_ETL.LAYERS l
where l.layer_name = KC.LAYER_NAME;
--------------------------------------------------------------------------------------------------------
REPLACE VIEW /*VER.02*/ GPROD1_ETL.V_PROCESS AS LOCK ROW FOR ACCESS
select 
	 p.PROCESS_NAME (varchar(200))
	,p.SOURCE_NAME
	,p.TGT_LAYER
	,p.TGT_TABLE
	,p.SRC_TABLE
	,p.APPLY_TYPE (VARCHAR(20)) 
	,p.MAIN_TABLE_NAME
	,p.KEY_SET_ID
	,p.CODE_SET_ID
	,p.DOMAIN_ID
	,tgt_l.src_db src_db 
	,tgt_l.lyr_db tgt_db
	,tgt_l.layer_Level
	,p.ACTIVE
from GPROD1_ETL.PROCESS P ,GPROD1_ETL.LAYERS tgt_l
where tgt_l.layer_name = p.tgt_layer
UNION ALL 
select
 lyr.layer_name ||'_'||t.TABLE_NAME  (varchar(200)) PROCESS_NAME
,T.SOURCE_NAME
,lyr.layer_name TGT_LAYER
,t.TABLE_NAME TGT_TABLE
,t.TABLE_NAME SRC_TABLE
, 'UPSERT' (VARCHAR(20)) APPLY_TYPE
, t.TABLE_NAME MAIN_TABLE_NAME
, NULL KEY_SET_ID
, NULL CODE_SET_ID
, NULL  DOMAIN_ID

,lyr.src_db src_db 
,lyr.lyr_db tgt_db
,lyr.layer_Level
,T.ACTIVE	
from GPROD1_ETL.SOURCE_SYSTEM_TABLES t,GPROD1_ETL.LAYERS lyr
where lyr.layer_name in ('STG','SRCI');
--------------------------------------------------------------------------------------------------------
REPLACE VIEW /*VER.05*/ GPROD1_ETL.V_EDW_SCRIPTS_COMPOSER AS LOCK ROW FOR ACCESS 
WITH 
CTE_HISTORY_COLS AS
(
	SELECT PROCESS_NAME
	, TRIM( TRAILING  ',' FROM (XMLAGG(trim(HISTORY_COLUMN)|| ',' ORDER BY HISTORY_COLUMN) (VARCHAR(10000)))) HISTORY_COLUMNS
	, TRIM( TRAILING  ',' FROM (XMLAGG(trim(HISTORY_COLUMN||' = X.'||HISTORY_COLUMN)|| ',' ORDER BY HISTORY_COLUMN) (VARCHAR(10000)))) SET_HISTORY_COLUMNS
	, TRIM( TRAILING  ',' FROM (XMLAGG(trim('X.'||HISTORY_COLUMN)|| ',' ORDER BY HISTORY_COLUMN) (VARCHAR(10000)))) X_HISTORY_COLUMNS
	, TRIM( TRAILING  ',' FROM (XMLAGG(trim('COALESCE(X.'||HISTORY_COLUMN||','''')') ||HISTORY_COLUMN||'_X,' ORDER BY HISTORY_COLUMN) (VARCHAR(10000)))) COALESCE_HIST_COLX
	, TRIM( TRAILING  ',' FROM (XMLAGG(trim('COALESCE(Y.'||HISTORY_COLUMN||','''')') ||HISTORY_COLUMN||'_Y,' ORDER BY HISTORY_COLUMN) (VARCHAR(10000)))) COALESCE_HIST_COLY
	, TRIM( TRAILING  '|' FROM (XMLAGG(HISTORY_COLUMN|| '_X||' ORDER BY HISTORY_COLUMN) (VARCHAR(10000)))) 	|| ' HC_X'					CONCATE_HIST_COL_X
	, TRIM( TRAILING  '|' FROM (XMLAGG(HISTORY_COLUMN|| '_Y||' ORDER BY HISTORY_COLUMN) (VARCHAR(10000))))	|| ' HC_Y'					CONCATE_HIST_COL_Y
	
	FROM GPROD1_ETL.HISTORY
	GROUP BY PROCESS_NAME
)
, CTE_PROCESS AS
(
	SELECT 
	P.PROCESS_NAME,
    P.SOURCE_NAME,
    P.SRC_DB,
    P.SRC_TABLE,
    P.TGT_DB,
    P.TGT_TABLE,
    P.tgt_layer,
    P.APPLY_TYPE,
    P.MAIN_TABLE_NAME,
    P.KEY_SET_ID,
    P.CODE_SET_ID,
    P.DOMAIN_ID,
    P.ACTIVE
    FROM GPROD1_ETL.V_PROCESS P
    	JOIN GPROD1_ETL.V_EDW_TABLES TBL
    	ON P.TGT_TABLE = TBL.TABLE_NAME
		AND P.tgt_layer = TBL.layer_name
)
,CTE_KEY_COLS as
(
	SELECT p.PROCESS_NAME, x.lyr_db db_name, X.TABLE_NAME
		, TRIM( TRAILING  ',' FROM (XMLAGG(trim(KEY_COLUMN)|| ',' ORDER BY KEY_COLUMN) (VARCHAR(10000)))) AS KEY_COLUMNS
		, OREPLACE(TRIM( TRAILING  ',' FROM (XMLAGG('X.'||KEY_COLUMN||' = Y.'||KEY_COLUMN || ',' ORDER BY KEY_COLUMN) (VARCHAR(10000)))), ', ', ' AND ' ) AS KEY_COLUMNS_JOIN
		, TRIM( TRAILING  ',' FROM (XMLAGG(('COALESCE(X.'||KEY_COLUMN||', Y.'||KEY_COLUMN||' ) '||KEY_COLUMN)|| ',' ORDER BY KEY_COLUMN) (VARCHAR(10000)))) AS COALESCE_KEY_COLUMN
	FROM CTE_PROCESS p ,GPROD1_ETL.V_TRANSFORM_KEYCOL X
	WHERE P.TGT_TABLE = X.TABLE_NAME
	and P.tgt_layer = X.LAYER_NAME
	and NOT EXISTS (
						SELECT 1 
						FROM GPROD1_ETL.V_EDW_TABLES Y 
						WHERE X.LAYER_NAME= Y.LAYER_NAME
						AND Y.TABLE_NAME=X.TABLE_NAME
						AND Y.START_DATE_COLUMN = X.KEY_COLUMN
						AND Y.IS_HISTORY = 1
						AND P.APPLY_TYPE = 'HISTORY'
					)
	AND NOT EXISTS (
						SELECT 1 
						FROM GPROD1_ETL.HISTORY Y 
						WHERE P.PROCESS_NAME=y.PROCESS_NAME
						AND X.KEY_COLUMN = Y.HISTORY_COLUMN
						AND P.APPLY_TYPE = 'HISTORY'
						)
	GROUP BY p.PROCESS_NAME, x.lyr_db, X.TABLE_NAME
)
, CTE_NORMAL_COMMON_COLS as
(
	SELECT
	PROCESS_NAME
	, TRIM( TRAILING  ',' FROM (XMLAGG(trim(columnname)|| ',' ORDER BY columnname) (VARCHAR(10000)))) NORMAL_COMMON_COLUMNS
	, TRIM( TRAILING  ',' FROM (XMLAGG('X.'||columnname || ',' ORDER BY columnname) (VARCHAR(10000)))) X_NORMAL_COMMON_COLUMNS
	, TRIM( TRAILING  ',' FROM (XMLAGG(trim(columnname||' = X.'||columnname)|| ',' ORDER BY columnname) (VARCHAR(10000)))) SET_NORMAL_COMMON_COLUMNS
	FROM 
	(
		select P.PROCESS_NAME, V.columnname 
		from dbc.columnsv v
		join CTE_PROCESS P 
		on P.SRC_DB=v.databasename and P.SRC_TABLE=v.tablename
		and V.columnname not in ('BATCH_ID', 'MODIFICATION_TYPE', 'REF_KEY', 'End_Date', 'Start_Date')
		
		INTERSECT
		
		select P.PROCESS_NAME, columnname 
		from dbc.columnsv v
		join CTE_PROCESS P 
		on P.TGT_DB=v.databasename and P.TGT_TABLE=v.tablename
		AND NOT EXISTS (
							SELECT 1 
							FROM GPROD1_ETL.V_EDW_TABLES Y 
							WHERE v.databasename=Y.lyr_db
							AND v.tablename=y.TABLE_NAME
							AND (v.columnname = Y.START_DATE_COLUMN or v.columnname = Y.END_DATE_COLUMN)
							AND Y.IS_HISTORY = 1
							AND P.APPLY_TYPE = 'HISTORY'
						)
		AND NOT EXISTS (
							SELECT 1 
							FROM GPROD1_ETL.V_TRANSFORM_KEYCOL Y 
							WHERE v.databasename=Y.lyr_db
							AND v.tablename=y.TABLE_NAME
							AND v.columnname = Y.KEY_COLUMN
						)
		AND NOT EXISTS (
							SELECT 1 
							FROM GPROD1_ETL.HISTORY Y 
							WHERE P.PROCESS_NAME=y.PROCESS_NAME
							AND v.columnname = Y.HISTORY_COLUMN
							AND P.APPLY_TYPE = 'HISTORY'
						)
	) X
	GROUP BY PROCESS_NAME
)
SELECT
	P.PROCESS_NAME,
    P.SOURCE_NAME,
    P.SRC_DB,
    P.SRC_TABLE,
    P.TGT_DB,
    P.TGT_TABLE,
   	KC.KEY_COLUMNS,
   	KC.KEY_COLUMNS_JOIN,
   	KC.COALESCE_KEY_COLUMN,
   	CTBL.START_DATE_COLUMN,
   	CTBL.END_DATE_COLUMN,
    HC.HISTORY_COLUMNS,
    HC.X_HISTORY_COLUMNS,
    HC.COALESCE_HIST_COLX,
    HC.COALESCE_HIST_COLY,
    HC.CONCATE_HIST_COL_X,
    HC.CONCATE_HIST_COL_Y,
    HC.SET_HISTORY_COLUMNS,
    P.APPLY_TYPE,
    P.KEY_SET_ID,
    P.CODE_SET_ID,
    P.DOMAIN_ID,
    P.ACTIVE,
    NCC.NORMAL_COMMON_COLUMNS,
    NCC.X_NORMAL_COMMON_COLUMNS,
    NCC.SET_NORMAL_COMMON_COLUMNS
FROM CTE_PROCESS P
	
	LEFT JOIN CTE_KEY_COLS KC
	ON P.TGT_DB=KC.DB_NAME
	AND P.TGT_TABLE=KC.TABLE_NAME
	and p.PROCESS_NAME=KC.PROCESS_NAME
	
	LEFT JOIN GPROD1_ETL.V_EDW_TABLES CTBL
	ON CTBL.TABLE_NAME=P.TGT_TABLE
	AND CTBL.layer_name = P.tgt_layer
	AND CTBL.IS_HISTORY = 1
	AND P.APPLY_TYPE = 'HISTORY'
	
	LEFT JOIN CTE_HISTORY_COLS HC
	ON HC.PROCESS_NAME=P.PROCESS_NAME
	AND P.APPLY_TYPE = 'HISTORY'
	
	LEFT JOIN CTE_NORMAL_COMMON_COLS NCC
	ON NCC.PROCESS_NAME=P.PROCESS_NAME;
--------------------------------------------------------------------------------------------------------
REPLACE VIEW /*VER.01*/ GPROD1_ETL.V_EXEC_SOURCE_LOGS AS LOCK ROW FOR ACCESS 
select
RUN_ID
,LOAD_ID
,SOURCE_NAME
,START_TIMESTAMP
,END_TIMESTAMP
,(COALESCE(END_TIMESTAMP, CURRENT_TIMESTAMP ) - START_TIMESTAMP ) DAY(4) TO SECOND TIME_ELAPSED
,STG_DONE
,BKEY_DONE
,SRCI_DONE
,CORE_DONE
,DONE
,DELETED
,ROWS_INSERTED_COUNT
,ROWS_UPDATED_COUNT
,ROWS_DELETED_COUNT
,ERROR_CODE
,ERROR_MSG
,CREATED_BY
from GPROD1_ETL.EXEC_SOURCE_LOGS;
--------------------------------------------------------------------------------------------------------
REPLACE VIEW /*VER.01*/ GPROD1_ETL.V_EXEC_SCRIPT_LOGS AS LOCK ROW FOR ACCESS 
select
 RUN_ID
,LOAD_ID
,SOURCE_NAME
,PROCESS_NAME
,START_TIMESTAMP
,END_TIMESTAMP
,(COALESCE(END_TIMESTAMP, CURRENT_TIMESTAMP ) - START_TIMESTAMP ) DAY(4) TO SECOND TIME_ELAPSED
,ROWS_COUNT
,ERROR_CODE
,ERROR_MSG
,CREATED_BY
,SQL_SCRIPT_SEQ
,SQL_SCRIPT
from GPROD1_ETL.EXEC_SCRIPT_LOGS;
--------------------------------------------------------------------------------------------------------
REPLACE VIEW /*VER.02*/ GPROD1_ETL.V_EXEC_PROCESS_LOGS AS LOCK ROW FOR ACCESS 
select
 pl.RUN_ID
,pl.LOAD_ID
,pl.SOURCE_NAME
,pl.PROCESS_NAME
,lyr.layer_name
,lyr.layer_Level
,pl.START_TIMESTAMP
,pl.END_TIMESTAMP
,(COALESCE(pl.END_TIMESTAMP, CURRENT_TIMESTAMP ) - pl.START_TIMESTAMP ) DAY(4) TO SECOND TIME_ELAPSED
,pl.ROWS_INSERTED_COUNT
,pl.ROWS_UPDATED_COUNT
,pl.ROWS_DELETED_COUNT
,pl.ERROR_CODE
,pl.ERROR_MSG
,pl.CREATED_BY
from GPROD1_ETL.EXEC_PROCESS_LOGS pl
left join GPROD1_ETL.LAYERS lyr
on 
case 
	when pl.PROCESS_NAME like 'STG_%' then 'STG' 
	when pl.PROCESS_NAME like 'BK_%' then 'BKEY' 
	when pl.PROCESS_NAME like 'SRCI_%' then 'SRCI' 
	when pl.PROCESS_NAME like 'TXF_%' then 'CORE' 
end = lyr.layer_name;
--------------------------------------------------------------------------------------------------------
REPLACE VIEW /*VER.02*/ GPROD1_ETL.V_EXEC_SOURCE_LOAD_LOGS AS LOCK ROW FOR ACCESS 
with 
current_run_id as
(
	SELECT RUN_ID, SOURCE_NAME,LOAD_ID 
	,min(START_TIMESTAMP ) START_TIMESTAMP
	,max(END_TIMESTAMP ) END_TIMESTAMP
	FROM GPROD1_ETL.EXEC_SOURCE_LOGS --WHERE RUN_ID = (select distinct RUN_ID from GPROD1_ETL.EXEC_SOURCE_LOGS where DONE=0)
	group by RUN_ID, SOURCE_NAME,LOAD_ID 
)
, process_count as
(
	SELECT p.TGT_LAYER, p.SOURCE_NAME, p.layer_level, COUNT(1) CNT_PROCESSES 
	FROM GPROD1_ETL.V_PROCESS p , GPROD1_ETL.SOURCE_SYSTEMS s
	WHERE P.ACTIVE = 1 AND S.SOURCE_NAME = P.SOURCE_NAME
	and ((p.TGT_LAYER in ('SRCI', 'STG') AND STG_ACTIVE = 1) OR (p.TGT_LAYER IN ('BKEY','CORE')AND BASE_ACTIVE = 1))
	GROUP BY p.TGT_LAYER, p.SOURCE_NAME, p.layer_level
)
,processes_cnt_per_src as 
(
	SELECT r.RUN_ID, p.TGT_LAYER, p.SOURCE_NAME, r.LOAD_ID, p.layer_level, p.CNT_PROCESSES , r.START_TIMESTAMP, r.END_TIMESTAMP
	FROM process_count p , current_run_id r	
	where p.SOURCE_NAME=r.SOURCE_NAME
	
) 
, LOGS_LAST_ERORR_CODE_MSG as
(
	select X.SOURCE_NAME, X.LOAD_ID, X.RUN_ID, X.LAYER_LEVEL, X.LAYER_NAME
				, X.ERROR_CODE LAST_ERROR_CODE
				, X.ERROR_MSG	LAST_ERROR_MSG
	from GPROD1_ETL.v_EXEC_PROCESS_LOGS x
	QUALIFY ROW_NUMBER()  OVER(PARTITION BY x.SOURCE_NAME, x.LOAD_ID, x.RUN_ID, X.LAYER_NAME  ORDER BY case when ERROR_CODE = 0 then 0 else 1 end desc, START_TIMESTAMP DESC)=1
)
, LOGS_COUNTS as
(
	select x.SOURCE_NAME, x.LOAD_ID, x.RUN_ID, X.layer_Level, x.LAYER_NAME
			, sum(CASE when ERROR_CODE = 0 /* successfull */  	then 1 else 0 end ) passed
			, sum(CASE when ERROR_CODE <> 0 /* Failed */  		then 1 else 0 end ) failed		
			, SUM(ROWS_INSERTED_COUNT) ROWS_INSERTED_COUNT
			, SUM(ROWS_UPDATED_COUNT) ROWS_UPDATED_COUNT
			, SUM(ROWS_DELETED_COUNT) ROWS_DELETED_COUNT
	from (
			select *
			from GPROD1_ETL.v_EXEC_PROCESS_LOGS
			QUALIFY ROW_NUMBER()  OVER(PARTITION BY SOURCE_NAME, LOAD_ID, RUN_ID, PROCESS_NAME ORDER BY START_TIMESTAMP DESC)=1
		) x
	
	GROUP BY x.SOURCE_NAME, x.LOAD_ID, x.RUN_ID, x.LAYER_NAME, X.layer_Level
)
, EXEC_PROCESS_LOGS_cntp as
(
	select 
		 Y.SOURCE_NAME 
		, Y.TGT_LAYER LAYER_NAME
		, Y.LAYER_LEVEL
		, Y.LOAD_ID
		, Y.RUN_ID		
		, Y.CNT_PROCESSES
		
		, COALESCE(X.PASSED,0) PASSED
		, COALESCE(X.FAILED,0) FAILED
		, CASE WHEN (COALESCE(X.PASSED,0) + COALESCE(X.FAILED,0))  = Y.CNT_PROCESSES THEN 1 ELSE 0 END LAYER_DONE
		
		, COALESCE(X.ROWS_INSERTED_COUNT	,0) ROWS_INSERTED_COUNT
		, COALESCE(X.ROWS_UPDATED_COUNT		,0) ROWS_UPDATED_COUNT
		, COALESCE(X.ROWS_DELETED_COUNT 	,0) ROWS_DELETED_COUNT
		
		, Y.START_TIMESTAMP
		, y.END_TIMESTAMP
		, Z.LAST_ERROR_CODE
		, Z.LAST_ERROR_MSG
	from processes_cnt_per_src y
	
	left join LOGS_COUNTS  x
		
		JOIN LOGS_LAST_ERORR_CODE_MSG Z
		ON  x.source_name = Z.source_name
		and x.LOAD_ID = Z.LOAD_ID
		and x.RUN_ID = Z.RUN_ID
		and x.LAYER_NAME = Z.LAYER_NAME
	
	on x.source_name = y.source_name
	and x.LAYER_NAME = y.TGT_LAYER
	and x.run_id = y.run_id
	and x.LOAD_ID = y.LOAD_ID
	
	
)
, logs as
(
	select 
		x.* 
		,LAG(FAILED) 				OVER(PARTITION BY SOURCE_NAME, LOAD_ID, RUN_ID ORDER BY LAYER_LEVEL) _PRECEDING_FAILED_CNT
		,LAG(LAYER_DONE) 			OVER(PARTITION BY SOURCE_NAME, LOAD_ID, RUN_ID ORDER BY LAYER_LEVEL) _PRECEDING_LAYER_DONE
		,sum(FAILED) 				OVER(PARTITION BY SOURCE_NAME, LOAD_ID, RUN_ID ORDER BY LAYER_LEVEL rows between  unbounded preceding and  1 preceding) _SUM_PREV_FAILS
		,case when coalesce(_SUM_PREV_FAILS,0) > 0 then 0 else CNT_PROCESSES end _CNT_PROCESSES
	from EXEC_PROCESS_LOGS_cntp x
)

select 
 x.SOURCE_NAME
,x.LAYER_NAME
,x.layer_Level
,x.LOAD_ID
,x.RUN_ID
,x._CNT_PROCESSES CNT_PROCESSES
,x.PASSED
,x.FAILED
, COALESCE (Y.DONE ,x.LAYER_DONE)  AS LAYER_DONE
,COALESCE (y.ROWS_INSERTED_COUNT,x.ROWS_INSERTED_COUNT ) as ROWS_INSERTED_COUNT
,COALESCE (Y.ROWS_UPDATED_COUNT,x.ROWS_UPDATED_COUNT  )  AS ROWS_UPDATED_COUNT
,COALESCE (Y.ROWS_DELETED_COUNT,x.ROWS_DELETED_COUNT  )   AS ROWS_DELETED_COUNT
, COALESCE( Y.START_TIMESTAMP,x.START_TIMESTAMP) AS  START_TIMESTAMP 
, COALESCE( Y.END_TIMESTAMP , x.END_TIMESTAMP ) AS END_TIMESTAMP
,(COALESCE(X.END_TIMESTAMP, CURRENT_TIMESTAMP ) - X.START_TIMESTAMP ) DAY(4) TO SECOND TIME_ELAPSED
,coalesce(x._PRECEDING_FAILED_CNT,0) PRECEDING_FAILED_CNT
,coalesce(x._PRECEDING_LAYER_DONE,1) PRECEDING_LAYER_DONE
, COALESCE (Y.ERROR_CODE, x.LAST_ERROR_CODE ) AS LAST_ERROR_CODE
,COALESCE (Y.ERROR_MSG , x.LAST_ERROR_MSG ) AS  LAST_ERROR_MSG
from logs x
LEFT JOIN   GPROD1_ETL.EXEC_SOURCE_LOGS  Y
ON  X.RUN_ID = Y.RUN_ID
AND X.LOAD_ID = Y.LOAD_ID
AND X.SOURCE_NAME = Y.SOURCE_NAME
AND  DONE = 1 ;
--------------------------------------------------------------------------------------------------------
REPLACE VIEW /*VER.02*/ GPROD1_ETL.V_collect_stats AS LOCK ROW FOR ACCESS  
with 
_ AS 
(SELECT 1 X)
,__bkey_keys as 
(
	select 'SOURCE_KEY' KEY_COLUMN FROM _
	UNION
	select 'DOMAIN_ID' KEY_COLUMN FROM _
)
,_bkey_keys AS
(
	SELECT distinct l.lyr_db, p.TGT_TABLE TABLE_NAME ,  k.KEY_COLUMN
	FROM GPROD1_ETL.v_process p , GPROD1_ETL.LAYERS l, __bkey_keys k
	where l.layer_name = 'BKEY'
	and l.layer_name = p.TGT_LAYER
	and exists (select 1 from dbc.TablesV v where v.DataBaseName=l.lyr_db and v.TableName=p.TGT_TABLE)
)
,bkey_keys AS
(	select b.* 
	, case when v.indexType is not null then 1 else 0 end is_col_indexed
	, case when pc.constraintText is not null then 1 else 0 end is_col_partitioned
	from _bkey_keys b
		
		left join DBC.Indicesv v
		on v.DatabaseName = b.lyr_db
		and v.tablename = b.TABLE_NAME
		and v.columnname = b.KEY_COLUMN
		
		left join DBC.PartitioningConstraintsV pc
		on pc.DatabaseName = b.lyr_db
		and pc.tablename = b.TABLE_NAME
		and lower(pc.constraintText) LIKE  '%'||lower(b.KEY_COLUMN)||'%'
		
	)
--SELECT * FROM bkey_keys
,kel_col_trnsfr as
(
	select * from GPROD1_ETL.V_TRANSFORM_KEYCOL 
	where layer_name in
		(
		'STG'
		,'SRCI'
		,'CORE'
		)
	and exists (select 1 from DBC.columnsv v where v.DataBaseName = lyr_db and v.TableName = TABLE_NAME and columnname = KEY_COLUMN)
)
, _all_keys as
(
	select LYR_DB, TABLE_NAME, KEY_COLUMN, 0 is_col_indexed, 0 is_col_partitioned from kel_col_trnsfr
	union all
	select LYR_DB, TABLE_NAME, KEY_COLUMN, is_col_indexed, is_col_partitioned from bkey_keys
)
,all_keys as
(
	select LYR_DB, TABLE_NAME, KEY_COLUMN, 0 is_tbl_partitioned , is_col_partitioned
	from _all_keys x where not exists (select 1 from _all_keys y where x.LYR_DB=y.LYR_DB and x.TABLE_NAME=y.TABLE_NAME and is_col_partitioned = 1)
	union all
	select LYR_DB, TABLE_NAME, KEY_COLUMN, 1 is_tbl_partitioned , is_col_partitioned
	from _all_keys x where exists (select 1 from _all_keys y where x.LYR_DB=y.LYR_DB and x.TABLE_NAME=y.TABLE_NAME and is_col_partitioned = 1)
)
,cols_in_row
as
(
	select  lyr_db, TABLE_NAME,is_tbl_partitioned, TRIM( TRAILING  ',' FROM (XMLAGG(trim(KEY_COLUMN)|| ',' ORDER BY KEY_COLUMN) (VARCHAR(10000)))) KEY_COLUMNs
	from all_keys
	group by lyr_db, TABLE_NAME,is_tbl_partitioned
)
, union_all as
(
	
	select lyr_db, TABLE_NAME,'COLLECT STATISTICS COLUMN ('||case when is_tbl_partitioned = 1 then ' PARTITION ' else '' end||')     	ON '||lyr_db||'.'||TABLE_NAME||';' collect_stats from cols_in_row where is_tbl_partitioned = 1
	union all
	select lyr_db, TABLE_NAME,'COLLECT STATISTICS COLUMN ('||case when is_tbl_partitioned = 1 then ' PARTITION, ' else '' end|| KEY_COLUMNs||')     	ON '||lyr_db||'.'||TABLE_NAME||';' collect_stats from cols_in_row 
	union all
	select lyr_db, TABLE_NAME, 'COLLECT STATISTICS COLUMN ('||KEY_COLUMN||')     	ON '||lyr_db||'.'||TABLE_NAME||';' collect_stats  from all_keys where is_col_partitioned = 0
)

select distinct * from union_all;