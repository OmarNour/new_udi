UPDATE	QT 
FROM GDEV1_ETL.TGT_TABLES_QUEUE QT, 
(
	select qq.* 	
	from
	(
							SELect Q.*
							FROM GDEV1_ETL.TGT_TABLES_QUEUE Q
							WHERE Q.TGT_TABLE_STATE = 0	
							AND  not EXISTS (
															SELECT 1 
															FROM GDEV1_ETL.TGT_TABLES_QUEUE x 
															WHERE ( x.TGT_TABLE_STATE > 0 and x.TGT_TABLE_STATE < 3) 
															AND x.TGT_TABLE = Q.TGT_TABLE AND x.TGT_LAYER = Q.TGT_LAYER
															) 
															
							and not exists (SELECT *
	  									FROM GDEV1_ETL.V_EXEC_PROCESS_LOGS l
	 									WHERE l.RUN_ID 			=   Q.RUN_ID
										and l.SOURCE_NAME		 =   Q.SOURCE_NAME
										and l.LOAD_ID 					=   Q.LOAD_ID										
										and l.LAYER_LEVEL 			<   Q.LAYER_LEVEL		
										and l.ERROR_CODE 		<>  0
										)
							QUALIFY ROW_NUMBER()  OVER(PARTITION BY Q.TGT_LAYER, Q.TGT_TABLE  ORDER BY Q.RUN_ID, Q.LAYER_LEVEL, Q.SOURCE_NAME)=1

	)  qq
	where not exists (select 1 
								from GDEV1_ETL.TGT_TABLES_QUEUE lvl 
								where lvl.RUN_ID 					=   qq.RUN_ID
								and lvl.SOURCE_NAME		 =   qq.SOURCE_NAME
								and lvl.LAYER_LEVEL 		<   qq.LAYER_LEVEL
								and lvl.TGT_TABLE_STATE < 3
								
								)

	QUALIFY dense_rank()  OVER(PARTITION BY qq.SOURCE_NAME  ORDER BY qq.RUN_ID, qq.LAYER_LEVEL)=1
) x
SET TGT_TABLE_STATE 			= 1
, LAST_UPDATE_TIME 				= CURRENT_TIMESTAMP
WHERE X.TGT_TABLE				= QT.TGT_TABLE
	AND X.TGT_LAYER					= QT.TGT_LAYER
	AND X.RUN_ID					= QT.RUN_ID
	AND X.SOURCE_NAME				= QT.SOURCE_NAME
	AND X.LOAD_ID					= QT.LOAD_ID
	AND QT.TGT_TABLE_STATE			= QT.TGT_TABLE_STATE;
------------------

SEL DISTINCT 
	Q.TGT_LAYER, Q.TGT_TABLE, Q.SOURCE_NAME, Q.LOAD_ID, Q.OVERRIDE_LOAD, Q.RUN_ID,
	(SELECT COUNT(1) 
		FROM GDEV1_ETL.PROCESS P 
		WHERE P.TGT_LAYER=Q.TGT_LAYER 
		and P.TGT_TABLE=Q.TGT_TABLE 
		and P.SOURCE_NAME=Q.SOURCE_NAME 
	) CNT_PROCESSES
FROM  GDEV1_ETL.TGT_TABLES_QUEUE	 Q
	
WHERE Q.TGT_TABLE_STATE = 1
ORDER BY CNT_PROCESSES DESC;
-------------------------------
update GDEV1_ETL.TGT_TABLES_QUEUE
set TGT_TABLE_STATE = 2, LAST_UPDATE_TIME = CURRENT_TIMESTAMP
where TGT_TABLE_STATE = 1;
-------------------------------
--delete from GDEV1_ETL.TGT_TABLES_QUEUE where TGT_TABLE_STATE = -1;