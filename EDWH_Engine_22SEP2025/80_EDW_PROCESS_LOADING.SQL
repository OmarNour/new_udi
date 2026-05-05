REPLACE PROCEDURE /*VER.13*/ GPROD1_ETL.EDW_PROCESS_LOADING
    /*
    ###################################################################################################################################################################
    ------------------------------------------------------------------------------------------------------------------------------------------------------------------
    #  Version History
    ------------------------------------------------------------------------------------------------------------------------------------------------------------------
    # 	Version 	|	Date        	    |	Author					| 	Comments
    #   1.0        	|	1 JAN, 2023	        |	 Omar Nour				|	Fisrt Cut.
	#   13          |	16 NOV, 2023		|	 AHMED EHAB				|	CHANGE THIS CONDITION TO BE "x.batch_id < y.batch_id" INSTEAD OF "x.batch_id <= y.batch_id" TO UPDATE THE RECORED EVEN IF THE ROW INSERTED BEFORE
  
	------------------------------------------------------------------------------------------------------------------------------------------------------------------
    #  Examples:
    #  
    ###################################################################################################################################################################
    */
    (
    IN 		I_PROCESSNAME  			VARCHAR(1000),
    IN 		I_APPLY_TYPE  			VARCHAR(500),
    IN 		I_RUN_ID 				BIGINT,
    IN		I_LOAD_ID				VARCHAR(500),
	IN		I_DELTA_MODE			INTEGER,
    OUT  	O_ROWS_INSERTED_COUNT	FLOAT,
    OUT  	O_ROWS_UPDATED_COUNT	FLOAT,
    OUT  	O_ROWS_DELETED_COUNT	FLOAT,
    OUT  	O_RETURN_CODE  			INTEGER,
    OUT  	O_RETURN_MSG 			VARCHAR(5000),
    OUT 	o_run_id 				BIGINT
    )
    BEGIN
		DECLARE 	v_DELTA_MODE				INTEGER DEFAULT 1;
    	DECLARE 	v_START_TIMESTAMP 			TIMESTAMP(6) default current_timestamp;
        DECLARE 	V_RETURN_CODE  				INTEGER DEFAULT 0;
        DECLARE 	V_RETURN_MSG 				VARCHAR(5000) DEFAULT 'Process Completed Successfully';
        DECLARE 	V_ROWS_IN_TMP, V_ROWS_COUNT				FLOAT DEFAULT 0;
        DECLARE 	V_KEYS_IN_BASE_CT						INTEGER DEFAULT 0;
        DECLARE		v_run_id, v_dbc_run_id		BIGINT;
        DECLARE 	V_SQL_SCRIPT_ID				INTEGER DEFAULT 0;
        DECLARE 	V_DBC_RETURN_CODE 			INTEGER;
		DECLARE 	V_DBC_RETURN_MSG  			VARCHAR(1000);
		DECLARE 	V_INSERTED_ROWS_COUNT,V_UPDATED_ROWS_COUNT,V_DELETED_ROWS_COUNT,V_DBC_ROWS_COUNT			FLOAT DEFAULT 0;
                                                                
        DECLARE 	V_SOURCE_NAME, V_LOADING_MODE VARCHAR(500);
        DECLARE    	V_SRC_DB					VARCHAR(500);
        DECLARE    	V_SRC_TABLE					VARCHAR(500);
        DECLARE    	V_TGT_DB					VARCHAR(500);
        DECLARE    	V_TGT_TABLE					VARCHAR(500);
        DECLARE    	V_APPLY_TYPE				VARCHAR(500);
        DECLARE 	V_NULLABLE_COLUMN			VARCHAR(200);
		DECLARE 	v_conflict_colname			VARCHAR(200);
        
        DECLARE		V_BATCH_ID					VARCHAR(150)	DEFAULT 'BATCH_ID';
        DECLARE	   	V_REF_KEY   				VARCHAR(150)	DEFAULT 'REF_KEY';
        DECLARE	   	V_MODIFICATION_TYPE 		VARCHAR(150)	DEFAULT 'MODIFICATION_TYPE';
        
        DECLARE		V_INSERT_TYPE				VARCHAR(150)	DEFAULT 'INSERT';
        DECLARE		V_UPSERT_TYPE				VARCHAR(150)	DEFAULT 'UPSERT';
        DECLARE		V_UPSERTDELETE_TYPE			VARCHAR(150)	DEFAULT 'UPSERTDELETE';
        DECLARE		V_HISTORY_TYPE				VARCHAR(150)	DEFAULT 'HISTORY';
        
		DECLARE		V_KEY_COLUMNS				VARCHAR(2000);
        DECLARE		V_HISTORY_COLUMNS			VARCHAR(2000)	DEFAULT '';
     	DECLARE	   	V_START_DATE_1SEC 			VARCHAR(5000)	DEFAULT '';
     	DECLARE	   	V_END_DATE_COLUMN, V_START_DATE_COLUMN 		VARCHAR(150)	DEFAULT '';
     	DECLARE	   	V_QUALIFY_BY_KEY 			VARCHAR(5000)	DEFAULT '';
     	DECLARE		V_FULL_KEY					VARCHAR(2000)	DEFAULT '';
     	DECLARE		V_FULL_KEY_JOIN				VARCHAR(2000)	DEFAULT '';     	
        DECLARE		V_NORMAL_COMMON_COLUMNS		VARCHAR(2000)	DEFAULT '';
        
        DECLARE 	V_SET_HISTORY_COLUMNS, V_SET_NORMAL_COMMON_COLUMNS, V_X_HISTORY_COLUMNS, v_X_NORMAL_COMMON_COLUMNS, v_CONCATE_HIST_COL_X
        			, v_select_clause, v_from_clause, v_where_clause, V_CONCATE_HIST_COL_Y, V_COALESCE_HIST_COLX, V_COALESCE_HIST_COLY
        			, v_calculate_end_date, v_coalesce_end_date, V_COALESCE_KEY_COLUMN, V_KEY_COLUMNS_JOIN			VARCHAR(2000)	DEFAULT '';
        			
        DECLARE 	V_START_DATE_COLUMN_JOIN	VARCHAR(2000)	DEFAULT '';
        DECLARE		V_TMP_INPUT_VIEW_NAME, V_BASE_VT_NAME		VARCHAR(150)	DEFAULT '';
        
     	DECLARE 	V_UPDATE_MODEL_DELETE_FLAG, V_FROM_BASE_IN_TMP, V_KEYS_IN_BASE, V_INSERT_INTO_MODEL_FROM_INPV, V_UPDATE_END_DATE_COLUMN
     				, V_REOPEN_END_DATE_COL, V_UPDATE_MODEL_DATA_FROM_INPV, V_DEL_FROM_MODEL_SAME_HC
     				, V_VT_UPSERT_MODEL, V_VT_CALCULATE_END_DATE , V_VT_ALL_KEYS, V_VT_BASE_TABLE, V_VT_INPUT_VIEW
     				, V_TMP_INPUT_VIEW		clob;
     	DECLARE 	V_CLOSE_END_DATE, V_DROP_UPSERT_MODEL, V_DROP_CALCULATE_END_DATE, V_DROP_ALL_KEYS, V_DROP_BASE_TABLE
     				, V_DROP_INPUT_VIEW, V_DROP_TMP_INPUT_VIEW								VARCHAR(2000);
     	
        DECLARE 	V_WITH_DATA_PI			VARCHAR(100) DEFAULT 'WITH DATA AND STATS PRIMARY INDEX';
		DECLARE 	V_WITH_DATA_UNIQUE_PI	VARCHAR(100) DEFAULT 'WITH DATA AND STATS UNIQUE PRIMARY INDEX';
	
		DECLARE 	v_is_history_table 			INTEGER;
		
        DECLARE EXIT HANDLER FOR SQLEXCEPTION
        BEGIN 	
	        SET V_RETURN_CODE = SQLCODE;
			GET DIAGNOSTICS EXCEPTION 1 V_RETURN_MSG = MESSAGE_TEXT; 			
		
			SET o_RETURN_CODE  = V_RETURN_CODE;
			SET o_RETURN_MSG = V_RETURN_MSG;
			
			SET O_ROWS_INSERTED_COUNT 	= V_INSERTED_ROWS_COUNT;
			SET O_ROWS_UPDATED_COUNT 	= V_UPDATED_ROWS_COUNT;
			SET O_ROWS_DELETED_COUNT 	= V_DELETED_ROWS_COUNT;
		
            INSERT INTO GPROD1_ETL.EXEC_PROCESS_LOGS  
            (RUN_ID, LOAD_ID, SOURCE_NAME, PROCESS_NAME, START_TIMESTAMP, END_TIMESTAMP, ROWS_INSERTED_COUNT, ROWS_UPDATED_COUNT, ROWS_DELETED_COUNT, ERROR_CODE, ERROR_MSG)  
            VALUES
            (coalesce(v_run_id,	i_run_id), i_LOAD_ID, V_SOURCE_NAME, I_PROCESSNAME, v_START_TIMESTAMP, current_timestamp, V_INSERTED_ROWS_COUNT, V_UPDATED_ROWS_COUNT, V_DELETED_ROWS_COUNT, V_RETURN_CODE, V_RETURN_MSG);
        END;
        
        IF i_run_id IS NULL THEN 
			select GPROD1_ETL.generate_run_id()
			into v_run_id;
		ELSE 	
			SET v_run_id=	i_run_id;
		END IF;
		
		MAINBLOCK:
        BEGIN
            SELECT 
            C.SOURCE_NAME,
			S.LOADING_MODE,
            SRC_DB,
            SRC_TABLE,
            TGT_DB,
            TGT_TABLE,
            coalesce(I_APPLY_TYPE,APPLY_TYPE) APPLY_TYPE
            ,KEY_COLUMNS
            ,COALESCE(START_DATE_COLUMN,'') START_DATE_COLUMN
            ,COALESCE(','||HISTORY_COLUMNS,'') HISTORY_COLUMNS
            ,COALESCE(','||NORMAL_COMMON_COLUMNS,'') NORMAL_COMMON_COLUMNS
            ,KEY_COLUMNS_JOIN
            ,COALESCE('X.'||START_DATE_COLUMN||' = Y.'||START_DATE_COLUMN,'') START_DATE_COLUMN_JOIN
            ,COALESCE_KEY_COLUMN
            ,END_DATE_COLUMN
            ,COALESCE_HIST_COLX
            ,COALESCE_HIST_COLY
            ,CONCATE_HIST_COL_X
            ,CONCATE_HIST_COL_Y
            ,coalesce(','||X_NORMAL_COMMON_COLUMNS,'') X_NORMAL_COMMON_COLUMNS
            ,COALESCE(','||X_HISTORY_COLUMNS,'') X_HISTORY_COLUMNS
            ,COALESCE(','||SET_HISTORY_COLUMNS,'') SET_HISTORY_COLUMNS
            ,COALESCE(','||SET_NORMAL_COMMON_COLUMNS,'') SET_NORMAL_COMMON_COLUMNS
            FROM GPROD1_ETL.V_EDW_SCRIPTS_COMPOSER C
				JOIN GPROD1_ETL.SOURCE_SYSTEMS S
				ON S.SOURCE_NAME = C.SOURCE_NAME
            WHERE PROCESS_NAME = I_PROCESSNAME
            AND  S.ACTIVE = 1 AND C.ACTIVE = 1
            INTO V_SOURCE_NAME, V_LOADING_MODE, V_SRC_DB, V_SRC_TABLE , V_TGT_DB, V_TGT_TABLE, V_APPLY_TYPE
            	,V_KEY_COLUMNS ,V_START_DATE_COLUMN ,V_HISTORY_COLUMNS, V_NORMAL_COMMON_COLUMNS
            	,V_KEY_COLUMNS_JOIN, V_START_DATE_COLUMN_JOIN, V_COALESCE_KEY_COLUMN
            	,V_END_DATE_COLUMN, V_COALESCE_HIST_COLX, V_COALESCE_HIST_COLY
            	,v_CONCATE_HIST_COL_X, V_CONCATE_HIST_COL_Y, v_X_NORMAL_COMMON_COLUMNS, V_X_HISTORY_COLUMNS
            	,V_SET_HISTORY_COLUMNS, V_SET_NORMAL_COMMON_COLUMNS;
            
            /*
			select is_history
			from GPROD1_ETL.V_EDW_TABLES 
			where table_name = V_TGT_TABLE 
			and LAYER_NAME = 'CORE' 
			into v_is_history_table;
			
			if v_is_history_table = 1 and V_APPLY_TYPE <> 'HISTORY'
			THEN
				set V_RETURN_CODE = -1;
            	SET V_RETURN_MSG = V_TGT_TABLE||' table is history, but process type is not!';
            	leave MAINBLOCK;
			END IF;
			
			
			Select top 1 colname from
			(
				SELECT  HISTORY_COLUMN colname
				FROM GPROD1_ETL.HISTORY
				WHERE PROCESS_NAME = I_PROCESSNAME
				
				INTERSECT 
				
				SELECT KEY_COLUMN  colname
				FROM GPROD1_ETL.TRANSFORM_KEYCOL
				WHERE LAYER_NAME = 'CORE'
				AND TABLE_NAME = V_TGT_TABLE
			) x into v_conflict_colname;
			
			if v_conflict_colname is not null
			then
				set V_RETURN_CODE = -1;
            	SET V_RETURN_MSG = V_TGT_TABLE||'.'||v_conflict_colname || ', is defined twice, both as history column & history key!';
            	leave MAINBLOCK;
			end if;
			*/
			IF V_SOURCE_NAME IS NULL
            THEN
            	set V_RETURN_CODE = -1;
            	SET V_RETURN_MSG = 'Invalid Proccess!';
            	leave MAINBLOCK;
            end if;
            
            IF coalesce(V_HISTORY_COLUMNS,'') = '' and V_APPLY_TYPE = V_HISTORY_TYPE
			THEN
				SET V_RETURN_CODE = -1;
				SET V_RETURN_MSG = 'History Columns are not defined!';
				LEAVE MainBlock;
			END IF;
			
            SET V_FULL_KEY = V_KEY_COLUMNS;
            SET V_FULL_KEY_JOIN = V_KEY_COLUMNS_JOIN;
            
            if V_APPLY_TYPE = V_HISTORY_TYPE
            then
            	SET V_FULL_KEY = V_FULL_KEY||', '||V_START_DATE_COLUMN;
            	SET V_FULL_KEY_JOIN = V_FULL_KEY_JOIN||' AND '||V_START_DATE_COLUMN_JOIN;
            	
            	--SET V_HISTORY_COLUMNS = ','||V_HISTORY_COLUMNS;
            	select 
					'/* ********************** START RN + 1 SEC ********************************** */
						, 	ROW_NUMBER () OVER (PARTITION BY '||V_FULL_KEY||' 
												ORDER BY  '||V_BATCH_ID||' ,'||V_MODIFICATION_TYPE||' ,'||V_REF_KEY||' ) - 1 AS RN
						, 	CAST(CAST('||V_START_DATE_COLUMN||' AS VARCHAR(19)) AS TIMESTAMP (0)) 
						+  	CAST((RN/3600) AS INTERVAL HOUR )
						+	CAST((RN/60)MOD 60 AS INTERVAL MINUTE ) 
						+	CAST((RN) MOD 60 AS INTERVAL SECOND ) AS '||V_START_DATE_COLUMN||'					 
					/* ********************** END RN + 1 SEC ********************************** */'
				into V_START_DATE_1SEC;
				
            end if;
			--------------------------------------------------------------------------------------------------------------------------------
            IF 
				coalesce(I_DELTA_MODE,0) = 1 
				AND (coalesce(i_LOAD_ID,'') <> '' and coalesce(i_LOAD_ID,'') not like '%REFRESH%' AND V_LOADING_MODE <> 'OFFLINE') 
			THEN
				SET v_DELTA_MODE = 1;
			ELSE
				SET v_DELTA_MODE = 0;
			END IF;
			
			SET V_QUALIFY_BY_KEY = 'QUALIFY ROW_NUMBER()  OVER(PARTITION BY '||V_FULL_KEY||' ORDER BY  '||V_BATCH_ID||' DESC, '||V_MODIFICATION_TYPE||' DESC, '||V_REF_KEY||' DESC)=1 ';
          	select 
          	 'TMP_INPUT_VIEW_'		|| V_TGT_TABLE  
          	,'DROP TABLE TMP_INPUT_VIEW_'		|| V_TGT_TABLE ||';'	V_DROP_TMP_INPUT_VIEW
          	,'CREATE VOLATILE MULTISET TABLE TMP_INPUT_VIEW_' || V_TGT_TABLE || ' , no fallback AS 
				(
					with 
					inpt_view as
					(
						SELECT 
							 '||V_BATCH_ID  ||	',' || V_MODIFICATION_TYPE	|| ',' ||V_REF_KEY||'
							,'||V_KEY_COLUMNS		|| V_NORMAL_COMMON_COLUMNS 		
							  ||V_HISTORY_COLUMNS 	|| V_START_DATE_1SEC||'
							,CASE WHEN ' ||V_MODIFICATION_TYPE|| ' = ''D'' THEN 1 ELSE 0 END INPV_DELETE_FLAG
							from ' || V_SRC_DB || '.' || V_SRC_TABLE ||' x
					)
					,QUALIFIED_inpt_view as
					(
						select * from inpt_view
						' || V_QUALIFY_BY_KEY || '		
					)
					select * from QUALIFIED_inpt_view x
					'||case WHEN v_DELTA_MODE = 1 
							then 'where not exists (
									select 1 from '||V_TGT_DB||'.' || V_TGT_TABLE || ' y
									where ' || V_FULL_KEY_JOIN || ' 
									and coalesce(y.UPDATE_PROCESS_NAME,y.PROCESS_NAME) = '''||I_PROCESSNAME||''' 
									and x.batch_id < y.batch_id
									)' 
							ELSE '' 
							END||'
					
				)  ' || V_WITH_DATA_UNIQUE_PI || '  (' || V_FULL_KEY || ') ON COMMIT PRESERVE ROWS;' 
			into V_TMP_INPUT_VIEW_NAME, V_DROP_TMP_INPUT_VIEW, V_TMP_INPUT_VIEW;
						
				
			IF CAST(V_TMP_INPUT_VIEW AS CHAR(10)) <> ''
			THEN
				SET V_SQL_SCRIPT_ID = V_SQL_SCRIPT_ID + 1;
				CALL GPROD1_ETL.DBC_SYSEXECSQL_WITH_LOG(V_DROP_TMP_INPUT_VIEW,V_SQL_SCRIPT_ID,v_run_id,V_SOURCE_NAME,I_PROCESSNAME,I_LOAD_ID,0/*1 log or 0 don't*/,V_DBC_RETURN_CODE,V_DBC_RETURN_MSG,V_DBC_ROWS_COUNT, v_dbc_run_id);
				CALL GPROD1_ETL.DBC_SYSEXECSQL_WITH_LOG(V_TMP_INPUT_VIEW,V_SQL_SCRIPT_ID,v_run_id,V_SOURCE_NAME,I_PROCESSNAME,I_LOAD_ID,1/*1 log or 0 don't*/,V_DBC_RETURN_CODE,V_DBC_RETURN_MSG,V_DBC_ROWS_COUNT, v_dbc_run_id);
				IF V_DBC_RETURN_CODE <> 0 THEN
					SET V_RETURN_CODE = V_DBC_RETURN_CODE;
					SET V_RETURN_MSG = V_DBC_RETURN_MSG;
					LEAVE MainBlock;
				END IF;
			END IF;

			CALL GPROD1_ETL.COUNT_ROWS('', V_TMP_INPUT_VIEW_NAME, '',V_ROWS_IN_TMP, V_DBC_RETURN_CODE, V_DBC_RETURN_MSG);
			IF V_DBC_RETURN_CODE <> 0 THEN
				SET V_RETURN_CODE = V_DBC_RETURN_CODE;
				SET V_RETURN_MSG = V_DBC_RETURN_MSG;
				LEAVE MainBlock;
			END IF;
			
			if V_ROWS_IN_TMP = 0
			then
				leave MainBlock;
			end if;
			
			CALL GPROD1_ETL.CHECK_MANDATORY_COLUMNS(NULL, V_TMP_INPUT_VIEW_NAME, V_FULL_KEY,V_NULLABLE_COLUMN, V_DBC_RETURN_CODE, V_DBC_RETURN_MSG);
			IF V_DBC_RETURN_CODE <> 0 THEN
				SET V_RETURN_CODE = V_DBC_RETURN_CODE;
				SET V_RETURN_MSG = V_DBC_RETURN_MSG;
				LEAVE MainBlock;
			END IF;
			
			--------------------------------------------------------------------------------------------------------------------------------
			set v_where_clause = ' AND ((X.INPV_DELETE_FLAG = 1 and END_TS IS NULL) or (X.INPV_DELETE_FLAG = 0 and END_TS IS not NULL))';
			SET V_CLOSE_END_DATE = '';
			
			if V_APPLY_TYPE = V_HISTORY_TYPE
			then
				set V_CLOSE_END_DATE = ','||V_END_DATE_COLUMN||' = case when X.INPV_DELETE_FLAG = 1 and '||V_END_DATE_COLUMN||' is null then Y.'||V_START_DATE_COLUMN||' else '||V_END_DATE_COLUMN||' end';
				set v_where_clause = ' AND ((X.INPV_DELETE_FLAG = 1 and (END_TS IS NULL or '||V_END_DATE_COLUMN||' is null)) or (X.INPV_DELETE_FLAG = 0 and END_TS IS not NULL))';
			end if;
			
			SELECT 
			'UPDATE Y 
			FROM '||V_TGT_DB||'.' || V_TGT_TABLE || ' Y
			-- SELECT * FROM '||V_TGT_DB||'.' || V_TGT_TABLE || ' Y
				, TMP_INPUT_VIEW_' || V_TGT_TABLE || ' X			
			SET  END_TS = 	case when X.INPV_DELETE_FLAG = 1 and END_TS IS NULL then CURRENT_TIMESTAMP else null end
				'||V_CLOSE_END_DATE||'				
				,UPDATE_TS = CURRENT_TIMESTAMP
				,UPDATE_PROCESS_NAME = '''||I_PROCESSNAME||'''
				,batch_id = x.batch_id
			where ' || V_FULL_KEY_JOIN || '
			' || v_where_clause || ';'
			
			INTO V_UPDATE_MODEL_DELETE_FLAG;
			--WHERE V_APPLY_TYPE <> V_INSERT_TYPE;
			
			IF CAST(V_UPDATE_MODEL_DELETE_FLAG AS CHAR(10)) <> ''
			then
				SET V_SQL_SCRIPT_ID = V_SQL_SCRIPT_ID + 1;
				CALL GPROD1_ETL.DBC_SYSEXECSQL_WITH_LOG(V_UPDATE_MODEL_DELETE_FLAG,V_SQL_SCRIPT_ID,v_run_id,V_SOURCE_NAME,I_PROCESSNAME,I_LOAD_ID,1/*1 log or 0 don't*/,V_DBC_RETURN_CODE,V_DBC_RETURN_MSG,V_DBC_ROWS_COUNT, v_dbc_run_id);
				IF V_DBC_RETURN_CODE <> 0 THEN
					SET V_RETURN_CODE = V_DBC_RETURN_CODE;
					SET V_RETURN_MSG = V_DBC_RETURN_MSG;
					LEAVE MainBlock;
				END IF;
				set V_UPDATED_ROWS_COUNT = V_UPDATED_ROWS_COUNT + V_DBC_ROWS_COUNT;
			end if;
			--------------------------------------------------------------------------------------------------------------------------------
			SELECT 
			/*
			 * get the last row in base table, if its not deleted and end data is not null, then make it null!
			 */
			'UPDATE Y 
			FROM '||V_TGT_DB||'.' || V_TGT_TABLE || ' Y
			-- SELECT * FROM '||V_TGT_DB||'.' || V_TGT_TABLE || ' Y
				, (
					select ' || V_FULL_KEY || ' 
					from '||V_TGT_DB||'.' || V_TGT_TABLE || ' X
					where exists (
								select 1 
								from TMP_INPUT_VIEW_' || V_TGT_TABLE || ' y 
								where ' || V_KEY_COLUMNS_JOIN || '
								)
					QUALIFY ROW_NUMBER()  OVER(PARTITION BY '||V_KEY_COLUMNS||' ORDER BY  '||V_START_DATE_COLUMN||' DESC)=1 
				) X			
			SET '||V_END_DATE_COLUMN||' = Null			
				,UPDATE_TS = CURRENT_TIMESTAMP
				,UPDATE_PROCESS_NAME = '''||I_PROCESSNAME||'''
			where ' || V_FULL_KEY_JOIN || '
			AND Y.END_TS IS NULL AND Y.'||V_END_DATE_COLUMN||' IS NOT NULL;'
			INTO V_REOPEN_END_DATE_COL
			WHERE V_APPLY_TYPE = V_HISTORY_TYPE;
			
			IF CAST(V_REOPEN_END_DATE_COL AS CHAR(10)) <> ''
			then
				SET V_SQL_SCRIPT_ID = V_SQL_SCRIPT_ID + 1;
				CALL GPROD1_ETL.DBC_SYSEXECSQL_WITH_LOG(V_REOPEN_END_DATE_COL,V_SQL_SCRIPT_ID,v_run_id,V_SOURCE_NAME,I_PROCESSNAME,I_LOAD_ID,1/*1 log or 0 don't*/,V_DBC_RETURN_CODE,V_DBC_RETURN_MSG,V_DBC_ROWS_COUNT, v_dbc_run_id);
				IF V_DBC_RETURN_CODE <> 0 THEN
					SET V_RETURN_CODE = V_DBC_RETURN_CODE;
					SET V_RETURN_MSG = V_DBC_RETURN_MSG;
					LEAVE MainBlock;
				END IF;
				set V_UPDATED_ROWS_COUNT = V_UPDATED_ROWS_COUNT + V_DBC_ROWS_COUNT;
			end if;
			--------------------------------------------------------------------------------------------------------------------------------
			
			if V_APPLY_TYPE <> V_HISTORY_TYPE
			then
			
				SET V_FROM_BASE_IN_TMP = 'FROM '||V_TGT_DB||'.' || V_TGT_TABLE || ' X
										where end_ts is null
										and exists (
													select 1 
													from TMP_INPUT_VIEW_' || V_TGT_TABLE || ' y 
													where ' || case when V_APPLY_TYPE = V_HISTORY_TYPE then V_KEY_COLUMNS_JOIN else V_FULL_KEY_JOIN end || '
													)';
				SET V_KEYS_IN_BASE = 'SELECT TOP 1 * '||V_FROM_BASE_IN_TMP;
				
				CALL GPROD1_ETL.COUNT_ROWS('', '', V_KEYS_IN_BASE, V_KEYS_IN_BASE_CT, V_DBC_RETURN_CODE, V_DBC_RETURN_MSG);
				IF V_DBC_RETURN_CODE <> 0 THEN
					SET V_RETURN_CODE = V_DBC_RETURN_CODE;
					SET V_RETURN_MSG = V_DBC_RETURN_MSG;
					LEAVE MainBlock;
				END IF;
			else
				set V_KEYS_IN_BASE_CT = 1; -- this to force the history process go through all steps
			end if;
			--------------------------------------------------------------------------------------------------------------------------------			
			set v_select_clause = 'select * ';
			set v_where_clause = 'WHERE INPV_DELETE_FLAG = 0';
			
			if V_APPLY_TYPE = V_HISTORY_TYPE
			then
				set v_calculate_end_date = 
				'(
					LEAD('||V_START_DATE_COLUMN||') OVER(PARTITION BY '||v_KEY_COLUMNS||' ORDER BY '||V_START_DATE_COLUMN||'  ) 
				) + INTERVAL - ''1'' SECOND '|| V_END_DATE_COLUMN;
				
						
				set v_select_clause = 'select x.*,'||v_calculate_end_date;
			end if;
			
			select 
			 'DROP TABLE INPUT_VIEW_' 			|| V_TGT_TABLE ||';'	
			,'CREATE VOLATILE MULTISET TABLE INPUT_VIEW_' || V_TGT_TABLE || ' , no fallback AS 
						(
							' || v_select_clause || '				 
							FROM TMP_INPUT_VIEW_' || V_TGT_TABLE || ' x
							' || v_where_clause || '	
						)  ' || V_WITH_DATA_UNIQUE_PI || '  (' || V_FULL_KEY || ') ON COMMIT PRESERVE ROWS;'
			into V_DROP_INPUT_VIEW, V_VT_INPUT_VIEW
			WHERE V_APPLY_TYPE <> V_INSERT_TYPE;
			--------------------------------------------------------------------------------------------------------------------------------
			select 
			'BASE_' || V_TGT_TABLE
			,'DROP TABLE BASE_' || V_TGT_TABLE || ';'
			,'CREATE VOLATILE MULTISET TABLE BASE_' || V_TGT_TABLE || ' , no fallback AS 
				(
					SELECT x.*
					FROM '||V_TGT_DB||'.' || V_TGT_TABLE || ' x
					where end_ts is null
					and exists (
								select 1 
								from TMP_INPUT_VIEW_' || V_TGT_TABLE || ' y 
								where ' || V_KEY_COLUMNS_JOIN || '
								)
				)  ' || V_WITH_DATA_UNIQUE_PI || ' (' || V_FULL_KEY || ') ON COMMIT PRESERVE ROWS;'
			into V_BASE_VT_NAME, V_DROP_BASE_TABLE, V_VT_BASE_TABLE
			WHERE V_APPLY_TYPE = V_HISTORY_TYPE and V_KEYS_IN_BASE_CT > 0;
			--------------------------------------------------------------------------------------------------------------------------------
			select
			 'DROP TABLE ALL_KEYS_'||V_TGT_TABLE
			,'CREATE VOLATILE MULTISET TABLE ALL_KEYS_'||V_TGT_TABLE||' , no fallback AS 
				(
					WITH 
					T1 AS 
					(
						SELECT 
						 COALESCE(x.batch_id,y.batch_id) batch_id
						,' || v_COALESCE_KEY_COLUMN||'
						,COALESCE(x.'|| V_START_DATE_COLUMN||',Y.'||V_START_DATE_COLUMN||') '||V_START_DATE_COLUMN||'
						,Y.'||V_END_DATE_COLUMN||' END_DATE_IN_MODEL
						,'||V_COALESCE_HIST_COLX||'
						,'||V_COALESCE_HIST_COLY||'
						,'||V_CONCATE_HIST_COL_X||'
						,'||V_CONCATE_HIST_COL_Y||'
						,CASE WHEN X.'||V_START_DATE_COLUMN||' IS NOT NULL THEN HC_X 	ELSE HC_Y	END HC' 		||'
						,CASE WHEN Y.'||V_START_DATE_COLUMN||' IS NOT NULL THEN 1 		ELSE 0 		END IN_MODEL' 	||'
						,CASE WHEN X.'||V_START_DATE_COLUMN||' IS NOT NULL THEN 1 		ELSE 0		END FROM_INPV'	||'
						'||v_X_HISTORY_COLUMNS||'
						'||v_X_NORMAL_COMMON_COLUMNS||'
						 FROM INPUT_VIEW_'||V_TGT_TABLE||' X
							FULL OUTER JOIN BASE_'||V_TGT_TABLE||' Y
							ON ' || V_FULL_KEY_JOIN || ' 
					) 
					SELECT  
					batch_id
					,'|| V_FULL_KEY ||'
					'|| v_HISTORY_COLUMNS||'
					'|| V_NORMAL_COMMON_COLUMNS||'
					,END_DATE_IN_MODEL
					,IN_MODEL
					,FROM_INPV
					,LAG(HC) 			OVER(PARTITION BY '|| v_KEY_COLUMNS ||' ORDER BY '||V_START_DATE_COLUMN||') PRECEDING_HC				
					,HC_X,HC_Y,HC
					,CASE WHEN IN_MODEL=1 AND PRECEDING_HC = HC THEN 1 ELSE 0 END DELETE_FROM_MODEL
					FROM T1

				)  ' || V_WITH_DATA_UNIQUE_PI || ' (' || V_FULL_KEY || ') ON COMMIT PRESERVE ROWS;'
			into V_DROP_ALL_KEYS, V_VT_ALL_KEYS
			WHERE V_APPLY_TYPE = V_HISTORY_TYPE and V_KEYS_IN_BASE_CT > 0;
			--------------------------------------------------------------------------------------------------------------------------------
			select 
			'DROP TABLE CALCULATE_END_DATE_'||V_TGT_TABLE
			,'CREATE VOLATILE MULTISET TABLE CALCULATE_END_DATE_'||V_TGT_TABLE||' , no fallback AS 
			(
				SELECT 
				T.* ,'||v_calculate_end_date||' 
				FROM ALL_KEYS_'||V_TGT_TABLE||' T
				WHERE DELETE_FROM_MODEL = 0
				AND (
						COALESCE(PRECEDING_HC,'''') <> COALESCE(HC,'''') 
						OR (PRECEDING_HC IS NULL AND FROM_INPV = 1 AND IN_MODEL = 0 )  
						OR (END_DATE_IN_MODEL IS NULL AND IN_MODEL = 1) 
					)
			)  ' || V_WITH_DATA_UNIQUE_PI || ' (' || V_FULL_KEY || ') ON COMMIT PRESERVE ROWS;'
			into V_DROP_CALCULATE_END_DATE, V_VT_CALCULATE_END_DATE
			WHERE V_APPLY_TYPE = V_HISTORY_TYPE and V_KEYS_IN_BASE_CT > 0;
			--------------------------------------------------------------------------------------------------------------------------------
			select 
			'DROP TABLE UPSERT_MODEL_'||V_TGT_TABLE
			,'CREATE VOLATILE MULTISET TABLE UPSERT_MODEL_'||V_TGT_TABLE||' , no fallback AS (
				SELECT * FROM CALCULATE_END_DATE_'||V_TGT_TABLE||' T
				WHERE (
						COALESCE(END_DATE_IN_MODEL,(''9999-12-31 00:00:00'' (TIMESTAMP))) <> COALESCE('||V_END_DATE_COLUMN||',(''9999-12-31 00:00:00'' (TIMESTAMP))) 
						OR (FROM_INPV = 1 AND COALESCE(HC_Y,'''') <> COALESCE(HC_X,'''') )
						OR IN_MODEL = 0
						)
				)  ' || V_WITH_DATA_UNIQUE_PI || ' (' || V_FULL_KEY || ') ON COMMIT PRESERVE ROWS;' 
			into V_DROP_UPSERT_MODEL, V_VT_UPSERT_MODEL
			WHERE V_APPLY_TYPE = V_HISTORY_TYPE and V_KEYS_IN_BASE_CT > 0;
			--------------------------------------------------------------------------------------------------------------------------------
			select
			'DELETE FROM '||V_TGT_DB||'.'||V_TGT_TABLE||' Y
			-- SELECT * FROM '||V_TGT_DB||'.'||V_TGT_TABLE||' Y
			where exists (
				select 1 
				from ALL_KEYS_'||V_TGT_TABLE||' X 
				where ' || V_FULL_KEY_JOIN || ' 							
				AND X.DELETE_FROM_MODEL=1
				);'
			into V_DEL_FROM_MODEL_SAME_HC
			WHERE V_APPLY_TYPE = V_HISTORY_TYPE and V_KEYS_IN_BASE_CT > 0;
			--------------------------------------------------------------------------------------------------------------------------------
			select
			'UPDATE Y 
			FROM '||V_TGT_DB||'.'||V_TGT_TABLE||' Y
			-- SELECT * FROM '||V_TGT_DB||'.'||V_TGT_TABLE||' Y
			, UPSERT_MODEL_'||V_TGT_TABLE||' X		
			SET '||V_END_DATE_COLUMN||' = X.'||V_END_DATE_COLUMN||'
				'||V_SET_HISTORY_COLUMNS||'
				'||V_SET_NORMAL_COMMON_COLUMNS||'
				,UPDATE_TS = CURRENT_TIMESTAMP				
				,END_TS = 	NULL 
				,batch_id = x.batch_id
				,UPDATE_PROCESS_NAME = '''||I_PROCESSNAME||'''
			where ' || V_FULL_KEY_JOIN || ' 						
			AND X.FROM_INPV = 1;'
			into V_UPDATE_MODEL_DATA_FROM_INPV
			WHERE V_APPLY_TYPE = V_HISTORY_TYPE and V_KEYS_IN_BASE_CT > 0;
			
			select
			'UPDATE Y
			FROM '||V_TGT_DB||'.'||V_TGT_TABLE||' Y
			-- SELECT * FROM '||V_TGT_DB||'.'||V_TGT_TABLE||' Y
			, INPUT_VIEW_'||V_TGT_TABLE||' X		
			SET UPDATE_PROCESS_NAME = '''||I_PROCESSNAME||'''
				, batch_id = x.batch_id
				'||V_SET_NORMAL_COMMON_COLUMNS||'	
				'||V_SET_HISTORY_COLUMNS||'		
				,UPDATE_TS = CURRENT_TIMESTAMP
				,END_TS = 	NULL 
			WHERE ' || V_FULL_KEY_JOIN || ';' 
			into V_UPDATE_MODEL_DATA_FROM_INPV
			WHERE V_APPLY_TYPE in( V_UPSERT_TYPE, V_UPSERTDELETE_TYPE ) and V_KEYS_IN_BASE_CT > 0;
			--------------------------------------------------------------------------------------------------------------------------------
			select
			'UPDATE Y 
			FROM '||V_TGT_DB||'.'||V_TGT_TABLE||' Y
			-- SELECT * FROM '||V_TGT_DB||'.'||V_TGT_TABLE||' Y
				, UPSERT_MODEL_'||V_TGT_TABLE||' X				
			SET '||V_END_DATE_COLUMN||' = X.'||V_END_DATE_COLUMN||'
				,UPDATE_TS = CURRENT_TIMESTAMP
				,UPDATE_PROCESS_NAME = '''||I_PROCESSNAME||'''
			where ' || V_FULL_KEY_JOIN || ' 						
			AND X.FROM_INPV = 0;'
			into V_UPDATE_END_DATE_COLUMN
			WHERE V_APPLY_TYPE = V_HISTORY_TYPE and V_KEYS_IN_BASE_CT > 0;
			--------------------------------------------------------------------------------------------------------------------------------
			if V_KEYS_IN_BASE_CT > 0
			then
				set v_from_clause = 'FROM UPSERT_MODEL_'||V_TGT_TABLE||' X';
				set v_where_clause = 'WHERE FROM_INPV = 1 AND IN_MODEL = 0';
			else
				set v_from_clause = 'FROM INPUT_VIEW_'||V_TGT_TABLE;
				set v_where_clause = '';
			end if;
			
			select 
			'INSERT INTO '||V_TGT_DB||'.'||V_TGT_TABLE||'
			('||V_FULL_KEY ||V_HISTORY_COLUMNS ||V_NORMAL_COMMON_COLUMNS||', '||V_END_DATE_COLUMN||'
			, START_TS, END_TS, PROCESS_NAME, batch_id)
			SELECT 
				'||V_FULL_KEY ||V_HISTORY_COLUMNS ||V_NORMAL_COMMON_COLUMNS||', '||V_END_DATE_COLUMN||'
				,CURRENT_TIMESTAMP START_TS ,NULL  END_TS ,'''||I_PROCESSNAME||''' ,batch_id
			'||v_from_clause||'
			'||v_where_clause||';'
			into V_INSERT_INTO_MODEL_FROM_INPV
			WHERE V_APPLY_TYPE = V_HISTORY_TYPE;
			
			select
			'INSERT INTO '||V_TGT_DB||'.'||V_TGT_TABLE||'
			('||V_FULL_KEY ||V_NORMAL_COMMON_COLUMNS||'
			, START_TS, END_TS, PROCESS_NAME, batch_id)
			SELECT 
				'||V_FULL_KEY ||V_NORMAL_COMMON_COLUMNS||'
				,CURRENT_TIMESTAMP(6) Start_Ts ,NULL END_TS, '''||I_PROCESSNAME||''', batch_id
			FROM TMP_INPUT_VIEW_'||V_TGT_TABLE||' X
			WHERE INPV_DELETE_FLAG = 0
			AND NOT EXISTS (SEL 1 FROM '||V_TGT_DB||'.'||V_TGT_TABLE||' Y WHERE ' || V_FULL_KEY_JOIN || ' ) ;' 
			into V_INSERT_INTO_MODEL_FROM_INPV
			WHERE V_APPLY_TYPE <> V_HISTORY_TYPE;

			--------------------------------------------------------------------------------------------------------------------------------
			--------------------------------------------------------------------------------------------------------------------------------						
			CALL GPROD1_ETL.DBC_SYSEXECSQL_WITH_LOG(V_DROP_INPUT_VIEW,V_SQL_SCRIPT_ID,v_run_id,V_SOURCE_NAME,I_PROCESSNAME,I_LOAD_ID,0/*1 log or 0 don't*/,V_DBC_RETURN_CODE,V_DBC_RETURN_MSG,V_DBC_ROWS_COUNT, v_dbc_run_id);
			CALL GPROD1_ETL.DBC_SYSEXECSQL_WITH_LOG(V_DROP_BASE_TABLE,V_SQL_SCRIPT_ID,v_run_id,V_SOURCE_NAME,I_PROCESSNAME,I_LOAD_ID,0/*1 log or 0 don't*/,V_DBC_RETURN_CODE,V_DBC_RETURN_MSG,V_DBC_ROWS_COUNT, v_dbc_run_id);
			CALL GPROD1_ETL.DBC_SYSEXECSQL_WITH_LOG(V_DROP_ALL_KEYS,V_SQL_SCRIPT_ID,v_run_id,V_SOURCE_NAME,I_PROCESSNAME,I_LOAD_ID,0/*1 log or 0 don't*/,V_DBC_RETURN_CODE,V_DBC_RETURN_MSG,V_DBC_ROWS_COUNT, v_dbc_run_id);
			CALL GPROD1_ETL.DBC_SYSEXECSQL_WITH_LOG(V_DROP_CALCULATE_END_DATE,V_SQL_SCRIPT_ID,v_run_id,V_SOURCE_NAME,I_PROCESSNAME,I_LOAD_ID,0/*1 log or 0 don't*/,V_DBC_RETURN_CODE,V_DBC_RETURN_MSG,V_DBC_ROWS_COUNT, v_dbc_run_id);
			CALL GPROD1_ETL.DBC_SYSEXECSQL_WITH_LOG(V_DROP_UPSERT_MODEL,V_SQL_SCRIPT_ID,v_run_id,V_SOURCE_NAME,I_PROCESSNAME,I_LOAD_ID,0/*1 log or 0 don't*/,V_DBC_RETURN_CODE,V_DBC_RETURN_MSG,V_DBC_ROWS_COUNT, v_dbc_run_id);

		
			IF CAST(V_VT_INPUT_VIEW AS CHAR(10)) <> ''
			THEN
				SET V_SQL_SCRIPT_ID = V_SQL_SCRIPT_ID + 1;
				CALL GPROD1_ETL.DBC_SYSEXECSQL_WITH_LOG(V_VT_INPUT_VIEW,V_SQL_SCRIPT_ID,v_run_id,V_SOURCE_NAME,I_PROCESSNAME,I_LOAD_ID,1/*1 log or 0 don't*/,V_DBC_RETURN_CODE,V_DBC_RETURN_MSG,V_DBC_ROWS_COUNT, v_dbc_run_id);
				IF V_DBC_RETURN_CODE <> 0 THEN
					SET V_RETURN_CODE = V_DBC_RETURN_CODE;
					SET V_RETURN_MSG = V_DBC_RETURN_MSG;
					LEAVE MainBlock;
				END IF;
			END IF;
			
			IF CAST(V_VT_BASE_TABLE AS CHAR(10)) <> ''
			THEN
				SET V_SQL_SCRIPT_ID = V_SQL_SCRIPT_ID + 1;
				CALL GPROD1_ETL.DBC_SYSEXECSQL_WITH_LOG(V_VT_BASE_TABLE,V_SQL_SCRIPT_ID,v_run_id,V_SOURCE_NAME,I_PROCESSNAME,I_LOAD_ID,1/*1 log or 0 don't*/,V_DBC_RETURN_CODE,V_DBC_RETURN_MSG,V_DBC_ROWS_COUNT, v_dbc_run_id);
				IF V_DBC_RETURN_CODE <> 0 THEN
					SET V_RETURN_CODE = V_DBC_RETURN_CODE;
					SET V_RETURN_MSG = V_DBC_RETURN_MSG;
					LEAVE MainBlock;
				END IF;
			END IF;
			
			IF CAST(V_VT_ALL_KEYS AS CHAR(10)) <> ''
			THEN
				SET V_SQL_SCRIPT_ID = V_SQL_SCRIPT_ID + 1;
				CALL GPROD1_ETL.DBC_SYSEXECSQL_WITH_LOG(V_VT_ALL_KEYS,V_SQL_SCRIPT_ID,v_run_id,V_SOURCE_NAME,I_PROCESSNAME,I_LOAD_ID,1/*1 log or 0 don't*/,V_DBC_RETURN_CODE,V_DBC_RETURN_MSG,V_DBC_ROWS_COUNT, v_dbc_run_id);
				IF V_DBC_RETURN_CODE <> 0 THEN
					SET V_RETURN_CODE = V_DBC_RETURN_CODE;
					SET V_RETURN_MSG = V_DBC_RETURN_MSG;
					LEAVE MainBlock;
				END IF;
			END IF;
			
			IF CAST(V_VT_CALCULATE_END_DATE AS CHAR(10)) <> ''
			THEN
				SET V_SQL_SCRIPT_ID = V_SQL_SCRIPT_ID + 1;
				CALL GPROD1_ETL.DBC_SYSEXECSQL_WITH_LOG(V_VT_CALCULATE_END_DATE,V_SQL_SCRIPT_ID,v_run_id,V_SOURCE_NAME,I_PROCESSNAME,I_LOAD_ID,1/*1 log or 0 don't*/,V_DBC_RETURN_CODE,V_DBC_RETURN_MSG,V_DBC_ROWS_COUNT, v_dbc_run_id);
				IF V_DBC_RETURN_CODE <> 0 THEN
					SET V_RETURN_CODE = V_DBC_RETURN_CODE;
					SET V_RETURN_MSG = V_DBC_RETURN_MSG;
					LEAVE MainBlock;
				END IF;
			END IF;
			
			IF CAST(V_VT_UPSERT_MODEL AS CHAR(10)) <> ''
			THEN
				SET V_SQL_SCRIPT_ID = V_SQL_SCRIPT_ID + 1;
				CALL GPROD1_ETL.DBC_SYSEXECSQL_WITH_LOG(V_VT_UPSERT_MODEL,V_SQL_SCRIPT_ID,v_run_id,V_SOURCE_NAME,I_PROCESSNAME,I_LOAD_ID,1/*1 log or 0 don't*/,V_DBC_RETURN_CODE,V_DBC_RETURN_MSG,V_DBC_ROWS_COUNT, v_dbc_run_id);
				IF V_DBC_RETURN_CODE <> 0 THEN
					SET V_RETURN_CODE = V_DBC_RETURN_CODE;
					SET V_RETURN_MSG = V_DBC_RETURN_MSG;
					LEAVE MainBlock;
				END IF;
			END IF;
				
			--end if /*end rows in base*/;
			
			SET V_SQL_SCRIPT_ID = V_SQL_SCRIPT_ID + 1;
			CALL GPROD1_ETL.DBC_SYSEXECSQL_WITH_LOG('BT',V_SQL_SCRIPT_ID,v_run_id,V_SOURCE_NAME,I_PROCESSNAME,I_LOAD_ID,1/*1 log or 0 don't*/,V_DBC_RETURN_CODE,V_DBC_RETURN_MSG,V_DBC_ROWS_COUNT, v_dbc_run_id);
			IF V_DBC_RETURN_CODE <> 0 THEN
				SET V_RETURN_CODE = V_DBC_RETURN_CODE;
				SET V_RETURN_MSG = V_DBC_RETURN_MSG;
				LEAVE MainBlock;
			END IF;	
			
			IF CAST(V_DEL_FROM_MODEL_SAME_HC AS CHAR(10)) <> ''
			THEN
				SET V_SQL_SCRIPT_ID = V_SQL_SCRIPT_ID + 1;
				CALL GPROD1_ETL.DBC_SYSEXECSQL_WITH_LOG(V_DEL_FROM_MODEL_SAME_HC,V_SQL_SCRIPT_ID,v_run_id,V_SOURCE_NAME,I_PROCESSNAME,I_LOAD_ID,1/*1 log or 0 don't*/,V_DBC_RETURN_CODE,V_DBC_RETURN_MSG,V_DBC_ROWS_COUNT, v_dbc_run_id);
				IF V_DBC_RETURN_CODE <> 0 THEN
					SET V_RETURN_CODE = V_DBC_RETURN_CODE;
					SET V_RETURN_MSG = V_DBC_RETURN_MSG;
					LEAVE MainBlock;
				END IF;
				set V_DELETED_ROWS_COUNT = V_DELETED_ROWS_COUNT + V_DBC_ROWS_COUNT;
			END IF;
			
			IF CAST(V_UPDATE_MODEL_DATA_FROM_INPV AS CHAR(10)) <> ''
			THEN
				SET V_SQL_SCRIPT_ID = V_SQL_SCRIPT_ID + 1;
				CALL GPROD1_ETL.DBC_SYSEXECSQL_WITH_LOG(V_UPDATE_MODEL_DATA_FROM_INPV,V_SQL_SCRIPT_ID,v_run_id,V_SOURCE_NAME,I_PROCESSNAME,I_LOAD_ID,1/*1 log or 0 don't*/,V_DBC_RETURN_CODE,V_DBC_RETURN_MSG,V_DBC_ROWS_COUNT, v_dbc_run_id);
				IF V_DBC_RETURN_CODE <> 0 THEN
					SET V_RETURN_CODE = V_DBC_RETURN_CODE;
					SET V_RETURN_MSG = V_DBC_RETURN_MSG;
					LEAVE MainBlock;
				END IF;
				set V_UPDATED_ROWS_COUNT = V_UPDATED_ROWS_COUNT + V_DBC_ROWS_COUNT;
			END IF;
			
			IF CAST(V_UPDATE_END_DATE_COLUMN AS CHAR(10)) <> ''
			THEN
				SET V_SQL_SCRIPT_ID = V_SQL_SCRIPT_ID + 1;
				CALL GPROD1_ETL.DBC_SYSEXECSQL_WITH_LOG(V_UPDATE_END_DATE_COLUMN,V_SQL_SCRIPT_ID,v_run_id,V_SOURCE_NAME,I_PROCESSNAME,I_LOAD_ID,1/*1 log or 0 don't*/,V_DBC_RETURN_CODE,V_DBC_RETURN_MSG,V_DBC_ROWS_COUNT, v_dbc_run_id);
				IF V_DBC_RETURN_CODE <> 0 THEN
					SET V_RETURN_CODE = V_DBC_RETURN_CODE;
					SET V_RETURN_MSG = V_DBC_RETURN_MSG;
					LEAVE MainBlock;
				END IF;
				set V_UPDATED_ROWS_COUNT = V_UPDATED_ROWS_COUNT + V_DBC_ROWS_COUNT;
			END IF;
			
			IF CAST(V_INSERT_INTO_MODEL_FROM_INPV AS CHAR(10)) <> ''
			THEN
				SET V_SQL_SCRIPT_ID = V_SQL_SCRIPT_ID + 1;
				CALL GPROD1_ETL.DBC_SYSEXECSQL_WITH_LOG(V_INSERT_INTO_MODEL_FROM_INPV,V_SQL_SCRIPT_ID,v_run_id,V_SOURCE_NAME,I_PROCESSNAME,I_LOAD_ID,1/*1 log or 0 don't*/,V_DBC_RETURN_CODE,V_DBC_RETURN_MSG,V_DBC_ROWS_COUNT, v_dbc_run_id);
				IF V_DBC_RETURN_CODE <> 0 THEN
					SET V_RETURN_CODE = V_DBC_RETURN_CODE;
					SET V_RETURN_MSG = V_DBC_RETURN_MSG;
					LEAVE MainBlock;
				END IF;
				set V_INSERTED_ROWS_COUNT = V_INSERTED_ROWS_COUNT + V_DBC_ROWS_COUNT;
			END IF;
			
			SET V_SQL_SCRIPT_ID = V_SQL_SCRIPT_ID + 1;
			CALL GPROD1_ETL.DBC_SYSEXECSQL_WITH_LOG('ET',V_SQL_SCRIPT_ID,v_run_id,V_SOURCE_NAME,I_PROCESSNAME,I_LOAD_ID,1/*1 log or 0 don't*/,V_DBC_RETURN_CODE,V_DBC_RETURN_MSG,V_DBC_ROWS_COUNT, v_dbc_run_id);
			IF V_DBC_RETURN_CODE <> 0 THEN
				SET V_RETURN_CODE = V_DBC_RETURN_CODE;
				SET V_RETURN_MSG = V_DBC_RETURN_MSG;
				LEAVE MainBlock;
			END IF;
        END;
        
		CALL GPROD1_ETL.DBC_SYSEXECSQL_WITH_LOG(V_DROP_TMP_INPUT_VIEW,V_SQL_SCRIPT_ID,v_run_id,V_SOURCE_NAME,I_PROCESSNAME,I_LOAD_ID,0/*1 log or 0 don't*/,V_DBC_RETURN_CODE,V_DBC_RETURN_MSG,V_DBC_ROWS_COUNT, v_dbc_run_id);
		CALL GPROD1_ETL.DBC_SYSEXECSQL_WITH_LOG(V_DROP_INPUT_VIEW,V_SQL_SCRIPT_ID,v_run_id,V_SOURCE_NAME,I_PROCESSNAME,I_LOAD_ID,0/*1 log or 0 don't*/,V_DBC_RETURN_CODE,V_DBC_RETURN_MSG,V_DBC_ROWS_COUNT, v_dbc_run_id);
		CALL GPROD1_ETL.DBC_SYSEXECSQL_WITH_LOG(V_DROP_BASE_TABLE,V_SQL_SCRIPT_ID,v_run_id,V_SOURCE_NAME,I_PROCESSNAME,I_LOAD_ID,0/*1 log or 0 don't*/,V_DBC_RETURN_CODE,V_DBC_RETURN_MSG,V_DBC_ROWS_COUNT, v_dbc_run_id);
		CALL GPROD1_ETL.DBC_SYSEXECSQL_WITH_LOG(V_DROP_ALL_KEYS,V_SQL_SCRIPT_ID,v_run_id,V_SOURCE_NAME,I_PROCESSNAME,I_LOAD_ID,0/*1 log or 0 don't*/,V_DBC_RETURN_CODE,V_DBC_RETURN_MSG,V_DBC_ROWS_COUNT, v_dbc_run_id);
		CALL GPROD1_ETL.DBC_SYSEXECSQL_WITH_LOG(V_DROP_CALCULATE_END_DATE,V_SQL_SCRIPT_ID,v_run_id,V_SOURCE_NAME,I_PROCESSNAME,I_LOAD_ID,0/*1 log or 0 don't*/,V_DBC_RETURN_CODE,V_DBC_RETURN_MSG,V_DBC_ROWS_COUNT, v_dbc_run_id);
		CALL GPROD1_ETL.DBC_SYSEXECSQL_WITH_LOG(V_DROP_UPSERT_MODEL,V_SQL_SCRIPT_ID,v_run_id,V_SOURCE_NAME,I_PROCESSNAME,I_LOAD_ID,0/*1 log or 0 don't*/,V_DBC_RETURN_CODE,V_DBC_RETURN_MSG,V_DBC_ROWS_COUNT, v_dbc_run_id);
			
        INSERT INTO GPROD1_ETL.EXEC_PROCESS_LOGS  
        (RUN_ID, LOAD_ID, SOURCE_NAME, PROCESS_NAME, START_TIMESTAMP, END_TIMESTAMP, ROWS_INSERTED_COUNT, ROWS_UPDATED_COUNT, ROWS_DELETED_COUNT, ERROR_CODE, ERROR_MSG)  
        VALUES
        (v_run_id, i_LOAD_ID, V_SOURCE_NAME, I_PROCESSNAME, v_START_TIMESTAMP, current_timestamp, V_INSERTED_ROWS_COUNT, V_UPDATED_ROWS_COUNT, V_DELETED_ROWS_COUNT, V_RETURN_CODE, V_RETURN_MSG);
                                                                                         
        SET O_RETURN_CODE = V_RETURN_CODE;
        SET O_RETURN_MSG = V_RETURN_MSG;
        SET O_ROWS_INSERTED_COUNT	= V_INSERTED_ROWS_COUNT;
    	SET	O_ROWS_UPDATED_COUNT	= V_UPDATED_ROWS_COUNT;
    	SET	O_ROWS_DELETED_COUNT	= V_DELETED_ROWS_COUNT;
    	set o_run_id 				= v_run_id;
    
    END;