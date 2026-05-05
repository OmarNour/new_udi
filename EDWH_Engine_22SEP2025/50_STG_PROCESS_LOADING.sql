SHOW PROCEDURE /*VER.3*/ GPROD1_ETL.STG_PROCESS_LOADING;

--------------------------------------------------------------------------------


REPLACE  PROCEDURE /*VER.07*/ GPROD1_ETL.STG_PROCESS_LOADING
	/*
	###################################################################################################################################################################
	------------------------------------------------------------------------------------------------------------------------------------------------------------------
	#  Version History
	------------------------------------------------------------------------------------------------------------------------------------------------------------------
	# 	Version		|	Date        			|	Author					| 	Comments
	#   1.0        	|	1 JAN, 2023	        |	 Omar Nour				|	Fisrt Cut.
	#	2.0			|	01 October, 2023		|	Ahmed Ehab				|	load direct into stg table, instead of loading into volatile table first
	#	2.1			|	07 DECEMBER, 2023		|	OMAR HOSNY				|	USE THE I_RELOAD INPUT PARAMETER TO LOAD THE PROCESSED LOADS WHEN SET TO 1
	------------------------------------------------------------------------------------------------------------------------------------------------------------------
	#  Examples:
	#  
	###################################################################################################################################################################
	*/
    (
    IN 		i_TABLE_NAME  			VARCHAR(1000),
    IN 		I_RUN_ID 				BIGINT,
    IN		I_LOAD_ID				VARCHAR(500),
    IN		I_RELOAD				INTEGER, /* 1 RELOAD ALL LOADS WHICH LOADED SUCCESSFULLY BEFORE, 0 IGNORE*/
    IN		I_ALL_BATCHES			INTEGER, /* 1 INCLUDES BATCH_ID COLUMN AS A KEY, SO IT GRASPS ALL DATA FROM ALL BATCHES, 0 IGNORE*/
    OUT  	O_ROWS_INSERTED_COUNT	FLOAT,
    OUT  	O_ROWS_UPDATED_COUNT	FLOAT,
    OUT  	O_ROWS_DELETED_COUNT	FLOAT,
    OUT  	O_RETURN_CODE  			INTEGER,
    OUT  	O_RETURN_MSG 			VARCHAR(5000),
    OUT 	o_run_id 				BIGINT
    )
    BEGIN
	    DECLARE 	v_START_TIMESTAMP 			TIMESTAMP(6) default current_timestamp;
	    
		DECLARE		V_LAYER_NAME				VARCHAR(10) DEFAULT 'STG';
		DECLARE		V_PROCESS_NAME				VARCHAR(200) DEFAULT '';
		
        DECLARE 	V_RETURN_CODE  				INTEGER DEFAULT 0;
        DECLARE 	V_RETURN_MSG 				VARCHAR(5000) DEFAULT 'Process Completed Successfully';
        DECLARE 	V_INSERTED_ROWS_COUNT,V_UPDATED_ROWS_COUNT,V_DELETED_ROWS_COUNT,
        			V_ROWS_IN_1batch, V_ROWS_IN_DELTA, V_ROWS_COUNT, v_UniqueFlag_count, v_batch_id_as_PI	FLOAT DEFAULT 0;
        DECLARE		v_run_id, v_dbc_run_id		BIGINT;
        DECLARE 	V_SQL_SCRIPT_ID				INTEGER DEFAULT 0;
        DECLARE 	V_DBC_RETURN_CODE 			INTEGER;
		DECLARE 	V_DBC_RETURN_MSG  			VARCHAR(1000);
		DECLARE 	V_DBC_ROWS_COUNT			FLOAT;
    	DECLARE 	v_drop_1batch
    				, v_drop_delta
    				, v_delete_from_tgt
    				, v_update_tgt
    				, v_insert_into_tgt
    				, v_create_1batch    				
					,v_recreate_stg_table CLOB;    
    				
        DECLARE    	V_SRC_DB					VARCHAR(500);
        DECLARE    	V_SRC_TABLE					VARCHAR(500);
        DECLARE    	V_TGT_DB					VARCHAR(500);
        DECLARE    	V_TGT_PERM_DB, V_TGT_TABLE	VARCHAR(500);
        DECLARE    	V_APPLY_TYPE				VARCHAR(500);
        DECLARE 	V_NULLABLE_COLUMN			VARCHAR(200);
        
        DECLARE		V_BATCH_ID					VARCHAR(150)	DEFAULT 'BATCH_ID';
        DECLARE	   	V_REF_KEY   				VARCHAR(150)	DEFAULT 'REF_KEY';
        DECLARE	   	V_MODIFICATION_TYPE 		VARCHAR(150)	DEFAULT 'MODIFICATION_TYPE';
     	
        DECLARE 	V_WITH_DATA_PI				VARCHAR(100) DEFAULT 'WITH DATA AND STATS PRIMARY INDEX';
		DECLARE 	V_WITH_DATA_UNIQUE_PI		VARCHAR(100) DEFAULT 'WITH DATA AND STATS UNIQUE PRIMARY INDEX';
	
		DECLARE		v_table_name_1batch,
					V_SOURCE_NAME,
				    V_LOADING_MODE,
				    V_REJECTION_TABLE_NAME,
				    V_BUSINESS_RULES_TABLE_NAME VARCHAR(500);
				    
		DECLARE		V_IS_TARANSACTIOANL, v_large_object_count, v_recreate_stg_table_required	INTEGER default 0;
		declare		v_online_load_id_filter,
					v_valid_batches_filter,
					V_UPDATE_ALL_TO_D,V_ALL_COLS,
					V_KEY_COLs, V_QUALIFY, v_, v_keys_eql,
					v_set_non_key_cols, v_valid_data_filter
					 varchar(8000) default '';
		
        DECLARE EXIT HANDLER FOR SQLEXCEPTION
        BEGIN 	
	        SET V_RETURN_CODE = SQLCODE;
			GET DIAGNOSTICS EXCEPTION 1 V_RETURN_MSG = MESSAGE_TEXT; 			
		
			SET o_RETURN_CODE  = V_RETURN_CODE;
			SET o_RETURN_MSG = V_RETURN_MSG;
			
			SET O_ROWS_INSERTED_COUNT 	= V_INSERTED_ROWS_COUNT;
			SET O_ROWS_UPDATED_COUNT 	= V_UPDATED_ROWS_COUNT;
			SET O_ROWS_DELETED_COUNT 	= V_DELETED_ROWS_COUNT;
			SET o_run_id 				= coalesce(v_run_id, i_run_id);
			
            INSERT INTO GPROD1_ETL.EXEC_PROCESS_LOGS  
            (RUN_ID, LOAD_ID, SOURCE_NAME, PROCESS_NAME, START_TIMESTAMP, END_TIMESTAMP, ROWS_INSERTED_COUNT, ROWS_UPDATED_COUNT, ROWS_DELETED_COUNT, ERROR_CODE, ERROR_MSG)  
            VALUES
            (coalesce(v_run_id,	i_run_id), i_LOAD_ID, V_SOURCE_NAME, V_LAYER_NAME||'_'||i_TABLE_NAME, v_START_TIMESTAMP, current_timestamp, V_INSERTED_ROWS_COUNT, V_UPDATED_ROWS_COUNT, V_DELETED_ROWS_COUNT, V_RETURN_CODE, V_RETURN_MSG);
        END;
        
        IF i_run_id IS NULL THEN 
			select GPROD1_ETL.generate_run_id()
			into v_run_id;
		ELSE 	
			SET v_run_id=	i_run_id;
		END IF;
		
        MAINBLOCK:
        BEGIN
	        SET V_PROCESS_NAME = V_LAYER_NAME||'_'||i_TABLE_NAME;
			
	        SELECT 
            SRC.SOURCE_NAME,
		    SRC.LOADING_MODE,
		    SRC.REJECTION_TABLE_NAME,
		    SRC.BUSINESS_RULES_TABLE_NAME,
		    SRC.lyr_db SOURCE_DB,
		    TBL.STG_db TGT_DB,
			TBL.SRCI_db TGT_PERM_DB,
		    TBL.IS_TARANSACTIOANL
		    FROM GPROD1_ETL.V_SOURCE_SYSTEM_TABLES TBL
		    	JOIN GPROD1_ETL.V_SOURCE_SYSTEMS SRC
		    	ON TBL.SOURCE_NAME=SRC.SOURCE_NAME
		    	and SRC.lyr_db = TBL.stg_src_db
		    WHERE SRC.ACTIVE = 1
		    AND  SRC.STG_ACTIVE = 1
		    AND  TBL.ACTIVE = 1
		    AND  TBL.TABLE_NAME = i_TABLE_NAME
		    
		    INTO V_SOURCE_NAME,
		    V_LOADING_MODE,
		    V_REJECTION_TABLE_NAME,
		    V_BUSINESS_RULES_TABLE_NAME,
		    V_SRC_DB,
		    V_TGT_DB,
			V_TGT_PERM_DB,
		    V_IS_TARANSACTIOANL;
            
		    IF V_SOURCE_NAME IS NULL
            THEN
            	set V_RETURN_CODE = -1;
            	SET V_RETURN_MSG = 'Invalid Proccess!';
            	leave MAINBLOCK;
            end if;
            
            if V_TGT_DB is null
	        then
	        	set V_RETURN_CODE = -1;
            	SET V_RETURN_MSG = 'Parameter STG_T_DB is not defined in PARAMETERS table!';
            	leave MAINBLOCK;
	        end if;
	        
            SELECT 
			 TRIM( TRAILING  ',' FROM (XMLAGG(trim(Key_Column)|| ',' ORDER BY Key_Column) (VARCHAR(10000)))) Key_Column
			, trim(XMLAGG( 'TGT.' || trim(Key_Column)|| ' = SRC.' ||trim(Key_Column) || ' AND ' ORDER BY Key_Column) (VARCHAR(10000))) _keys_eql
			, left(_keys_eql,(length(_keys_eql)-3)(int)) Keys_eql
			from GPROD1_ETL.V_TRANSFORM_KEYCOL
			WHERE lyr_db = V_TGT_DB
			AND TABLE_NAME  = i_TABLE_NAME
			INTO V_KEY_COLs, v_, v_keys_eql;
			
			if coalesce(V_KEY_COLs,'') = ''
			then
				set V_RETURN_CODE = -1;
            	SET V_RETURN_MSG = 'No keys defined for table '||i_TABLE_NAME||'!';
            	leave MAINBLOCK;
			end if;
			
			
			select count(1) from DBC.indicesv
			where DATABASEname = V_TGT_DB
			and TABLEname = i_TABLE_NAME
			and columnname = 'batch_id'
			into v_batch_id_as_PI;
				
			IF I_ALL_BATCHES = 1
			THEN				
				SET V_KEY_COLs = V_KEY_COLs || ', ' || V_BATCH_ID;
				SET v_keys_eql = v_keys_eql || ' AND TGT.' || V_BATCH_ID || ' = SRC.' || V_BATCH_ID;
				SET V_QUALIFY = 'QUALIFY ROW_NUMBER()  OVER(PARTITION BY '||V_KEY_COLs||' ORDER BY MODIFICATION_TYPE DESC,REF_KEY DESC)=1';				
			ELSE
				SET V_QUALIFY = 'QUALIFY ROW_NUMBER()  OVER(PARTITION BY '||V_KEY_COLs||' ORDER BY BATCH_ID DESC,MODIFICATION_TYPE DESC,REF_KEY DESC)=1';
			END IF;
			
				
			SELECT 
			TRIM( TRAILING  ',' FROM (XMLAGG(trim(COLUMNNAME)|| ' = SRC.' ||trim(COLUMNNAME) || ',' ORDER BY COLUMNNAME) (VARCHAR(10000)))) SET_COLS
			FROM DBC.COLUMNSV v
			WHERE DATABASENAME = V_TGT_DB
			AND TABLENAME  = i_TABLE_NAME
			and COLUMNNAME not in ('INS_DTTM' ,'UPD_DTTM')
			and not exists (select 1 
							from GPROD1_ETL.TRANSFORM_KEYCOL k 
							where k.TABLE_NAME = v.DATABASENAME 
							and k.TABLE_NAME = v.TABLENAME
							and k.Key_Column = v.COLUMNNAME)
			AND (coalesce(I_ALL_BATCHES,0)=0 OR (I_ALL_BATCHES=1 AND COLUMNNAME<>V_BATCH_ID))
			GROUP BY DATABASENAME, TABLENAME
			INTO v_set_non_key_cols;
			
			SELECT 
			TRIM( TRAILING  ',' FROM (XMLAGG(trim(COLUMNNAME)|| ',' ORDER BY COLUMNNAME) (VARCHAR(10000)))) COLS
			FROM DBC.COLUMNSV v
			WHERE DATABASENAME = V_TGT_DB
			AND TABLENAME  = i_TABLE_NAME
			and COLUMNNAME not in ('INS_DTTM' ,'UPD_DTTM')
			GROUP BY DATABASENAME, TABLENAME
			INTO V_ALL_COLS;
			
			select 
			count(1) cnt
			FROM DBC.columnsV 
			where DATABASENAME = V_TGT_DB
			and tablename=i_TABLE_NAME
			and ColumnType = 'CO'
			into v_large_object_count;
			
		    set v_table_name_1batch = i_TABLE_NAME||'_1batch';
		    
		    if V_REJECTION_TABLE_NAME is not null and V_BUSINESS_RULES_TABLE_NAME is not null
		    then
			    set v_valid_data_filter = 
					'and not exists
						(
							sel 1 
							from '||V_SRC_DB||'.'||V_REJECTION_TABLE_NAME||' r  
							where r.REF_KEY  = x.REF_KEY
							and exists (
											select 1 
											from '||V_SRC_DB||'.'||V_BUSINESS_RULES_TABLE_NAME||' br 
											where r.RULE_ID = br.RULE_ID 
											and br.active = 1 /*hard rejection rules*/							
										)
						)';
			end if;
            
            if V_LOADING_MODE = 'ONLINE'
            then
            	if i_LOAD_ID is not null 
				then
					set v_online_load_id_filter = ' and x.LOAD_ID = '''||i_LOAD_ID||'''';		
				end if;
				
				if I_RELOAD = 1
				then
					set v_online_load_id_filter = v_online_load_id_filter ||
													' and exists (
																select 1 
																from GPROD1_ETL.EXEC_SOURCE_LOGS c 
																where c.LOAD_ID = x.LOAD_ID 
																and c.DELETED <> ' || COALESCE(I_RELOAD, 0) || 
																' and c.STG_DONE = 1
																)';		
				end if;
				set v_valid_batches_filter = 
						' and exists 
						(
							SELECT 1
							FROM GPROD1_ETL.CDC_AUDIT adt
							WHERE adt.PROCESSED = '|| COALESCE(I_RELOAD, 0) ||
							' AND adt.LOAD_ID = x.LOAD_ID
							AND valid_batch=1
							AND adt.SOURCE_NAME = '''||V_SOURCE_NAME||'''
							--AND adt.TABLE_NAME = '''||i_TABLE_NAME||'''
						)';						
            
            end if;
            
			
        --   set v_drop_1batch = 'DROP TABLE '||v_table_name_1batch||';';            
        --   
        --   set v_create_1batch = 
		--	'create multiset volatile TABLE '||v_table_name_1batch||'  , no fallback as 
		--	(
		--		sel * from '||V_SRC_DB||'.'||i_TABLE_NAME||' x
		--		where 1=1
		--		'||v_valid_data_filter||'
		--		'||v_valid_batches_filter||' 		
		--		'||v_online_load_id_filter||'
		--		'||V_QUALIFY||' 
		--	) with data and stats primary index ('||V_KEY_COLs||') on commit preserve rows;';
			
			--set V_SRC_TABLE = v_table_name_1batch;
			
			set v_delete_from_tgt = 'delete from '||V_TGT_DB||'.'||i_TABLE_NAME||';';
			set v_update_tgt = '';
			set v_insert_into_tgt = 
							'insert into '||V_TGT_DB||'.'||i_TABLE_NAME||'
							('||V_ALL_COLS||', INS_DTTM)
							SELECT 
								 '||V_ALL_COLS||'
								, current_timestamp INS_DTTM 
							FROM '||V_SRC_DB||'.'||i_TABLE_NAME||' x
							where 1=1
							'||v_valid_data_filter||'
							'||v_valid_batches_filter||' 		
							'||v_online_load_id_filter||'
							'||V_QUALIFY||' ;';
							

		--	CALL GPROD1_ETL.DBC_SYSEXECSQL_WITH_LOG(v_drop_1batch,V_SQL_SCRIPT_ID,v_run_id,V_SOURCE_NAME,V_PROCESS_NAME,I_LOAD_ID,0/*1 log or 0 don't*/,V_DBC_RETURN_CODE,V_DBC_RETURN_MSG,V_DBC_ROWS_COUNT,v_dbc_run_id);
			
		--	SET V_SQL_SCRIPT_ID = V_SQL_SCRIPT_ID + 1;
		--	CALL GPROD1_ETL.DBC_SYSEXECSQL_WITH_LOG(v_create_1batch,V_SQL_SCRIPT_ID,v_run_id,V_SOURCE_NAME,V_PROCESS_NAME,I_LOAD_ID,1/*1 log or 0 don't*/,V_DBC_RETURN_CODE,V_DBC_RETURN_MSG,V_DBC_ROWS_COUNT,v_dbc_run_id);
		--	IF V_DBC_RETURN_CODE <> 0 THEN
		--		SET V_RETURN_CODE = V_DBC_RETURN_CODE;
		--		SET V_RETURN_MSG = V_DBC_RETURN_MSG;
		--		LEAVE MainBlock;
		--	END IF;
		--	
		--	CALL GPROD1_ETL.CHECK_MANDATORY_COLUMNS(NULL, v_table_name_1batch, V_KEY_COLs,V_NULLABLE_COLUMN, V_DBC_RETURN_CODE, V_DBC_RETURN_MSG);
		--	IF V_DBC_RETURN_CODE <> 0 THEN
		--		SET V_RETURN_CODE = V_DBC_RETURN_CODE;
		--		SET V_RETURN_MSG = V_DBC_RETURN_MSG;
		--		LEAVE MainBlock;
		--	END IF;			
		
			
			if cast(v_delete_from_tgt as char(10)) <> ''
			then
				SET V_SQL_SCRIPT_ID = V_SQL_SCRIPT_ID + 1;
				CALL GPROD1_ETL.DBC_SYSEXECSQL_WITH_LOG(v_delete_from_tgt,V_SQL_SCRIPT_ID,v_run_id,V_SOURCE_NAME,V_PROCESS_NAME,I_LOAD_ID,1/*1 log or 0 don't*/,V_DBC_RETURN_CODE,V_DBC_RETURN_MSG,V_DBC_ROWS_COUNT,v_dbc_run_id);
				IF V_DBC_RETURN_CODE <> 0 THEN
					SET V_RETURN_CODE = V_DBC_RETURN_CODE;
					SET V_RETURN_MSG = V_DBC_RETURN_MSG;
					LEAVE MainBlock;
				END IF;
				set V_DELETED_ROWS_COUNT = V_DELETED_ROWS_COUNT + V_DBC_ROWS_COUNT;
			end if;
			
			CALL GPROD1_ETL.COUNT_ROWS(V_TGT_DB, i_TABLE_NAME, '',V_ROWS_IN_1batch, V_DBC_RETURN_CODE, V_DBC_RETURN_MSG);
			IF V_DBC_RETURN_CODE <> 0 THEN
				SET V_RETURN_CODE = V_DBC_RETURN_CODE;
				SET V_RETURN_MSG = V_DBC_RETURN_MSG;
				LEAVE MainBlock;
			END IF;
			
			--if V_ROWS_IN_1batch = 0
			--then
			--	LEAVE MainBlock;
			--end if;
			
			if cast(v_insert_into_tgt as char(10)) <> ''
			then
				SET V_SQL_SCRIPT_ID = V_SQL_SCRIPT_ID + 1;
				CALL GPROD1_ETL.DBC_SYSEXECSQL_WITH_LOG(v_insert_into_tgt,V_SQL_SCRIPT_ID,v_run_id,V_SOURCE_NAME,V_PROCESS_NAME,I_LOAD_ID,1/*1 log or 0 don't*/,V_DBC_RETURN_CODE,V_DBC_RETURN_MSG,V_DBC_ROWS_COUNT,v_dbc_run_id);
				IF V_DBC_RETURN_CODE <> 0 THEN
					SET V_RETURN_CODE = V_DBC_RETURN_CODE;
					SET V_RETURN_MSG = V_DBC_RETURN_MSG;
					LEAVE MainBlock;
				END IF;
				set V_INSERTED_ROWS_COUNT = V_INSERTED_ROWS_COUNT + V_DBC_ROWS_COUNT;
			end if;
			
        END;
        
		--CALL GPROD1_ETL.DBC_SYSEXECSQL_WITH_LOG(v_drop_1batch,V_SQL_SCRIPT_ID,v_run_id,V_SOURCE_NAME,V_PROCESS_NAME,I_LOAD_ID,0/*1 log or 0 don't*/,V_DBC_RETURN_CODE,V_DBC_RETURN_MSG,V_DBC_ROWS_COUNT,v_dbc_run_id);
        
		INSERT INTO GPROD1_ETL.EXEC_PROCESS_LOGS  
        (RUN_ID, LOAD_ID, SOURCE_NAME, PROCESS_NAME, START_TIMESTAMP, END_TIMESTAMP, ROWS_INSERTED_COUNT, ROWS_UPDATED_COUNT, ROWS_DELETED_COUNT, ERROR_CODE, ERROR_MSG)  
        VALUES
        (v_run_id, i_LOAD_ID, V_SOURCE_NAME, V_PROCESS_NAME, v_START_TIMESTAMP, current_timestamp, V_INSERTED_ROWS_COUNT, V_UPDATED_ROWS_COUNT, V_DELETED_ROWS_COUNT, V_RETURN_CODE, V_RETURN_MSG);
        
                                                                                         
        SET O_RETURN_CODE = V_RETURN_CODE;
        SET O_RETURN_MSG = V_RETURN_MSG;
        SET O_ROWS_INSERTED_COUNT	= V_INSERTED_ROWS_COUNT;
    	SET	O_ROWS_UPDATED_COUNT	= V_UPDATED_ROWS_COUNT;
    	SET	O_ROWS_DELETED_COUNT	= V_DELETED_ROWS_COUNT;
    	set o_run_id				= v_run_id;
    END;