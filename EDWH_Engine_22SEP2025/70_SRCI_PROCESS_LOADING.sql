
REPLACE  PROCEDURE /*VER.09*/ GPROD1_ETL.SRCI_PROCESS_LOADING
    /*
    ###################################################################################################################################################################
    ------------------------------------------------------------------------------------------------------------------------------------------------------------------
    #  Version History
    ------------------------------------------------------------------------------------------------------------------------------------------------------------------
    # 	Version 	|	Date        			|	Author					| 	Comments
    #   1.0        	|	1 JAN, 2023	       		|	 Omar Nour				|	Fisrt Cut.
	#   9.0        	|	22 NOV, 2023	        |	 Ashraf Elhawary		|	change the length of variables :v_insert_into_tmp_srci,v_update_tgt,v_update_tgt_SET_D,v_set_non_key_cols,v_insert_into_tgt .
    ------------------------------------------------------------------------------------------------------------------------------------------------------------------
    #  Examples:
    #  
    ###################################################################################################################################################################
    */
    (
    IN 		i_TABLE_NAME  			VARCHAR(1000),
    IN 		I_RUN_ID 				BIGINT,
    IN		I_LOAD_ID				VARCHAR(500),
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
	    	    
		DECLARE		V_LAYER_NAME				VARCHAR(10) DEFAULT 'SRCI';
		DECLARE		V_PROCESS_NAME				VARCHAR(200) DEFAULT '';
		
        DECLARE 	V_RETURN_CODE  				INTEGER DEFAULT 0;
        DECLARE 	V_RETURN_MSG 				VARCHAR(5000) DEFAULT 'Process Completed Successfully';
        
        DECLARE 	V_TGT_TABLE_FOUND, V_INSERTED_ROWS_COUNT,V_UPDATED_ROWS_COUNT,V_DELETED_ROWS_COUNT,
        			V_IS_TARANSACTIOANL, V_ROWS_IN_1batch, V_ROWS_IN_DELTA, V_ROWS_COUNT, V_DBC_ROWS_COUNT				FLOAT DEFAULT 0;

        DECLARE		v_run_id, v_dbc_run_id		BIGINT;
        DECLARE 	V_SQL_SCRIPT_ID				INTEGER DEFAULT 0;
        DECLARE 	V_DBC_RETURN_CODE 			INTEGER;
		DECLARE 	V_DBC_RETURN_MSG  			VARCHAR(1000);
  
    				
        DECLARE    	V_SRC_DB					VARCHAR(500);
        DECLARE    	V_SRC_TABLE					VARCHAR(500);
        DECLARE    	V_TGT_DB					VARCHAR(500);
        DECLARE    	V_TGT_TABLE					VARCHAR(500);
        
     	
        DECLARE 	V_WITH_DATA_PI				VARCHAR(100) DEFAULT 'WITH DATA AND STATS PRIMARY INDEX';
		DECLARE 	V_WITH_DATA_UNIQUE_PI		VARCHAR(100) DEFAULT 'WITH DATA AND STATS UNIQUE PRIMARY INDEX';
	    DECLARE 	V_WITH_NO_DATA_PI				VARCHAR(100) DEFAULT 'WITH NO DATA AND STATS PRIMARY INDEX';

		DECLARE		V_SOURCE_NAME, V_LOADING_MODE			VARCHAR(500);
		DECLARE		V_BATCH_ID					VARCHAR(150)	DEFAULT 'BATCH_ID';
		DECLARE 	v_insert_into_tmp_srci,
					v_update_tgt,
					v_update_tgt_SET_D,
					v_set_non_key_cols,
					v_insert_into_tgt VARCHAR(30000) DEFAULT '';
		
		declare		V_KEY_COLs, V_PI_COLs
					, v_create_new_table
					, v_rename_current_table
					, v_rename_new_table
					, v_drop_old_table
					, v_drop_new_table 
					, v_drop_tmp_srci
					, v_create_tmp_srci
					, v_delete_from_tgt
					, v_, v_keys_eql
					, V_ALL_COLS
					varchar(19900) default '';
		
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
        
        
        MAINBLOCK:
        BEGIN
			IF i_run_id IS NULL THEN 
			select GPROD1_ETL.generate_run_id()
			into v_run_id;
			ELSE 	
				SET v_run_id=	i_run_id;
			END IF;
		
	        SET V_PROCESS_NAME = V_LAYER_NAME||'_'||i_TABLE_NAME;
			
            SELECT SRC.SOURCE_NAME
					,SRC.LOADING_MODE
            		,TBL.srci_src_db V_SRC_DB
            		,TBL.SRCI_DB  V_TGT_DB
					,TBL.IS_TARANSACTIOANL
		    FROM GPROD1_ETL.V_SOURCE_SYSTEM_TABLES TBL
		    	JOIN GPROD1_ETL.SOURCE_SYSTEMS SRC
		    	ON TBL.SOURCE_NAME=SRC.SOURCE_NAME
		    WHERE SRC.ACTIVE = 1
		    AND  SRC.STG_ACTIVE = 1
		    AND  TBL.ACTIVE = 1
		    AND  TBL.TABLE_NAME = i_TABLE_NAME
		    
		    INTO V_SOURCE_NAME, V_LOADING_MODE, V_SRC_DB, V_TGT_DB, V_IS_TARANSACTIOANL;
            
			begin
				-- validatons 
				
				IF V_SOURCE_NAME IS NULL
				THEN
					set V_RETURN_CODE = -1;
					SET V_RETURN_MSG = 'Invalid Proccess!';
					leave MAINBLOCK;
				end if;
				
				if V_SRC_DB is null or V_TGT_DB is null
				then
					set V_RETURN_CODE = -1;
					SET V_RETURN_MSG = 'INVALID source or target DB!';
					leave MAINBLOCK;
				end if;
				
				SELECT 
				 TRIM( TRAILING  ',' FROM (XMLAGG(trim(PI_Column)|| ',' ORDER BY PI_Column) (VARCHAR(10000)))) PI_Column
				from GPROD1_ETL.PICOLs
				WHERE LAYER_NAME = V_TGT_DB
				AND TABLE_NAME  = i_TABLE_NAME
				INTO V_PI_COLs;
							
				SELECT 
				 TRIM( TRAILING  ',' FROM (XMLAGG(trim(Key_Column)|| ',' ORDER BY Key_Column) (VARCHAR(20000)))) Key_Column
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
				
				IF I_ALL_BATCHES = 1
				THEN				
					SET V_KEY_COLs = V_KEY_COLs || ', ' || V_BATCH_ID;
					SET v_keys_eql = v_keys_eql || ' AND TGT.' || V_BATCH_ID || ' = SRC.' || V_BATCH_ID;					
				
				END IF;
				
				if coalesce(V_PI_COLs,'') = ''
				then
					set V_PI_COLs = V_KEY_COLs;
				end if;
			end;
			
			begin
				-- check if table exists or not.
				-- if not, then create it.
				select count(1) cnt
				from dbc.tablesv v
				where v.databasename = V_TGT_DB
				and tablename = i_TABLE_NAME
				into V_TGT_TABLE_FOUND;
				
				if V_TGT_TABLE_FOUND = 0
				then
					set v_create_new_table = 'create table '||V_TGT_DB||'.'||i_TABLE_NAME||' as
									( select * from '||V_SRC_DB||'.'||i_TABLE_NAME||') 
									'||V_WITH_DATA_PI||' ('||V_PI_COLs||');';
				
					SET V_SQL_SCRIPT_ID = V_SQL_SCRIPT_ID + 1;
					CALL GPROD1_ETL.DBC_SYSEXECSQL_WITH_LOG(v_create_new_table,V_SQL_SCRIPT_ID,v_run_id,V_SOURCE_NAME,V_PROCESS_NAME,I_LOAD_ID,1/*1 log or 0 don't*/,V_DBC_RETURN_CODE,V_DBC_RETURN_MSG,V_DBC_ROWS_COUNT, v_dbc_run_id);
					IF V_DBC_RETURN_CODE <> 0 THEN
						SET V_RETURN_CODE = V_DBC_RETURN_CODE;
						SET V_RETURN_MSG = V_DBC_RETURN_MSG;
						LEAVE MainBlock;
					END IF;
					
					CALL GPROD1_ETL.COUNT_ROWS(V_TGT_DB, i_TABLE_NAME, '',V_INSERTED_ROWS_COUNT, V_DBC_RETURN_CODE, V_DBC_RETURN_MSG);
					IF V_DBC_RETURN_CODE <> 0 THEN
						SET V_RETURN_CODE = V_DBC_RETURN_CODE;
						SET V_RETURN_MSG = V_DBC_RETURN_MSG;
						LEAVE MainBlock;
					END IF;
				end if;
			end;
			
		    
			if V_TGT_TABLE_FOUND = 1
			then
			
				SELECT 
				TRIM( TRAILING  ',' FROM (XMLAGG(trim(COLUMNNAME)|| ' = SRC.' ||trim(COLUMNNAME) || ',' ORDER BY COLUMNNAME) (VARCHAR(20000)))) SET_COLS
				FROM DBC.COLUMNSV v
				WHERE DATABASENAME = V_TGT_DB
				AND TABLENAME  = i_TABLE_NAME		
				--and COLUMNNAME not in ('INS_DTTM' ,'UPD_DTTM')				
				and not exists (select 1 
								from GPROD1_ETL.TRANSFORM_KEYCOL k 
								where k.TABLE_NAME = v.DATABASENAME 
								and k.TABLE_NAME = v.TABLENAME
								and k.Key_Column = v.COLUMNNAME)			
				GROUP BY DATABASENAME, TABLENAME
				INTO v_set_non_key_cols;
				
				SELECT 
				TRIM( TRAILING  ',' FROM (XMLAGG(trim(COLUMNNAME)|| ',' ORDER BY COLUMNNAME) (VARCHAR(20000)))) COLS
				FROM DBC.COLUMNSV v
				WHERE DATABASENAME = V_TGT_DB
				AND TABLENAME  = i_TABLE_NAME	
				--and COLUMNNAME not in ('INS_DTTM' ,'UPD_DTTM')								
				GROUP BY DATABASENAME, TABLENAME
				INTO V_ALL_COLS;
				
				------------------------------------------------------------------------------------------------------------------------------------
				set v_drop_tmp_srci = 'DROP TABLE '||i_TABLE_NAME||'_tmp;';
				set v_create_tmp_srci = 'create multiset volatile TABLE '||i_TABLE_NAME||'_tmp as
									'||V_TGT_DB||'.'||i_TABLE_NAME||'
									'||V_WITH_NO_DATA_PI||' ('||V_PI_COLs||') on commit preserve rows;';
									
				set v_insert_into_tmp_srci = 
								'insert into '||i_TABLE_NAME||'_tmp 
								('||V_ALL_COLS||')
								SELECT 
									 '||V_ALL_COLS||'									
								FROM '||V_SRC_DB||'.'||i_TABLE_NAME||' src';
								
				if V_IS_TARANSACTIOANL = 1
				THEN
					set v_delete_from_tgt = 'delete from '||V_TGT_DB||'.'||i_TABLE_NAME||';';
				else
					if V_LOADING_MODE = 'OFFLINE' 
					THEN
						set v_update_tgt_SET_D = 'UPDATE '||V_TGT_DB||'.'||i_TABLE_NAME||' SET MODIFICATION_TYPE = ''D'';';
					END IF;
				end if;
				
				if V_IS_TARANSACTIOANL = 0
				then
					set v_update_tgt = 
									'update tgt
									from '||V_TGT_DB||'.'||i_TABLE_NAME||' tgt, '||i_TABLE_NAME||'_tmp src
									set '||v_set_non_key_cols||'								
									where '||v_keys_eql||';		
									';
				end if;
				
				set v_insert_into_tgt = 
								'insert into '||V_TGT_DB||'.'||i_TABLE_NAME||'
								('||V_ALL_COLS||')
								SELECT 
									 '||V_ALL_COLS||'									
								FROM '||i_TABLE_NAME||'_tmp src
								where not exists (
												select 1 
												from '||V_TGT_DB||'.'||i_TABLE_NAME||' tgt 
												where '||v_keys_eql||'
												);
								';
				------------------------------------------------------------------------------------------------------------------------------------		
				CALL GPROD1_ETL.DBC_SYSEXECSQL_WITH_LOG(v_drop_tmp_srci,V_SQL_SCRIPT_ID,v_run_id,V_SOURCE_NAME,V_PROCESS_NAME,I_LOAD_ID,0/*1 log or 0 don't*/,V_DBC_RETURN_CODE,V_DBC_RETURN_MSG,V_DBC_ROWS_COUNT, v_dbc_run_id);
				
				SET V_SQL_SCRIPT_ID = V_SQL_SCRIPT_ID + 1;
				CALL GPROD1_ETL.DBC_SYSEXECSQL_WITH_LOG(v_create_tmp_srci,V_SQL_SCRIPT_ID,v_run_id,V_SOURCE_NAME,V_PROCESS_NAME,I_LOAD_ID,1/*1 log or 0 don't*/,V_DBC_RETURN_CODE,V_DBC_RETURN_MSG,V_DBC_ROWS_COUNT, v_dbc_run_id);
				IF V_DBC_RETURN_CODE <> 0 THEN
					SET V_RETURN_CODE = V_DBC_RETURN_CODE;
					SET V_RETURN_MSG = V_DBC_RETURN_MSG;
					LEAVE MainBlock;
				END IF;
				
				SET V_SQL_SCRIPT_ID = V_SQL_SCRIPT_ID + 1;
				CALL GPROD1_ETL.DBC_SYSEXECSQL_WITH_LOG(v_insert_into_tmp_srci,V_SQL_SCRIPT_ID,v_run_id,V_SOURCE_NAME,V_PROCESS_NAME,I_LOAD_ID,1/*1 log or 0 don't*/,V_DBC_RETURN_CODE,V_DBC_RETURN_MSG,V_DBC_ROWS_COUNT, v_dbc_run_id);
				IF V_DBC_RETURN_CODE <> 0 THEN
					SET V_RETURN_CODE = V_DBC_RETURN_CODE;
					SET V_RETURN_MSG = V_DBC_RETURN_MSG;
					LEAVE MainBlock;
				END IF;
				
				IF CAST(v_delete_from_tgt AS CHAR(10)) <> ''
				then
					SET V_SQL_SCRIPT_ID = V_SQL_SCRIPT_ID + 1;
					CALL GPROD1_ETL.DBC_SYSEXECSQL_WITH_LOG(v_delete_from_tgt,V_SQL_SCRIPT_ID,v_run_id,V_SOURCE_NAME,V_PROCESS_NAME,I_LOAD_ID,1/*1 log or 0 don't*/,V_DBC_RETURN_CODE,V_DBC_RETURN_MSG,V_DBC_ROWS_COUNT, v_dbc_run_id);
					IF V_DBC_RETURN_CODE <> 0 THEN
						SET V_RETURN_CODE = V_DBC_RETURN_CODE;
						SET V_RETURN_MSG = V_DBC_RETURN_MSG;
						LEAVE MainBlock;
					END IF;
				end if;
				
				IF CAST(v_update_tgt_SET_D AS CHAR(10)) <> ''
				then
					SET V_SQL_SCRIPT_ID = V_SQL_SCRIPT_ID + 1;
					CALL GPROD1_ETL.DBC_SYSEXECSQL_WITH_LOG(v_update_tgt_SET_D,V_SQL_SCRIPT_ID,v_run_id,V_SOURCE_NAME,V_PROCESS_NAME,I_LOAD_ID,1/*1 log or 0 don't*/,V_DBC_RETURN_CODE,V_DBC_RETURN_MSG,V_DBC_ROWS_COUNT, v_dbc_run_id);
					IF V_DBC_RETURN_CODE <> 0 THEN
						SET V_RETURN_CODE = V_DBC_RETURN_CODE;
						SET V_RETURN_MSG = V_DBC_RETURN_MSG;
						LEAVE MainBlock;
					END IF;
				end if;
				
				IF CAST(v_update_tgt AS CHAR(10)) <> ''
				then
					SET V_SQL_SCRIPT_ID = V_SQL_SCRIPT_ID + 1;
					CALL GPROD1_ETL.DBC_SYSEXECSQL_WITH_LOG(v_update_tgt,V_SQL_SCRIPT_ID,v_run_id,V_SOURCE_NAME,V_PROCESS_NAME,I_LOAD_ID,1/*1 log or 0 don't*/,V_DBC_RETURN_CODE,V_DBC_RETURN_MSG,V_DBC_ROWS_COUNT, v_dbc_run_id);
					IF V_DBC_RETURN_CODE <> 0 THEN
						SET V_RETURN_CODE = V_DBC_RETURN_CODE;
						SET V_RETURN_MSG = V_DBC_RETURN_MSG;
						LEAVE MainBlock;
					END IF;
					set V_UPDATED_ROWS_COUNT = V_DBC_ROWS_COUNT;
				end if;
				
				SET V_SQL_SCRIPT_ID = V_SQL_SCRIPT_ID + 1;
				CALL GPROD1_ETL.DBC_SYSEXECSQL_WITH_LOG(v_insert_into_tgt,V_SQL_SCRIPT_ID,v_run_id,V_SOURCE_NAME,V_PROCESS_NAME,I_LOAD_ID,1/*1 log or 0 don't*/,V_DBC_RETURN_CODE,V_DBC_RETURN_MSG,V_DBC_ROWS_COUNT, v_dbc_run_id);
				IF V_DBC_RETURN_CODE <> 0 THEN
					SET V_RETURN_CODE = V_DBC_RETURN_CODE;
					SET V_RETURN_MSG = V_DBC_RETURN_MSG;
					LEAVE MainBlock;
				END IF;
				set V_INSERTED_ROWS_COUNT = V_DBC_ROWS_COUNT;	
				
				CALL GPROD1_ETL.DBC_SYSEXECSQL_WITH_LOG(v_drop_tmp_srci,V_SQL_SCRIPT_ID,v_run_id,V_SOURCE_NAME,V_PROCESS_NAME,I_LOAD_ID,0/*1 log or 0 don't*/,V_DBC_RETURN_CODE,V_DBC_RETURN_MSG,V_DBC_ROWS_COUNT, v_dbc_run_id);
				
			end if;
			
        END;
                                    
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