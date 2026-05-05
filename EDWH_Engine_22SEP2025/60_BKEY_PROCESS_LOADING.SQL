REPLACE PROCEDURE /*VER.2*/ GPROD1_ETL.BKEY_PROCESS_LOADING
    /*
    ###################################################################################################################################################################
    ------------------------------------------------------------------------------------------------------------------------------------------------------------------
    #  Version History
    ------------------------------------------------------------------------------------------------------------------------------------------------------------------
    # 	Version 	|	Date        			|	Author					| 	Comments
    #   1.0        	|	1 JAN, 2023	        |	 Omar Nour				|	Fisrt Cut.
    ------------------------------------------------------------------------------------------------------------------------------------------------------------------
    #  Examples:
    #  
    ###################################################################################################################################################################
    */
(
	IN i_LOAD_ID 			VARCHAR(100), 
	IN i_run_id 			BIGINT,
	IN i_PROCESS_NAME  		VARCHAR(500), 
	OUT o_RETURN_CODE    		INTEGER,
	OUT o_RETURN_MSG  		VARCHAR(1000),
	OUT o_ROWS_COUNT_VALUE	INTEGER,
	OUT o_run_id 			BIGINT
)

------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Definition Of Procedure:
------------------------------------------------------------------------------------------------------------------------------------------------------------------


BEGIN

	declare v_START_TIMESTAMP TIMESTAMP(6) default current_timestamp;

	DECLARE v_RETURN_CODE, V_DBC_RETURN_CODE 			INTEGER default 0;
	DECLARE v_RETURN_MSG, V_DBC_RETURN_MSG  			VARCHAR(1000) DEFAULT 'Process Completed Successfully';
	DECLARE V_DBC_ROWS_COUNT			float default 0;
	
	DECLARE	v_run_id, v_dbc_run_id		BIGINT;
	
	DECLARE v_Ctl_Id					INTEGER;
	DECLARE v_src_DB					VARCHAR(500);
	DECLARE v_src_table					VARCHAR(500);
	DECLARE v_tgt_db					VARCHAR(500);
	DECLARE v_tgt_table					VARCHAR(500);
	DECLARE v_Key_Set_Id				INTEGER;
	DECLARE v_Domain_Id					INTEGER;
	declare v_statment					CLOB;
	declare V_SOURCE_NAME 				VARCHAR(500);
	DECLARE C1 CURSOR FOR S1;

	DECLARE EXIT HANDLER FOR SQLEXCEPTION
        BEGIN 	
	        SET V_RETURN_CODE = SQLCODE;
			GET DIAGNOSTICS EXCEPTION 1 V_RETURN_MSG = MESSAGE_TEXT; 			
		
			SET o_RETURN_CODE  = V_RETURN_CODE;
			SET o_RETURN_MSG = V_RETURN_MSG;
			
			SET o_ROWS_COUNT_VALUE 		= 0;
			SET o_run_id 				= coalesce(v_run_id, i_run_id);
			
            INSERT INTO GPROD1_ETL.EXEC_PROCESS_LOGS  
            (RUN_ID, LOAD_ID, SOURCE_NAME, PROCESS_NAME, START_TIMESTAMP, END_TIMESTAMP, ROWS_INSERTED_COUNT, ROWS_UPDATED_COUNT, ROWS_DELETED_COUNT, ERROR_CODE, ERROR_MSG)  
            VALUES
            (coalesce(v_run_id,	i_run_id), i_LOAD_ID, V_SOURCE_NAME, i_PROCESS_NAME, v_START_TIMESTAMP, current_timestamp, 0, 0, 0, V_RETURN_CODE, V_RETURN_MSG);
        END;
	
	
	BEGIN
		IF i_run_id IS NULL THEN 
			select GPROD1_ETL.generate_run_id()
			into v_run_id;
		ELSE 	
			SET v_run_id=	i_run_id;
		END IF;
		
		select 
			 P.SRC_DB
			,P.SRC_TABLE
			,P.TGT_DB  
			,P.TGT_TABLE               
			,P.Key_Set_Id
			,P.Domain_Id
			,p.source_name
		from GPROD1_ETL.V_PROCESS P
		where  p.active = 1
		and p.TGT_LAYER ='BKEY'
		and P.PROCESS_NAME = I_PROCESS_NAME
		into v_src_DB
			,v_src_table
			,v_tgt_db
			,v_tgt_table
			,v_Key_Set_Id
			,v_Domain_Id
			,V_SOURCE_NAME;
			
		-- Model Block			
		if v_Domain_Id is not null
		then
			set v_statment = '
						insert into '||v_tgt_db||'.'||v_tgt_table||'
						(source_key, domain_id, key_set_id, edw_key, start_date, process_name)

						with t_max_edw_key as
						(
							select coalesce(max(edw_key), 1000) max_edw_key 
							from  '||v_tgt_db||'.'||v_tgt_table||' 						
						)
						select src.source_key
						, '||v_Domain_Id||' domain_id
						, '||v_Key_Set_Id||' key_set_id					
						,sum(1) over (order by src.source_key ROWS UNBOUNDED PRECEDING) + MK.max_edw_key edw_key
						, current_date start_date
						, '''||i_PROCESS_NAME||''' process_name
						from '||v_src_DB||'.'||v_src_table||' src, t_max_edw_key MK
						where not exists (select 1 
										from '||v_tgt_db||'.'||v_tgt_table||'  tgt
										where src.source_key=tgt.source_key
										and tgt.domain_id = '||v_Domain_Id||'
										);';
										
			CALL GPROD1_ETL.DBC_SYSEXECSQL_WITH_LOG(v_statment,1,v_run_id,V_SOURCE_NAME,i_PROCESS_NAME,I_LOAD_ID,1/*1 log or 0 don't*/,V_DBC_RETURN_CODE,V_DBC_RETURN_MSG,V_DBC_ROWS_COUNT, v_dbc_run_id);
			set V_RETURN_CODE = V_DBC_RETURN_CODE;
			set V_RETURN_MSG = V_DBC_RETURN_MSG;
		else
			set V_RETURN_CODE = -1;
			set V_RETURN_MSG = 'BKEY, is not registered in PROCESS table';
		end if;
	end;
	
	SET o_RETURN_CODE =  V_RETURN_CODE;
	SET o_RETURN_MSG =  V_RETURN_MSG;
	SET o_ROWS_COUNT_VALUE = V_DBC_ROWS_COUNT;
	SET o_run_id = v_run_id;
	
	INSERT INTO GPROD1_ETL.EXEC_PROCESS_LOGS  
    (RUN_ID, LOAD_ID, SOURCE_NAME, PROCESS_NAME, START_TIMESTAMP, END_TIMESTAMP, ROWS_INSERTED_COUNT, ROWS_UPDATED_COUNT, ROWS_DELETED_COUNT, ERROR_CODE, ERROR_MSG)  
    VALUES
    (v_run_id, i_LOAD_ID, V_SOURCE_NAME, i_PROCESS_NAME, v_START_TIMESTAMP, current_timestamp, V_DBC_ROWS_COUNT, 0, 0, V_RETURN_CODE, V_RETURN_MSG);
	
END;