REPLACE  PROCEDURE /*VER.02*/ GPROD1_ETL.BKEY_LOADING
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
    IN 		i_SOURCE_NAME  			VARCHAR(200),
    IN 		I_PROCESS_NAME  		VARCHAR(200),
    IN 		i_TABLE_NAME  			VARCHAR(200),
    IN 		I_RUN_ID 				BIGINT,
    IN		I_LOAD_ID				VARCHAR(500),
    OUT  	O_ROWS_INSERTED_COUNT	FLOAT,
    OUT  	O_RETURN_CODE  			INTEGER,
    OUT  	O_RETURN_MSG 			VARCHAR(5000),
    OUT 	o_run_id 				BIGINT
    )
    BEGIN
        DECLARE 	V_RETURN_CODE  				INTEGER DEFAULT 0;
        DECLARE 	V_RETURN_MSG 				VARCHAR(5000) DEFAULT 'Process Completed Successfully';
        DECLARE 	V_INSERTED_ROWS_COUNT,V_UPDATED_ROWS_COUNT,V_DELETED_ROWS_COUNT,
		        	V_dbc_INSERTED_ROWS_COUNT,V_dbc_UPDATED_ROWS_COUNT,V_dbc_DELETED_ROWS_COUNT 
        			FLOAT DEFAULT 0;
        DECLARE		v_run_id,v_dbc_run_id		BIGINT;
        
        DECLARE 	V_DBC_RETURN_CODE 			INTEGER;
		DECLARE 	V_DBC_RETURN_MSG  			VARCHAR(1000);
		DECLARE 	V_DBC_ROWS_COUNT			FLOAT;
		DECLARE 	V_PROCESS_DONE  	       INTEGER DEFAULT 0;
		
        DECLARE EXIT HANDLER FOR SQLEXCEPTION
        BEGIN 	
	        set O_ROWS_INSERTED_COUNT = V_INSERTED_ROWS_COUNT;
	        SET o_run_id = coalesce(v_run_id, i_run_id);
            SET O_RETURN_CODE = SQLCODE;
            GET DIAGNOSTICS EXCEPTION 1 O_RETURN_MSG = MESSAGE_TEXT;
        END;
        
        
        MAINBLOCK:
        BEGIN
	        
	        IF i_run_id IS NULL THEN 
				select GPROD1_ETL.generate_run_id()
				into v_run_id;
			ELSE 	
				SET v_run_id=	i_run_id;
			END IF;
            
			FOR	loop1 AS loopx CURSOR FOR 
						select 
						 p.PROCESS_NAME,
						 P.SOURCE_NAME
					from GPROD1_ETL.V_PROCESS P
					where p.active = 1
					and p.TGT_LAYER ='BKEY'
					and (P.PROCESS_NAME = I_PROCESS_NAME or I_PROCESS_NAME is null)
					and (P.SOURCE_NAME = i_SOURCE_NAME or i_SOURCE_NAME is null) 
					and (P.TGT_TABLE = i_TABLE_NAME or i_TABLE_NAME is null)
					        
			DO
			  SEL SUM(1) INTO V_PROCESS_DONE 
				FROM GPROD1_ETL.EXEC_PROCESS_LOGS pl
				WHERE pl.RUN_ID = v_run_id
					AND pl.LOAD_ID = I_LOAD_ID
					AND pl.SOURCE_NAME = loop1.SOURCE_NAME
					AND pl.PROCESS_NAME = loop1.PROCESS_NAME;
					
				IF COALESCE (V_PROCESS_DONE,0) = 0 
				THEN 
					call GPROD1_ETL.BKEY_PROCESS_LOADING
													(
														I_LOAD_ID	--IN i_LOAD_ID 			VARCHAR(100), 
														,v_run_id--IN i_run_id 			BIGINT,
														,loop1.PROCESS_NAME --i_PROCESS_NAME  		VARCHAR(500), 
														,V_DBC_RETURN_CODE	--OUT RETURN_CODE    		INTEGER,
														,V_DBC_RETURN_MSG 	--OUT RETURN_MSG  		VARCHAR(1000),
														,V_dbc_INSERTED_ROWS_COUNT	--OUT ROWS_COUNT_VALUE	INTEGER
														,v_dbc_run_id
													);	
													
					if v_dbc_return_code <> 0 and V_RETURN_CODE = 0
					then
						set V_RETURN_CODE = v_dbc_return_code;
						set V_RETURN_MSG = V_DBC_RETURN_MSG;
					end if;
					
					set V_INSERTED_ROWS_COUNT = V_INSERTED_ROWS_COUNT + V_dbc_INSERTED_ROWS_COUNT;
			    END IF;
			end for;
        END;
                                                                                        
                                                                                         
        SET O_RETURN_CODE = V_RETURN_CODE;
        SET O_RETURN_MSG = V_RETURN_MSG;
        
        SET O_ROWS_INSERTED_COUNT	= V_INSERTED_ROWS_COUNT;
    	SET	o_run_id				= v_run_id;
    END;