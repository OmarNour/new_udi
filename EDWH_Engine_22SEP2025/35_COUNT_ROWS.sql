REPLACE PROCEDURE /*VER.1*/ GPROD1_ETL.COUNT_ROWS 
    /*
    ###################################################################################################################################################################
    ------------------------------------------------------------------------------------------------------------------------------------------------------------------
    #  Version History
    ------------------------------------------------------------------------------------------------------------------------------------------------------------------
    # 	Version 	|	Date        			|	Author					| 	Comments
    #	1.0        	|	23 December, 2019		|	Omar Nour				|	Fisrt Cut.
    ------------------------------------------------------------------------------------------------------------------------------------------------------------------
    #  Examples:
    #  
    ###################################################################################################################################################################
    */
(
	in I_DB 			VARCHAR (200),
	in I_TBL 			VARCHAR (200),
	in I_QUERY 			VARCHAR (5000),
	out O_CNT 			FLOAT,
	out O_RETURN_CODE 	INTEGER,
	out O_RETURN_MSG 	VARCHAR (5000)
)
BEGIN
	
	DECLARE C1 CURSOR FOR S1;
	BEGIN
		DECLARE V_TABLE_NAME 	VARCHAR(500) DEFAULT '';
		DECLARE v_cnt_query_stmt, v_cnt_tbl_stmt 	VARCHAR(5000);
		DECLARE v_cnt_qry, v_cnt_tbl 	FLOAT DEFAULT 0;
		
		DECLARE EXIT HANDLER FOR SQLEXCEPTION
		BEGIN
			SET O_CNT = 0;
			SET O_RETURN_CODE = SQLCODE;
			GET DIAGNOSTICS EXCEPTION 1 O_RETURN_MSG = MESSAGE_TEXT;
		END;
		
		IF COALESCE (I_TBL, '') <>'' 
		THEN
			IF COALESCE (I_DB, '') <>'' 
			THEN
				SET V_TABLE_NAME = I_DB ||'.'|| I_TBL;
			ELSE
				SET V_TABLE_NAME = I_TBL;
			END IF;
			
			SET v_cnt_tbl_stmt = 'SEL COUNT (1) (float) FROM ' ||V_TABLE_NAME ||';' ;
			
			PREPARE S1 FROM v_cnt_tbl_stmt;
			OPEN C1;
			FETCH C1 INTO v_cnt_tbl;
			CLOSE C1;
			
		END IF;
		
		IF COALESCE (I_QUERY, '') <>'' 
		THEN
			
			SET v_cnt_query_stmt = 'SEL COUNT (1) (float) FROM (' ||I_QUERY ||') x;' ;
			
			PREPARE S1 FROM v_cnt_query_stmt;
			OPEN C1;
			FETCH C1 INTO v_cnt_qry;
			CLOSE C1;
			
		END IF;
		
		
		SET O_CNT = v_cnt_tbl + v_cnt_qry;
		SET O_RETURN_CODE = 0;
		SET O_RETURN_MSG = 'Process Completed Successfully';
	END;
END;
