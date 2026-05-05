REPLACE PROCEDURE  /*VER.5*/ GPROD1_ETL.CHECK_MANDATORY_COLUMNS
/*
###################################################################################################################################################################
------------------------------------------------------------------------------------------------------------------------------------------------------------------
#  Version History
------------------------------------------------------------------------------------------------------------------------------------------------------------------
#	Version 		| Date        	| Author					| COMMENT
#	1.0				| 28 DEC, 2021 	| Omar Nour					| Initial version
#	2.0				| 14 FEB, 2022 	| Omar Nour					| Fix issue, set O_RETURN_CODE = -1, if null column found
#	3.0				| 25 DEC, 2022 	| ASHRAF ELHAWARY 			| ADDING NEW PARAMETER
#	4.0				| 07 Feb, 2023 	| omar Nour					| Check for keys only from GCFR_TRANSFORM_KEYCOL
#	5.0				| 27 Feb, 2023 	| ASHRAF ELHAWARY			| Check for CLOUMNS from DBC.COLUMNSV
------------------------------------------------------------------------------------------------------------------------------------------------------------------
#  Examples:
CALL GPROD1_ETL.CHECK_MANDATORY_COLUMNS('GPROD1V_base', 'PRTY', I_MORE_COLUMNS,O_NULLABLE_COLUMN, O_RETURN_CODE, O_RETURN_MSG);
###################################################################################################################################################################
*/

(	
	IN	I_DATABASE_NAME		VARCHAR(300) ,
	IN	I_TABLE_NAME		VARCHAR(300) ,
	IN 	I_MORE_COLUMNS		VARCHAR(1000),
	
	OUT	O_NULLABLE_COLUMN	VARCHAR(300) ,
	OUT O_RETURN_CODE		INTEGER,
	OUT O_RETURN_MSG		VARCHAR(10000)
	
)

BEGIN
	
	DECLARE V_IS_NULL			INTEGER DEFAULT 0;
	DECLARE V_NULLABLE_COLUMN	VARCHAR(300);
	DECLARE V_TABLE_NAME		VARCHAR(300);
	DECLARE V_DATABASE_NAME		VARCHAR(300);
	DECLARE V_RETURN_CODE		INTEGER;
	DECLARE V_RETURN_MSG		VARCHAR(10000);
	
	
    DECLARE EXIT HANDLER FOR SQLEXCEPTION 
	BEGIN 		
		
		SET O_RETURN_CODE = SQLCODE;
		GET DIAGNOSTICS EXCEPTION 1 O_RETURN_MSG = MESSAGE_TEXT; 				
		
	END;


	set V_DATABASE_NAME = I_DATABASE_NAME;
	set V_TABLE_NAME = I_TABLE_NAME;
	
	
	MAINLOOP:
	BEGIN
		FOR PROCESS_KEY_COLUMNS AS C1 CURSOR FOR
			
			SEL Y.KEY_COL 
			FROM (				
				
				SELECT TRIM(KEY_COLUMN) as KEY_COL
				FROM GPROD1_ETL.V_TRANSFORM_KEYCOL
				where lyr_db = V_DATABASE_NAME
				and TABLE_Name = V_TABLE_NAME
				
				UNION
				
				SELECT TRIM(X.COL) KEY_COL
				FROM table (
				STRTOK_SPLIT_TO_TABLE (	1, 
										(
											SEL  I_MORE_COLUMNS  COL
									),	
									',')
							RETURNS (OUTKEY INTEGER,  TOKEN_NUM INTEGER, COL VARCHAR(300))
							) AS X 
				WHERE I_MORE_COLUMNS IS not NULL
				
				UNION 
				
				SEL TRIM(ColumnName)  AS KEY_COL
				FROM DBC.COLUMNSV 
				WHERE DatabaseName  = V_DATABASE_NAME
				AND TableName = V_TABLE_NAME
				AND Nullable  = 'N'
				
			
		) Y WHERE Y.KEY_COL IS NOT NULL
		DO
			CALL GPROD1_ETL.COLUMN_IS_NULL(V_DATABASE_NAME, V_TABLE_NAME, PROCESS_KEY_COLUMNS.KEY_COL, V_IS_NULL, V_RETURN_CODE, V_RETURN_MSG);
			IF V_IS_NULL = 1 OR V_RETURN_CODE <> 0
			THEN
				SET V_NULLABLE_COLUMN = PROCESS_KEY_COLUMNS.KEY_COL;
				LEAVE MAINLOOP;
			END IF;
		END FOR;
	END;
	
	
	IF V_NULLABLE_COLUMN IS NULL
	THEN
		SET O_RETURN_CODE = 0;
		SET O_RETURN_MSG =  'Process Completed Successfully';			
	ELSE	
		SET O_NULLABLE_COLUMN = V_NULLABLE_COLUMN;
		SET O_RETURN_CODE = -1;
		
		IF V_RETURN_CODE = 0 THEN
			SET O_RETURN_MSG =  'Column '||V_NULLABLE_COLUMN||' Cannot Accept Null!';
		ELSE
			SET O_RETURN_MSG =  V_RETURN_MSG;
		END IF;
		
	END IF;
END;


