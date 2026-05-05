REPLACE PROCEDURE GPROD1_ETL.COLUMN_IS_NULL
/*
###################################################################################################################################################################
------------------------------------------------------------------------------------------------------------------------------------------------------------------
#  Version History
------------------------------------------------------------------------------------------------------------------------------------------------------------------
#  Version 		| Date        			| Author		| COMMENT
#  1.0        	| 28 December, 2021 	| Omar Nour		| Initial version
------------------------------------------------------------------------------------------------------------------------------------------------------------------
#  Examples:
CALL GPROD1_ETL.COLUMN_IS_NULL('GDEV1V_base', 'PRTY', 'UPDATE_PROCESS_NAME', IS_NULL, RETURN_CODE, RETURN_MSG);
CALL GPROD1_ETL.COLUMN_IS_NULL('GDEV1V_base', 'PRTY', 'PROCESS_NAME', IS_NULL, RETURN_CODE, RETURN_MSG);
###################################################################################################################################################################
*/

(	IN	I_DATABASE_NAME		VARCHAR(300) ,
	IN	I_TABLE_NAME		VARCHAR(300) ,
	IN	I_COLUMN_NAME		VARCHAR(300) ,
	OUT	O_IS_NULL			INTEGER,
	OUT O_RETURN_CODE		INTEGER,
	OUT O_RETURN_MSG		VARCHAR(10000)
	
)

BEGIN
	
	DECLARE	V_DATABASE_NAME	VARCHAR(300)	DEFAULT '';
	DECLARE V_SCRIPT 		VARCHAR(10000)	DEFAULT '';
	DECLARE V_CURSOR_RESULT	INTEGER;
	DECLARE V_CURSOR		CURSOR FOR V_STATEMENT;
	
	
    DECLARE EXIT HANDLER FOR SQLEXCEPTION 
	BEGIN 		
		
		SET O_RETURN_CODE = SQLCODE;
		GET DIAGNOSTICS EXCEPTION 1 O_RETURN_MSG = MESSAGE_TEXT; 				
		
	END;
	IF COALESCE(TRIM(I_DATABASE_NAME),'') <> ''
	THEN
		SET V_DATABASE_NAME = I_DATABASE_NAME||'.';
	END IF;
	
	SET V_SCRIPT = 'sel 1 where exists (sel 1 from '||V_DATABASE_NAME||I_TABLE_NAME||' where '||I_COLUMN_NAME||' is null)';
		
	PREPARE V_STATEMENT FROM V_SCRIPT;
	OPEN V_CURSOR;
	FETCH V_CURSOR INTO V_CURSOR_RESULT;
	CLOSE V_CURSOR;
	
	IF V_CURSOR_RESULT IS NULL
	THEN
		SET O_IS_NULL =  0;
	ELSE
		SET O_IS_NULL =  1;
	END IF;
	
	SET O_RETURN_CODE = 0;
	SET O_RETURN_MSG =  'Process Completed Successfully';	
END;