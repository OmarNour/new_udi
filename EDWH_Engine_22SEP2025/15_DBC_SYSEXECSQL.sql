REPLACE PROCEDURE /*VER.01*/ GPROD1_ETL.DBC_SYSEXECSQL
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
    IN i_sql_script 		CLOB CHARACTER SET UNICODE,
    IN i_QUERY_BAND			VARCHAR(64000),
    OUT o_RETURN_CODE    	INTEGER,
    OUT o_RETURN_MSG  		VARCHAR(10000),
    OUT o_Rows_Count 		float
    )
    BEGIN
        	
           
        DECLARE EXIT HANDLER FOR SQLEXCEPTION
        BEGIN 		
            set o_Rows_Count = 0;	
            SET o_RETURN_CODE = SQLCODE;
            GET DIAGNOSTICS EXCEPTION 1 o_RETURN_MSG = MESSAGE_TEXT;
        END;
        BEGIN TRANSACTION;
        SET QUERY_BAND = i_QUERY_BAND
        FOR TRANSACTION;	
        CALL DBC.SYSEXECSQL(i_sql_script);
        SET o_Rows_Count = ACTIVITY_COUNT;
        END TRANSACTION;	
        	
        
        SET o_RETURN_CODE =  0;
        SET o_RETURN_MSG =  'Process Completed Successfully';
    END;