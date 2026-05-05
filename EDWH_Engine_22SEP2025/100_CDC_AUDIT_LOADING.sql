SHOW  PROCEDURE /*VER. 01*/ GPROD1_ETL.CDC_AUDIT_LOADING;

REPLACE PROCEDURE /*VER. 01*/ GPROD1_ETL.CDC_AUDIT_LOADING
    /*
    ###################################################################################################################################################################
    ------------------------------------------------------------------------------------------------------------------------------------------------------------------
    # Version History
    ------------------------------------------------------------------------------------------------------------------------------------------------------------------
    # Version | Date | Author | Comments
    # 1.0 | 11 October, 2023 | Omar Hosny | Fisrt Cut.
    ------------------------------------------------------------------------------------------------------------------------------------------------------------------
    # Examples:
    #
    ###################################################################################################################################################################
    */
(
    IN I_RUN_ID BIGINT,
    OUT o_RETURN_CODE INTEGER,
    OUT o_RETURN_MSG VARCHAR(10000),
    OUT o_Rows_Count float,
    OUT o_run_id BIGINT
)

/*
###################################################################################################################################################################
------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Version History
------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Version | Date | Author | COMMENT
# 1.0 | 12 SEP, 2023 | Omar HOSNY | Initial
# 2.0 | 4 OCT, 2023 | Omar HOSNY | UPSERT INSTEAD OF INSERT EXCEPT FOR PROCESSED COLUMN WHICH IS UPDATED BY THE ORCHESTRATOR
------------------------------------------------------------------------------------------------------------------------------------------------------------------

###################################################################################################################################################################
*/
------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Definition Of Procedure: COPIES THE CHANGES IN THE IBM AUDIT TABLE TO THE CDC AUDIT TABLE IN THE NEW ENGINE
------------------------------------------------------------------------------------------------------------------------------------------------------------------

BEGIN
    
    -- VARIABLES TO CHANGE WHEN CHANGING ENVIRONMENT
    DECLARE V_SOURCE_DB VARCHAR(20) DEFAULT 'STG_ONLINE';
    DECLARE V_ETL_DB VARCHAR(20) DEFAULT 'GPROD1_ETL';
    DECLARE V_IBM_AUDIT_TABLE VARCHAR(20) DEFAULT 'IBM_AUDIT_TABLE';
    DECLARE V_CDC_AUDIT VARCHAR(20) DEFAULT 'CDC_AUDIT';
    
    -- HELPFUL VARIABLES
    DECLARE V_CREATE_QUERY VARCHAR(5000);
    DECLARE V_UPDATE_QUERY VARCHAR(5000);
    DECLARE V_INSERT_QUERY VARCHAR(5000);
    DECLARE v_run_id, v_dbc_run_id BIGINT;
    DECLARE V_SQL_SCRIPT_ID INTEGER;
    DECLARE V_DBC_RETURN_CODE INTEGER;
    DECLARE V_DBC_RETURN_MSG VARCHAR(1000);
    DECLARE V_DBC_ROWS_COUNT FLOAT DEFAULT 0;
    DECLARE V_TMP_TABLE VARCHAR(25);
    
    -- SET RUN ID
    IF i_run_id IS NULL THEN
            select GPROD1_ETL.generate_run_id()
            into v_run_id;
        ELSE
            SET v_run_id= i_run_id;
        END IF;
    
    
    -- SET TEMP TABLE
    SET V_TMP_TABLE = V_CDC_AUDIT || '_TMP';
    
        -- 1ST QUERY
    SET V_CREATE_QUERY =
    'CREATE MULTISET VOLATILE TABLE ' || V_TMP_TABLE || ' AS (
    SELECT
            SOURCE_NAME
            ,LOAD_ID
            ,BATCH_ID
            ,TABLE_NAME
            ,RECORDS_COUNT
            ,VALID_BATCH
            ,PROCESSED
    FROM
        (
            SELECT DISTINCT
            CASE
            WHEN MIC_GANAME =' || '''AlternativeFamiliesDB''' || ' THEN ' || '''AlternativeFamily''' ||
            ' WHEN MIC_GANAME = ' || '''CSODB''' || ' THEN ' || '''CSO''' ||
            ' WHEN MIC_GANAME = ' || '''DXCDB_Inbound''' || ' THEN ' || '''UHI_DXC''' ||
            ' WHEN MIC_GANAME = ' || '''E-finance''' || ' THEN ' || '''E-Finance_Mortabat''' ||
            ' WHEN MIC_GANAME = ' ||'''E-Finance''' || ' THEN ' || '''NEW_MORTABAT''' ||
            ' WHEN MIC_GANAME = ' || '''ElectricityDB''' || ' THEN ' || '''Electricity_New''' ||
            ' WHEN MIC_GANAME = ' || '''Fund_TreatmentDB''' || ' THEN ' || '''FUNDED_TREATMENT''' ||
            ' WHEN MIC_GANAME = ' || '''GAS_SubscriberDB''' || ' THEN ' || '''Gas''' ||
            ' WHEN MIC_GANAME = ' || '''GASVIPDB''' || ' THEN ' || '''GasVIP''' ||
            ' WHEN MIC_GANAME = ' || '''Health''' || ' THEN ' || '''BIRTH_DEATH''' ||
            ' WHEN MIC_GANAME = ' || '''Health''' || ' THEN ' || '''HEALTH''' ||
            ' WHEN MIC_GANAME = ' || '''HeyazaDB''' || ' THEN ' || '''Heyaza_Agriculture''' ||
            ' WHEN MIC_GANAME = ' || '''InsurancePensionGDB''' || ' THEN ' || '''Insurance_Pension''' ||
            ' WHEN MIC_GANAME = ' || '''InsurancePensionPDB''' || ' THEN ' || '''Insurance_Pension''' ||
            ' WHEN MIC_GANAME = ' || '''MOSSDB''' || ' THEN ' || '''E-FINANCE_TK_TRANSACTION''' ||
            ' WHEN MIC_GANAME = ' || '''MOSSDB''' || ' THEN ' || '''TAKAFUL_KARAMA''' ||
            ' WHEN MIC_GANAME = ' || '''RealStateinvstDB''' || ' THEN ' || '''Tamweel''' ||
            ' WHEN MIC_GANAME = ' || '''SupplyDB''' || ' THEN ' || '''Tamwin''' ||
            ' WHEN MIC_GANAME = ' || '''Telecom_Consolidated''' || ' THEN ' || '''Telecom''' ||
            ' WHEN MIC_GANAME = ' || '''TrafficDB''' || ' THEN ' || '''MROR''' ||
            ' WHEN MIC_GANAME = ' || '''VACCINE''' || ' THEN ' || '''VACCINE''' ||
            ' WHEN MIC_GANAME = ' || '''WaitingListDB''' || ' THEN ' || '''Waiting_Lists''' ||
            ' WHEN MIC_GANAME = ' || '''InvestmentDB''' || ' THEN ' || '''Investment''' ||
            ' WHEN MIC_GANAME = ' || '''TakafulKaramaDB''' || ' THEN ' || '''TADAMON''' ||
            ' WHEN MIC_GANAME = ' || '''SMBDB''' || ' THEN ' || '''SpecialNeeds''' ||
            ' WHEN MIC_GANAME = ' || '''MarriedDivorcedDB''' || ' THEN ' || '''NEYABA''' ||
            ' WHEN MIC_GANAME = ' || '''HCVDB''' || ' THEN ' || '''VIRUS_C''' ||
            ' WHEN MIC_GANAME = ' || '''IDADB''' || ' THEN ' || '''IDA''' ||
            ' WHEN MIC_GANAME = ' || '''CommercialRegisterDB''' || ' THEN ' || '''CRA''' ||
            ' WHEN MIC_GANAME = ' || '''TAXDB''' || ' THEN ' || '''Tax_Authority''' ||
            ' WHEN MIC_GANAME = ' || '''GamarekNafezaDB''' || ' THEN ' || '''NAFEZA''' ||
            ' WHEN MIC_GANAME = ' || '''GPSDB''' || ' THEN ' || '''GOVERNMENTAL_PAYMENTS''' ||
			' WHEN MIC_GANAME = ' || '''VATDB''' || ' THEN ' || '''VAT''' ||
        ' END SOURCE_NAME
        , MIC_EXECUTIONGUID LOAD_ID
        , MIC_RUNID BATCH_ID
        , TERA_TABLE_NAME TABLE_NAME
        , TERA_INPUT_RECORD_COUNT RECORDS_COUNT
        , CASE WHEN TERA_JOB_END_TIME IS NOT NULL AND DB2_TO_TERA_STATUS like ' || '''%success%''' || ' THEN 1 ELSE 0 END VALID_BATCH
        , 0 PROCESSED
        FROM ' || V_SOURCE_DB || '.' || V_IBM_AUDIT_TABLE ||
        ' WHERE COALESCE(SOURCE_NAME,'''') <> ''''
        AND COALESCE(TABLE_NAME,'''') <> ''''
        AND COALESCE(LOAD_ID,'''') <> ''''
        ) src
    ) WITH DATA AND STATS PRIMARY INDEX (SOURCE_NAME ,LOAD_ID ,BATCH_ID ,TABLE_NAME) on commit preserve rows;';
        
    -- SECOND QUERY
    SET V_UPDATE_QUERY =
    'update tgt
    from ' || V_ETL_DB || '.' || V_CDC_AUDIT || ' tgt, ' || V_TMP_TABLE || ' src
    set
         RECORDS_COUNT = SRC.RECORDS_COUNT,
        VALID_BATCH = SRC.VALID_BATCH,
        UPDATED_AT = CURRENT_TIMESTAMP(6),
        UPDATED_BY = USER
    WHERE
               TGT.SOURCE_NAME = SRC.SOURCE_NAME
    AND TGT.LOAD_ID = SRC.LOAD_ID
    AND TGT.BATCH_ID = SRC.BATCH_ID
    AND TGT.TABLE_NAME = SRC.TABLE_NAME;';
    
    SET V_INSERT_QUERY =
    'INSERT INTO ' || V_ETL_DB || '.' || V_CDC_AUDIT || ' (
        SOURCE_NAME
        ,LOAD_ID
        ,BATCH_ID
        ,TABLE_NAME
        ,RECORDS_COUNT
        ,VALID_BATCH
        ,PROCESSED
    )
    SELECT
        SOURCE_NAME
        ,LOAD_ID
        ,BATCH_ID
        ,TABLE_NAME
        ,RECORDS_COUNT
        ,VALID_BATCH
        ,PROCESSED
    FROM ' || V_TMP_TABLE || ' SRC
    WHERE NOT EXISTS (
                            SELECT 1
                            FROM ' || V_ETL_DB || '.' || V_CDC_AUDIT || ' tgt
                            WHERE tgt.SOURCE_NAME=src.SOURCE_NAME
                            AND tgt.LOAD_ID=src.LOAD_ID
                            AND tgt.BATCH_ID=src.BATCH_ID
                            AND tgt.TABLE_NAME=src.TABLE_NAME
                            );';
    
    -- EXECUTE QUERIES
    CALL GPROD1_ETL.DBC_SYSEXECSQL_WITH_LOG(V_CREATE_QUERY, 1 ,v_run_id ,NULL,NULL,NULL,1/*1 log or 0 don't*/,V_DBC_RETURN_CODE,V_DBC_RETURN_MSG,V_DBC_ROWS_COUNT, v_dbc_run_id);
    CALL GPROD1_ETL.DBC_SYSEXECSQL_WITH_LOG(V_UPDATE_QUERY, 2 ,v_run_id ,NULL,NULL,NULL,1/*1 log or 0 don't*/,V_DBC_RETURN_CODE,V_DBC_RETURN_MSG,V_DBC_ROWS_COUNT, v_dbc_run_id);
    CALL GPROD1_ETL.DBC_SYSEXECSQL_WITH_LOG(V_INSERT_QUERY, 3 ,v_run_id ,NULL,NULL,NULL,1/*1 log or 0 don't*/,V_DBC_RETURN_CODE,V_DBC_RETURN_MSG,V_DBC_ROWS_COUNT, v_dbc_run_id);
    SET o_RETURN_CODE = V_DBC_RETURN_CODE;
    SET o_RETURN_MSG = V_DBC_RETURN_MSG;
    SET o_Rows_Count = V_DBC_ROWS_COUNT;
    SET o_run_id = v_dbc_run_id;
END;