---CSO_CHECK_Test_Case_1---
SEL alias.PRTY_ID from GPROD1T_BASE.PRTY alias left join GPROD1V_UTLFW.BKEY_1_PRTY on alias.PRTY_ID= GPROD1V_UTLFW.BKEY_1_PRTY.EDW_KEY left join STG_LAYER.CSO_NEW_PERSON B on trim(cast B.national_id) as varchar(100))) = GPROD1V_UTLFW.BKEY_1_PRTY.source_key where trim(cast B.national_id) as varchar(100))) is null AND GPROD1V_UTLFW.BKEY_1_PRTY.DOMAIN_ID=1 AND alias.PROCESS_NAME LIKE '%VIRUS_C%';


---CSO_CHECK_Test_Case_2---
SEL alias.INDIV_PRTY_ID from GPROD1T_BASE.INDIV_NAME alias left join GPROD1V_UTLFW.BKEY_1_PRTY on alias.INDIV_PRTY_ID= GPROD1V_UTLFW.BKEY_1_PRTY.EDW_KEY left join STG_LAYER.CSO_NEW_PERSON B on trim(cast B.national_id) as varchar(100))) = GPROD1V_UTLFW.BKEY_1_PRTY.source_key where trim(cast B.national_id) as varchar(100))) is null AND GPROD1V_UTLFW.BKEY_1_PRTY.DOMAIN_ID=1 AND alias.PROCESS_NAME LIKE '%VIRUS_C%';


---CSO_CHECK_Test_Case_3---
SEL alias.PRTY_ID from GPROD1T_BASE.PRTY_LOCTR alias left join GPROD1V_UTLFW.BKEY_1_PRTY on alias.PRTY_ID= GPROD1V_UTLFW.BKEY_1_PRTY.EDW_KEY left join STG_LAYER.CSO_NEW_PERSON B on trim(cast B.national_id) as varchar(100))) = GPROD1V_UTLFW.BKEY_1_PRTY.source_key where trim(cast B.national_id) as varchar(100))) is null AND GPROD1V_UTLFW.BKEY_1_PRTY.DOMAIN_ID=1 AND alias.PROCESS_NAME LIKE '%VIRUS_C%';


---CSO_CHECK_Test_Case_4---
SEL alias.PRTY_ID from GPROD1T_BASE.PRTY_RLTD alias left join GPROD1V_UTLFW.BKEY_1_PRTY on alias.PRTY_ID= GPROD1V_UTLFW.BKEY_1_PRTY.EDW_KEY left join STG_LAYER.CSO_NEW_PERSON B on trim(cast B.national_id) as varchar(100))) = GPROD1V_UTLFW.BKEY_1_PRTY.source_key where trim(cast B.national_id) as varchar(100))) is null AND GPROD1V_UTLFW.BKEY_1_PRTY.DOMAIN_ID=1 AND alias.PROCESS_NAME LIKE '%VIRUS_C%';


---CSO_CHECK_Test_Case_5---
SEL alias.INDIV_PRTY_ID from GPROD1T_BASE.INDIV_DISEASE_STS alias left join GPROD1V_UTLFW.BKEY_1_PRTY on alias.INDIV_PRTY_ID= GPROD1V_UTLFW.BKEY_1_PRTY.EDW_KEY left join STG_LAYER.CSO_NEW_PERSON B on trim(cast B.national_id) as varchar(100))) = GPROD1V_UTLFW.BKEY_1_PRTY.source_key where trim(cast B.national_id) as varchar(100))) is null AND GPROD1V_UTLFW.BKEY_1_PRTY.DOMAIN_ID=1 AND alias.PROCESS_NAME LIKE '%VIRUS_C%';


---CSO_CHECK_Test_Case_6---
SEL alias.INDIV_PRTY_ID from GPROD1T_BASE.INDIV_MEDCL_MSR alias left join GPROD1V_UTLFW.BKEY_1_PRTY on alias.INDIV_PRTY_ID= GPROD1V_UTLFW.BKEY_1_PRTY.EDW_KEY left join STG_LAYER.CSO_NEW_PERSON B on trim(cast B.national_id) as varchar(100))) = GPROD1V_UTLFW.BKEY_1_PRTY.source_key where trim(cast B.national_id) as varchar(100))) is null AND GPROD1V_UTLFW.BKEY_1_PRTY.DOMAIN_ID=1 AND alias.PROCESS_NAME LIKE '%VIRUS_C%';


---CSO_CHECK_Test_Case_7---
SEL alias.PRTY_ID from GPROD1T_BASE.PRTY_DEMOG alias left join GPROD1V_UTLFW.BKEY_1_PRTY on alias.PRTY_ID= GPROD1V_UTLFW.BKEY_1_PRTY.EDW_KEY left join STG_LAYER.CSO_NEW_PERSON B on trim(cast B.national_id) as varchar(100))) = GPROD1V_UTLFW.BKEY_1_PRTY.source_key where trim(cast B.national_id) as varchar(100))) is null AND GPROD1V_UTLFW.BKEY_1_PRTY.DOMAIN_ID=1 AND alias.PROCESS_NAME LIKE '%VIRUS_C%';


