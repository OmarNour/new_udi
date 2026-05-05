INSERT INTO GPROD1_ETL.LAYERS  
		(layer_name, lyr_db, src_db, layer_Level)  
VALUES	('SRC', 'GPROD1V_STG_ONLINE', 'STG_ONLINE', 0) ;

INSERT INTO GPROD1_ETL.LAYERS  
		(layer_name, lyr_db, src_db, layer_Level)  
VALUES	('STG', 'GPROD1T_STG', 'GPROD1V_STG_ONLINE', 1) ;

INSERT INTO GPROD1_ETL.LAYERS  
		(layer_name, lyr_db, src_db, layer_Level)  
VALUES	('BKEY', 'GPROD1T_UTLFW', 'GPROD1V_INP', 2) ;

INSERT INTO GPROD1_ETL.LAYERS  
		(layer_name, lyr_db, src_db, layer_Level)  
VALUES	('BMAP', 'GPROD1T_UTLFW', 'GPROD1V_INP', 2) ;

INSERT INTO GPROD1_ETL.LAYERS  
		(layer_name, lyr_db, src_db, layer_Level)  
VALUES	('SRCI', 'GPROD1T_SRCI', 'GPROD1V_SRCI', 3) ;

INSERT INTO GPROD1_ETL.LAYERS  
		(layer_name, lyr_db, src_db, layer_Level)  
VALUES	('CORE', 'GPROD1T_BASE', 'GPROD1V_INP', 4) ;