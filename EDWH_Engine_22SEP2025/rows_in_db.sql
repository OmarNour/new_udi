REPLACE PROCEDURE GPROD1_ETL.COUNT_ROWS_IN_DB
(
	IN i_DB_NAME VARCHAR (200),
	IN i_table_NAME VARCHAR (200),
	IN i_out_db	VARCHAR (200),
	in i_out_table	VARCHAR (200),
	out O_CNT 			FLOAT,
	out O_RETURN_CODE 	INTEGER,
	out O_RETURN_MSG 	VARCHAR (5000)
)

/*
call GPROD1_ETL.COUNT_ROWS_IN_DB
(
	'GPROD1_ETL'--IN i_DB_NAME VARCHAR (200),
	,''--IN i_table_NAME VARCHAR (200),
	,'GPROD1_ETL'--IN i_out_db	VARCHAR (200),
	,'Marawan_el3aaaq'--in i_out_table	VARCHAR (200),
	,x--out O_CNT 			FLOAT,
	,y--out O_RETURN_CODE 	INTEGER,
	,z--out O_RETURN_MSG 	VARCHAR (5000)
)

*/

BEGIN
	DECLARE C1 CURSOR FOR S1;
	begin
		declare v_out_table_found integer default 0;
		declare v_cnt_tbl, v_total_cnt float default 0;
		DECLARE v_cnt_tbl_stmt 	VARCHAR(5000);
		
		DECLARE EXIT HANDLER FOR SQLEXCEPTION
		BEGIN
			SET O_CNT = 0;
			SET O_RETURN_CODE = SQLCODE;
			GET DIAGNOSTICS EXCEPTION 1 O_RETURN_MSG = MESSAGE_TEXT;
		END;
		
		MAINBLOCK:
		begin
			
			if coalesce(i_DB_NAME,'') = '' or coalesce(i_out_db,'') = '' or coalesce(i_out_table,'') = ''
			then
				set O_RETURN_CODE = -1;
				set O_RETURN_MSG = 'missing input parameters';
				leave MAINBLOCK;
			end if;
			
			select count(1)
			from dbc.tablesv
			where tableKind = 'T'
			and databasename = i_out_db	
			and tablename = i_out_table
			into v_out_table_found;
			
			if v_out_table_found > 0
			then
				CALL DBC.SYSEXECSQL('delete from '||i_out_db||'.'||i_out_table||';');
			else
				CALL DBC.SYSEXECSQL('
									CREATE MULTISET TABLE '||i_out_db||'.'||i_out_table||' ,no FALLBACK ,
									 NO BEFORE JOURNAL,
									 NO AFTER JOURNAL,
									 CHECKSUM = DEFAULT,
									 DEFAULT MERGEBLOCKRATIO,
									 MAP = TD_MAP1
									 (
									  db_name VARCHAR(130) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
									  TABLE_NAME VARCHAR(130) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
									  rows_count float NOT NULL
									 )
									PRIMARY INDEX ( db_name, TABLE_NAME );'
									);
			end if;
				
			FOR	loop1 AS loopx CURSOR FOR 
				select tablename
				from dbc.tablesv
				where tableKind = 'T'
				and databasename = i_DB_NAME	
				and (tablename = i_table_NAME or coalesce(i_table_NAME,'') = '')
			DO
				SET v_cnt_tbl_stmt = 'SEL COUNT (1) (float) FROM '||i_DB_NAME||'.'||loop1.tablename||';' ;
				PREPARE S1 FROM v_cnt_tbl_stmt;
				OPEN C1;
				FETCH C1 INTO v_cnt_tbl;
				CLOSE C1;
				
				set v_total_cnt = v_total_cnt + v_cnt_tbl;
				
				CALL DBC.SYSEXECSQL('insert into '||i_out_db||'.'||i_out_table||' ('''||i_DB_NAME||''','''||loop1.tablename||''', '||v_cnt_tbl||');');
			end for;
		end /*MAINBLOCK*/;
		SET O_CNT = v_total_cnt;
		set O_RETURN_CODE = 0;
		set O_RETURN_MSG = 'Done';
	end;
END;