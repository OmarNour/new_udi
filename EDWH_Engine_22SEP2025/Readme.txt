1- connect using DBC user
2- run the following scripts in the same order:
	01_CREATE_SCHEMAS.sql
	02_USERS.SQL
	03_GRANTS.sql
4- connect using power_user user, use the same password in script 02_USERS.SQL
5- run the following scripts in the same order:
	04_TABLES.sql
	05_DATA.SQL
	06_VIEWS.SQL
	10_GENERATE_RUN_ID.sql
	15_DBC_SYSEXECSQL.sql
	20_DBC_SYSEXECSQL_WITH_LOG.sql
	25_COLUMN_IS_NULL.sql
	30_CHECK_MANDATORY_COLUMNS.sql
	35_COUNT_ROWS.sql
	50_STG_PROCESS_LOADING.sql
	51_STG_LOADING.sql
	60_BKEY_PROCESS_LOADING.SQL
	61_BKEY_LOADING.sql
	70_SRCI_PROCESS_LOADING.sql
	71_SRCI_LOADING.sql
	80_EDW_PROCESS_LOADING.SQL
	81_EDW_LOADING.sql