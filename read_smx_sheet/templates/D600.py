from app_Lib import functions as funcs
from app_Lib import TransformDDL
import traceback
from Logging_Decorator import Logging_decorator


@Logging_decorator
def d600(cf, source_output_path, Table_mapping, Core_tables):
    file_name = funcs.get_file_name(__file__)
    f = funcs.WriteFile(source_output_path, file_name, "sql")
    core_tables_list = TransformDDL.get_src_core_tbls(Table_mapping)
    Data_mover_flag = cf.Data_mover_flag
    if Data_mover_flag == 1:
        Run_date_column = ", RUN_DATE TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP\n"
        partition_statement = "PARTITION BY RANGE_N(RUN_DATE BETWEEN TIMESTAMP '2020-03-03 00:00:00.000000+00:00' AND TIMESTAMP '2100-03-03 00:00:00.000000+00:00' EACH INTERVAL'1'DAY)\n"
    else:
        Run_date_column = ""
        partition_statement = ""

    for tbl_name in core_tables_list:
        col_ddl = ''
        core_tbl_header = 'CREATE SET TABLE ' + cf.core_table + '.' + tbl_name + ', FALLBACK (\n'

        for core_tbl_index, core_tbl_row in Core_tables[(Core_tables['Table name'] == tbl_name)].iterrows():
            col_ddl += core_tbl_row['Column name'] + ' ' + core_tbl_row['Data type'] + ' '
            if (core_tbl_row['Data type'].find('VARCHAR') != -1):
                col_ddl += 'CHARACTER SET UNICODE NOT CASESPECIFIC' + ' '
            if (core_tbl_row['Mandatory'] == 'Y'):
                col_ddl += 'NOT NULL '
            col_ddl += '\n ,'
        #  col_ddl= col_ddl[0:len(col_ddl)-1]
        core_tech_cols = 'Start_Ts	TIMESTAMP(6) WITH TIME ZONE \n' + ',End_Ts	TIMESTAMP(6) WITH TIME ZONE \n'
        core_tech_cols += ",Start_Date	DATE FORMAT 'YYYY-MM-DD' \n" + ",End_Date	DATE FORMAT 'YYYY-MM-DD' \n"
        core_tech_cols += ',Record_Deleted_Flag	BYTEINT \n' + ',Ctl_Id	SMALLINT COMPRESS(997) \n'
        core_tech_cols += ',Process_Name	VARCHAR(128)\n' + ',Process_Id	INTEGER \n'
        core_tech_cols += ',Update_Process_Name	VARCHAR(128)\n' + ',Update_Process_Id	INTEGER \n' + Run_date_column
        core_tbl_pk = ') UNIQUE PRIMARY INDEX (' + TransformDDL.get_trgt_pk(Core_tables, tbl_name) + ')\n'+partition_statement+'; \n  \n'
        core_tbl_ddl = core_tbl_header + col_ddl + core_tech_cols + core_tbl_pk
        f.write(core_tbl_ddl)
    f.close()
