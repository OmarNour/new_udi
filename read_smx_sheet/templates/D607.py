from read_smx_sheet.app_Lib import functions as funcs
from read_smx_sheet.app_Lib import TransformDDL
from read_smx_sheet.Logging_Decorator import Logging_decorator


# def d607(source_output_path, Core_tables):
#     file_name = funcs.get_file_name(__file__)
#     f = open(source_output_path + "/" + file_name + ".sql", "w+", encoding="utf-8")
#
#     lkp_tables = TransformDDL.get_lkp_tbls(Core_tables)
#     lkp_tables_names=TransformDDL.get_lkp_tbls_names(Core_tables)
#
#     for tbl_name in lkp_tables_names:
#         lkp_ddl = ''
#         lkp_tbl_header = 'CREATE SET TABLE ' + pm.core_view + '.' + tbl_name + ', FALLBACK (\n'
#         for lkp_tbl_indx, lkp_tbl_row in lkp_tables[(lkp_tables['Table name'] == tbl_name)].iterrows():
#             lkp_ddl += lkp_tbl_row['Column name'] + ' ' + lkp_tbl_row['Data type'] + ' '
#             if (lkp_tbl_row['Data type'].find('VARCHAR') != -1):
#                 lkp_ddl += 'CHARACTER SET UNICODE NOT CASESPECIFIC' + ' '
#             if (lkp_tbl_row['Mandatory'] == 'Y'):
#                 lkp_ddl += 'NOT NULL '
#
#             lkp_ddl += ',\n'
#         # lkp_ddl = lkp_ddl[0:len(lkp_ddl) - 2]
#         core_tech_cols=	'Start_Ts	TIMESTAMP(6) WITH TIME ZONE \n'+',End_Ts	TIMESTAMP(6) WITH TIME ZONE \n'
#         core_tech_cols+=",Start_Date	DATE FORMAT 'YYYY-MM-DD' \n"+",End_Date	DATE FORMAT 'YYYY-MM-DD' \n"
#         core_tech_cols+=',Record_Deleted_Flag	BYTEINT \n'+',Ctl_Id	SMALLINT COMPRESS(997) \n'
#         core_tech_cols+=',Process_Name	VARCHAR(128)\n'+',Process_Id	INTEGER \n'
#         core_tech_cols+= ',Update_Process_Name	VARCHAR(128)\n'+',Update_Process_Id	INTEGER \n'
#         lkp_tbl_pk = ') UNIQUE PRIMARY INDEX (' + TransformDDL.get_trgt_pk(lkp_tables, tbl_name) + '); \n  \n'
#         lkp_tbl_ddl = lkp_tbl_header + lkp_ddl +core_tech_cols+"\n"+ lkp_tbl_pk
#         f.write(lkp_tbl_ddl)
#     f.close()

@Logging_decorator
def d607(cf, source_output_path, Core_tables, BMAP_values):
    file_name = funcs.get_file_name(__file__)
    f = funcs.WriteFile(source_output_path, file_name, "sql")
    core_tables_list = TransformDDL.get_core_tables_list(Core_tables)
    code_set_names = TransformDDL.get_code_set_names(BMAP_values)
    Data_mover_flag = cf.Data_mover_flag
    if Data_mover_flag == 1:
        Run_date_column = ", RUN_DATE TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP\n"
        partition_statement = "PARTITION BY RANGE_N(RUN_DATE BETWEEN TIMESTAMP '2020-03-03 00:00:00.000000+00:00' AND TIMESTAMP '2100-03-03 00:00:00.000000+00:00' EACH INTERVAL'1'DAY)\n"
    else:
        Run_date_column = ""
        partition_statement = ""

    for code_set in code_set_names:
        lkp_ddl = ''
        lkp_tbl_header = 'CREATE SET TABLE ' + cf.core_table + '.' + code_set + ', FALLBACK (\n'

        if code_set not in core_tables_list:
            error_txt = "--Error: Table " + code_set + " Not Found in Core tables. Can't generate its ddl. \n"
            f.write(error_txt)

        for lkp_tbl_indx, lkp_tbl_row in Core_tables[(Core_tables['Table name'] == code_set)].iterrows():
            lkp_ddl += lkp_tbl_row['Column name'] + ' ' + lkp_tbl_row['Data type'] + ' '
            if lkp_tbl_row['Data type'].find('VARCHAR') != -1:
                lkp_ddl += 'CHARACTER SET UNICODE NOT CASESPECIFIC' + ' '
            if lkp_tbl_row['Mandatory'] == 'Y':
                lkp_ddl += 'NOT NULL '

            lkp_ddl += ',\n'

        core_tech_cols = 'Start_Ts	TIMESTAMP(6) WITH TIME ZONE \n' + ',End_Ts	TIMESTAMP(6) WITH TIME ZONE \n'
        core_tech_cols += ",Start_Date	DATE FORMAT 'YYYY-MM-DD' \n" + ",End_Date	DATE FORMAT 'YYYY-MM-DD' \n"
        core_tech_cols += ',Record_Deleted_Flag	BYTEINT \n' + ',Ctl_Id	SMALLINT COMPRESS(997) \n'
        core_tech_cols += ',Process_Name	VARCHAR(128)\n' + ',Process_Id	INTEGER \n'
        core_tech_cols += ',Update_Process_Name	VARCHAR(128)\n' + ',Update_Process_Id	INTEGER \n' + Run_date_column
        lkp_tbl_pk = ') UNIQUE PRIMARY INDEX (' + TransformDDL.get_trgt_pk(Core_tables, code_set) + ')\n' + partition_statement + '; \n  \n'
        lkp_tbl_ddl = lkp_tbl_header + lkp_ddl + core_tech_cols + "\n" + lkp_tbl_pk
        f.write(lkp_tbl_ddl)
    f.close()
