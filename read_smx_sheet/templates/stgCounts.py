from read_smx_sheet.app_Lib import functions as funcs
from read_smx_sheet.Logging_Decorator import Logging_decorator
from read_smx_sheet.parameters import parameters as pm


@Logging_decorator
def stgCounts(cf, source_output_path, System, STG_tables):
    file_name = funcs.get_file_name(__file__)
    f = funcs.WriteFile(source_output_path, file_name, "sql")
    smx_path = cf.smx_path
    template_path = cf.templates_path + "/" + pm.compareSTGcounts_template_filename
    template_string = ""
    try:
        REJ_TABLE_NAME = System['Rejection Table Name']
    except:
        REJ_TABLE_NAME = ''
    try:
        REJ_TABLE_RULE = System['Rejection Table Rules']
    except:
        REJ_TABLE_RULE = ''
    try:
        source_DB = System['Source DB']
    except:
        source_DB = ''

    try:
        template_file = open(template_path, "r")
    except:
        template_file = open(smx_path, "r")

    for i in template_file.readlines():
        if i != "":
            template_string = template_string + i
    stg_table_names = funcs.get_stg_tables(STG_tables)
    for stg_tables_df_index, stg_tables_df_row in stg_table_names[(stg_table_names['Table name'] != REJ_TABLE_NAME) & (stg_table_names['Table name'] != REJ_TABLE_RULE)].iterrows():
        TABLE_NAME = stg_tables_df_row['Table name']
        output_script = template_string.format(TABLE_NAME=TABLE_NAME,
                                               STG_DATABASE=cf.T_STG,
                                               WRK_DATABASE=cf.t_WRK,
                                               source_DB=source_DB
                                               )
        seperation_line = '--------------------------------------------------------------------------------------------------------------------------------------------------------------------'
        output_script = output_script.upper() + '\n' + seperation_line + '\n'  + seperation_line + '\n'
        f.write(output_script.replace('Â', ' '))
    f.close()
