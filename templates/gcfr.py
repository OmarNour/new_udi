import parameters.parameters as pm
import app_Lib.functions as funcs


def gcfr(output_path):
    file_name = funcs.get_file_name(__file__)
    f = open(output_path + "/" + "000_run_first_" + file_name.upper() + ".sql", "w+")
    system_name = funcs.single_quotes(pm.gcfr_system_name)
    stream_name = funcs.single_quotes(pm.gcfr_stream_name)
    register_system = "exec " + pm.M_GCFR + ".GCFR_Register_System(" + str(pm.gcfr_ctl_Id) + ", " + system_name + ", '',  " + system_name + ");"
    register_stream = "call " + pm.P_UT + ".GCFR_UT_Register_Stream(" + str(pm.gcfr_stream_key) + ", 1, " + stream_name + ", cast('2019-01-01' as date));"

    delete_parameters = "delete from " + str(pm.GCFR_t) + ".PARAMETERS where PARAMETER_ID in (11, 7, 10);\n"

    insert_into_parameters = "insert into " + str(pm.GCFR_t) + ".PARAMETERS values "
    insert_into_parameters = insert_into_parameters + "(11, 'LRD T DB', " + funcs.single_quotes(pm.SI_DB) + ");\n"

    insert_into_parameters = insert_into_parameters + "insert into " + str(pm.GCFR_t) + ".PARAMETERS values "
    insert_into_parameters = insert_into_parameters + "(7, 'INPUT V DB', " + funcs.single_quotes(pm.INPUT_VIEW_DB) + ");\n"

    insert_into_parameters = insert_into_parameters + "insert into " + str(pm.GCFR_t) + ".PARAMETERS values "
    insert_into_parameters = insert_into_parameters + "(10, 'BASE T DB', " + funcs.single_quotes(pm.core_table) + ");\n"

    # "11	LRD T DB	GDEV1T_SRCI"
    # "7	INPUT V DB	GDEV1V_INP"
    # "10	BASE T DB	GDEV1T_BASE"

    f.write(register_system+"\n")
    f.write(register_stream + "\n\n")
    f.write(delete_parameters + "\n")
    f.write(insert_into_parameters + "\n")
    f.close()
