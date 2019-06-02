from read_smx_sheet.app_Lib import functions as funcs
from read_smx_sheet.app_Lib import TransformDDL
import traceback


def d002(cf, source_output_path, Core_tables, Table_mapping):
    file_name = funcs.get_file_name(__file__)
    f = funcs.WriteFile(source_output_path, file_name, "sql")
    try:
        # Core_tables=TransformDDL.get_src_core_tbls(source_name, Core_tables, Table_mapping)
        Table_mappings = Table_mapping
        hist_key_insert_header=""
        history_tbl = cf.GCFR_t+"."+cf.history_tbl
        hist_key_insert_header += "INSERT INTO "+ history_tbl
        hist_key_insert_header += "( TRF_TABLE_NAME,PROCESS_NAME,TABLE_NAME,RECORD_ID,START_DATE_COLUMN,END_DATE_COLUMN,HISTORY_COLUMN, HISTORY_KEY)\n"
        hist_key_insert_header += "VALUES ('"
        tbl_mapping_name = ""
        process_name = ""
        trgt_tbl = ""
        start_date_column = ""
        end_date_column = ""
        history_key = ""
        history_column = ""
        for tbl_mapping_index, table_maping_row in Table_mappings[Table_mappings['Historization algorithm'] == "HISTORY"].iterrows():
            tbl_mapping_name = table_maping_row ['Mapping name']
            trgt_layer = table_maping_row ['Layer']
            process_name = "TXF_"+trgt_layer+"_"+tbl_mapping_name
            trgt_tbl = table_maping_row ['Target table name']
            start_date_column = TransformDDL.get_core_tbl_sart_date_column(Core_tables, trgt_tbl)
            end_date_column = TransformDDL.get_core_tbl_end_date_column(Core_tables, trgt_tbl)
            history_key_list = TransformDDL.get_core_tbl_hist_keys_list(Core_tables, trgt_tbl)
            # history_column_vals = table_maping_row ['Historization columns']
            # history_column_list=pd.unique(list(history_column_vals)).split(',')
            history_column_list = table_maping_row ['Historization columns'].split(',')
            del_st = " DELETE FROM " + history_tbl + " WHERE PROCESS_NAME = '"+ process_name +"'; \n"
            f.write(del_st)
            f.write("--History_keys \n")
            for hist_key in history_key_list:
                hist_key_insert_st = process_name + "','" + process_name + "','" + trgt_tbl + "','" + tbl_mapping_name + "','" + start_date_column
                hist_key_insert_st += "','" + end_date_column + "'," + "null,"
                if hist_key != "undefined":
                    hist_key = funcs.single_quotes(hist_key)

                hist_key_insert_st += hist_key + "); \n"
                f.write(hist_key_insert_header)
                f.write(hist_key_insert_st)

            f.write("--History_columns \n")
            # f.write(str(history_column_list))
            # f.write(str(len(history_column_list)))
            # f.write("\n")

            for hist_col in history_column_list:
                if hist_col == '':
                    hist_col = "undefined"
                else:
                    hist_col = funcs.single_quotes(hist_col)

                hist_col_insert_st = process_name + "','" + process_name + "','" + trgt_tbl + "','" + tbl_mapping_name + "','" + start_date_column
                hist_col_insert_st += "','" + end_date_column + "'," + hist_col + "," +"null); \n"
                f.write(hist_key_insert_header)
                f.write(hist_col_insert_st)
            f.write("\n \n")
    except:
        funcs.TemplateLogError(cf.output_path, source_output_path, file_name, traceback.format_exc()).log_error()
    f.close()
