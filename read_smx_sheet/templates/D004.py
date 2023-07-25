from app_Lib import functions as funcs
from app_Lib import TransformDDL
from Logging_Decorator import Logging_decorator
import pandas as pd

@Logging_decorator
def d004(cf, source_output_path, BMAP_values, BMAP, STG_TABLES_SHEET):
    file_name = funcs.get_file_name(__file__)
    f = funcs.WriteFile(source_output_path, file_name, "sql")
    Stage_Code_domain_names = list(STG_TABLES_SHEET[STG_TABLES_SHEET['Code domain name'] != '']['Code domain name'])
    code_set_names = []
    code_domain_ids = []
    code_set_ids = []
    Physical_able = []
    data = []
    for index, row in BMAP.iterrows():
        if row['Code domain name'] in Stage_Code_domain_names:
            continue
        else:
            list_df = []
            # code_set_names.append(row['Code set name'])
            # code_domain_ids.append(row['Code domain ID'])
            # code_set_ids.append(row['Code set ID'])
            # Physical_able.append(row['Physical table'])
            list_df.append(row['Code set name'])
            list_df.append(row['Code domain ID'])
            list_df.append(row['Code set ID'])
            list_df.append(row['Physical table'])
            data.append(list_df)

    Filtered_bmap_df = pd.DataFrame(data, columns=['Code set name','Code domain ID','Code set ID','Physical table'])
    insert_st_header = "INSERT INTO " + cf.UTLFW_t + ".BMAP_STANDARD_MAP ( \n"
    bm_tbl_cols = "Source_Code \n" + ",Domain_Id  \n" + ",Code_Set_Id  \n" + ",EDW_Code  \n" + ",Description  \n"
    bm_tbl_cols += ",Start_Date  \n" + ",End_Date  \n" + ",Record_Deleted_Flag  \n" + ",Ctl_Id  \n" + ",Process_Name \n"
    bm_tbl_cols += ",Process_Id  \n" + ",Update_Process_Name  \n" + ",Update_Process_Id  \n) VALUES ( \n"
    insert_st_header += bm_tbl_cols

    for filtered_bmap_index,filtered_bmap_row in Filtered_bmap_df.iterrows():
        for bmap_values_index,bmap_values_row in BMAP_values.iterrows():
            if filtered_bmap_row['Code set name'] == bmap_values_row['Code set name'] and filtered_bmap_row['Code domain ID'] == bmap_values_row['Code domain ID']:
                source_code = str(bmap_values_row["Source code"]).strip()
                code_set_id =  str(filtered_bmap_row["Code set ID"])
                edw_code = ""
                domain_id = ""
                if bmap_values_row["EDW code"] != '':
                    edw_code = int(bmap_values_row["EDW code"])
                    edw_code = str(edw_code)
                if bmap_values_row["Code domain ID"] != '':
                    domain_id = int(bmap_values_row["Code domain ID"])#int( str(bmap_row["Code domain ID"]).strip())
                    domain_id = str(domain_id)

                process_name = ",'" + filtered_bmap_row["Physical table"]+ "'"
                insert_vals = "'" + source_code + "'\n" + ",'" + domain_id + "'\n"
                insert_vals += ",'" + code_set_id + "'\n" + ",'" + edw_code + "'\n"
                insert_vals += ",'" + str(
                    bmap_values_row["Description"]).strip() + "'\n" + ",CURRENT_DATE \n ,DATE  '2999-12-31' \n ,0 \n ,0 \n"
                insert_vals += process_name + "\n,0\n ,NULL \n ,NULL \n);"

                insert_st = insert_st_header + insert_vals
                del_st = "DELETE FROM " + cf.UTLFW_t + ".BMAP_STANDARD_MAP \n WHERE Domain_Id = '" + domain_id + "'\n"
                del_st += "AND Source_Code = '" + source_code + "' \n AND Code_Set_Id = '" + code_set_id + "';"
                f.write(del_st)
                f.write("\n")
                f.write(insert_st)
                f.write("\n\n")
    f.close()





