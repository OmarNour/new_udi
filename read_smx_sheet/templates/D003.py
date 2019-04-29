from read_smx_sheet.app_Lib import functions as funcs
from read_smx_sheet.app_Lib import TransformDDL


def d003(cf, source_output_path,BMAP_values, BMAP):
    file_name = funcs.get_file_name(__file__)
    f = open(source_output_path + "/" + file_name + ".sql", "w+", encoding="utf-8")

    try:
        BMAP_values = BMAP_values[BMAP_values["Code set name"] != '']
        insert_st_header = "INSERT INTO " + cf.UTLFW_v + ".BMAP_STANDARD_MAP ( \n"
        bm_tbl_cols = "Source_Code \n"+",Domain_Id  \n" + ",Code_Set_Id  \n" + ",EDW_Code  \n" + ",Description  \n"
        bm_tbl_cols += ",Start_Date  \n" + ",End_Date  \n" + ",Record_Deleted_Flag  \n" + ",Ctl_Id  \n" + ",Process_Name \n"
        bm_tbl_cols += ",Process_Id  \n" + ",Update_Process_Name  \n" + ",Update_Process_Id  \n) VALUES ( \n"
        insert_st_header += bm_tbl_cols

        for bmap_index, bmap_row in BMAP_values.iterrows():
            domain_id = ""
            edw_code = ""
            if (bmap_row["Code domain ID"] != ''):
                domain_id = int(bmap_row["Code domain ID"])#int( str(bmap_row["Code domain ID"]).strip())
                domain_id = str(domain_id)
            code_set_id = TransformDDL.get_bmap_code_set_id(BMAP, bmap_row["Code set name"])

            if (bmap_row["EDW code"] != ''):
                 edw_code = int(bmap_row["EDW code"])
                 edw_code = str(edw_code)

            process_name = "'" + TransformDDL.get_bmap_physical_tbl_name(BMAP, bmap_row["Code set name"]) + "'"
            insert_vals = "'" + str(bmap_row["Source code"]).strip()+ "'\n" + ",'"+ domain_id + "'\n"
            insert_vals += ",'" + code_set_id+"'\n" + ",'"+ edw_code + "'\n"
            insert_vals += ",'"+ str(bmap_row["Description"]).strip() + "'\n" + ",CURRENT_DATE \n ,DATE  '2999-12-31' \n ,0 \n ,0 \n"
            insert_vals += process_name +"\n,0\n ,NULL \n ,NULL \n);"

            insert_st= insert_st_header + insert_vals

            del_st = "DELETE FROM " + cf.UTLFW_v + ".BMAP_STANDARD_MAP ( \n WHERE Domain_Id = '" + domain_id + "'\n"
            del_st += "AND EDW_Code = '" + edw_code + "' \n AND Code_Set_Id = '" + code_set_id + "';"
            f.write(del_st)
            f.write("\n")
            f.write(insert_st)
            f.write("\n")

    except:
        pass
    f.close()