from read_smx_sheet.app_Lib import functions as funcs
import traceback


def source_testing_script(cf, source_output_path, source_name, Table_mapping, Core_tables):
    file_name = funcs.get_file_name(__file__)
    f = funcs.WriteFile(source_output_path, file_name, "sql")

    try:

        ########################################
        # TARGET_TABLE_NAME         Table mapping   done
        # TABLE_MAPPING_NAME        Table mapping   done
        # SOURCE_TABLE              Table mapping   done
        # TARGET_COLUMN_NAME        Column mapping  done
        # NATURAL_KEY               STG tables      done
        # PHYSICAL_NAME             BKEY            done
        # KEY_DOMAIN_ID_VALUE       BKEY            done
        #
        # select a.*
        # from input_view a
        #   left join base_table b
        #   on key_join
        # where b.process_name is null
        ########################################
        script = """select a.*\nfrom {input_view} a\nleft join {base_table} b\n\ton {key_join}\nwhere b.process_name is null;"""
        # 1- get all
        process_type = "TXF"
        Table_mapping_df = Table_mapping[(Table_mapping['Source'] == source_name)
                                         & (Table_mapping['Layer'] == 'CORE')][['Target table name', 'Mapping name', 'Layer']]
        Table_mapping_df = Table_mapping_df.sort_values(['Target table name', 'Mapping name'])
        for Table_mapping_df_index, Table_mapping_df_row in Table_mapping_df.iterrows():

            layer = Table_mapping_df_row['Layer']
            TARGET_TABLE_NAME = Table_mapping_df_row['Target table name']
            TABLE_MAPPING_NAME = Table_mapping_df_row['Mapping name']

            inp_view = cf.INPUT_VIEW_DB + "." + process_type + "_" + layer + "_" + TABLE_MAPPING_NAME + "_IN"
            core_table = cf.core_table + "." + TARGET_TABLE_NAME

            key_columns = Core_tables[(Core_tables['Table name'] == TARGET_TABLE_NAME) &
                                      (Core_tables['PK'] == "Y")]['Column name'].tolist()

            complete_on_clause = ""
            for index, i in enumerate(key_columns):
                on_clause = " a.{}=b.{} "
                if index == 0:
                    and_ = ""
                else:
                    and_ = "\n\tand"

                complete_on_clause = complete_on_clause + and_ + on_clause.format(i, i)

            script_ = script.format(input_view=inp_view, base_table=core_table, key_join=complete_on_clause)

            f.write(script_.strip()+"\n\n")

    except:
        funcs.TemplateLogError(cf.output_path, source_output_path, file_name, traceback.format_exc()).log_error()
    f.close()
