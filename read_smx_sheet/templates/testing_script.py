from read_smx_sheet.app_Lib import functions as funcs
import traceback


def source_testing_script(cf, source_output_path, source_name, Table_mapping, Column_mapping, STG_tables, BKEY):
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
        ########################################

        Table_mapping_df = Table_mapping[Table_mapping['Source'] == source_name][['Target table name', 'Mapping name', 'Main source']]
        for Table_mapping_df_index, Table_mapping_df_row in Table_mapping_df.iterrows():
            TARGET_TABLE_NAME = Table_mapping_df_row['Target table name']
            TABLE_MAPPING_NAME = Table_mapping_df_row['Mapping name']
            SOURCE_TABLE = Table_mapping_df_row['Main source']

            Column_mapping_df = Column_mapping[(Column_mapping['Mapping name'] == TABLE_MAPPING_NAME)
                                               & (Column_mapping['Mapped to table'] == SOURCE_TABLE)][['Column name', 'Mapped to column']]
            STG_tables_df = STG_tables[(STG_tables['Source system name'] == source_name)
                                       & (STG_tables['Table name'] == SOURCE_TABLE)
                                       & (STG_tables['Key domain name'] != "")][['Natural key', 'Key domain name', 'Column name']]

            merge_df = Column_mapping_df.merge(STG_tables_df,
                                               left_on=['Mapped to column'],
                                               right_on=['Column name'],
                                               suffixes=('_clnM', '_stgT'),
                                               how='inner')
            for merge_df_index, merge_df_row in merge_df.iterrows():
                TARGET_COLUMN_NAME = merge_df_row['Column name_clnM']
                NATURAL_KEY = merge_df_row['Natural key']
                key_domain_name = merge_df_row['Key domain name']

                BKEY_df = BKEY[BKEY['Key domain name'] == key_domain_name]
                for BKEY_df_index, BKEY_df_row in BKEY_df.iterrows():
                    PHYSICAL_NAME = BKEY_df_row['Physical table']
                    KEY_DOMAIN_ID_VALUE = str(BKEY_df_row['Key domain ID'])

                    select_script = "-- " + TABLE_MAPPING_NAME + " -- " + SOURCE_TABLE + " -- " + PHYSICAL_NAME + \
                                    "\nSELECT\t" + funcs.single_quotes(TARGET_TABLE_NAME) + " AS CORE_TABLE," \
                                    "\n\t\t"+ funcs.single_quotes(TARGET_COLUMN_NAME) + " AS CORE_COLUMN," \
                                    "\n\t\t"+ funcs.single_quotes(TABLE_MAPPING_NAME) + " AS MAPPING_NAME," \
                                    "\n\t\tCASE WHEN BKEY_CNT > 0 THEN 'BKEY_FAILED' ELSE 'BKEY_SUCCEEDED' END AS BKEY_STATUS," \
                                    "\n\t\tCASE WHEN CORE_CNT > 0 THEN 'CORE_FAILED' ELSE 'CORE_SUCCEEDED' END AS CORE_STATUS" \
                                    "\nFROM" \
                                    "\n(" \
                                    "\n\tSELECT COUNT(*) BKEY_CNT" \
                                    "\n\tFROM " + cf.v_stg + "." + SOURCE_TABLE + " X" \
                                    "\n\tLEFT JOIN " + cf.UTLFW_v + "." + PHYSICAL_NAME + " AS BK1" \
                                    "\n\tON BK1.SOURCE_KEY = TRIM(" + NATURAL_KEY + ") AND BK1.DOMAIN_ID = " + KEY_DOMAIN_ID_VALUE + "" \
                                    "\n\tWHERE EDW_KEY IS NULL" \
                                    "\n)BK_CHECK," \
                                    "\n(" \
                                    "\n\tSELECT COUNT(*) CORE_CNT" \
                                    "\n\tFROM (SELECT * FROM " + cf.UTLFW_v + "." + PHYSICAL_NAME + " WHERE DOMAIN_ID=" + KEY_DOMAIN_ID_VALUE + ")BK1" \
                                    "\n\tINNER JOIN " + cf.v_stg + "." + SOURCE_TABLE + " SRC ON BK1.SOURCE_KEY = TRIM(" + NATURAL_KEY + ")" \
                                    "\n\tLEFT JOIN " + cf.core_view + "." + TARGET_TABLE_NAME + " CORE ON EDW_KEY = " + TARGET_COLUMN_NAME + "" \
                                    "\n\tWHERE " + TARGET_COLUMN_NAME + " IS NULL" \
                                    "\n)CORE_CHECK;\n\n"

                    f.write(select_script)

    except:
        funcs.TemplateLogError(cf.output_path, source_output_path, file_name, traceback.format_exc()).log_error()
    f.close()
