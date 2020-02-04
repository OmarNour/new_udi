import os, sys, subprocess
sys.path.append(os.getcwd())
from read_smx_sheet.app_Lib import manage_directories as md, functions as funcs
from dask import compute, delayed, config
from dask.diagnostics import ProgressBar
from read_smx_sheet.templates import D110, D300, D320, D200, D330, D400, D610, D640
from read_smx_sheet.templates import testing_script_01, testing_script_02
from read_smx_sheet.templates import PROCESS_CHECK_TEST_SHEET, CSO_TEST_SHEET, NULLS_TEST_SHEET, DUP_TEST_SHEET
from read_smx_sheet.templates import BMAP_DUP_CD_TEST_SHEET,BMAP_DUP_DESC_TEST_SHEET,BMAP_NULL_TEST_SHEET
from read_smx_sheet.templates import DATA_SRC_TEST_SHEET, BMAP_CHECK_TEST_SHEET , BMAP_UNMATCHED_TEST_SHEET
from read_smx_sheet.templates import HIST_STRT_END_NULL_TEST_SHEET, HIST_DUP_TEST_SHEET ,HIST_STRT_GRT_END_TEST_SHEET,HIST_TIME_GAP_TEST_SHEET
from read_smx_sheet.templates import HIST_STRT_NULL_TEST_SHEET, RI_TEST_SHEET
from read_smx_sheet.templates import D410, D415, D003, D630, D420, D210, D608, D615, D000, gcfr, D620, D001, D600, D607, D002, D340
from read_smx_sheet.parameters import parameters as pm
import traceback
import datetime as dt
import shutil



class LogFile(funcs.WriteFile):
    def __init__(self, output_path):
        self.output_path = output_path
        self.file_name = "log"
        self.ext = "txt"
        super().__init__(self.output_path, self.file_name, self.ext, "a+", True)


class ConfigFile:
    def __init__(self, config_file=None, config_file_values=None):
        self.config_file_values = funcs.get_config_file_values(config_file) if config_file_values is None else config_file_values
        self.output_path = self.config_file_values["output_path"]
        self.read_sheets_parallel = self.config_file_values["read_sheets_parallel"]
        self.smx_path = self.config_file_values["smx_path"]
        self.source_names = self.config_file_values["source_names"]
        self.gcfr_system_name = self.config_file_values["gcfr_system_name"]
        self.gcfr_ctl_Id = self.config_file_values["gcfr_ctl_Id"]
        self.gcfr_stream_key = self.config_file_values["gcfr_stream_key"]
        self.gcfr_stream_name = self.config_file_values["gcfr_stream_name"]
        self.gcfr_bkey_process_type = self.config_file_values["gcfr_bkey_process_type"]
        self.gcfr_snapshot_txf_process_type = self.config_file_values["gcfr_snapshot_txf_process_type"]
        self.gcfr_insert_txf_process_type = self.config_file_values["gcfr_insert_txf_process_type"]
        self.gcfr_others_txf_process_type = self.config_file_values["gcfr_others_txf_process_type"]
        self.M_GCFR = self.config_file_values["M_GCFR"]
        self.P_UT = self.config_file_values["P_UT"]
        self.GCFR_t = self.config_file_values["GCFR_t"]
        self.SI_DB = self.config_file_values["SI_DB"]
        self.INPUT_VIEW_DB = self.config_file_values["INPUT_VIEW_DB"]
        self.core_table = self.config_file_values["core_table"]
        self.etl_process_table = self.config_file_values["etl_process_table"]
        self.core_view = self.config_file_values["core_view"]
        self.GCFR_V = self.config_file_values["GCFR_V"]
        self.SOURCE_TABLES_LKP_table = self.config_file_values["SOURCE_TABLES_LKP_table"]
        self.SOURCE_NAME_LKP_table = self.config_file_values["SOURCE_NAME_LKP_table"]
        self.history_tbl = self.config_file_values["history_tbl"]
        self.db_prefix = self.config_file_values["db_prefix"]
        self.T_STG = self.config_file_values["T_STG"]
        self.t_WRK = self.config_file_values["t_WRK"]
        self.v_stg = self.config_file_values["v_stg"]
        self.MACRO_DB = self.config_file_values["MACRO_DB"]
        self.UT_DB = self.config_file_values["UT_DB"]
        self.UTLFW_v = self.config_file_values["UTLFW_v"]
        self.UTLFW_t = self.config_file_values["UTLFW_t"]
        self.TMP_DB = self.config_file_values["TMP_DB"]
        self.APPLY_DB = self.config_file_values["APPLY_DB"]
        self.base_DB = self.config_file_values["base_DB"]
        self.SI_VIEW = self.config_file_values["SI_VIEW"]

        self.online_source_t = self.config_file_values["online_source_t"]
        self.online_source_v = self.config_file_values["online_source_v"]
        self.offline_source_t = self.config_file_values["offline_source_t"]
        self.offline_source_v = self.config_file_values["offline_source_v"]
        try:
            self.scripts_flag = self.config_file_values["scripts_flag"]
        except:
            self.scripts_flag = "All"

class GenerateScripts:
    def __init__(self, config_file=None, config_file_values=None):
        self.start_time = dt.datetime.now()
        self.cf = ConfigFile(config_file, config_file_values)
        md.remove_folder(self.cf.output_path)
        md.create_folder(self.cf.output_path)
        self.log_file = LogFile(self.cf.output_path)
        self.error_message = ""
        self.parallel_remove_output_home_path = []
        self.parallel_create_output_home_path = []
        self.parallel_create_smx_copy_path = []
        self.parallel_used_smx_copy = []
        self.parallel_create_output_source_path = []
        self.parallel_templates = []
        self.count_sources = 0
        self.count_smx = 0
        self.smx_ext = pm.smx_ext
        self.sheets = pm.sheets
        self.STG_tables_sht = pm.STG_tables_sht
        self.Table_mapping_sht = pm.Table_mapping_sht
        self.Core_tables_sht = pm.Core_tables_sht
        self.BKEY_sht = pm.BKEY_sht
        self.BMAP_sht = pm.BMAP_sht
        self.BMAP_values_sht = pm.BMAP_values_sht
        self.Column_mapping_sht = pm.Column_mapping_sht
        self.RI_relations_sht = pm.RI_relations_sht
        self.System_sht = pm.System_sht
        self.Supplements_sht = pm.Supplements_sht

    def generate_scripts(self):
        self.log_file.write("Reading from: \t" + self.cf.smx_path)
        self.log_file.write("Output folder: \t" + self.cf.output_path)
        self.log_file.write("SMX files:")
        print("Reading from: \t" + self.cf.smx_path)
        print("Output folder: \t" + self.cf.output_path)
        print("Scripts to be generated: \t"+self.scripts_flag)
        print("SMX files:")
        filtered_sources = []
        self.start_time = dt.datetime.now()
        try:
            smx_files = funcs.get_smx_files(self.cf.smx_path, self.smx_ext, self.sheets)
            for smx in smx_files:
                try:
                    self.count_smx = self.count_smx + 1
                    smx_file_path = self.cf.smx_path + "/" + smx
                    smx_file_name = os.path.splitext(smx)[0]
                    print("\t" + smx_file_name)
                    self.log_file.write("\t" + smx_file_name)
                    home_output_path = self.cf.output_path + "/" + smx_file_name + "/"

                    # self.parallel_remove_output_home_path.append(delayed(md.remove_folder)(home_output_path))
                    self.parallel_create_output_home_path.append(delayed(md.create_folder)(home_output_path))

                    # COPY SMX USED INTO PATH OF ITS UDI SCRIPTS
                    smx_file_path_destination = os.path.join(home_output_path, "USED_SMX_FILE")
                    self.parallel_create_smx_copy_path.append(delayed(md.create_folder)(smx_file_path_destination))
                    smx_file_path_destination += '/' + smx_file_name + '.xlsx'
                    self.parallel_used_smx_copy.append(delayed(shutil.copy)(smx_file_path, smx_file_path_destination))

                    self.parallel_templates.append(delayed(gcfr.gcfr)(self.cf, home_output_path))
                    ##################################### end of read_smx_folder ################################
                    if self.cf.source_names:
                        System_sht_filter = [['Source system name', self.cf.source_names]]
                    else:
                        System_sht_filter = None

                    System = funcs.read_excel(smx_file_path, sheet_name=self.System_sht)
                    teradata_sources = System[System['Source type'] == 'TERADATA']
                    teradata_sources = funcs.df_filter(teradata_sources, System_sht_filter, False)
                    self.count_sources = self.count_sources + len(teradata_sources.index)

                    Supplements = delayed(funcs.read_excel)(smx_file_path, sheet_name=self.Supplements_sht)
                    Column_mapping = delayed(funcs.read_excel)(smx_file_path, sheet_name=self.Column_mapping_sht)
                    BMAP_values = delayed(funcs.read_excel)(smx_file_path, sheet_name=self.BMAP_values_sht)
                    BMAP = delayed(funcs.read_excel)(smx_file_path, sheet_name=self.BMAP_sht)
                    BKEY = delayed(funcs.read_excel)(smx_file_path, sheet_name=self.BKEY_sht)
                    Core_tables = delayed(funcs.read_excel)(smx_file_path, sheet_name=self.Core_tables_sht)
                    Core_tables = delayed(funcs.rename_sheet_reserved_word)(Core_tables, Supplements, 'TERADATA', ['Column name', 'Table name'])
                    RI_relations = delayed(funcs.read_excel)(smx_file_path, sheet_name=self.RI_relations_sht)
                    ##################################### end of read_smx_sheet ################################

                    for system_index, system_row in teradata_sources.iterrows():
                        try:
                            Loading_Type = system_row['Loading type'].upper()
                            if Loading_Type != "":
                                source_name = system_row['Source system name']
                                filtered_sources.append(source_name)

                                source_name_filter = [['Source', [source_name]]]
                                core_layer_filter = [['Layer', ["CORE"]]]
                                stg_layer_filter = [['Layer', ["STG"]]]
                                stg_source_name_filter = [['Source system name', [source_name]]]

                                Table_mapping = delayed(funcs.read_excel)(smx_file_path, self.Table_mapping_sht, source_name_filter)

                                core_Table_mapping = delayed(funcs.df_filter)(Table_mapping, core_layer_filter, False)
                                stg_Table_mapping = delayed(funcs.df_filter)(Table_mapping, stg_layer_filter, False)


                                STG_tables = delayed(funcs.read_excel)(smx_file_path, self.STG_tables_sht, stg_source_name_filter)
                                STG_tables = delayed(funcs.rename_sheet_reserved_word)(STG_tables, Supplements, 'TERADATA', ['Column name', 'Table name'])


                                main_output_path = home_output_path + "/" + Loading_Type + "/" + source_name
                                source_output_path = os.path.join(main_output_path, "UDI")

                                output_path_testing = os.path.join(main_output_path, "TestCases_scripts")
                                process_check_output_path_testing = os.path.join(output_path_testing, "PROCESS_CHECK_Cases_scripts")
                                cso_output_path_testing = os.path.join(output_path_testing, "CSO_Cases_scripts")
                                nulls_output_path_testing = os.path.join(output_path_testing, "NULLS_Cases_scripts")
                                duplicate_output_path_testing = os.path.join(output_path_testing, "DUPLICATE_Cases_scripts")
                                data_src_output_path_testing = os.path.join(output_path_testing, "DATA_SRC_Cases_scripts")
                                bmaps_output_path_testing = os.path.join(output_path_testing, "BMAPS_Cases_scripts")
                                history_output_path_testing = os.path.join(output_path_testing, "HISTORY_Cases_scripts")
                                ri_output_path_testing = os.path.join(output_path_testing, "RI_Cases_scripts")

                                self.parallel_create_output_source_path.append(delayed(md.create_folder)(main_output_path))

                                #UDI SCRIPTS
                                if self.scripts_flag=='All' or self.scripts_flag=='UDI':
                                    self.parallel_create_output_source_path.append(delayed(md.create_folder)(source_output_path))
                                    self.parallel_templates.append(delayed(D000.d000)(self.cf, source_output_path, source_name, core_Table_mapping, STG_tables, BKEY))
                                    self.parallel_templates.append(delayed(D001.d001)(self.cf, source_output_path, source_name, STG_tables))
                                    self.parallel_templates.append(delayed(D002.d002)(self.cf, source_output_path, Core_tables, core_Table_mapping))
                                    self.parallel_templates.append(delayed(D003.d003)(self.cf, source_output_path, BMAP_values, BMAP))

                                    self.parallel_templates.append(delayed(D110.d110)(self.cf, source_output_path, stg_Table_mapping, STG_tables, Loading_Type))

                                    self.parallel_templates.append(delayed(D200.d200)(self.cf, source_output_path, STG_tables, Loading_Type))
                                    self.parallel_templates.append(delayed(D210.d210)(self.cf, source_output_path, STG_tables, Loading_Type))

                                    self.parallel_templates.append(delayed(D320.d320)(self.cf, source_output_path, STG_tables, BKEY))
                                    self.parallel_templates.append(delayed(D330.d330)(self.cf, source_output_path, STG_tables, BKEY))
                                    self.parallel_templates.append(delayed(D340.d340)(self.cf, source_output_path, STG_tables, BKEY))

                                    self.parallel_templates.append(delayed(D300.d300)(self.cf, source_output_path, STG_tables, BKEY))
                                    # self.parallel_templates.append(delayed(D400.d400)(self.cf, source_output_path, STG_tables))
                                    # self.parallel_templates.append(delayed(D410.d410)(self.cf, source_output_path, STG_tables))
                                    # self.parallel_templates.append(delayed(D415.d415)(self.cf, source_output_path, STG_tables))
                                    self.parallel_templates.append(delayed(D420.d420)(self.cf, source_output_path, STG_tables, BKEY, BMAP, Loading_Type))

                                    self.parallel_templates.append(delayed(D600.d600)(self.cf, source_output_path, core_Table_mapping, Core_tables))
                                    self.parallel_templates.append(delayed(D607.d607)(self.cf, source_output_path, Core_tables, BMAP_values))
                                    self.parallel_templates.append(delayed(D608.d608)(self.cf, source_output_path, Core_tables, BMAP_values))
                                    self.parallel_templates.append(delayed(D610.d610)(self.cf, source_output_path, core_Table_mapping))
                                    self.parallel_templates.append(delayed(D615.d615)(self.cf, source_output_path, Core_tables))
                                    self.parallel_templates.append(delayed(D620.d620)(self.cf, source_output_path, core_Table_mapping, Column_mapping, Core_tables, Loading_Type))
                                    self.parallel_templates.append(delayed(D630.d630)(self.cf, source_output_path, core_Table_mapping))
                                    self.parallel_templates.append(delayed(D640.d640)(self.cf, source_output_path, source_name, core_Table_mapping))

                                #TESTING SCRIPTS
                                if self.scripts_flag=='All' or self.scripts_flag=='Testing':
                                    #CREATING  PATHS FOR THE OUTPUT SCRIPTS
                                    self.parallel_create_output_source_path.append(delayed(md.create_folder)(output_path_testing))
                                    self.parallel_create_output_source_path.append(delayed(md.create_folder)(process_check_output_path_testing))
                                    self.parallel_create_output_source_path.append(delayed(md.create_folder)(cso_output_path_testing))
                                    self.parallel_create_output_source_path.append(delayed(md.create_folder)(nulls_output_path_testing))
                                    self.parallel_create_output_source_path.append(delayed(md.create_folder)(duplicate_output_path_testing))
                                    self.parallel_create_output_source_path.append(delayed(md.create_folder)(data_src_output_path_testing))
                                    self.parallel_create_output_source_path.append(delayed(md.create_folder)(bmaps_output_path_testing))
                                    self.parallel_create_output_source_path.append(delayed(md.create_folder)(history_output_path_testing))
                                    self.parallel_create_output_source_path.append(delayed(md.create_folder)(ri_output_path_testing))

                                    self.parallel_templates.append(delayed(testing_script_01.source_testing_script)(self.cf, output_path_testing,source_name,core_Table_mapping,Column_mapping, STG_tables,BKEY))
                                    self.parallel_templates.append(delayed(testing_script_02.source_testing_script)(self.cf, output_path_testing,source_name,core_Table_mapping, Core_tables))
                                    self.parallel_templates.append(delayed(PROCESS_CHECK_TEST_SHEET.process_check)(self.cf, process_check_output_path_testing,source_name, core_Table_mapping))
                                    self.parallel_templates.append(delayed(CSO_TEST_SHEET.cso_check)(self.cf, cso_output_path_testing,source_name,core_Table_mapping,Column_mapping))
                                    self.parallel_templates.append(delayed(NULLS_TEST_SHEET.nulls_check)(self.cf, nulls_output_path_testing, core_Table_mapping, Core_tables))
                                    self.parallel_templates.append(delayed(DUP_TEST_SHEET.duplicates_check)(self.cf, duplicate_output_path_testing,core_Table_mapping,Core_tables))
                                    self.parallel_templates.append(delayed(DATA_SRC_TEST_SHEET.data_src_check)(self.cf, data_src_output_path_testing,source_name,core_Table_mapping,Column_mapping))
                                    self.parallel_templates.append(delayed(BMAP_CHECK_TEST_SHEET.bmap_check)(self.cf, bmaps_output_path_testing,source_name,core_Table_mapping,Core_tables))
                                    self.parallel_templates.append(delayed(BMAP_DUP_CD_TEST_SHEET.bmap_dup_check)(self.cf, bmaps_output_path_testing, core_Table_mapping, Core_tables))
                                    self.parallel_templates.append(delayed(BMAP_DUP_DESC_TEST_SHEET.bmap_dup_desc_check)(self.cf, bmaps_output_path_testing,core_Table_mapping,Core_tables))
                                    self.parallel_templates.append(delayed(BMAP_NULL_TEST_SHEET.bmap_null_check)(self.cf, bmaps_output_path_testing,core_Table_mapping,Core_tables))
                                    self.parallel_templates.append(delayed(BMAP_UNMATCHED_TEST_SHEET.bmap_unmatched_values_check)(self.cf, bmaps_output_path_testing,core_Table_mapping,Core_tables,BMAP))
                                    self.parallel_templates.append(delayed(HIST_STRT_END_NULL_TEST_SHEET.hist_start_end_null_check)(self.cf, history_output_path_testing, core_Table_mapping, Core_tables))
                                    self.parallel_templates.append(delayed(HIST_DUP_TEST_SHEET.hist_dup_check)(self.cf, history_output_path_testing, core_Table_mapping, Core_tables))
                                    self.parallel_templates.append(delayed(HIST_STRT_GRT_END_TEST_SHEET.hist_start_end_null_check)(self.cf, history_output_path_testing, core_Table_mapping, Core_tables))
                                    self.parallel_templates.append(delayed(HIST_TIME_GAP_TEST_SHEET.hist_timegap_check)(self.cf, history_output_path_testing, core_Table_mapping, Core_tables))
                                    self.parallel_templates.append(delayed(HIST_STRT_NULL_TEST_SHEET.hist_start_null_check)(self.cf, history_output_path_testing, core_Table_mapping, Core_tables))
                                    self.parallel_templates.append(delayed(RI_TEST_SHEET.ri_check)(self.cf, ri_output_path_testing, core_Table_mapping, RI_relations))

                        except Exception as e_source:
                            # print(error)

                            # log: smx_file_name, source_name
                            print(system_row.to_dict())
                            funcs.SMXFilesLogError(self.cf.output_path, smx, str(system_row.to_dict()), traceback.format_exc()).log_error()
                            self.count_sources = self.count_sources - 1

                except Exception as e_smx_file:
                    # print(error)
                    funcs.SMXFilesLogError(self.cf.output_path, smx, None, traceback.format_exc()).log_error()
                    self.count_smx = self.count_smx - 1

        except Exception as e1:
            # print(error)
            # traceback.print_exc()
            self.elapsed_time = dt.datetime.now() - self.start_time
            funcs.SMXFilesLogError(self.cf.output_path, None, None, traceback.format_exc()).log_error()

        if len(self.parallel_templates) > 0:
            sources = funcs.list_to_string(filtered_sources, ', ')
            print("Sources:", sources)
            self.log_file.write("Sources:" + sources)
            scheduler_value = 'processes' if self.cf.read_sheets_parallel == 1 else ''
            with config.set(scheduler=scheduler_value):
                # compute(*self.parallel_remove_output_home_path)
                compute(*self.parallel_create_output_home_path)
                compute(*self.parallel_create_smx_copy_path)
                compute(*self.parallel_used_smx_copy)
                compute(*self.parallel_create_output_source_path)

                with ProgressBar():
                    smx_files = " smx files" if self.count_smx > 1 else " smx file"
                    smx_file_sources = " sources" if self.count_sources > 1 else " source"
                    print("Start generating " + str(len(self.parallel_templates)) + " script for " + str(self.count_sources) + smx_file_sources + " from " + str(self.count_smx) + smx_files)
                    compute(*self.parallel_templates)
                    self.log_file.write(str(len(self.parallel_templates)) + " script generated for " + str(self.count_sources) + smx_file_sources + " from " + str(self.count_smx) + smx_files)
                    self.elapsed_time = dt.datetime.now() - self.start_time
                    self.log_file.write("Elapsed Time: " + str(self.elapsed_time))
            self.error_message = ""
            if sys.platform == "win32":
                os.startfile(self.cf.output_path)
            else:
                opener = "open" if sys.platform == "darwin" else "xdg-open"
                subprocess.call([opener, self.cf.output_path])
        else:
            self.error_message = "No SMX Files Found!"

        self.log_file.close()



