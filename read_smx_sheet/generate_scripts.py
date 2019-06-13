import os
import sys
sys.path.append(os.getcwd())
from read_smx_sheet.app_Lib import manage_directories as md, functions as funcs
from dask import compute, delayed, config
from dask.diagnostics import ProgressBar
from read_smx_sheet.templates import D110, D300, D320, D200, D330, D400, D610, D640, testing_script
from read_smx_sheet.templates import D410, D415, D003, D630, D420, D210, D608, D615, D000, gcfr, D620, D001, D600, D607, D002, D340
from read_smx_sheet.parameters import parameters as pm
import traceback
import datetime as dt


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
        self.SI_VIEW = self.config_file_values["SI_VIEW"]

        self.online_source_t = self.config_file_values["online_source_t"]
        self.online_source_v = self.config_file_values["online_source_v"]
        self.offline_source_t = self.config_file_values["offline_source_t"]
        self.offline_source_v = self.config_file_values["offline_source_v"]


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
        self.System_sht = pm.System_sht
        self.Supplements_sht = pm.Supplements_sht

    def generate_scripts(self):
        self.log_file.write("Reading from: \t" + self.cf.smx_path)
        self.log_file.write("Output folder: \t" + self.cf.output_path)
        self.log_file.write("SMX files:")
        print("Reading from: \t" + self.cf.smx_path)
        print("Output folder: \t" + self.cf.output_path)
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
                    ##################################### end of read_smx_sheet ################################

                    for system_index, system_row in teradata_sources.iterrows():
                        try:
                            Loading_Type = system_row['Loading type'].upper()
                            if Loading_Type != "":
                                source_name = system_row['Source system name']
                                filtered_sources.append(source_name)
                                source_name_filter = [['Source', [source_name]]]
                                stg_source_name_filter = [['Source system name', [source_name]]]

                                Table_mapping = delayed(funcs.read_excel)(smx_file_path, self.Table_mapping_sht, source_name_filter)

                                STG_tables = delayed(funcs.read_excel)(smx_file_path, self.STG_tables_sht, stg_source_name_filter)
                                STG_tables = delayed(funcs.rename_sheet_reserved_word)(STG_tables, Supplements, 'TERADATA', ['Column name', 'Table name'])

                                source_output_path = home_output_path + "/" + Loading_Type + "/" + source_name

                                self.parallel_create_output_source_path.append(delayed(md.create_folder)(source_output_path))

                                self.parallel_templates.append(delayed(D000.d000)(self.cf, source_output_path, source_name, Table_mapping, STG_tables, BKEY))
                                self.parallel_templates.append(delayed(D001.d001)(self.cf, source_output_path, source_name, STG_tables))
                                self.parallel_templates.append(delayed(D002.d002)(self.cf, source_output_path, Core_tables, Table_mapping))
                                self.parallel_templates.append(delayed(D003.d003)(self.cf, source_output_path, BMAP_values, BMAP))

                                self.parallel_templates.append(delayed(D110.d110)(self.cf, source_output_path, STG_tables, Loading_Type))

                                self.parallel_templates.append(delayed(D200.d200)(self.cf, source_output_path, STG_tables, Loading_Type))
                                self.parallel_templates.append(delayed(D210.d210)(self.cf, source_output_path, STG_tables, Loading_Type))

                                self.parallel_templates.append(delayed(D300.d300)(self.cf, source_output_path, STG_tables, BKEY))
                                self.parallel_templates.append(delayed(D320.d320)(self.cf, source_output_path, STG_tables, BKEY))
                                self.parallel_templates.append(delayed(D330.d330)(self.cf, source_output_path, STG_tables, BKEY))
                                self.parallel_templates.append(delayed(D340.d340)(self.cf, source_output_path, STG_tables, BKEY))

                                # self.parallel_templates.append(delayed(D400.d400)(self.cf, source_output_path, STG_tables))
                                # self.parallel_templates.append(delayed(D410.d410)(self.cf, source_output_path, STG_tables))
                                # self.parallel_templates.append(delayed(D415.d415)(self.cf, source_output_path, STG_tables))
                                self.parallel_templates.append(delayed(D420.d420)(self.cf, source_output_path, STG_tables, BKEY, BMAP, Loading_Type))

                                self.parallel_templates.append(delayed(D600.d600)(self.cf, source_output_path, Table_mapping, Core_tables))
                                self.parallel_templates.append(delayed(D607.d607)(self.cf, source_output_path, Core_tables, BMAP_values))
                                self.parallel_templates.append(delayed(D608.d608)(self.cf, source_output_path, Core_tables, BMAP_values))
                                self.parallel_templates.append(delayed(D610.d610)(self.cf, source_output_path, Table_mapping))
                                self.parallel_templates.append(delayed(D615.d615)(self.cf, source_output_path, Core_tables))
                                self.parallel_templates.append(delayed(D620.d620)(self.cf, source_output_path, Table_mapping, Column_mapping, Core_tables, Loading_Type))
                                self.parallel_templates.append(delayed(D630.d630)(self.cf, source_output_path, Table_mapping))
                                self.parallel_templates.append(delayed(D640.d640)(self.cf, source_output_path, source_name, Table_mapping))

                                self.parallel_templates.append(delayed(testing_script.source_testing_script)(self.cf, source_output_path, source_name, Table_mapping, Column_mapping, STG_tables, BKEY))

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
            funcs.SMXFilesLogError(self.cf.output_path, None, None, traceback.format_exc()).log_error()

        if len(self.parallel_templates) > 0:
            sources = funcs.list_to_string(filtered_sources, ', ')
            print("Sources:", sources)
            self.log_file.write("Sources:" + sources)
            scheduler_value = 'processes' if self.cf.read_sheets_parallel == 1 else ''
            with config.set(scheduler=scheduler_value):
                # compute(*self.parallel_remove_output_home_path)
                compute(*self.parallel_create_output_home_path)
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
            os.startfile(self.cf.output_path)
        else:
            self.error_message = "No SMX Files Found!"

        self.log_file.close()



