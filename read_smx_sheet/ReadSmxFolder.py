import os
import sys
sys.path.append(os.getcwd())
from parameters import parameters as pm
from app_Lib import manage_directories as md, functions as funcs
import datetime as dt
from templates import gcfr
import subprocess
import pandas as pd
import warnings
warnings.filterwarnings("ignore")


class ReadSmxFolder:
    def __init__(self):
        self.module_path = os.path.dirname(sys.modules['__main__'].__file__)
        self.output_path = pm.output_path

        self.smx_process_no = 0
        self.smx_processes_names = {}
        self.smx_processes_run_status = {}
        self.smx_processes_numbers = []

        self.build_scripts_process_no = 0
        self.build_scripts_processes_names = {}
        self.build_scripts_processes_run_status = {}
        self.build_scripts_processes_numbers = []

        self.count_smx = 0
        self.count_sources = 0

    def is_smx_file(self, file):
        file_sheets = pd.ExcelFile(file).sheet_names
        required_sheets = list(pm.sheets)
        for required_sheet in pm.sheets:
            for file_sheet in file_sheets:
                if file_sheet == required_sheet:
                    required_sheets.remove(required_sheet)

        return True if len(required_sheets) == 0 else False

    def get_smx_files(self):
        smx_files = []
        all_files = md.get_files_in_dir(pm.smx_path, pm.smx_ext)
        for i in all_files:
            file = pm.smx_path + "/" + i
            smx_files.append(i) if self.is_smx_file(file) else None
        return smx_files

    def read_smx_folder(self):
        print("\nReading from: \t", pm.smx_path)
        smx_files = self.get_smx_files()
        if len(smx_files) > 0:
            print("Output folder: \t", self.output_path)
            print("SMX files:")

            read_smx_source_inputs = []
            reading_sheets_start_time = dt.datetime.now()
            for smx in smx_files:
                self.count_smx = self.count_smx + 1
                smx_file_name = os.path.splitext(smx)[0]
                print("\t" + smx_file_name)
                home_output_path = self.output_path + "/" + smx_file_name
                smx_file_path = pm.smx_path + "/" + smx
                read_smx_source_inputs.append([home_output_path,smx_file_path])

                md.remove_folder(home_output_path)
                md.create_folder(home_output_path)
                gcfr.gcfr(home_output_path)

                if pm.read_sheets_parallel:
                    self.read_smx_sheets(smx_file_path)
                else:
                    self.read_smx_file(smx_file_path)

            if pm.read_sheets_parallel:
                print("\nStart reading " + str(len(self.smx_processes_numbers)) + " sheets in " + str(int(len(self.smx_processes_numbers) / len(pm.sheets))) + " SMX files...")
            else:
                print("\nStart reading " + str(len(self.smx_processes_numbers)) + " SMX files...")
            print("---------------------------------------------------------------------------")
            funcs.wait_for_processes_to_finish(self.smx_processes_numbers, self.smx_processes_run_status, self.smx_processes_names)
            reading_sheets_end_time = dt.datetime.now()
            print("\nReading elapsed time: ", reading_sheets_end_time - reading_sheets_start_time)

            building_start_time = dt.datetime.now()
            for i in read_smx_source_inputs:
                i_home_output_path = i[0]
                i_smx_file_path = i[1]
                self.build_smx_source_scripts(i_home_output_path, i_smx_file_path)

            print("\nStart building scripts for " + str(len(self.build_scripts_processes_numbers)) + " sources...")
            print("---------------------------------------------------------------------------")
            funcs.wait_for_processes_to_finish(self.build_scripts_processes_numbers, self.build_scripts_processes_run_status, self.build_scripts_processes_names)
            building_end_time = dt.datetime.now()
            print("\nBuilding scripts elapsed time: ", building_end_time - building_start_time)
            os.startfile(self.output_path)
        else:
            print("No SMX files found!")

    def read_smx_file(self, smx_file_path):
        #################################################################################
        smx_file_name = funcs.get_file_name(smx_file_path)
        self.smx_processes_names[self.smx_process_no] = smx_file_name + "\t"
        self.smx_processes_numbers.append(self.smx_process_no)
        main_inputs = "smx_file_path=" + funcs.single_quotes(smx_file_path)
        main_inputs = main_inputs + pm.sys_argv_separator + "output_path=" + funcs.single_quotes(self.output_path)
        main_inputs = main_inputs + pm.sys_argv_separator + "task=" + funcs.single_quotes(0)

        to_run = self.module_path + '/ReadSMX.py'
        self.smx_processes_run_status[self.smx_process_no] = subprocess.Popen(['python',
                                                                               to_run,
                                                                               main_inputs])
        self.smx_process_no += 1
        #################################################################################

    def read_smx_sheets(self, smx_file_path):
        #################################################################################
        smx_file_name = funcs.get_file_name(smx_file_path)
        for sheet_name in pm.sheets:
            self.smx_processes_names[self.smx_process_no] = smx_file_name + "\t" + sheet_name
            self.smx_processes_numbers.append(self.smx_process_no)
            main_inputs = "smx_file_path=" + funcs.single_quotes(smx_file_path)
            main_inputs = main_inputs + pm.sys_argv_separator + "output_path=" + funcs.single_quotes(self.output_path)
            main_inputs = main_inputs + pm.sys_argv_separator + "sheet_name=" + funcs.single_quotes(sheet_name)
            main_inputs = main_inputs + pm.sys_argv_separator + "task=" + funcs.single_quotes(1)

            to_run = self.module_path + '/ReadSMX.py'
            self.smx_processes_run_status[self.smx_process_no] = subprocess.Popen(['python',
                                                                                   to_run,
                                                                                   main_inputs])
            self.smx_process_no += 1
        #################################################################################

    def build_smx_source_scripts(self, home_output_path, smx_file_path):
        if pm.source_names:
            System_sht_filter = [['Source system name', pm.source_names]]
        else:
            System_sht_filter = None

        teradata_sources = funcs.get_sheet_data(smx_file_path, self.output_path, pm.System_sht, System_sht_filter)
        for system_index, system_row in teradata_sources.iterrows():
            try:
                Loading_Type = system_row['Loading type'].upper()
                source_name = system_row['Source system name']

                source_output_path = home_output_path + "/" + Loading_Type + "/" + source_name
                md.create_folder(source_output_path)
                self.count_sources = self.count_sources + 1
                #################################################################################
                smx_file_name = funcs.get_file_name(smx_file_path)
                self.build_scripts_processes_names[self.build_scripts_process_no] = smx_file_name + "\t" + Loading_Type + "\t" + source_name
                self.build_scripts_processes_numbers.append(self.build_scripts_process_no)
                main_inputs = "smx_file_path=" + funcs.single_quotes(smx_file_path)
                main_inputs = main_inputs + pm.sys_argv_separator + "source_output_path=" + funcs.single_quotes(source_output_path)
                main_inputs = main_inputs + pm.sys_argv_separator + "output_path=" + funcs.single_quotes(self.output_path)
                main_inputs = main_inputs + pm.sys_argv_separator + "source_name=" + funcs.single_quotes(source_name)
                main_inputs = main_inputs + pm.sys_argv_separator + "Loading_Type=" + funcs.single_quotes(Loading_Type)
                main_inputs = main_inputs + pm.sys_argv_separator + "task=" + funcs.single_quotes(2)

                to_run = self.module_path+ '/ReadSMX.py'
                self.build_scripts_processes_run_status[self.build_scripts_process_no] = subprocess.Popen(['python',
                                                                                                           to_run,
                                                                                                           main_inputs])
                self.build_scripts_process_no += 1
                #################################################################################

            except Exception as error:
                # print("1", error)
                pass
                # traceback.print_exc()
                self.count_sources = self.count_sources - 1


if __name__ == '__main__':
    start_time = dt.datetime.now()

    read_smx = ReadSmxFolder()
    read_smx.read_smx_folder()

    end_time = dt.datetime.now()
    print("\nTotal Elapsed time: ", end_time - start_time)