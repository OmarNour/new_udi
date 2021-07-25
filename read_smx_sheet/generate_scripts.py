import os, sys, subprocess

sys.path.append(os.getcwd())
from read_smx_sheet.app_Lib import manage_directories as md, functions as funcs
from dask import compute, delayed, config
from dask.diagnostics import ProgressBar
from read_smx_sheet.templates import D000
from read_smx_sheet.parameters import parameters as pm
import datetime as dt


class LogFile(funcs.WriteFile):
    def __init__(self, output_path):
        self.output_path = output_path
        self.file_name = "log"
        self.ext = "txt"
        super().__init__(self.output_path, self.file_name, self.ext, "a+", True)


class ConfigFile:
    def __init__(self, config_file=None, config_file_values=None):
        self.config_file_values = funcs.get_config_file_values(
            config_file) if config_file_values is None else config_file_values
        self.output_folder_name = self.config_file_values["output_folder_name"]
        self.output_folder_path = self.config_file_values["output_folder_path"]
        self.source_location = self.config_file_values["source_location"]
        self.destination_location = self.config_file_values["destinations_file_path"]

        try:
            self.staging_view_db = self.config_file_values["staging_view_db"]
        except:
            self.staging_view_db = ''

        try:
            self.Data_mover_flag = self.config_file_values["Data_mover_flag"]
        except:
            self.Data_mover_flag = 0
        try:
            self.scripts_flag = self.config_file_values["scripts_flag"]
        except:
            self.scripts_flag = "All"


class GenerateScripts:
    def __init__(self, config_file=None, config_file_values=None):
        self.start_time = dt.datetime.now()
        self.cf = ConfigFile(config_file, config_file_values)
        md.remove_folder(self.cf.output_folder_path)
        md.create_folder(self.cf.output_folder_path)
        self.log_file = LogFile(self.cf.output_folder_path)
        self.error_message = ""
        self.parallel_remove_output_home_path = []
        self.parallel_create_output_home_path = []
        self.parallel_create_smx_copy_path = []
        self.parallel_used_smx_copy = []
        self.parallel_create_output_source_path = []
        self.parallel_templates = []
        self.count_sources = 1
        self.count_smx = 1
        self.smx_ext = pm.smx_ext
        self.read_sheets_parallel = 1

    def generate_scripts(self):
        self.log_file.write("Reading from: \t" + self.cf.destination_location)
        self.log_file.write("Output folder: \t" + self.cf.output_folder_path)
        home_output_path = self.cf.output_folder_path
        self.parallel_create_output_home_path.append(delayed(md.create_folder)(home_output_path))
        self.parallel_templates.append(delayed(D000.parse_file)(self.cf,self.log_file))

        if len(self.parallel_templates) > 0:
            scheduler_value = 'processes' if self.read_sheets_parallel == 1 else ''
            with config.set(scheduler=scheduler_value):
                compute(*self.parallel_create_output_home_path)
                compute(*self.parallel_templates)
            self.error_message = ""
        else:
            self.error_message = "No SMX Files Found!"

        with ProgressBar():
            smx_files = " smx files" if self.count_smx > 1 else " smx file"
            smx_file_sources = " sources" if self.count_sources > 1 else " source"
            print("Start generating " + str(len(self.parallel_templates)) + " script for " + str(
                self.count_sources) + smx_file_sources + " from " + str(self.count_smx) + smx_files)
            self.log_file.write(str(len(self.parallel_templates)) + " script generated for " + str(
                self.count_sources) + smx_file_sources + " from " + str(self.count_smx) + smx_files)
            self.elapsed_time = dt.datetime.now() - self.start_time
            self.log_file.write("Elapsed Time: " + str(self.elapsed_time))

        if sys.platform == "win32":
            os.startfile(self.cf.output_folder_path)
        else:
            opener = "open" if sys.platform == "darwin" else "xdg-open"
            subprocess.call([opener, self.cf.output_folder_path])

        self.log_file.close()
