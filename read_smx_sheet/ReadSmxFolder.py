import os
import sys
sys.path.append(os.getcwd())
from parameters import parameters as pm
from dask import compute, delayed
from app_Lib import manage_directories as md
from dask.diagnostics import ProgressBar
import traceback
import datetime as dt
from read_smx_sheet.ReadSMX import ReadSmx
from templates import gcfr


class ReadSmxFolder:
    def __init__(self):
        self.count_smx = 0
        self.count_sources = 0

    def read_smx_folder(self):
        dt_now = dt.datetime.now()
        dt_folder = dt_now.strftime("%Y") + "_" + dt_now.strftime("%b").upper() + "_" + dt_now.strftime("%d") + "_" + dt_now.strftime("%H") + "_" + dt_now.strftime("%M")
        output_path = pm.output_path + "/" + dt_folder
        print("Reading from: \t", pm.smx_path)
        print("Output folder: \t", output_path)
        print(pm.smx_ext + " files:")

        parallel_remove_output_home_path = []
        parallel_create_output_home_path = []

        parallel_gcfr_all_sources = []
        parallel_read_smx_sheet = []
        for smx in md.get_files_in_dir(pm.smx_path, pm.smx_ext):
            read_smx = ReadSmx()
            self.count_smx = self.count_smx + 1
            smx_file_name = os.path.splitext(smx)[0]
            print("\t" + smx_file_name)
            home_output_path = output_path + "/" + smx_file_name + "/"
            smx_file_path = pm.smx_path + "/" + smx

            delayed_remove_home_output_path = delayed(md.remove_folder)(home_output_path)
            parallel_remove_output_home_path.append(delayed_remove_home_output_path)
            delayed_create_home_output_path = delayed(md.create_folder)(home_output_path)
            parallel_create_output_home_path.append(delayed_create_home_output_path)

            parallel_gcfr_all_sources.append(delayed(gcfr.gcfr)(home_output_path))
            delayed_read_smx_sheet = delayed(read_smx.read_smx_sheet)(home_output_path, smx_file_path)
            parallel_read_smx_sheet.append(delayed_read_smx_sheet)

        if len(parallel_read_smx_sheet) > 0:
            compute(*parallel_remove_output_home_path)
            compute(*parallel_create_output_home_path)
            compute(*parallel_gcfr_all_sources)
            # with ProgressBar():
            #     print("\nReading smx sheets...")
            compute(*parallel_read_smx_sheet)
            # with ProgressBar():
            #     print("Reading Sources...")
            #     compute(*self.parallel_read_smx_source)
            # compute(*self.parallel_create_output_source_path)
            # with ProgressBar():
            #     smx_files = " smx files" if self.count_smx > 1 else " smx file"
            #     print("Start preparing scripts for " + str(self.count_sources) + " sources from " + str(self.count_smx) + smx_files)
            #     compute(*self.parallel_build_scripts)
            #     print("Start generating " + str(len(self.parallel_templates)) + " script")
            #     compute(*self.parallel_templates)
            os.startfile(pm.output_path)


if __name__ == '__main__':
    start_time = dt.datetime.now()

    read_smx = ReadSmxFolder()
    read_smx.read_smx_folder()

    end_time = dt.datetime.now()
    print("\nTotal Elapsed time: ", end_time - start_time)