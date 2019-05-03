import os
import sys
sys.path.append(os.getcwd())
from read_smx_sheet.app_Lib import manage_directories as md, functions as funcs
import multiprocessing
from tkinter import *
from tkinter import messagebox, filedialog, ttk
from read_smx_sheet.parameters import parameters as pm
import read_smx_sheet.generate_scripts as gs
import datetime as dt
import traceback
import time
import threading
import random

global stop_threads

class FrontEnd:
    def __init__(self):
        self.root = Tk()
        self.root.wm_title("SMX Scripts Builder v.20")
        self.root.resizable(width="false", height="false")
        self.msg_no_config_file = "No Config File Found!"
        self.color_msg_no_config_file = "red"
        self.msg_ready = "Ready"
        self.color_msg_ready = "green"
        self.msg_generating = "Generating... "
        self.color_msg_generating = "blue"
        self.msg_done = "Done, Elapsed Time: "
        self.color_msg_done = "green"
        self.color_msg_done_with_error = "red"
        self.color_error_messager = "red"

        frame_row2 = Frame(self.root, borderwidth="2", relief="ridge")
        frame_row2.grid(column=0, row=2, sticky=W + E)

        self.status_label_text = StringVar()
        self.status_label = Label(frame_row2)
        self.status_label.grid(column=0, row=0, sticky=W + E)

        frame_row0 = Frame(self.root, borderwidth="2", relief="ridge")
        frame_row0.grid(column=0, row=0)

        config_file_label = Label(frame_row0, text="Config File")
        config_file_label.grid(row=0, column=0, sticky='e')
        config_file_browse_button = Button(frame_row0, text="...", command=self.browsefunc)
        config_file_browse_button.grid(row=0, column=3, sticky='w')
        self.config_file_entry_txt = StringVar()
        self.config_file_entry = Entry(frame_row0, textvariable=self.config_file_entry_txt, width=100)
        config_file_path = os.path.join(funcs.get_config_file_path(), pm.default_config_file_name)
        try:
            x = open(config_file_path)
        except:
            config_file_path = ""
        self.config_file_entry.insert(END, config_file_path)
        self.config_file_entry.grid(row=0, column=1)

        frame_row1 = Frame(self.root, borderwidth="2", relief="ridge")
        frame_row1.grid(column=0, row=1, sticky=W)

        frame_buttons = Frame(frame_row1, borderwidth="2", relief="ridge")
        frame_buttons.grid(column=1, row=0)
        self.generate_button = Button(frame_buttons, text="Generate", width=12, height=2, command=self.start)
        self.generate_button.grid(row=2, column=0)
        close_button = Button(frame_buttons, text="Close", width=12, height=2, command=self.root.destroy)
        close_button.grid(row=3, column=0)

        frame_config_file_values = Frame(frame_row1, borderwidth="2", relief="ridge")
        frame_config_file_values.grid(column=0, row=0, sticky="w")

        self.get_config_file_values()
        frame_config_file_values_entry_width = 84

        read_from_smx_label = Label(frame_config_file_values, text="SMXs Folder")
        read_from_smx_label.grid(row=0, column=0, sticky='e')

        self.text_field_read_from_smx = StringVar()
        self.entry_field_read_from_smx = Entry(frame_config_file_values, textvariable=self.text_field_read_from_smx, width=frame_config_file_values_entry_width)
        self.entry_field_read_from_smx.grid(row=0, column=1, sticky="w")

        output_path_label = Label(frame_config_file_values, text="Output Folder")
        output_path_label.grid(row=1, column=0, sticky='e')

        self.text_field_output_path = StringVar()
        self.entry_field_output_path = Entry(frame_config_file_values, textvariable=self.text_field_output_path, width=frame_config_file_values_entry_width)
        self.entry_field_output_path.grid(row=1, column=1, sticky="w")

        source_names_label = Label(frame_config_file_values, text="Sources")
        source_names_label.grid(row=2, column=0, sticky='e')

        self.text_field_source_names = StringVar()
        self.entry_field_source_names = Entry(frame_config_file_values, textvariable=self.text_field_source_names, width=frame_config_file_values_entry_width)
        self.entry_field_source_names.grid(row=2, column=1, sticky="w", columnspan=1)

        db_prefix_label = Label(frame_config_file_values, text="DB Prefix")
        db_prefix_label.grid(row=3, column=0, sticky='e')

        self.text_db_prefix = StringVar()
        self.entry_db_prefix = Entry(frame_config_file_values, textvariable=self.text_db_prefix, width=frame_config_file_values_entry_width)
        self.entry_db_prefix.grid(row=3, column=1, sticky="w", columnspan=1)

        self.populate_config_file_values()
        self.config_file_entry_txt.trace("w", self.refresh_config_file_values)

        self.root.mainloop()

    def change_status_label(self, msg, color):
        self.status_label_text.set(msg)
        self.status_label.config(fg=color, text=self.status_label_text.get())

    def get_config_file_values(self):
        try:
            self.config_file_values = funcs.get_config_file_values(self.config_file_entry_txt.get())
            self.smx_path = self.config_file_values["smx_path"]
            self.output_path = self.config_file_values["output_path"]
            source_names = self.config_file_values["source_names"]
            self.source_names = "All" if source_names is None else source_names
            self.db_prefix = self.config_file_values["db_prefix"]
            self.generate_button.config(state=NORMAL)
            self.change_status_label(self.msg_ready, self.color_msg_ready)
        except:
            self.change_status_label(self.msg_no_config_file, self.color_msg_no_config_file)
            self.generate_button.config(state=DISABLED)
            self.smx_path = ""
            self.output_path = ""
            self.source_names = ""
            self.db_prefix = ""

    def refresh_config_file_values(self, *args):
        self.get_config_file_values()
        self.populate_config_file_values()

    def populate_config_file_values(self):
        self.entry_field_read_from_smx.config(state=NORMAL)
        self.entry_field_read_from_smx.delete(0, END)
        self.entry_field_read_from_smx.insert(END, self.smx_path)
        self.entry_field_read_from_smx.config(state=DISABLED)

        self.entry_field_output_path.config(state=NORMAL)
        self.entry_field_output_path.delete(0, END)
        self.entry_field_output_path.insert(END, self.output_path)
        self.entry_field_output_path.config(state=DISABLED)

        self.entry_field_source_names.config(state=NORMAL)
        self.entry_field_source_names.delete(0, END)
        self.entry_field_source_names.insert(END, self.source_names)
        self.entry_field_source_names.config(state=DISABLED)

        self.entry_db_prefix.config(state=NORMAL)
        self.entry_db_prefix.delete(0, END)
        self.entry_db_prefix.insert(END, self.db_prefix)
        self.entry_db_prefix.config(state=DISABLED)

    def browsefunc(self):
        current_file = self.config_file_entry_txt.get()
        filename = filedialog.askopenfilename(initialdir=md.get_dirs()[1])
        filename = current_file if filename == "" else filename
        self.config_file_entry.delete(0, END)
        self.config_file_entry.insert(END, filename)
        self.refresh_config_file_values()

    def pb(self, tasks, task_len):
        self.progress_var = IntVar()
        pb = ttk.Progressbar(self.root, orient="horizontal",
                             length=300, maximum=task_len - 1,
                             mode="determinate",
                             var=self.progress_var)
        pb.grid(row=3, column=1)

        for i, task in enumerate(tasks):
            self.progress_var.set(i)
            i += 1
            # time.sleep(1 / 60)
            # compute(task)
            self.root.update_idletasks()

    def enable_disable_fields(self, f_state):
        self.generate_button.config(state=f_state)
        self.config_file_entry.config(state=f_state)

    def generate_scripts_thread(self):
        try:
            config_file_path = self.config_file_entry_txt.get()
            x = open(config_file_path)
            try:
                self.is_generating = 1
                self.refresh_config_file_values()
                self.start_time = dt.datetime.now()
                self.enable_disable_fields(DISABLED)
                self.change_status_label(self.msg_generating, self.color_msg_generating)
                self.g = gs.GenerateScripts(None, self.config_file_values)
                self.g.generate_scripts()
                self.enable_disable_fields(NORMAL)
                self.elapsed_time = dt.datetime.now() - self.start_time
                print("Total Elapsed time: ", self.elapsed_time, "\n")

            except Exception as error:
                try:
                    error_messager = self.g.error_message
                except:
                    error_messager = error
                self.change_status_label(error_messager, self.color_error_messager)
                self.generate_button.config(state=NORMAL)
                self.config_file_entry.config(state=NORMAL)
                traceback.print_exc()
        except:
            self.change_status_label(self.msg_no_config_file, self.color_msg_no_config_file)

    def start(self):
        thread1 = GenerateScriptsThread(1, "Thread-1", self)
        thread1.start()

        thread2 = GenerateScriptsThread(2, "Thread-2", self, thread1)
        thread2.start()

    def generating_indicator(self, thread):
        start_time = dt.datetime.now()
        while thread.is_alive():
            elapsed_time = dt.datetime.now() - start_time
            msg = self.msg_generating + str(elapsed_time)
            color_list = ["white", "black", "red", "green", "blue", "cyan", "yellow", "magenta"]
            color = random.choice(color_list)
            self.change_status_label(msg, color)

        message = self.g.error_message if self.g.error_message != "" else self.msg_done + str(self.elapsed_time)
        color = self.color_msg_done_with_error if self.g.error_message != "" else self.color_msg_done
        self.change_status_label(message, color)


class GenerateScriptsThread(threading.Thread):
    def __init__(self,threadID ,name, front_end_c, thread=None):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.FrontEndC = front_end_c
        self.thread = thread

    def run(self):
        if self.threadID == 1:
            self.FrontEndC.generate_scripts_thread()
        if self.threadID == 2:
            self.FrontEndC.generating_indicator(self.thread)


if __name__ == '__main__':
    multiprocessing.freeze_support()
    FrontEnd()
