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
import _thread


class FrontEnd:
    def __init__(self):
        self.root = Tk()
        self.root.wm_title("SMX Scripts Builder v.17")
        self.root.resizable(width="false", height="false")

        frame_config_file_entry = Frame(self.root, borderwidth="2", relief="ridge")
        frame_config_file_entry.grid(column=0, row=0)
        l1 = Label(frame_config_file_entry, text="Config File")
        l1.grid(row=0, column=0, sticky='e')
        browsebutton = Button(frame_config_file_entry, text="...", command=self.browsefunc)
        browsebutton.grid(row=0, column=3, sticky='w')
        self.title_text = StringVar()
        self.e1 = Entry(frame_config_file_entry, textvariable=self.title_text, width=100)
        config_file_path = os.path.join(funcs.get_config_file_path(), pm.default_config_file_name)
        try:
            x = open(config_file_path)
        except:
            config_file_path = ""
        self.e1.insert(END, config_file_path)
        self.e1.grid(row=0, column=1)

        frame_row1 = Frame(self.root, borderwidth="2", relief="ridge")
        frame_row1.grid(column=0, row=1, sticky=W)

        frame_buttons = Frame(frame_row1, borderwidth="2", relief="ridge")
        frame_buttons.grid(column=1, row=0)
        self.b1 = Button(frame_buttons, text="Generate", width=12, height=2, command=self.start)
        self.b1.grid(row=2, column=0)
        b2 = Button(frame_buttons, text="Close", width=12, height=2, command=self.root.destroy)
        b2.grid(row=3, column=0)

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
        self.title_text.trace("w", self.refresh_config_file_values)
        self.root.mainloop()

    def get_config_file_values(self):
        try:
            self.config_file_values = funcs.get_config_file_values(self.title_text.get())
            self.smx_path = self.config_file_values["smx_path"]
            self.output_path = self.config_file_values["output_path"]
            source_names = self.config_file_values["source_names"]
            self.source_names = "All" if source_names is None else source_names
            self.db_prefix = self.config_file_values["db_prefix"]
            self.b1.config(state=NORMAL)
        except:
            self.b1.config(state=DISABLED)
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
        current_file = self.title_text.get()
        filename = filedialog.askopenfilename(initialdir=md.get_dirs()[1])
        filename = current_file if filename == "" else filename
        self.e1.delete(0, END)
        self.e1.insert(END, filename)
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

    def run_thread(self):
        try:
            config_file_path = self.title_text.get()
            x = open(config_file_path)
            try:
                self.refresh_config_file_values()
                g = gs.GenerateScripts(None, self.config_file_values)
                start_time = dt.datetime.now()
                self.b1.config(state=DISABLED)
                self.e1.config(state=DISABLED)

                g.generate_scripts()
                self.b1.config(state=NORMAL)
                self.e1.config(state=NORMAL)
                end_time = dt.datetime.now()
                print("Total Elapsed time: ", end_time - start_time, "\n")

            except:
                self.b1.config(state=NORMAL)
                self.e1.config(state=NORMAL)
                traceback.print_exc()
        except:
            messagebox.showerror("Error", "Invalid File!")

    def start(self):
        _thread.start_new_thread(self.run_thread, ())


if __name__ == '__main__':
    multiprocessing.freeze_support()
    FrontEnd()
