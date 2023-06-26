import logging
# from .model import *
from functions import log_error_decorator, time_elapsed_decorator, create_folder, WriteFile, filter_dataframe, single_quotes, merge_multiple_spaces, parse_data_type
import os
import datetime as dt
from model import Server, Table, DataSource, DataBaseEngine, Ip, Credential, LayerType, Layer, LayerTable, Schema, DataSetType, Pipeline, DataType, Column, DataSet, Domain, DomainValue, ColumnMapping, Filter, GroupBy, JoinType, JoinWith, JoinOn
from config import CORE_MODEL_FOLDER_NAME, SHEETS, STG_TECHNICAL_COLS, SRC_SYSTEMS_FOLDER_NAME, DB_NAME, USER, PASSWORD, CORE_TECHNICAL_COLS, DS_BKEY, DS_BMAP, UNIFIED_SOURCE_SYSTEMS, INSERT_INTO_SOURCE_SYSTEMS, INSERT_INTO_SOURCE_SYSTEM_TABLES, INSERT_INTO_EDW_TABLES, INSERT_INTO_TRANSFORM_KEYCOL, INSERT_INTO_PROCESS, INSERT_INTO_HISTORY, BK_PROCESS_NAME_TEMPLATE, BK_VIEW_NAME_TEMPLATE, BMAP_PROCESS_NAME_TEMPLATE, BMAP_VIEW_NAME_TEMPLATE, CORE_PROCESS_NAME_TEMPLATE, CORE_VIEW_NAME_TEMPLATE, LOADING_MODE, DELETE_DATABASE_TEMPLATE, DROP_DATABASE_TEMPLATE, DATABASE_TEMPLATE, DROP_BEFORE_CREATE, DROP_TABLE_TEMPLATE
import pandas as pd
import numpy as np
from collections import namedtuple

# # PREFIX = "TEST"

class SMX:
    def __init__(self, path, run_id, output_path, db_prefix):
        self.LAYER_TYPES, self.LAYERS, self.MAIN_DB_NAME, self.MAIN_DATABASE_TEMPLATE, self.OTHER_SCHEMAS, self.GRANTS = self.set_configs(db_prefix)
        self.PREFIX = db_prefix
        self.run_id = run_id
        # self.run_id = str(generate_run_id())
        print(f"Run started with ID: {self.run_id}")
        # self.path = smx_path
        self.path = path
        # self.current_scripts_path = os.path.join(scripts_path, self.run_id)
        self.current_scripts_path = os.path.join(output_path)
        self.metadata_scripts = os.path.join(self.current_scripts_path, "metadata")
        self.log_error_path = self.current_scripts_path
        self.log_file_name = f"{self.run_id}.log"
        create_folder(self.current_scripts_path)
        create_folder(self.metadata_scripts)
        separator = "**********************************************************************************"
        logging.basicConfig(encoding='utf-8'
                            , level=logging.DEBUG
                            , format=f"[%(levelname)s] %(message)s\n{separator}\n"
                            , handlers=[logging.FileHandler(os.path.join(self.log_error_path, self.log_file_name))
                                        # ,logging.StreamHandler()
                                        ]
                            )
        logging.info(f"Run ID {self.run_id}, started at {dt.datetime.now()}\n")

        self.xls = None
        self._source_systems = []
        # self.reserved_words = {}
        self.data = {}
        self.init_model()

    def set_configs(self, db_prefix):
        PREFIX = db_prefix
        LayerDtl = namedtuple("LayerDetail", "type level v_db t_db")
        LAYER_TYPES = ['META', 'SRC', 'STG', 'SK', 'SRCI', 'CORE']  # Important: DO NOT CHANGE THE ORDER OF THIS LIST!
        LAYERS = {
            'META': LayerDtl(LAYER_TYPES[0], 0, f'{PREFIX}_ETL', f'{PREFIX}_ETL')
            , 'SRC': LayerDtl(LAYER_TYPES[1], 0, f'{PREFIX}V_STG_ONLINE', 'STG_ONLINE')
            , 'STG': LayerDtl(LAYER_TYPES[2], 1, f'{PREFIX}V_STG', f'{PREFIX}T_STG')
            , 'TXF_BKEY': LayerDtl(LAYER_TYPES[3], 2, f'{PREFIX}V_INP', '')
            , 'TXF_BMAP': LayerDtl(LAYER_TYPES[3], 2, f'{PREFIX}V_INP', '')
            , 'BKEY': LayerDtl(LAYER_TYPES[3], 3, f'{PREFIX}V_UTLFW', f'{PREFIX}T_UTLFW')
            , 'BMAP': LayerDtl(LAYER_TYPES[3], 3, f'{PREFIX}V_UTLFW', f'{PREFIX}T_UTLFW')
            , 'SRCI': LayerDtl(LAYER_TYPES[4], 4, f'{PREFIX}V_SRCI', f'{PREFIX}T_SRCI')
            , 'TXF_CORE': LayerDtl(LAYER_TYPES[4], 5, f'{PREFIX}V_INP', '')
            , 'CORE': LayerDtl(LAYER_TYPES[5], 6, f'{PREFIX}V_BASE', f'{PREFIX}T_BASE')
        }

        MAIN_DB_NAME = f"{PREFIX}_EDWH"
        MAIN_DATABASE_TEMPLATE = f"""
        CREATE DATABASE {MAIN_DB_NAME}
        AS PERMANENT = 5000e6, -- 5GB
            SPOOL = 1000e6; -- 1GB
        """
        OTHER_SCHEMAS = [f"{PREFIX}_ETL"]

        GRANTS = f"""
        GRANT CREATE PROCEDURE ON {PREFIX}_ETL TO DBC WITH GRANT OPTION;
        GRANT CREATE FUNCTION ON {PREFIX}_ETL TO DBC WITH GRANT OPTION;
        GRANT EXECUTE PROCEDURE ON {PREFIX}_ETL TO DBC WITH GRANT OPTION;
        GRANT EXECUTE FUNCTION ON {PREFIX}_ETL TO DBC WITH GRANT OPTION;

        GRANT SELECT ON STG_ONLINE TO {PREFIX}V_STG_ONLINE WITH GRANT OPTION;
        GRANT SELECT ON {PREFIX}T_STG TO {PREFIX}V_STG WITH GRANT OPTION;
        GRANT SELECT ON {PREFIX}T_UTLFW TO {PREFIX}V_UTLFW WITH GRANT OPTION;
        GRANT SELECT ON {PREFIX}V_UTLFW TO {PREFIX}V_SRCI WITH GRANT OPTION;
        GRANT SELECT ON {PREFIX}T_UTLFW TO {PREFIX}V_SRCI WITH GRANT OPTION;
        GRANT SELECT ON {PREFIX}V_STG TO {PREFIX}V_SRCI WITH GRANT OPTION;
        GRANT SELECT ON {PREFIX}T_STG TO {PREFIX}V_SRCI WITH GRANT OPTION;
        GRANT SELECT ON {PREFIX}T_STG TO {PREFIX}V_INP WITH GRANT OPTION;
        GRANT SELECT ON {PREFIX}T_SRCI TO {PREFIX}V_INP WITH GRANT OPTION;
        GRANT SELECT ON {PREFIX}T_SRCI TO {PREFIX}_ETL WITH GRANT OPTION;
        GRANT SELECT ON {PREFIX}V_SRCI TO {PREFIX}_ETL WITH GRANT OPTION;
        GRANT CREATE TABLE ON {PREFIX}T_SRCI TO {PREFIX}_ETL WITH GRANT OPTION;
        GRANT DROP TABLE ON {PREFIX}T_SRCI TO {PREFIX}_ETL WITH GRANT OPTION;
        GRANT SELECT ON {PREFIX}T_BASE TO {PREFIX}V_BASE WITH GRANT OPTION;

        GRANT SELECT ON DBC TO {PREFIX}_ETL WITH GRANT OPTION;
        GRANT SELECT ON {PREFIX}V_INP TO {PREFIX}_ETL WITH GRANT OPTION;
        GRANT SELECT ON {PREFIX}T_UTLFW TO {PREFIX}_ETL WITH GRANT OPTION;
        GRANT INSERT ON {PREFIX}T_UTLFW TO {PREFIX}_ETL WITH GRANT OPTION;
        GRANT SELECT ON {PREFIX}T_BASE TO {PREFIX}_ETL WITH GRANT OPTION;
        GRANT DELETE ON {PREFIX}T_BASE TO {PREFIX}_ETL WITH GRANT OPTION;
        GRANT UPDATE ON {PREFIX}T_BASE TO {PREFIX}_ETL WITH GRANT OPTION;
        GRANT INSERT ON {PREFIX}T_BASE TO {PREFIX}_ETL WITH GRANT OPTION;
        GRANT SELECT ON {PREFIX}V_BASE TO {PREFIX}_ETL WITH GRANT OPTION;
        GRANT SELECT ON {PREFIX}V_STG_ONLINE TO {PREFIX}_ETL WITH GRANT OPTION;
        GRANT SELECT ON {PREFIX}T_STG TO {PREFIX}_ETL WITH GRANT OPTION;
        GRANT DELETE ON {PREFIX}T_STG TO {PREFIX}_ETL WITH GRANT OPTION;
        GRANT UPDATE ON {PREFIX}T_STG TO {PREFIX}_ETL WITH GRANT OPTION;
        GRANT INSERT ON {PREFIX}T_STG TO {PREFIX}_ETL WITH GRANT OPTION;
        GRANT EXECUTE PROCEDURE ON {PREFIX}_ETL TO {PREFIX}_ETL WITH GRANT OPTION;
        GRANT EXECUTE FUNCTION ON {PREFIX}_ETL TO {PREFIX}_ETL WITH GRANT OPTION;
        """
        return LAYER_TYPES, LAYERS, MAIN_DB_NAME, MAIN_DATABASE_TEMPLATE, OTHER_SCHEMAS, GRANTS


    def __del__(self):
        print("I am a destructor")
        # logger = logging.getLogger()
        # for handler in list(logger.handlers):
        #     print(handler)
        #     logger.removeHandler(handler) 
        #     handler.close()
            
        
        # logger.close()
        
        # logging.shutdown()




    @log_error_decorator()
    def init_model(self):
        self.server = Server(server_name='TDVM')
        self.db_engine = DataBaseEngine(server_id=self.server.id, name=DB_NAME)
        Ip(server_id=self.server.id, ip='localhost')
        Credential(db_engine_id=self.db_engine.id, user_name=USER, password=PASSWORD)
        # print(self.conn)
        [LayerType(type_name=lt) for lt in self.LAYER_TYPES]
        for layer_key, layer_value in self.LAYERS.items():
            layer_type = LayerType.get_instance(_key=layer_value.type)
            Layer(type_id=layer_type.id, layer_name=layer_key, abbrev=layer_key, layer_level=layer_value.level)
            if layer_value.t_db:
                Schema(db_id=self.db_engine.id, schema_name=layer_value.t_db, main_db_name=self.MAIN_DB_NAME, _override=1)
            if layer_value.v_db:
                Schema(db_id=self.db_engine.id, schema_name=layer_value.v_db, main_db_name=self.MAIN_DB_NAME, _override=1)

        for other_schema in self.OTHER_SCHEMAS:
            Schema(db_id=self.db_engine.id, schema_name=other_schema, main_db_name=self.MAIN_DB_NAME, _override=1)

        DataSetType(name=DS_BKEY)
        DataSetType(name=DS_BMAP)

        self.bkey_set_type = DataSetType.get_instance(_key=DS_BKEY)
        self.bmap_set_type = DataSetType.get_instance(_key=DS_BMAP)

        self.src_layer = Layer.get_instance(_key='SRC')
        self.stg_layer = Layer.get_instance(_key='STG')
        self.txf_bkey_layer = Layer.get_instance(_key='TXF_BKEY')
        self.txf_bmap_layer = Layer.get_instance(_key='TXF_BMAP')
        self.bmap_layer = Layer.get_instance(_key='BMAP')
        self.bkey_layer = Layer.get_instance(_key='BKEY')
        self.srci_layer = Layer.get_instance(_key='SRCI')
        self.txf_core_layer = Layer.get_instance(_key='TXF_CORE')
        self.core_layer = Layer.get_instance(_key='CORE')

        self.meta_t_schema = Schema.get_instance(_key=(self.db_engine.id, self.LAYERS['META'].t_db))
        self.src_t_schema = Schema.get_instance(_key=(self.db_engine.id, self.LAYERS['SRC'].t_db))
        self.stg_t_schema = Schema.get_instance(_key=(self.db_engine.id, self.LAYERS['STG'].t_db))
        self.bmap_t_schema = Schema.get_instance(_key=(self.db_engine.id, self.LAYERS['BMAP'].t_db))
        self.bkey_t_schema = Schema.get_instance(_key=(self.db_engine.id, self.LAYERS['BKEY'].t_db))
        self.srci_t_schema = Schema.get_instance(_key=(self.db_engine.id, self.LAYERS['SRCI'].t_db))
        self.core_t_schema = Schema.get_instance(_key=(self.db_engine.id, self.LAYERS['CORE'].t_db))

        self.meta_v_schema = Schema.get_instance(_key=(self.db_engine.id, self.LAYERS['META'].v_db))
        self.src_v_schema = Schema.get_instance(_key=(self.db_engine.id, self.LAYERS['SRC'].v_db))
        self.stg_v_schema = Schema.get_instance(_key=(self.db_engine.id, self.LAYERS['STG'].v_db))
        self.bmap_v_schema = Schema.get_instance(_key=(self.db_engine.id, self.LAYERS['BMAP'].v_db))
        self.bkey_v_schema = Schema.get_instance(_key=(self.db_engine.id, self.LAYERS['BKEY'].v_db))
        self.txf_bkey_v_schema = Schema.get_instance(_key=(self.db_engine.id, self.LAYERS['TXF_BKEY'].v_db))
        self.txf_bmap_v_schema = Schema.get_instance(_key=(self.db_engine.id, self.LAYERS['TXF_BMAP'].v_db))
        self.srci_v_schema = Schema.get_instance(_key=(self.db_engine.id, self.LAYERS['SRCI'].v_db))
        self.txf_core_v_schema = Schema.get_instance(_key=(self.db_engine.id, self.LAYERS['TXF_CORE'].v_db))
        self.core_v_schema = Schema.get_instance(_key=(self.db_engine.id, self.LAYERS['CORE'].v_db))

    @log_error_decorator()
    @time_elapsed_decorator
    def parse_file(self):
        self.xls = pd.ExcelFile(self.path)
        for sheet in self.xls.sheet_names:
            self.parse_sheet(sheet)

        if 'supplements' in self.data.keys():
            reserved_words_df = self.data['supplements'].applymap(lambda x: x.lower())
            self.db_engine.reserved_words = reserved_words_df[reserved_words_df['reserved_words_source'] == self.db_engine.name]['reserved_words'].unique().tolist()

    @log_error_decorator()
    def parse_sheet(self, sheet):
        sheet_name = sheet.replace('  ', ' ').replace(' ', '_').lower()
        if sheet_name in SHEETS:
            df = self.xls.parse(sheet, encoding='utf-8').replace(np.nan, value='', regex=True)
            df = df.applymap(lambda x: x.replace('\ufeff', '').replace('`', '').strip() if type(x) is str else int(x) if type(x) is float else x)
            df.drop_duplicates()
            df.columns = [c.replace('  ', ' ').replace(' ', '_').lower() for c in df]
            self.data[sheet_name] = df

    @property
    def sheet_names(self):
        return self.data.keys()

    @property
    def source_systems(self):
        return self._source_systems

    @log_error_decorator()
    @time_elapsed_decorator
    def populate_model(self, source_name):
        @log_error_decorator()
        def extract_all():
            @log_error_decorator()
            def extract_system(row):
                ds = DataSource(source_name=row.schema, source_level=1, scheduled=1)
                self._source_systems.append(ds.source_name)

            @log_error_decorator()
            def extract_data_types(row):
                data_type_lst = row.data_type.split(sep='(')
                DataType(db_id=self.db_engine.id, dt_name=data_type_lst[0], _override=1)

            @log_error_decorator()
            def extract_stg_tables(row):
                if row.table_name != '':
                    ds = DataSource.get_instance(_key=row.schema)
                    if ds:
                        src_t = Table(schema_id=self.src_t_schema.id, table_name=row.table_name, table_kind='T', source_id=ds.id)
                        stg_t = Table(schema_id=self.stg_t_schema.id, table_name=row.table_name, table_kind='T', source_id=ds.id)
                        srci_t = Table(schema_id=self.srci_t_schema.id, table_name=row.table_name, table_kind='T', source_id=ds.id)

                        src_lt = LayerTable(layer_id=self.src_layer.id, table_id=src_t.id)
                        stg_lt = LayerTable(layer_id=self.stg_layer.id, table_id=stg_t.id)
                        srci_lt = LayerTable(layer_id=self.srci_layer.id, table_id=srci_t.id)
                        #############################################################
                        src_v = Table(schema_id=self.src_v_schema.id, table_name=row.table_name, table_kind='V', source_id=ds.id)
                        src_lv = LayerTable(layer_id=self.src_layer.id, table_id=src_v.id)
                        Pipeline(src_lyr_table_id=src_lt.id, tgt_lyr_table_id=stg_lt.id, lyr_view_id=src_lv.id)
                        #############################################################
                        stg_v = Table(schema_id=self.stg_v_schema.id, table_name=row.table_name, table_kind='V', source_id=ds.id)
                        stg_lv = LayerTable(layer_id=self.stg_layer.id, table_id=stg_v.id)
                        Pipeline(src_lyr_table_id=stg_lt.id, tgt_lyr_table_id=None, lyr_view_id=stg_lv.id, select_asterisk=True)
                        #############################################################
                        srci_v = Table(schema_id=self.srci_v_schema.id, table_name=row.table_name, table_kind='V', source_id=ds.id)
                        srci_vl = LayerTable(layer_id=self.srci_layer.id, table_id=srci_v.id)
                        Pipeline(src_lyr_table_id=stg_lt.id, tgt_lyr_table_id=srci_lt.id, lyr_view_id=srci_vl.id)
                    else:
                        logging.error(f""" Invalid Source name, {row.schema}, processing row:\n{row}""")

            @log_error_decorator()
            def extract_core_tables(row):
                history_table = True if row.table_name in history_tables_lst else False
                is_lkp = True if row.table_name in _core_tables_bmap else False
                table = Table(schema_id=self.core_t_schema.id, table_name=row.table_name
                              , table_kind='T', history_table=history_table, is_lkp=is_lkp)
                LayerTable(layer_id=self.core_layer.id, table_id=table.id)

            @log_error_decorator()
            def extract_stg_srci_table_columns(row):
                if row.column_name != '':
                    src_table = Table.get_instance(_key=(self.src_t_schema.id, row.table_name))
                    stg_table = Table.get_instance(_key=(self.stg_t_schema.id, row.table_name))
                    srci_table = Table.get_instance(_key=(self.srci_t_schema.id, row.table_name))

                    data_type_lst = row.data_type.split(sep='(')
                    _data_type = data_type_lst[0]
                    data_type = DataType.get_instance(_key=(self.db_engine.id, _data_type))
                    if not data_type:
                        logging.error(f"Invalid data type, processing row:\n{row}")
                    else:
                        pk = 1 if row.pk.upper() == 'Y' else 0
                        mandatory = 1 if row.pk.upper() == 'Y' else 0
                        precision = data_type_lst[1].split(sep=')')[0] if len(data_type_lst) > 1 else None
                        domain_id = None

                        if row.natural_key == '':
                            if src_table:
                                Column(table_id=src_table.id, column_name=row.column_name, is_pk=pk, mandatory=mandatory, data_type_id=data_type.id, dt_precision=precision)

                            if stg_table:
                                Column(table_id=stg_table.id, column_name=row.column_name, is_pk=pk, mandatory=mandatory, data_type_id=data_type.id, dt_precision=precision)
                        else:
                            domain = None
                            domain_error_msg = f"Domain not found for {row.table_name}.{row.column_name} column, please check 'Stg tables' sheet!"
                            if row.key_set_name and row.key_domain_name:

                                data_set = DataSet.get_by_name(self.bkey_set_type.id, row.key_set_name)
                                if not data_set:
                                    logging.error(f"invalid key set name '{row.key_set_name}', processing row:\n{row}")
                                else:
                                    domain = Domain.get_by_name(data_set.id, row.key_domain_name)
                                    domain_error_msg = f"Bkey Domain not found for {row.table_name}.{row.column_name} column, " \
                                                       f"\n key set name:'{row.key_set_name}'" \
                                                       f"\n key domain name: '{row.key_domain_name}'" \
                                                       f"\n please check 'Stg tables' sheet!"

                            elif row.code_set_name and row.code_domain_name:

                                data_set = DataSet.get_by_name(self.bmap_set_type.id, row.code_set_name)
                                if not data_set:
                                    logging.error(f"invalid code set name '{row.code_set_name}', processing row:\n{row}")
                                else:
                                    domain = Domain.get_by_name(data_set.id, row.code_domain_name)
                                    domain_error_msg = f"Bmap Domain not found for {row.table_name}.{row.column_name} column, " \
                                                       f"\n code set name:'{row.code_set_name}'" \
                                                       f"\n code domain name: '{row.code_domain_name}'" \
                                                       f"\n please check 'Stg tables' sheet!"

                            if domain:
                                domain_id = domain.id
                            else:
                                logging.error(f"{domain_error_msg}, processing row:\n{row}")

                        if srci_table:
                            Column(table_id=srci_table.id, column_name=row.column_name, is_pk=pk, mandatory=mandatory
                                   , data_type_id=data_type.id, dt_precision=precision, domain_id=domain_id)

            @log_error_decorator()
            def extract_src_view_columns(row):
                if row.natural_key == '' and row.column_name != '':
                    src_table = Table.get_instance(_key=(self.src_t_schema.id, row.table_name))
                    stg_table = Table.get_instance(_key=(self.stg_t_schema.id, row.table_name))

                    src_v = Table.get_instance(_key=(self.src_v_schema.id, row.table_name))
                    src_lv = LayerTable.get_instance(_key=(self.src_layer.id, src_v.id))

                    pipeline = Pipeline.get_instance(_key=src_lv.id)
                    src_col = Column.get_instance(_key=(src_table.id, row.column_name))
                    stg_col = Column.get_instance(_key=(stg_table.id, row.column_name))

                    if src_col and stg_col:
                        ColumnMapping(pipeline_id=pipeline.id
                                      , col_seq=0
                                      , src_col_id=src_col.id
                                      , tgt_col_id=stg_col.id
                                      , src_col_trx=row.column_transformation_rule
                                      )
                    else:
                        logging.error(f"Invalid column '{row.column_name}', processing row:\n{row}")

            # @log_error_decorator()
            # def extract_stg_view_columns(row):
            #     if row.natural_key == '':
            #         stg_t = Table.get_instance(_key=(self.stg_t_schema.id, row.table_name))
            #         stg_col = Column.get_instance(_key=(stg_t.id, row.column_name))
            #
            #         stg_v = Table.get_instance(_key=(self.stg_v_schema.id, row.table_name))
            #         stg_lv = LayerTable.get_instance(_key=(self.stg_layer.id, stg_v.id))
            #
            #         pipeline = Pipeline.get_instance(_key=stg_lv.id)
            #
            #         if stg_col:
            #             ColumnMapping(pipeline_id=pipeline.id
            #                           , col_seq=0
            #                           , src_col_id=stg_col.id
            #                           , tgt_col_id=stg_col.id
            #                           , src_col_trx=None
            #                           )
            #         else:
            #             logging.error(f"Invalid column '{row.column_name}', processing row:\n{row}")

            @log_error_decorator()
            def extract_core_columns(row):
                if row.column_name != '':
                    core_table: Table
                    core_table = Table.get_instance(_key=(self.core_t_schema.id, row.table_name))
                    data_type_lst = row.data_type.split(sep='(')
                    _data_type = data_type_lst[0]
                    data_type = DataType.get_instance(_key=(self.db_engine.id, _data_type))
                    if data_type:

                        if core_table.history_table:
                            is_start_date = 1 if row.historization_key.upper() == 'S' else 0
                            is_end_date = 1 if row.historization_key.upper() == 'E' else 0
                            pk = 1 if (row.historization_key.upper() == 'Y' or is_start_date) else 0
                        else:
                            is_start_date = 0
                            is_end_date = 0
                            pk = 1 if row.pk.upper() == 'Y' else 0

                        mandatory = 1 if (row.mandatory.upper() == 'Y' or pk) else 0
                        precision = data_type_lst[1].split(sep=')')[0] if len(data_type_lst) > 1 else None

                        # if is_start_date and not pk:
                        #     logging.error(f"Start date column '{row.column_name}', should be primary key as well!, processing row:\n{row}")
                        unicode = False
                        if core_table.is_lkp:
                            unicode = True
                        Column(table_id=core_table.id, column_name=row.column_name, is_pk=pk, mandatory=mandatory
                               , data_type_id=data_type.id, dt_precision=precision
                               , is_start_date=is_start_date, is_end_date=is_end_date, unicode=unicode)

            @log_error_decorator()
            def extract_bkey_tables(row):
                if row.physical_table != '':
                    table = Table(schema_id=self.bkey_t_schema.id, table_name=row.physical_table, table_kind='T')
                    LayerTable(layer_id=self.bkey_layer.id, table_id=table.id)

                    Column(table_id=table.id, column_name='SOURCE_KEY', is_pk=1, mandatory=1
                           , data_type_id=vchar_data_type.id, dt_precision=100
                           , is_start_date=0, is_end_date=0)

                    Column(table_id=table.id, column_name='DOMAIN_ID', is_pk=1, mandatory=1
                           , data_type_id=int_data_type.id, dt_precision=None
                           , is_start_date=0, is_end_date=0)

                    Column(table_id=table.id, column_name='EDW_KEY', is_pk=0, mandatory=1
                           , data_type_id=int_data_type.id, dt_precision=None
                           , is_start_date=0, is_end_date=0)

                    Column(table_id=table.id, column_name='KEY_SET_ID', is_pk=0, mandatory=1
                           , data_type_id=int_data_type.id, dt_precision=None
                           , is_start_date=0, is_end_date=0)

                    Column(table_id=table.id, column_name='START_DATE', is_pk=0, mandatory=1
                           , data_type_id=timestamp_data_type.id, dt_precision=None
                           , is_start_date=1, is_end_date=0)

                    Column(table_id=table.id, column_name='PROCESS_NAME', is_pk=0, mandatory=1
                           , data_type_id=vchar_data_type.id, dt_precision=200
                           , is_start_date=0, is_end_date=0)

            @log_error_decorator()
            def extract_bkey_datasets(row):
                if row.physical_table != '':
                    surrogate_table: Table
                    set_table: Table
                    ds :DataSet
                    surrogate_table = Table.get_instance(_key=(self.bkey_t_schema.id, row.physical_table))
                    set_table = Table.get_instance(_key=(self.core_t_schema.id, row.key_set_name))

                    ds = DataSet.get_by_name(self.bkey_set_type.id, row.key_set_name)
                    valid_ds = True
                    if ds:
                        if ds.set_code != row.key_set_id:
                            valid_ds = False
                            logging.error(f"Duplicate dataset '{row.key_set_id}', processing row:\n{row}")

                    if valid_ds:
                        if set_table and surrogate_table:
                            DataSet(set_type_id=self.bkey_set_type.id, set_code=row.key_set_id, set_table_id=set_table.id, surrogate_table_id=surrogate_table.id)
                            surrogate_table.is_bkey = True
                        else:
                            logging.error(f"Invalid set table {row.key_set_name} or surrogate table {row.physical_table}, processing row:\n {row}")

            @log_error_decorator()
            def extract_bkey_domains(row):
                data_set = DataSet.get_instance(_key=(self.bkey_set_type.id, row.key_set_id))
                if data_set and row.key_domain_id:
                    Domain(data_set_id=data_set.id, domain_code=row.key_domain_id, domain_name=row.key_domain_name)
                else:
                    logging.error(f"invalid data set {row.key_set_id} or domain ID, processing row:\n{row}")

            @log_error_decorator()
            def extract_bmap_tables(row):
                if row.physical_table != '':
                    table = Table(schema_id=self.bmap_t_schema.id, table_name=row.physical_table, table_kind='T')
                    LayerTable(layer_id=self.bmap_layer.id, table_id=table.id)

                    Column(table_id=table.id, column_name='SOURCE_CODE', is_pk=1, mandatory=1
                           , data_type_id=vchar_data_type.id, dt_precision=50
                           , is_start_date=0, is_end_date=0)

                    Column(table_id=table.id, column_name='DOMAIN_ID', is_pk=1, mandatory=1
                           , data_type_id=int_data_type.id, dt_precision=None
                           , is_start_date=0, is_end_date=0)

                    Column(table_id=table.id, column_name='CODE_SET_ID', is_pk=1, mandatory=1
                           , data_type_id=int_data_type.id, dt_precision=None
                           , is_start_date=0, is_end_date=0)

                    Column(table_id=table.id, column_name='EDW_CODE', is_pk=0, mandatory=1
                           , data_type_id=int_data_type.id, dt_precision=None
                           , is_start_date=0, is_end_date=0)

                    Column(table_id=table.id, column_name='DESCRIPTION', is_pk=0, mandatory=1
                           , data_type_id=vchar_data_type.id, dt_precision=None
                           , is_start_date=0, is_end_date=0, unicode=1)

            @log_error_decorator()
            def extract_bmap_datasets(row):
                if row.physical_table != '':
                    set_table: Table
                    surrogate_table: Table
                    surrogate_table = Table.get_instance(_key=(self.bkey_t_schema.id, row.physical_table))
                    set_table = Table.get_instance(_key=(self.core_t_schema.id, row.code_set_name))

                    ds = DataSet.get_by_name(self.bmap_set_type.id, row.code_set_name)
                    valid_ds = True
                    if ds:
                        if ds.set_code != row.code_set_id:
                            valid_ds = False
                            logging.error(f"Duplicate dataset '{row.code_set_id}', processing row:\n{row}")

                    if valid_ds:
                        if set_table and row.code_set_id and surrogate_table:
                            DataSet(set_type_id=self.bmap_set_type.id, set_code=row.code_set_id, set_table_id=set_table.id, surrogate_table_id=surrogate_table.id)
                            # set_table.is_lkp = True
                            surrogate_table.is_bmap = True
                        else:
                            logging.error(f"Invalid set table '{row.code_set_name}' or surrogate table '{row.physical_table}', processing row:\n{row}")

            @log_error_decorator()
            def extract_bmap_domains(row):
                data_set = DataSet.get_instance(_key=(self.bmap_set_type.id, row.code_set_id))
                if data_set and row.code_domain_id:
                    Domain(data_set_id=data_set.id, domain_code=row.code_domain_id, domain_name=row.code_domain_name)
                else:
                    logging.error(f"invalid data set or domain ID, processing row:\n{row}")

            @log_error_decorator()
            def extract_bmap_values(row):
                ds: DataSet
                data_set_lst = [ds for ds in bmaps_data_sets if ds.set_table.table_name == row.code_set_name.upper()]
                if len(data_set_lst) > 0:
                    data_set = data_set_lst[0]
                    domain = Domain.get_instance(_key=(data_set.id, row.code_domain_id))
                    if domain:
                        DomainValue(domain_id=domain.id, source_key=row.source_code, edw_key=row.edw_code, description=row.description)

            @log_error_decorator()
            def extract_bmap_views(row):
                domain: Domain
                stg_col: Column
                stg_t: Table
                stg_lt: LayerTable
                srci_col: Column
                if row.code_domain_name != '':
                    ds = DataSource.get_instance(_key=row.schema)
                    if not ds:
                        logging.error(f"Invalid source name '{row.schema}', processing row:\n{row}")
                    else:
                        data_set = DataSet.get_by_name(self.bmap_set_type.id, row.code_set_name)
                        if not data_set:
                            logging.error(f"Invalid code set '{row.code_set_name}', processing row:\n{row}")
                        else:
                            domain = Domain.get_by_name(data_set_id=data_set.id, domain_name=row.code_domain_name)
                            if not domain:
                                logging.error(f"Invalid code domain name '{row.code_domain_name}', processing row:\n{row}")
                            else:
                                stg_t = Table.get_instance(_key=(self.stg_t_schema.id, row.table_name))
                                stg_lt = LayerTable.get_instance(_key=(self.stg_layer.id, stg_t.id))

                                srci_t = Table.get_instance(_key=(self.srci_t_schema.id, row.table_name))
                                srci_col = Column.get_instance(_key=(srci_t.id, row.column_name))

                                bmap_process_name = BMAP_PROCESS_NAME_TEMPLATE.format(set_id=data_set.set_code
                                                                                      , src_table_name=stg_t.table_name
                                                                                      , column_name=srci_col.column_name
                                                                                      , domain_id=srci_col.domain.domain_code
                                                                                      )
                                bmap_view_name = BMAP_VIEW_NAME_TEMPLATE.format(view_name=bmap_process_name)
                                bmap_v = Table(schema_id=self.txf_bmap_v_schema.id, table_name=bmap_view_name, table_kind='V', source_id=ds.id)
                                bmap_vl = LayerTable(layer_id=self.txf_bmap_layer.id, table_id=bmap_v.id)

                                Pipeline(src_lyr_table_id=stg_lt.id
                                         , tgt_lyr_table_id=domain.data_set.surrogate_table.id
                                         , lyr_view_id=bmap_vl.id
                                         , domain_id=srci_col.domain.id
                                         , name=bmap_process_name
                                         , distinct=True)

            @log_error_decorator()
            def extract_bkey_views(row):
                if row.natural_key != '' and row.key_domain_name != '':
                    ds = DataSource.get_instance(_key=row.schema)
                    if not ds:
                        logging.error(f"Invalid source name '{row.schema}', processing row:\n{row}")
                    else:
                        domain: Domain
                        stg_col: Column
                        stg_t: Table
                        stg_lt: LayerTable
                        srci_col: Column

                        data_set = DataSet.get_by_name(self.bkey_set_type.id, row.key_set_name)
                        if not data_set:
                            logging.error(f"Invalid key set '{row.key_set_name}', processing row:\n{row}")
                        else:
                            domain = Domain.get_by_name(data_set_id=data_set.id, domain_name=row.key_domain_name)
                            if not domain:
                                logging.error(f"Invalid key domain name '{row.key_domain_name}', processing row:\n{row}")
                            else:
                                stg_t = Table.get_instance(_key=(self.stg_t_schema.id, row.table_name))
                                stg_lt = LayerTable.get_instance(_key=(self.stg_layer.id, stg_t.id))

                                srci_t = Table.get_instance(_key=(self.srci_t_schema.id, row.table_name))
                                srci_col = Column.get_instance(_key=(srci_t.id, row.column_name))

                                if data_set and stg_t and srci_col:
                                    bk_process_name = BK_PROCESS_NAME_TEMPLATE.format(set_id=data_set.set_code
                                                                                      , src_table_name=stg_t.table_name
                                                                                      , column_name=srci_col.column_name
                                                                                      , domain_id=srci_col.domain.domain_code
                                                                                      )
                                    bk_view_name = BK_VIEW_NAME_TEMPLATE.format(view_name=bk_process_name)
                                    bkey_v = Table(schema_id=self.txf_bkey_v_schema.id, table_name=bk_view_name, table_kind='V', source_id=ds.id)
                                    bkey_vl = LayerTable(layer_id=self.txf_bkey_layer.id, table_id=bkey_v.id)

                                    bkey_pipeline = Pipeline(src_lyr_table_id=stg_lt.id
                                                             , tgt_lyr_table_id=domain.data_set.surrogate_table.id
                                                             , lyr_view_id=bkey_vl.id
                                                             , domain_id=srci_col.domain.id
                                                             , name=bk_process_name)
                                    if row.bkey_filter:
                                        Filter(pipeline_id=bkey_pipeline.id, filter_seq=1, complete_filter_expr=row.bkey_filter)
                                else:
                                    logging.error(f"Invalid key set '{row.key_set_name}', processing row:\n{row}")

            @log_error_decorator()
            def extract_srci_view_columns(row):
                if row.column_name != '':
                    stg_t = Table.get_instance(_key=(self.stg_t_schema.id, row.table_name))
                    # stg_lt = LayerTable.get_instance(_key=(self.stg_layer.id, stg_t.id))
                    stg_t_col = None

                    srci_t = Table.get_instance(_key=(self.srci_t_schema.id, row.table_name))
                    srci_t_col = Column.get_instance(_key=(srci_t.id, row.column_name))

                    if row.natural_key != '':
                        if row.key_set_name != '':
                            data_set = DataSet.get_by_name(self.bkey_set_type.id, row.key_set_name)
                            if not data_set:
                                logging.error(f"key set '{row.key_set_name}', is not defined, please check the 'BKEY' sheet!, processing row:\n{row}")
                            else:
                                domain = Domain.get_by_name(data_set_id=data_set.id, domain_name=row.key_domain_name)
                                if not domain:
                                    logging.error(f"""{row.key_domain_name}, is not defined, please check the 'BKEY' sheet!, processing row:\n{row}""")
                                else:
                                    txf_process_name = BK_PROCESS_NAME_TEMPLATE.format(set_id=data_set.set_code
                                                                                       , src_table_name=stg_t.table_name
                                                                                       , column_name=srci_t_col.column_name
                                                                                       , domain_id=srci_t_col.domain.domain_code
                                                                                       )
                                    txf_view_name = BK_VIEW_NAME_TEMPLATE.format(view_name=txf_process_name)
                                    # txf_view_name = f"BKEY_{row.table_name}_{row.column_name}_{srci_t_col.domain.domain_code}"
                                    txf_v = Table.get_instance(_key=(self.txf_bkey_v_schema.id, txf_view_name))
                                    txf_bkey_lt = LayerTable.get_instance(_key=(self.txf_bkey_layer.id, txf_v.id))
                                    bkey_txf_pipeline = Pipeline.get_instance(_key=txf_bkey_lt.id)

                                    # for col_seq, stg_t_col in enumerate(stg_t_cols):
                                    source_key_col = Column.get_instance(_key=(domain.data_set.surrogate_table.id, 'source_key'))
                                    ColumnMapping(pipeline_id=bkey_txf_pipeline.id
                                                  , col_seq=0
                                                  , src_col_id=None
                                                  , tgt_col_id=source_key_col.id
                                                  , src_col_trx=row.natural_key
                                                  )
                                    GroupBy(pipeline_id=bkey_txf_pipeline.id, col_id=source_key_col.id)

                    else:
                        stg_t_col = Column.get_instance(_key=(stg_t.id, row.column_name))
                        if not stg_t_col:
                            logging.error(f"Invalid column {row.column_name}, processing row:\n{row}")

                    if not srci_t_col:
                        logging.error(f"Invalid column {row.column_name}, processing row:\n{row}")
                    else:
                        srci_v = Table.get_instance(_key=(self.srci_v_schema.id, row.table_name))
                        srci_vl = LayerTable.get_instance(_key=(self.srci_layer.id, srci_v.id))

                        srci_v_pipeline = Pipeline.get_instance(_key=srci_vl.id)
                        ColumnMapping(pipeline_id=srci_v_pipeline.id
                                      , col_seq=0
                                      , src_col_id=stg_t_col.id if stg_t_col else None
                                      , tgt_col_id=srci_t_col.id
                                      , src_col_trx=row.natural_key if row.natural_key else None
                                      )

            @log_error_decorator()
            def extract_core_txf_views(row):
                def parse_join(join_txt: str):
                    join_type: JoinType
                    with_srci_lt: LayerTable

                    join_type = None
                    _join_txt = ' ' + merge_multiple_spaces(join_txt).lower() + ' '
                    if (' select ' or ' sel ' or '(sel ' or '(select ') in _join_txt:
                        logging.error(f"Query cannot be found in join!, processing row:\n{row}")
                    _join_txt = _join_txt.replace(' inner ', ' ').replace(' outer ', ' ').strip()

                    new_input_join = 'join '

                    _split = _join_txt.split(' ', 1)
                    if len(_split) >= 2:
                        # print("===-=-=-=-=-=-=-=-==-=-=-====")
                        if _split[0].lower() == 'join':
                            # print('inner join found')
                            join_type = JoinType.get_instance(_key='ij')
                        elif _split[0].lower() == 'left':
                            # print('left join found')
                            join_type = JoinType.get_instance(_key='lj')
                        elif _split[0].lower() == 'right':
                            # print('right join found')
                            join_type = JoinType.get_instance(_key='rj')
                        elif _split[0].lower() == 'full':
                            # print('full outer join found')
                            join_type = JoinType.get_instance(_key='fj')

                        if not join_type:
                            logging.error(f"No join found!, processing row:\n{row}")

                        _split = _split[1].split(' on ', 1)
                        # print('on split: ', _split)
                        if len(_split) >= 2:
                            _split_0 = (' ' + _split[0] + ' ').replace(' join ', '')
                            _split_1 = _split[1]

                            table__alias = merge_multiple_spaces(_split_0).split(' ', 1)
                            table_name = table__alias[0]
                            table_alias = table__alias[1] if len(table__alias) >= 2 else table_name
                            with_srci_t = Table.get_instance(_key=(self.srci_t_schema.id, table_name))

                            if not with_srci_t:
                                logging.error(f"invalid join table '{table_name}', processing row:\n{row}")
                            else:
                                with_srci_lt = LayerTable.get_instance(_key=(self.srci_layer.id, with_srci_t.id))
                                # print(f"table name {table_name}, alias {table_alias}")
                                join_with = JoinWith(pipeline_id=core_pipeline.id
                                                     , master_lyr_table_id=srci_lt.id
                                                     , master_alias=core_pipeline.src_table_alias
                                                     , join_type_id=join_type.id
                                                     , with_lyr_table_id=with_srci_lt.id
                                                     , with_alias=table_alias)

                                _split = _split_1.split(' join ', 1)
                                join_on = _split[0]
                                if join_on.endswith(' left'):
                                    join_on = join_on.removesuffix(' left')
                                    new_input_join = 'left join '
                                elif join_on.endswith(' full'):
                                    join_on = join_on.removesuffix(' full')
                                    new_input_join = 'full join '
                                elif join_on.endswith(' right'):
                                    join_on = join_on.removesuffix(' right')
                                    new_input_join = 'right join '

                                # print(f"joined on {join_on}")
                                JoinOn(join_with_id=join_with.id, complete_join_on_expr=join_on)

                                # print("===-=-=-=-=-=-=-=-==-=-=-====")
                                if len(_split) >= 2:
                                    parse_join(new_input_join + _split[1])

                @log_error_decorator()
                def column_mapping(_row):
                    # 'column_name', 'mapped_to_table', 'mapped_to_column', 'transformation_rule', 'transformation_type'
                    # transformation_type: COPY, SQL, CONST

                    tgt_col: Column
                    transformation_type = _row.transformation_type.upper()
                    if transformation_type not in ('COPY', 'SQL', 'CONST'):
                        logging.error(f"Transformation Type, should be one of the following COPY, SQL or CONST, processing row:\n{_row}")
                    else:
                        tgt_col = Column.get_instance(_key=(core_t.id, _row.column_name))
                        if not tgt_col:
                            logging.error(f"Invalid Target Column Name, {_row.column_name}, processing row:\n{_row}")
                        else:
                            src_col = None
                            src_table_alias = None
                            src_table: Table
                            scd_type = 1
                            if row.historization_algorithm.upper() == 'INSERT':
                                scd_type = 0
                            elif row.historization_algorithm.upper() in ('UPSERT', 'UPSERTDELETE'):
                                scd_type = 1
                            elif row.historization_algorithm.upper() == 'HISTORY':
                                scd_type = 2 if _row.column_name in row.historization_columns else 1
                            else:
                                logging.error(f"Invalid historization algorithm, processing row:\n{row}")

                            scd_type = 0 if tgt_col.is_pk else scd_type

                            if transformation_type == 'COPY':
                                transformation_rule = None
                                if _row.mapped_to_column:
                                    src_table_alias = _row.mapped_to_table
                                    src_table = core_pipeline.get_table_by_alias(src_table_alias)
                                    if src_table:
                                        src_t = Table.get_instance(_key=(self.srci_t_schema.id, src_table.table_name))
                                        if src_t:
                                            src_col = Column.get_instance(_key=(src_t.id, _row.mapped_to_column))
                                            if src_col is None:
                                                logging.error(f"Invalid Source Column '{_row.mapped_to_column}', processing row:\n{_row}")
                                        else:
                                            logging.error(f"Invalid Table '{src_table.table_name}', processing row:\n{_row}")
                                    else:
                                        logging.error(f"Invalid alias '{src_table_alias}' for mapping {row.mapping_name}, processing row:\n{_row}")
                            elif transformation_type == 'CONST':
                                if str(_row.transformation_rule).upper() == '':
                                    transformation_rule = None
                                else:
                                    transformation_rule = single_quotes(_row.transformation_rule) if isinstance(_row.transformation_rule, str) else _row.transformation_rule
                            else:
                                # means SQL
                                src_table_alias = _row.mapped_to_table
                                transformation_rule = _row.transformation_rule

                            ColumnMapping(pipeline_id=core_pipeline.id
                                          , tgt_col_id=tgt_col.id
                                          , src_col_id=src_col.id if src_col else None
                                          , src_table_alias=src_table_alias
                                          , col_seq=0
                                          , src_col_trx=transformation_rule if transformation_rule else None
                                          , constant_value=True if transformation_type == 'CONST' else False
                                          , scd_type=scd_type
                                          )

                ###########################################################################################################
                core_t: Table
                ds = DataSource.get_instance(_key=row.source)
                if not ds:
                    logging.error(f"Invalid source name {row.source}, processing row:\n{row}")
                else:
                    main_tables_name = row.main_source.split(',')
                    main_table_name = main_tables_name[0].strip()
                    main_table_alias = row.main_source_alias if row.main_source_alias else main_table_name

                    srci_t = Table.get_instance(_key=(self.srci_t_schema.id, main_table_name))
                    if not srci_t:
                        logging.error(f"invalid main table, {main_table_name}, processing row:\n{row}")
                    else:
                        srci_lt = LayerTable.get_instance(_key=(self.srci_layer.id, srci_t.id))

                        core_t = Table.get_instance(_key=(self.core_t_schema.id, row.target_table_name))
                        core_lt = LayerTable.get_instance(_key=(self.core_layer.id, core_t.id))

                        if row.historization_algorithm.upper() == 'HISTORY' and not core_t.history_table:
                            logging.error(f"History process should deal with history table only, processing row:\n{row}")

                        if row.historization_algorithm.upper() != 'HISTORY' and core_t.history_table:
                            logging.error(f"Non-history process should not deal with history table, processing row:\n{row}")

                        txf_process_name = CORE_PROCESS_NAME_TEMPLATE.format(mapping_name=row.mapping_name)
                        txf_view_name = CORE_VIEW_NAME_TEMPLATE.format(view_name=txf_process_name)
                        core_txf_v = Table(schema_id=self.txf_core_v_schema.id, table_name=txf_view_name, table_kind='V', source_id=ds.id)

                        core_txf_vl = LayerTable(layer_id=self.txf_core_layer.id, table_id=core_txf_v.id)
                        core_pipeline = Pipeline(src_lyr_table_id=srci_lt.id
                                                 , tgt_lyr_table_id=core_lt.id
                                                 , src_table_alias=main_table_alias
                                                 , lyr_view_id=core_txf_vl.id
                                                 , name=txf_process_name)

                        if row.join:
                            parse_join(row.join)

                        if row.filter_criterion:
                            Filter(pipeline_id=core_pipeline.id, filter_seq=1, complete_filter_expr=row.filter_criterion)

                        if 'column_mapping' in self.data.keys():
                            column_mapping_df = filter_dataframe(self.data['column_mapping'], 'mapping_name', row.mapping_name)
                            if not column_mapping_df.empty:
                                column_mapping_df[['column_name'
                                    , 'mapped_to_table'
                                    , 'mapped_to_column'
                                    , 'transformation_rule'
                                    , 'transformation_type']].drop_duplicates().apply(column_mapping, axis=1)
                            else:
                                logging.error(f"No column mapping found for {row.mapping_name}, processing row:\n{row}")

            ####################################################  Begin DFs  ####################################################
            if ('system' or 'stg_tables') not in self.data.keys():
                logging.error("Make sure 'System' and 'Stg Tables' sheets are both exists!")
            else:
                _core_tables = []
                _core_tables_bkey = []
                _core_tables_bmap = []
                _source_names = None
                history_tables_lst = []
                if source_name:
                    _source_names = source_name if isinstance(source_name, list) else [source_name]
                    _source_names.extend(UNIFIED_SOURCE_SYSTEMS)

                system_df = filter_dataframe(self.data['system'], 'schema', _source_names)
                stg_tables_df = filter_dataframe(self.data['stg_tables'], 'schema', _source_names)

                if system_df.empty or stg_tables_df.empty:
                    logging.error("Please make sure there are data in both 'System' and 'Stg Tables' sheets!")
                else:
                    table_mapping_df = pd.DataFrame()
                    if 'table_mapping' in self.data.keys():
                        table_mapping_df = filter_dataframe(filter_dataframe(self.data['table_mapping'], 'source', _source_names), 'layer', 'CORE')
                        _core_tables = [i for i in set(table_mapping_df['target_table_name'].values.tolist()) if i]

                        history_core_tables_df = filter_dataframe(table_mapping_df, 'historization_algorithm', 'HISTORY')
                        if not history_core_tables_df.empty:
                            history_tables_lst = history_core_tables_df['target_table_name'].drop_duplicates().tolist()

                    bkey_df = pd.DataFrame()
                    if 'bkey' in self.data.keys():
                        bkey_df = filter_dataframe(self.data['bkey'], 'key_domain_name', [i for i in set(stg_tables_df['key_domain_name'].values.tolist()) if i])
                        _core_tables_bkey = [i for i in set(bkey_df['key_set_name'].values.tolist()) if i]

                    bmap_df = pd.DataFrame()
                    bmap_values_df = pd.DataFrame()
                    if 'bmap' in self.data.keys():
                        bmap_df = filter_dataframe(self.data['bmap'], 'code_domain_name', [i for i in set(stg_tables_df['code_domain_name'].values.tolist()) if i])
                        _core_tables_bmap = [i for i in set(bmap_df['code_set_name'].values.tolist()) if i]
                        if 'bmap_values' in self.data.keys():
                            bmap_values_df = filter_dataframe(self.data['bmap_values'], 'code_domain_id', [i for i in set(bmap_df['code_domain_id'].values.tolist()) if i])

                    all_core_tables = list(set(_core_tables + _core_tables_bkey + _core_tables_bmap))
                    core_tables_df = pd.DataFrame()

                    if 'core_tables' in self.data.keys():
                        core_tables_df = filter_dataframe(self.data['core_tables'], 'table_name', all_core_tables)

                    ####################################################  End DFs  ####################################################
                    ####################################################  Begin   ####################################################
                    system_df.drop_duplicates().apply(extract_system, axis=1)
                    stg_tables_df[['data_type']].drop_duplicates().apply(extract_data_types, axis=1)
                    if not core_tables_df.empty:
                        core_tables_df[['data_type']].drop_duplicates().apply(extract_data_types, axis=1)

                    int_data_type = DataType.get_instance(_key=(self.db_engine.id, 'INTEGER'))
                    vchar_data_type = DataType.get_instance(_key=(self.db_engine.id, 'VARCHAR'))
                    timestamp_data_type = DataType.get_instance(_key=(self.db_engine.id, 'TIMESTAMP'))

                    if not core_tables_df.empty:
                        core_tables_df[['table_name']].drop_duplicates().apply(extract_core_tables, axis=1)

                    if not bkey_df.empty:
                        bkey_df[['physical_table']].drop_duplicates().apply(extract_bkey_tables, axis=1)

                    if not bmap_df.empty:
                        bmap_df[['physical_table']].drop_duplicates().apply(extract_bmap_tables, axis=1)

                    stg_tables_df[['schema', 'table_name']].drop_duplicates().apply(extract_stg_tables, axis=1)
                    ##########################  Start bkey & bmaps   #####################
                    if not bkey_df.empty:
                        bkey_df[['key_set_name', 'key_set_id', 'physical_table']].drop_duplicates().apply(extract_bkey_datasets, axis=1)
                        bkey_df[['key_set_id', 'key_domain_id', 'key_domain_name']].drop_duplicates().apply(extract_bkey_domains, axis=1)

                    if not bmap_df.empty:
                        bmap_df[['code_set_name', 'code_set_id', 'physical_table']].drop_duplicates().apply(extract_bmap_datasets, axis=1)

                    bmaps_data_sets = DataSetType.get_instance(_key=DS_BMAP).data_sets

                    if not bmap_df.empty:
                        bmap_df[['code_set_id', 'code_domain_id', 'code_domain_name']].drop_duplicates().apply(extract_bmap_domains, axis=1)

                    if not bmap_values_df.empty:
                        bmap_values_df[['code_set_name', 'code_domain_id', 'edw_code', 'source_code', 'description']].drop_duplicates().apply(extract_bmap_values, axis=1)
                    ##########################  End bkey & bmaps     #####################

                    stg_tables_df[['table_name', 'column_name', 'data_type', 'mandatory', 'natural_key', 'pk',
                                   'key_set_name', 'key_domain_name', 'code_set_name', 'code_domain_name']].drop_duplicates().apply(extract_stg_srci_table_columns, axis=1)

                    stg_tables_df[['schema', 'table_name', 'natural_key', 'column_name', 'column_transformation_rule']].drop_duplicates().apply(extract_src_view_columns, axis=1)
                    # stg_tables_df[['schema', 'table_name', 'natural_key', 'column_name']].drop_duplicates().apply(extract_stg_view_columns, axis=1)

                    ##########################  Start bkey & bmap TXF view  #####################
                    stg_tables_df[['schema', 'table_name', 'natural_key', 'column_name'
                        , 'key_set_name', 'key_domain_name', 'bkey_filter']].drop_duplicates().apply(extract_bkey_views, axis=1)

                    # stg_tables_df[['schema', 'column_name'
                    #     , 'code_set_name', 'code_domain_name']].drop_duplicates().apply(extract_bmap_views, axis=1)

                    ##########################  End bkey TXF view    #####################
                    stg_tables_df[['schema', 'table_name', 'column_name', 'natural_key'
                        , 'key_set_name', 'key_domain_name', 'code_set_name', 'code_domain_name']].drop_duplicates().apply(extract_srci_view_columns, axis=1)
                    ##########################      Start Core TXF view     #####################
                    if not core_tables_df.empty:
                        core_tables_df[['table_name', 'column_name', 'data_type', 'pk'
                            , 'mandatory', 'historization_key']].drop_duplicates().apply(extract_core_columns, axis=1)
                        table: Table
                        col: Column
                        invalid_history_tables = []
                        invalid_lookup_tables = []
                        for table in [x for x in Table.get_all_instances() if x.table_name in all_core_tables]:
                            if table.history_table:
                                if not (table.key_col and table.start_date_col and table.end_date_col):
                                    invalid_history_tables.append(table.table_name)
                            elif table.is_lkp:
                                col_names = [col.column_name for col in table.columns]
                                if f"{table.table_name}_CD" not in col_names:
                                    invalid_lookup_tables.append(table.table_name)
                                elif f"{table.table_name}_DESC" not in col_names:
                                    invalid_lookup_tables.append(table.table_name)

                            if not table.key_col and table.table_kind == 'T':
                                logging.error(f"No Primary key defined for '{table.schema.schema_name}.{table.table_name}' table!")

                        if invalid_history_tables:
                            logging.error(f"These are invalid history tables, {invalid_history_tables}")

                        if invalid_lookup_tables:
                            logging.error(f"These are invalid lookup tables, {invalid_lookup_tables}.\nCODE & DESC columns should follow the pattern <table_name>_CD & <table_name>_DESC")

                    if not table_mapping_df.empty:
                        table_mapping_df[
                            [
                                'target_table_name', 'source'
                                , 'main_source', 'main_source_alias'
                                , 'filter_criterion', 'mapping_name', 'join'
                                , 'historization_algorithm', 'historization_columns'
                            ]
                        ].drop_duplicates().apply(extract_core_txf_views, axis=1)

                    layer_table: LayerTable
                    for table_name in [layer_table.table.table_name for layer_table in LayerTable.get_all_instances()
                                       if layer_table.layer.layer_name == 'STG'
                                          and not layer_table.table.key_col
                                          and layer_table.table.table_kind == 'T']:
                        logging.warning(f"No Primary key defined for '{table_name}' staging table!")
                    ##########################      End Core TXF view       #####################
                    ####################################################  End   ####################################################

                    # MyID.serialize_all()

        @log_error_decorator()
        def add_technical_columns(tables, technical_column_objs):
            table: Table
            column: Column
            technical_col: TechColumn
            missing_tech_col: TechColumn

            for table in tables:
                src_table = False
                if table.schema.id == self.src_t_schema.id:
                    table.set_key(None)
                    src_table = True

                for missing_tech_col in technical_column_objs:
                    data_type_name, precision = parse_data_type(missing_tech_col.data_type)
                    data_type = DataType(db_id=self.db_engine.id, dt_name=data_type_name, _override=1)
                    Column(table_id=table.id
                           , column_name=missing_tech_col.column_name
                           , data_type_id=data_type.id
                           , dt_precision=precision
                           , is_created_at=missing_tech_col.is_created_at
                           , is_created_by=missing_tech_col.is_created_by
                           , is_updated_at=missing_tech_col.is_updated_at
                           , is_updated_by=missing_tech_col.is_updated_by
                           , is_modification_type=missing_tech_col.is_modification_type
                           , is_load_id=missing_tech_col.is_load_id
                           , is_batch_id=missing_tech_col.is_batch_id
                           , is_row_identity=missing_tech_col.is_row_identity
                           , is_delete_flag=missing_tech_col.is_delete_flag
                           , mandatory=missing_tech_col.mandatory
                           , _override=1
                           , is_pk=1 if src_table and missing_tech_col.column_name.upper() == 'REF_KEY' else 0
                           )

        extract_all()
        stg_tables = (table for table in Table.get_all_instances() if table.schema.id in (self.src_t_schema.id
                                                                                          , self.stg_t_schema.id
                                                                                          , self.srci_t_schema.id
                                                                                          )
                      )
        core_tables = (table for table in Table.get_all_instances() if table.schema.id == self.core_t_schema.id)

        add_technical_columns(stg_tables, STG_TECHNICAL_COLS)
        add_technical_columns(core_tables, CORE_TECHNICAL_COLS)
        # map_technical_columns(stg_tables)
        # map_technical_columns(core_tables)


@log_error_decorator()
def layer_table_scripts(row):
    row.layer_table: LayerTable
    ddl = row.layer_table.table.ddl
    drop_table = ""
    table_kind = row.layer_table.table.table_kind
    if DROP_BEFORE_CREATE:
        if table_kind == 'T':
            drop_table = DROP_TABLE_TEMPLATE.format(schema_name=row.layer_table.table.schema.schema_name, table_name=row.layer_table.table.table_name) + "\n"
    if ddl:
        kind_folder = 'tables' if table_kind == 'T' else 'views'
        tables_file = WriteFile(row.out_path, kind_folder, "sql", 'a')
        tables_file.write(drop_table + ddl)
        tables_file.write('\n')
        tables_file.close()

    dml = None
    if row.layer_table.table.is_bmap:
        dml = row.layer_table.dml
    elif row.layer_table.table.is_lkp:
        schema_name = row.layer_table.table.schema.schema_name
        table_name = row.layer_table.table.table_name
        core_lookup_ds = row.layer_table.core_lookup_ds
        if core_lookup_ds:
            src_schema = core_lookup_ds.surrogate_table.schema.schema_name
            src_table = core_lookup_ds.surrogate_table.table_name
            dml = f"insert into {schema_name}.{table_name}\n({table_name}_CD, {table_name}_DESC, PROCESS_NAME, START_TS, BATCH_ID)\n" \
                  f"select distinct EDW_CODE,DESCRIPTION, '{src_table}', CURRENT_TIMESTAMP(0), 0\n" \
                  f"from {src_schema}.{src_table};\n"

    if dml:
        data_file = WriteFile(row.out_path, 'data', "sql", 'a')
        data_file.write(dml)
        data_file.write('\n')
        data_file.close()


@log_error_decorator()
def generate_schemas_ddl(smx: SMX):
    schema: Schema
    db_create_file = WriteFile(smx.current_scripts_path, "create_schemas", "sql")
    db_drop_file = WriteFile(smx.current_scripts_path, "drop_schemas", "sql")

    db_create_file.write(smx.MAIN_DATABASE_TEMPLATE + "\n")
    for schema in Schema.get_all_instances():
        ddl = schema.ddl
        delete_schema_ddl = DELETE_DATABASE_TEMPLATE.format(db_name=schema.schema_name)
        drop_schema_ddl = DROP_DATABASE_TEMPLATE.format(db_name=schema.schema_name)
        if ddl:
            db_drop_file.write(delete_schema_ddl + "\n" + drop_schema_ddl + "\n\n")
            db_create_file.write(ddl + "\n")

    db_drop_file.write(DROP_DATABASE_TEMPLATE.format(db_name=self.MAIN_DB_NAME) + "\n")

    db_create_file.close()
    db_drop_file.close()

    grants_file = WriteFile(smx.current_scripts_path, "grants", "sql")
    grants_file.write(smx.GRANTS)
    grants_file.close()


@log_error_decorator()
@time_elapsed_decorator
def generate_scripts(smx: SMX):
    source_dict: dict = {}
    core_model_path = os.path.join(smx.current_scripts_path, CORE_MODEL_FOLDER_NAME)
    # core_model_path = os.path.join(smx.path, CORE_MODEL_FOLDER_NAME)
    src_systems_path = os.path.join(smx.current_scripts_path, SRC_SYSTEMS_FOLDER_NAME)
    # src_systems_path = os.path.join(smx.path, SRC_SYSTEMS_FOLDER_NAME)

    # create_folder(core_model_path)

    for src in smx.source_systems:
        source_path = os.path.join(smx.current_scripts_path, src_systems_path, src)
        source_dict[src.upper()] = source_path
        # create_folder(source_path)
        del source_path

    def layer_table_out_path(row):
        layer_table: LayerTable
        layer_table = row.layer_table

        if layer_table.table.data_source:
            main_folder = source_dict[layer_table.table.data_source.source_name]
        else:
            main_folder = core_model_path

        layer_folder_name = f"Layer_{layer_table.layer.layer_level}_{layer_table.layer.layer_name}"

        path = os.path.join(smx.current_scripts_path, main_folder, layer_folder_name)
        return path

    layer_tables_df = pd.DataFrame(LayerTable.get_all_instances(), columns=['layer_table'])
    layer_tables_df['out_path'] = layer_tables_df.apply(layer_table_out_path, axis=1)
    layer_tables_df[['out_path']].drop_duplicates().apply(lambda row: create_folder(row.out_path), axis=1)

    if not layer_tables_df.empty:
        # print('start generating scripts!')
        layer_tables_df.apply(layer_table_scripts, axis=1)
        # layer_tables_df.swifter.apply(layer_table_scripts, axis=1)
        # layer_tables_df.parallel_apply(layer_table_scripts, axis=1)


@log_error_decorator()
@time_elapsed_decorator
def generate_metadata_scripts(smx: SMX):
    """
    todo:
        scripts to populate the below tables:
            source_name_lkp
            source_table_lkp
            etl_process
            history
            gcfr_process
            gcfr_transfer_key_col

    :param smx:
    :return:
    """
    # INSERT_INTO_SOURCE_NAME_LKP smx.metadata_scripts
    source_sytems_file = WriteFile(smx.metadata_scripts, 'SOURCE_SYSTEMS', "sql")
    source_system_tables_file = WriteFile(smx.metadata_scripts, 'SOURCE_SYSTEM_TABLES', "sql")
    edw_tables_file = WriteFile(smx.metadata_scripts, 'EDW_TABLES', "sql")
    transform_keycol_file = WriteFile(smx.metadata_scripts, 'TRANSFORM_KEYCOL', "sql")
    history_file = WriteFile(smx.metadata_scripts, 'HISTORY', 'sql')
    process_file = WriteFile(smx.metadata_scripts, 'PROCESS', "sql")

    source: DataSource
    table: Table
    layer_table: LayerTable
    pk_col: Column
    hist_col_mapping: ColumnMapping
    pipeline: Pipeline
    data_set: DataSet

    for source in DataSource.get_all_instances():
        source_sytems_file.write(INSERT_INTO_SOURCE_SYSTEMS.format(meta_db=smx.meta_t_schema.schema_name,
                                                                   SOURCE_NAME=single_quotes(source.source_name),
                                                                   REJECTION_TABLE_NAME='NULL',
                                                                   BUSINESS_RULES_TABLE_NAME='NULL',
                                                                   LOADING_MODE=single_quotes(LOADING_MODE),
                                                                   SOURCE_LAYER=single_quotes(Layer.get_instance(_key='SRC').layer_name),
                                                                   DATA_SRC_CD='NULL'
                                                                   )
                                 )
        for table in source.tables:
            if table.schema.id == smx.src_v_schema.id:
                source_system_tables_file.write(INSERT_INTO_SOURCE_SYSTEM_TABLES.format(meta_db=smx.meta_t_schema.schema_name,
                                                                                        SOURCE_NAME=single_quotes(source.source_name),
                                                                                        TABLE_NAME=single_quotes(table.table_name),
                                                                                        IS_TARANSACTIOANL=1 if table.transactional_data else 0
                                                                                        )
                                                )
    for layer_table in LayerTable.get_all_instances():
        table = layer_table.table
        for pk_col in table.key_col:
            transform_keycol_file.write(INSERT_INTO_TRANSFORM_KEYCOL.format(meta_db=smx.meta_t_schema.schema_name,
                                                                            LAYER_NAME=single_quotes(layer_table.layer.layer_name),
                                                                            TABLE_NAME=single_quotes(table.table_name),
                                                                            KEY_COLUMN=single_quotes(pk_col.column_name)
                                                                            )
                                        )
        if table.schema.id == smx.core_t_schema.id:
            edw_tables_file.write(INSERT_INTO_EDW_TABLES.format(meta_db=smx.meta_t_schema.schema_name,
                                                                LAYER_NAME=single_quotes(layer_table.layer.layer_name),
                                                                TABLE_NAME=single_quotes(table.table_name),
                                                                IS_LOOKUP=1 if table.is_lkp else 0,
                                                                IS_HISTORY=1 if table.history_table else 0,
                                                                START_DATE_COLUMN=single_quotes(table.start_date_col.column_name) if table.history_table and table.start_date_col else 'NULL',
                                                                END_DATE_COLUMN=single_quotes(table.end_date_col.column_name) if table.history_table and table.end_date_col else 'NULL'
                                                                )
                                  )

    for pipeline in Pipeline.get_all_instances():
        if pipeline.tgt_lyr_table:
            process_type = None
            apply_type = 'INSERT'

            key_sets = [None]
            if pipeline.lyr_view.layer.id == smx.txf_bkey_layer.id:
                process_type = 'BKEY'
                key_sets = [data_set.set_code for data_set in pipeline.tgt_lyr_table.table.surrogate_data_sets]

            elif pipeline.lyr_view.layer.id == smx.txf_core_layer.id:
                process_type = 'TXF'
                if pipeline.scd_type2_col:
                    apply_type = 'HISTORY'
                elif pipeline.scd_type1_col:
                    apply_type = 'UPSERTDELETE'

            if process_type:
                for key_set in key_sets:
                    domain_id = 'NULL'
                    key_set_id = 'NULL'
                    code_set_id = 'NULL'

                    if key_set and process_type == 'BKEY':
                        domain_id = pipeline.domain.domain_code
                        key_set_id = key_set

                    process_file.write(INSERT_INTO_PROCESS.format(meta_db=smx.meta_t_schema.schema_name,
                                                                  PROCESS_NAME=single_quotes(pipeline.name),
                                                                  SOURCE_NAME=single_quotes(pipeline.src_lyr_table.table.data_source.source_name),
                                                                  TGT_LAYER=single_quotes(pipeline.tgt_lyr_table.layer.layer_name),
                                                                  TGT_TABLE=single_quotes(pipeline.tgt_lyr_table.table.table_name),
                                                                  SRC_TABLE=single_quotes(pipeline.lyr_view.table.table_name),
                                                                  APPLY_TYPE=single_quotes(apply_type),
                                                                  MAIN_TABLE_NAME=single_quotes(pipeline.src_lyr_table.table.table_name),
                                                                  KEY_SET_ID=key_set_id,
                                                                  DOMAIN_ID=domain_id,
                                                                  CODE_SET_ID=code_set_id
                                                                  )
                                       )

            if pipeline.tgt_lyr_table.table.history_table:
                for hist_col_mapping in pipeline.scd_type2_col:
                    history_file.write(INSERT_INTO_HISTORY.format(meta_db=smx.meta_t_schema.schema_name,
                                                                  PROCESS_NAME=single_quotes(pipeline.name),
                                                                  HISTORY_COLUMN=single_quotes(hist_col_mapping.tgt_col.column_name)
                                                                  )
                                       )
    source_sytems_file.close()
    source_system_tables_file.close()
    transform_keycol_file.close()
    process_file.close()
    history_file.close()
    edw_tables_file.close()


@log_error_decorator()
@time_elapsed_decorator
def deploy():
    db: DataBaseEngine
    schema: Schema
    table: Table
    for db in DataBaseEngine.get_all_instances():
        db.conn = db.get_connection()

        if db.conn:
            # for schema in db.schemas:
            #     db.execute(schema.ddl)
            # grants = string_to_list(GRANTS, ';')
            # for grant in grants:
            #     db.execute(grant)

            for schema in db.schemas:
                for table in schema.kind_T_tables:
                    if DROP_BEFORE_CREATE:
                        db.execute(DROP_TABLE_TEMPLATE.format(schema_name=schema.schema_name, table_name=table.table_name))
                    db.execute(table.ddl)

            for schema in db.schemas:
                for table in schema.kind_V_tables:
                    cursor = db.execute(table.ddl)
                    if cursor:
                        db.execute(f"select top 1 * from {schema.schema_name}.{table.table_name}")

            if db.conn:
                db.conn.close()
        else:
            logging.warning("Unable to deploy scripts, failed to connect!")


