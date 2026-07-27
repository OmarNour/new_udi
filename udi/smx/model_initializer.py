import logging
import re
from collections import namedtuple

from config import DB_NAME, DS_BKEY, DS_BMAP, PASSWORD, USER
from functions import log_error_decorator
from model import (
    Credential,
    DataBaseEngine,
    DataSetType,
    Ip,
    Layer,
    LayerType,
    Schema,
    Server,
)

from .builders.base import SmxComponent


def build_runtime_config(db_prefix, source_layer0):
    PREFIX = db_prefix
    LayerDtl = namedtuple('LayerDetail', 'type level v_db t_db')
    LAYER_TYPES = ['META', 'SRC', 'STG', 'SK', 'SRCI', 'CORE']
    LAYERS = {'META': LayerDtl(LAYER_TYPES[0], 0, f'{PREFIX}_ETL', f'{PREFIX}_ETL'), 'SRC': LayerDtl(LAYER_TYPES[1], 0, f"{PREFIX}V_{re.findall('stg.*', source_layer0)[0]}", source_layer0), 'STG': LayerDtl(LAYER_TYPES[2], 1, f'{PREFIX}V_STG', f'{PREFIX}T_STG'), 'TXF_BKEY': LayerDtl(LAYER_TYPES[3], 2, f'{PREFIX}V_INP', ''), 'TXF_BMAP': LayerDtl(LAYER_TYPES[3], 2, f'{PREFIX}V_INP', ''), 'BKEY': LayerDtl(LAYER_TYPES[3], 3, f'{PREFIX}V_UTLFW', f'{PREFIX}T_UTLFW'), 'BMAP': LayerDtl(LAYER_TYPES[3], 3, f'{PREFIX}V_UTLFW', f'{PREFIX}T_UTLFW'), 'SRCI': LayerDtl(LAYER_TYPES[4], 4, f'{PREFIX}V_SRCI', f'{PREFIX}T_SRCI'), 'TXF_CORE': LayerDtl(LAYER_TYPES[4], 5, f'{PREFIX}V_INP', ''), 'CORE': LayerDtl(LAYER_TYPES[5], 6, f'{PREFIX}V_BASE', f'{PREFIX}T_BASE')}
    MAIN_DB_NAME = f'{PREFIX}_EDWH'
    MAIN_DATABASE_TEMPLATE = f'\n        CREATE DATABASE {MAIN_DB_NAME}\n        AS PERMANENT = 5000e6, -- 5GB\n            SPOOL = 1000e6; -- 1GB\n        '
    OTHER_SCHEMAS = [f'{PREFIX}_ETL']
    GRANTS = f'\n        GRANT CREATE PROCEDURE ON {PREFIX}_ETL TO DBC WITH GRANT OPTION;\n        GRANT CREATE FUNCTION ON {PREFIX}_ETL TO DBC WITH GRANT OPTION;\n        GRANT EXECUTE PROCEDURE ON {PREFIX}_ETL TO DBC WITH GRANT OPTION;\n        GRANT EXECUTE FUNCTION ON {PREFIX}_ETL TO DBC WITH GRANT OPTION;\n\n        GRANT SELECT ON STG_ONLINE TO {PREFIX}V_STG_ONLINE WITH GRANT OPTION;\n        GRANT SELECT ON {PREFIX}T_STG TO {PREFIX}V_STG WITH GRANT OPTION;\n        GRANT SELECT ON {PREFIX}T_UTLFW TO {PREFIX}V_UTLFW WITH GRANT OPTION;\n        GRANT SELECT ON {PREFIX}V_UTLFW TO {PREFIX}V_SRCI WITH GRANT OPTION;\n        GRANT SELECT ON {PREFIX}T_UTLFW TO {PREFIX}V_SRCI WITH GRANT OPTION;\n        GRANT SELECT ON {PREFIX}V_STG TO {PREFIX}V_SRCI WITH GRANT OPTION;\n        GRANT SELECT ON {PREFIX}T_STG TO {PREFIX}V_SRCI WITH GRANT OPTION;\n        GRANT SELECT ON {PREFIX}T_STG TO {PREFIX}V_INP WITH GRANT OPTION;\n        GRANT SELECT ON {PREFIX}T_SRCI TO {PREFIX}V_INP WITH GRANT OPTION;\n        GRANT SELECT ON {PREFIX}T_SRCI TO {PREFIX}_ETL WITH GRANT OPTION;\n        GRANT SELECT ON {PREFIX}V_SRCI TO {PREFIX}_ETL WITH GRANT OPTION;\n        GRANT CREATE TABLE ON {PREFIX}T_SRCI TO {PREFIX}_ETL WITH GRANT OPTION;\n        GRANT DROP TABLE ON {PREFIX}T_SRCI TO {PREFIX}_ETL WITH GRANT OPTION;\n        GRANT SELECT ON {PREFIX}T_BASE TO {PREFIX}V_BASE WITH GRANT OPTION;\n\n        GRANT SELECT ON DBC TO {PREFIX}_ETL WITH GRANT OPTION;\n        GRANT SELECT ON {PREFIX}V_INP TO {PREFIX}_ETL WITH GRANT OPTION;\n        GRANT SELECT ON {PREFIX}T_UTLFW TO {PREFIX}_ETL WITH GRANT OPTION;\n        GRANT INSERT ON {PREFIX}T_UTLFW TO {PREFIX}_ETL WITH GRANT OPTION;\n        GRANT SELECT ON {PREFIX}T_BASE TO {PREFIX}_ETL WITH GRANT OPTION;\n        GRANT DELETE ON {PREFIX}T_BASE TO {PREFIX}_ETL WITH GRANT OPTION;\n        GRANT UPDATE ON {PREFIX}T_BASE TO {PREFIX}_ETL WITH GRANT OPTION;\n        GRANT INSERT ON {PREFIX}T_BASE TO {PREFIX}_ETL WITH GRANT OPTION;\n        GRANT SELECT ON {PREFIX}V_BASE TO {PREFIX}_ETL WITH GRANT OPTION;\n        GRANT SELECT ON {PREFIX}V_STG_ONLINE TO {PREFIX}_ETL WITH GRANT OPTION;\n        GRANT SELECT ON {PREFIX}T_STG TO {PREFIX}_ETL WITH GRANT OPTION;\n        GRANT DELETE ON {PREFIX}T_STG TO {PREFIX}_ETL WITH GRANT OPTION;\n        GRANT UPDATE ON {PREFIX}T_STG TO {PREFIX}_ETL WITH GRANT OPTION;\n        GRANT INSERT ON {PREFIX}T_STG TO {PREFIX}_ETL WITH GRANT OPTION;\n        GRANT EXECUTE PROCEDURE ON {PREFIX}_ETL TO {PREFIX}_ETL WITH GRANT OPTION;\n        GRANT EXECUTE FUNCTION ON {PREFIX}_ETL TO {PREFIX}_ETL WITH GRANT OPTION;\n        '
    return (LAYER_TYPES, LAYERS, MAIN_DB_NAME, MAIN_DATABASE_TEMPLATE, OTHER_SCHEMAS, GRANTS)


class ModelInitializer(SmxComponent):
    @log_error_decorator()
    def initialize(self):
        try:
            self.server = Server(server_name='TDVM')
        except Exception as e:
            print(e)
        self.db_engine = DataBaseEngine(server_id=self.server.id, name=DB_NAME)
        Ip(server_id=self.server.id, ip='localhost')
        Credential(db_engine_id=self.db_engine.id, user_name=USER, password=PASSWORD)
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
