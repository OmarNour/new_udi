from functions import log_error_decorator, upper_string_in_list, list_to_string
import functools
from config import cls_keys, JOIN_TYPES, ALPHABETS, DDL_VIEW_TEMPLATE, COL_DTYPE_TEMPLATE, CREATE_REPLACE, FROM_TEMPLATE, PI_TEMPLATE, DDL_TABLE_TEMPLATE, CAST_DTYPE_TEMPLATE, DS_BKEY, QUERY_TEMPLATE, CAST_DTYPE_TEMPLATE, GROUP_BY_TEMPLATE, CAST_DTYPE_TEMPLATE, COL_MAPPING_TEMPLATE, SRCI_V_BKEY_TEMPLATE_QUERY, WHERE_TEMPLATE, DS_BMAP, SRCI_V_BMAP_TEMPLATE_QUERY, JOIN_CLAUSE_TEMPLATE
import re
class Meta(type):
    """
    This is a custom metaclass that is used to generate unique IDs for instances of the MyID class.
    """

    def __call__(cls, *args, **kwargs):
        """
        This method is called when an instance of the MyID class is created.
        It checks that named parameters are passed, generates a key from the named parameters and adds the key to the kwargs dictionary.
        """
        assert len(kwargs) > 0, f'Call {cls.__name__} with named parameters!'
        key = cls.__get_key(cls.__name__, **kwargs)
        kwargs.update({'_key': key})
        return super().__call__(*args, **kwargs)

    @staticmethod
    def _normalize_key(_key):
        """
        This method normalizes the key passed.
        """
        if isinstance(_key, tuple):
            return tuple(s.lower().strip() if isinstance(s, str) else s for s in _key)
        if isinstance(_key, str):
            return _key.lower().strip()
        return _key

    @classmethod
    def __get_key(cls, obj_name, **kwargs):
        """
        This method generates a key from the named parameters passed to the MyID class.
        """
        try:
            lower_cls_keys = {k.lower(): v for k, v in cls_keys.items()}
            cls_key = lower_cls_keys[obj_name.lower()]

            if isinstance(cls_key, tuple):
                list_keys = []
                for key in cls_key:
                    key_value = kwargs.get(key)
                    assert key_value is not None, f"Key value cannot be None!"
                    n_key_value = cls._normalize_key(key_value)
                    list_keys.append(n_key_value)
                return tuple(list_keys)
            if isinstance(cls_key, str):
                key_value = kwargs.get(cls_key)
                assert key_value is not None, f"{cls_key} not found!"
                return cls._normalize_key(key_value)
        except KeyError:
            raise Exception(f'{obj_name}, has no key defined!')


class MyID(metaclass=Meta):
    """
    This class uses the Meta metaclass to generate unique IDs for instances of the class.
    """
    __instances = {}
    __ids = {}
    __next_id = 1

    
    @classmethod
    def reset_class_variable(cls):
        cls.__instances = {}
        cls.ids = {}
        cls.__next_id = 1
    


    def __init__(self, *args, **kwargs):
        """
        This method assigns a unique ID to the instance and stores the key passed in the kwargs dictionary.
        """
        override = kwargs.get('_override', 0)
        key = kwargs.get('_key', '_')

        try:
            self.id = self.get_instance(_key=key).id if override else self.__generate_unique_id()
        except:
            self.id = self.__generate_unique_id()

        self.__ids[self.__class__.__name__][self.id] = key

    @classmethod
    def __generate_unique_id(cls):
        """
        This method generates a unique ID for the instance and increments the next ID counter.
        """
        new_id = cls.__next_id
        cls.__next_id += 1
        return new_id

    @classmethod
    def __set_instance(cls, _key, instance):
        """
        This method stores the instance in the __instances dictionary using the key passed.
        """
        if cls.__name__ not in cls.__instances.keys():
            cls.__instances[cls.__name__] = {}
            cls.__ids[cls.__name__] = {}

        cls.__instances[cls.__name__][_key] = instance

    @classmethod
    # @lru_cache
    def get_all_instances(cls) -> []:
        try:
            return cls.__instances[cls.__name__].values()
        except KeyError:
            return []

    @classmethod
    def count_instances(cls):
        try:
            return len(cls.__instances[cls.__name__])
        except KeyError:
            return 0

    @classmethod
    def get_instance(cls, _key=None, _id: int = None):
        """
        This method is used to retrieve an instance from the __instances dictionary using either the key or the ID of the instance.
        """
        instance_key = cls._normalize_key(_key)
        instance_id = _id

        if cls.__name__ in cls.__instances.keys():

            if instance_id in cls.__ids[cls.__name__].keys():
                instance_key = cls.__ids[cls.__name__][instance_id]

            if instance_key in cls.__instances[cls.__name__].keys():
                return cls.__instances[cls.__name__][instance_key]

    @classmethod
    def __del_instance(cls, _key=None, _id: int = None):
        """
        This method is used to delete an instance from the __instances dictionary using either the key or the ID of the instance.
        """
        instance_key = cls._normalize_key(_key)
        instance_id = _id

        if instance_id is not None and instance_key is None:
            instance_key = cls.__ids[cls.__name__][instance_id]

        if instance_key is None:
            cls.__instances[cls.__name__] = {}
        else:
            del cls.__instances[cls.__name__][instance_key]

    @classmethod
    def get_all_classes_instances(cls):
        return cls.__instances

    def __new__(cls, *args, **kwargs):
        """
        This method overrides the default new method and is used to return existing instances of the class
        instead of creating new ones. It also allows for the option to override an existing instance with a new one.
        """
        key = kwargs.get('_key', '_')  # The key to identify the instance
        override = kwargs.get('_override', 0)  # Flag to indicate whether to override an existing instance or not
        # raise_if_exist = kwargs.get('_raise_if_exist', 1)  # Flag to indicate whether to raise error if instance exists or not
        instance = cls.get_instance(_key=key)  # Check if an instance with the same key already exists
        if instance:
            if override == 1:
                # cls.__del_instance(key)  # delete the existing instance if override is set to 1
                cls.__set_instance(key, instance)
            else:
                # if raise_if_exist:
                error_message = f'For {cls.__name__}, {key} Already Exists!'
                raise ValueError(error_message)

        else:
            instance = super().__new__(cls)
            cls.__set_instance(key, instance)

        return instance

    @classmethod
    def _serialize(cls, instance):
        full_path = os.path.join(f"{pickle_path}/{cls.__name__}/{instance.id}.pkl")
        pickle.dump(instance, open(full_path, 'wb'), protocol=pickle.HIGHEST_PROTOCOL)

    @classmethod
    def _deserialize(cls, _id: int):
        try:
            full_path = os.path.join(f"{pickle_path}/{cls.__name__}/{_id}.pkl")
            return pickle.load(open(full_path, 'rb'))
        except FileNotFoundError:
            pass

    @classmethod
    def serialize_all(cls):
        instance: MyID
        create_folder(pickle_path)
        for class_name in cls.__instances.keys():
            cls_instances = eval(f"{class_name}.get_all_instances()")
            class_path = f"{pickle_path}/{class_name}"
            create_folder(class_path)
            for instance in cls_instances:
                instance._serialize(instance)


class Server(MyID):
    def __init__(self, server_name: str, **kwargs):
        super().__init__(**kwargs)
        self._server_name = server_name

    @property
    def server_name(self):
        return self._server_name

    @property
    def ips(self) -> []:
        ip: Ip
        return [ip for ip in Ip.get_all_instances() if ip.server.id == self.id]


class Ip(MyID):
    def __init__(self, server_id: int, ip: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._server_id = server_id
        self.ip = ip

    @property
    def server(self) -> Server:
        return Server.get_instance(_id=self._server_id)


class DataBaseEngine(MyID):
    def __init__(self, server_id: int, name: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.conn = None
        self._server_id = server_id
        self._name = name
        self._reserved_words = None
        [JoinType(code=x.code, name=x.name) for x in JOIN_TYPES]

    @property
    def name(self):
        return self._name.upper()

    @property
    def reserved_words(self) -> []:
        return self._reserved_words

    @reserved_words.setter
    def reserved_words(self, rd_list: []):
        self._reserved_words = list(set(rd_list + ['null']))

    @property
    def schemas(self) -> []:
        schema: Schema
        return [schema for schema in Schema.get_all_instances() if self.id == schema.db_engine.id]

    @property
    def data_types(self) -> []:
        data_type: DataType
        return [data_type for data_type in DataType.get_all_instances() if self.id == data_type.db_engine.id]

    @property
    def server(self) -> Server:
        return Server.get_instance(_id=self._server_id)

    @property
    def credentials(self) -> []:
        crd: Credential
        return [crd for crd in Credential.get_all_instances() if crd.db_engine.id == self.id]

    def get_connection(self, ip_idx=0):
        crd: Credential
        self.__ip_idx = ip_idx
        crd_lst = self.credentials
        if crd_lst:
            crd = crd_lst[0]
            try:
                ip = self.server.ips[self.__ip_idx]
                return teradatasql.connect(host=ip.ip, user=crd.user_name, password=crd.password)
            except IndexError:
                return None
            except:
                return self.get_connection(self.__ip_idx + 1)

    @log_error_decorator()
    def execute(self, stmt: str):
        try:
            if not self.conn:
                self.conn = self.get_connection()

            if self.conn:
                cursor = self.conn.cursor()
                cursor.execute(stmt)
                return cursor
        except teradatasql.OperationalError as e:
            error_msg = e.__str__()
            if "[Error 5612]" in error_msg:
                tailored_msg = f"A user, database, role, or zone with the specified name already exists.\nStatment:\n{stmt}"
                logging.warning(tailored_msg)
            elif "[Error 3803]" in error_msg:
                tailored_msg = f"Table already exists.\n\nStatment:\n{stmt}"
                logging.warning(tailored_msg)
            elif "[Error 3804]" in error_msg:
                tailored_msg = f"View already exists.\n\nStatment:\n{stmt}"
                logging.warning(tailored_msg)
            elif "[Error 3706]" in error_msg or "[Error 3807]" in error_msg:
                tailored_msg = f"\n{error_msg}\n\nStatment:\n{stmt}"
                logging.error(tailored_msg)
            # elif "[Error 3807]" in error_msg:
            #     pass  # Object 'XxX' does not exist.
            else:
                logging.error(error_msg + f"\n\nStatment:\n{stmt}")

    def valid_trx(self, trx: str, extra_words: [] = None) -> bool:
        data_type: DataType
        _trx = trx
        if _trx:
            replace_ch = ''
            single_quotes_pattern = r"'[^']*'"
            _trx = re.sub(single_quotes_pattern, replace_ch, _trx)

            if extra_words is not None:
                words = self.reserved_words + extra_words
            else:
                words = self.reserved_words

            words = words + [data_type.dt_name for data_type in self.data_types]
            sorted_words = sorted(words, key=len, reverse=True)
            word: str
            for word in sorted_words:
                _trx = _trx.lower().replace(word.lower(), replace_ch).strip()

            for spc in SPECIAL_CHARACTERS + NUMBERS:
                _trx = _trx.replace(str(spc), replace_ch).strip()

            if _trx != '':
                return False

        return True


class Credential(MyID):
    def __init__(self, db_engine_id: int, user_name: str, password: str, port: int = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._db_engine_id = db_engine_id
        self.user_name = user_name
        self.password = password
        self.port = port

    @property
    def db_engine(self) -> DataBaseEngine:
        return DataBaseEngine.get_instance(_id=self._db_engine_id)


class Schema(MyID):
    def __init__(self, db_id, schema_name: str, main_db_name: str, is_tmp: int = 0, notes: str = None, **kwargs):
        super().__init__(**kwargs)
        self._schema_name = schema_name
        self._main_db_name = main_db_name
        self._db_id = db_id
        self.is_tmp = is_tmp
        self.notes = notes

    @property
    def main_db_name(self):
        return self._main_db_name

    @property
    def schema_name(self):
        return self._schema_name.upper()

    @property
    def db_engine(self) -> DataBaseEngine:
        return DataBaseEngine.get_instance(_id=self._db_id)

    @property
    def tables(self) -> []:
        table: Table
        return [table for table in Table.get_all_instances() if self.id == table.schema.id]

    @property
    def kind_T_tables(self) -> []:
        table: Table
        return [table for table in Table.get_all_instances() if self.id == table.schema.id and table.table_kind == 'T']

    @property
    def kind_V_tables(self) -> []:
        return [table for table in Table.get_all_instances() if self.id == table.schema.id and table.table_kind == 'V']

    @property
    def ddl(self) -> str:
        return DATABASE_TEMPLATE.format(db_name=self.schema_name, main_db_name=main_db_name)

    @property
    def layers(self) -> []:
        table: Table
        return list(set(itertools.chain.from_iterable([table.layers for table in self.tables])))


class DataType(MyID):
    def __init__(self, db_id, dt_name: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._dt_name = dt_name
        self._db_id = db_id

    @property
    def dt_name(self):
        return self._dt_name.upper()

    @property
    def db_engine(self) -> DataBaseEngine:
        return DataBaseEngine.get_instance(_id=self._db_id)


class DataSource(MyID):
    def __init__(self, source_name: str, source_level: int, scheduled: int, active: int = 1, **kwargs):
        super().__init__(**kwargs)
        self._source_name = source_name
        self.source_level = source_level
        self.scheduled = scheduled
        self.active = active

    @property
    def source_name(self):
        return self._source_name.upper()

    @property
    def tables(self) -> []:
        return [table for table in Table.get_all_instances() if table.data_source if self.id == table.data_source.id]


class Table(MyID):
    """
    key is: schema_id & table_name
    """

    def __init__(self, schema_id: int, table_name: str, table_kind: str
                 , source_id: int = None, multiset: int = 1, active: int = 1
                 , history_table: bool = False, is_lkp=False, transactional_data: bool = False
                 , unique_pi=True, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._schema_id = schema_id
        self._table_name = table_name
        self._source_id = source_id
        self._table_kind = table_kind
        self.active = active
        self.multiset = multiset
        self._ddl = None
        self.history_table = history_table
        self.transactional_data = transactional_data
        self.is_lkp = is_lkp
        self.is_bkey = False
        self.is_bmap = False
        self.unique_pi = unique_pi

    def set_key(self, key: str | list | None):
        _new_key = key if isinstance(key, list) else [key] if key else []
        col: Column
        for col in self.columns:
            col.is_pk = 0
            if col.column_name in upper_string_in_list(_new_key):
                col.is_pk = 1

    @property
    def data_source(self) -> DataSource:
        return DataSource.get_instance(_id=self._source_id)

    @property
    def columns(self) -> []:
        col: Column
        return [col for col in Column.get_all_instances() if col.table.id == self.id]

    @property
    def table_kind(self):
        return self._table_kind.strip().upper()

    @property
    def table_name(self):
        return self._table_name.strip().upper()

    def get_column_datatype(self, column_name: str):
        col: Column
        for col in self.columns:
            if col.column_name == column_name.upper():
                return col.data_type.dt_name, col.dt_precision

    @property
    def schema(self) -> Schema:
        return Schema.get_instance(_id=self._schema_id)

    @functools.cached_property
    def surrogate_data_sets(self) -> list:
        _data_set: DataSet
        return [_data_set for _data_set in DataSet.get_all_instances() if _data_set.surrogate_table.id == self.id]

    @property
    def key_col(self):
        col: Column
        return [col for col in self.columns if col.table.id == self.id and col.is_pk == 1]

    @property
    def start_date_col(self):
        col: Column
        col_list = [col for col in self.columns if col.table.id == self.id and col.is_start_date == 1]
        return col_list[0] if col_list else None

    @property
    def end_date_col(self):
        col: Column
        col_list = [col for col in self.columns if col.table.id == self.id and col.is_end_date == 1]
        return col_list[0] if col_list else None

    @property
    # @functools.cached_property
    def ddl(self) -> str:
        self._ddl = None
        if self.table_kind == 'T':
            col_dtype = ''
            set_multiset = 'multiset' if self.multiset == 1 else 'set'
            schema_name = self.schema.schema_name
            table_name = self.table_name
            pi_cols_lst = []
            col: Column
            for idx, col in enumerate(self.columns):
                comma = ',' if idx > 0 else ' '
                col_name = col.column_name
                data_type = col.data_type.dt_name.upper()
                precision = "(" + str(col.dt_precision) + ")" if col.dt_precision else ''
                if 'CHAR' in data_type:
                    latin_unicode = "CHARACTER SET " + ("unicode" if col.unicode == 1 else "latin")
                    case_sensitive = ("not " if col.case_sensitive == 0 else '') + "CASESPECIFIC"
                    precision = "(50)" if not precision else precision
                else:
                    latin_unicode = ''
                    case_sensitive = ''

                not_null = 'not null' if col.mandatory == 1 else ''
                col_dtype += COL_DTYPE_TEMPLATE.format(col_name=col_name
                                                       , data_type=data_type, precision=precision, latin_unicode=latin_unicode
                                                       , case_sensitive=case_sensitive
                                                       , not_null=not_null
                                                       , comma=comma)

            pi_cols_lst = list_to_string([col.column_name for col in self.key_col], separator=',')
            pi_index = PI_TEMPLATE.format(unique_pi='unique' if self.unique_pi else ''
                                          , pi_cols=pi_cols_lst) if pi_cols_lst else 'No Primary Index'
            self._ddl = DDL_TABLE_TEMPLATE.format(set_multiset=set_multiset
                                                  , schema_name=schema_name
                                                  , table_name=table_name
                                                  , col_dtype=col_dtype
                                                  , pi_index=pi_index
                                                  , si_index='')

        elif self.table_kind == 'V':
            self.pipeline: Pipeline
            if self.pipeline:
                self._ddl = DDL_VIEW_TEMPLATE.format(create_replace=CREATE_REPLACE
                                                     , schema_name=self.schema.schema_name
                                                     , view_name=self.table_name
                                                     , query_txt=self.pipeline.query)

        return (self._ddl.strip() + ';\n') if self._ddl is not None else None

    @property
    def layers(self) -> []:
        lt: LayerTable
        return [lt.layer for lt in LayerTable.get_all_instances() if lt.table.id == self.id]

    @property
    def pipeline(self):
        pipe: Pipeline
        for pipe in Pipeline.get_all_instances():
            if pipe.lyr_view:
                if pipe.lyr_view.table.id == self.id:
                    return pipe


class DataSetType(MyID):
    def __init__(self, name: str, **kwargs):
        super().__init__(**kwargs)
        self.name = name

    @property
    def data_sets(self) -> []:
        return [ds for ds in DataSet.get_all_instances() if ds.data_set_type.id == self.id]


class DataSet(MyID):
    def __init__(self, set_type_id: int, set_code: str, set_table_id: int, surrogate_table_id: int, *args, **kwargs):
        assert set_table_id, "set_table_id is mandatory!"
        assert surrogate_table_id, "surrogate_table_id is mandatory!"
        super().__init__(*args, **kwargs)
        self._set_type_id = set_type_id
        self.set_code = set_code
        self._set_table_id = set_table_id
        self._surrogate_table_id = surrogate_table_id

    @property
    def data_set_type(self) -> DataSetType:
        return DataSetType.get_instance(_id=self._set_type_id)

    @property
    def surrogate_table(self) -> Table:
        return Table.get_instance(_id=self._surrogate_table_id)

    @property
    def set_table(self) -> Table:
        return Table.get_instance(_id=self._set_table_id)

    @property
    def domains(self) -> []:
        domain: Domain
        return [domain for domain in Domain.get_all_instances() if domain.data_set.id == self.id]

    @classmethod
    def get_by_name(cls, set_type_id: int, set_name: str):
        ds: cls
        for ds in cls.get_all_instances():
            if ds._set_type_id == set_type_id and ds.set_table.table_name == set_name.upper():
                return ds


class Domain(MyID):
    __domain_name_dic = {}

    def __init__(self, data_set_id, domain_code, domain_name, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._data_set_id = data_set_id
        self.domain_code = domain_code
        self._domain_name = domain_name
        self.__add_name_dic()

    def __add_name_dic(self):
        if self._data_set_id not in self.__domain_name_dic.keys():
            self.__domain_name_dic[self._data_set_id] = {}

        if self.domain_name not in self.__domain_name_dic[self._data_set_id].keys():
            self.__domain_name_dic[self._data_set_id][self.domain_name] = self.domain_code

    @property
    def data_set(self) -> DataSet:
        return DataSet.get_instance(_id=self._data_set_id)

    @property
    def domain_name(self) -> str:
        return self._domain_name.upper()

    @classmethod
    def get_by_name(cls, data_set_id: int, domain_name: str):
        try:
            domain_code = cls.__domain_name_dic[data_set_id][domain_name.upper()]
            return cls.get_instance(_key=(data_set_id, domain_code))
        except:
            return None

    @property
    def values(self) -> []:
        dv: DomainValue
        return [dv for dv in DomainValue.get_all_instances() if dv.domain.id == self.id]


class DomainValue(MyID):
    __edw_keys = {}

    def __init__(self, domain_id, source_key, edw_key, description, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._domain_id = domain_id
        self.source_key = source_key
        self.edw_key = edw_key
        self._description = description
        assert self.valid_edw_key_desc(edw_key, description), "Invalid EDW Key Description!"

        if self.domain.data_set.set_table.id not in self.__edw_keys.keys():
            self.__edw_keys[self.domain.data_set.set_table.id] = {}

        if self.edw_key not in self.__edw_keys[self.domain.data_set.set_table.id].keys():
            self.__edw_keys[self.domain.data_set.set_table.id][self.edw_key] = self.description



    @property
    def description(self):
        if isinstance(self._description, str): 
            return self._description.lower()
        return self._description
    @property
    def domain(self) -> Domain:
        return Domain.get_instance(_id=self._domain_id)

    def valid_edw_key_desc(self, edw_key, desc:str):
        try:
            return self.__edw_keys[self.domain.data_set.set_table.id][edw_key] == desc.lower()
        except:
            return True

class Column(MyID):
    def __init__(self, table_id: int, column_name: str
                 , is_pk: int = 0, mandatory: int = 0
                 , is_start_date: int = 0, is_end_date: int = 0
                 , is_created_at: int = 0, is_updated_at: int = 0
                 , is_created_by: int = 0, is_updated_by: int = 0
                 , is_delete_flag: int = 0, is_modification_type: int = 0
                 , is_load_id: int = 0, is_batch_id: int = 0
                 , is_row_identity: int = 0
                 , domain_id: int = None, data_type_id: int = None
                 , dt_precision: int = None, unicode: int = 0
                 , case_sensitive: int = 0, active: int = 1, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._table_id = table_id
        self._domain_id = domain_id
        self._data_type_id = data_type_id

        self._column_name = column_name
        self.mandatory = mandatory
        self.dt_precision = dt_precision
        self.active = active
        self.unicode = unicode
        self.case_sensitive = case_sensitive

        self.is_pk = is_pk
        self.is_start_date = is_start_date
        self.is_end_date = is_end_date
        self.is_created_at = is_created_at
        self.is_created_by = is_created_by
        self.is_updated_at = is_updated_at
        self.is_updated_by = is_updated_by
        self.is_delete_flag = is_delete_flag
        self.is_modification_type = is_modification_type
        self.is_load_id = is_load_id
        self.is_batch_id = is_batch_id
        self.is_row_identity = is_row_identity

        self.is_technical_col = True if (self.is_batch_id or self.is_row_identity or self.is_load_id or self.is_modification_type
                                         or self.is_created_at or self.is_created_by
                                         or self.is_updated_at or self.is_updated_by or self.is_delete_flag) else False

    @property
    def data_type(self) -> DataType:
        return DataType.get_instance(_id=self._data_type_id)

    @property
    def column_name(self):
        return self._column_name.upper()

    @property
    def table(self) -> Table:
        return Table.get_instance(_id=self._table_id)

    @property
    def domain(self) -> Domain:
        return Domain.get_instance(_id=self._domain_id)


class LayerType(MyID):
    def __init__(self, type_name, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.type_name = type_name


class Layer(MyID):
    def __init__(self, layer_name: str, type_id: int, abbrev: str = None, layer_level: int = None, active: int = 1, notes: str = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        assert layer_level is not None
        self._type_id = type_id
        self.layer_name = layer_name
        self.abbrev = abbrev
        self.layer_level = layer_level
        self.active = active
        self.notes = notes

    @property
    def layer_type(self) -> LayerType:
        return LayerType.get_instance(_id=self._type_id)

    @property
    def layer_tables(self) -> []:
        return [lt for lt in LayerTable.get_all_instances() if lt.layer.id == self.id]


class LayerTable(MyID):
    def __init__(self, layer_id: int, table_id: int, active: int = 1, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._layer_id = layer_id
        self._table_id = table_id
        self.active = active

    @property
    def layer(self) -> Layer:
        return Layer.get_instance(_id=self._layer_id)

    @property
    def table(self) -> Table:
        return Table.get_instance(_id=self._table_id)

    @property
    def src_pipelines(self) -> []:
        return [pipe for pipe in Pipeline.get_all_instances() if pipe.src_lyr_table.id == self.id]

    @property
    def tgt_pipelines(self) -> []:
        return [pipe for pipe in Pipeline.get_all_instances() if pipe.tgt_lyr_table.id == self.id]

    @property
    def core_lookup_ds(self) -> DataSet:
        if self.layer.layer_type.type_name == 'CORE':
            bmap_dst: DataSetType
            ds: DataSet

            bmap_dst = DataSetType.get_instance(_key=DS_BMAP)
            core_ds = DataSet.get_by_name(set_type_id=bmap_dst.id, set_name=self.table.table_name)
            return core_ds

    @property
    def dml(self) -> str:
        # for bmap data and base lookups

        schema_name = self.table.schema.schema_name
        table_name = self.table.table_name
        columns = list_to_string([col.column_name for col in self.table.columns], ',')
        dml = ''
        if self.table.is_bmap:
            # based on the layer type, select the data from DomainValues
            data_set: DataSet
            domain: Domain
            dv: DomainValue
            for data_set in self.table.surrogate_data_sets:
                for domain in data_set.domains:
                    domain_code = domain.domain_code
                    set_code = data_set.set_code
                    for dv in domain.values:
                        src_code = str(dv.source_key).replace("'", "''")
                        edw_code = dv.edw_key
                        desc = str(dv.description).replace("'", "''")
                        dml += f"""insert into {schema_name}.{table_name}\n({columns})\nvalues ('{src_code}', {domain_code}, {set_code}, {edw_code}, '{desc}' );\n"""
        return dml


class Pipeline(MyID):
    def __init__(self
                 , lyr_view_id: int
                 , name: str = None
                 , src_lyr_table_id: int | None = None
                 , tgt_lyr_table_id: int | None = None
                 , select_asterisk: bool = False
                 , src_table_alias: str = None
                 , domain_id: int = None
                 , distinct: bool = False
                 , active: int = 1

                 , *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.distinct = distinct
        self._src_lyr_table_id = src_lyr_table_id
        self._tgt_lyr_table_id = tgt_lyr_table_id
        self._lyr_view_id = lyr_view_id
        self._alphabets = ALPHABETS.copy()
        self._src_table_alias = None
        self._aliases: dict = {}
        self.all_cols_constant = True if not select_asterisk else False
        self._domain_id = domain_id
        self.select_asterisk = select_asterisk
        self._name = name if name else self.lyr_view.table.table_name

        assert self._name, "Pipeline name is missing!"

        if self.src_lyr_table:
            self._src_table_alias = src_table_alias if src_table_alias else self._alphabets.pop(0)
            self.add_new_alias(self._src_table_alias, self.src_lyr_table.table)

        self.active = active

        if self.tgt_lyr_table:
            if self.tgt_lyr_table.table.table_kind == 'V':
                self._lyr_view_id = self.tgt_lyr_table.table.id

        assert self.lyr_view.table.table_kind == 'V', 'Pipeline must be linked to a view only!'
        assert self._src_lyr_table_id, "Source Table is missing!"

    @property
    def name(self):
        return self._name.upper()

    @property
    def domain(self) -> Domain:
        return Domain.get_instance(_id=self._domain_id)

    @property
    def lyr_view(self) -> LayerTable:
        return LayerTable.get_instance(_id=self._lyr_view_id)

    @property
    def src_lyr_table(self) -> LayerTable:
        return LayerTable.get_instance(_id=self._src_lyr_table_id)

    @property
    def tgt_lyr_table(self) -> LayerTable:
        return LayerTable.get_instance(_id=self._tgt_lyr_table_id)

    @property
    def src_table_alias(self):
        return self._src_table_alias if self._src_table_alias else ''

    @property
    def column_mapping(self) -> []:
        cm: ColumnMapping
        return [cm for cm in ColumnMapping.get_all_instances() if cm.pipeline.id == self.id]

    @property
    def scd_type0_col(self) -> []:
        cm: ColumnMapping
        if self.column_mapping:
            return [cm for cm in self.column_mapping if cm.scd_type == 0]

    @property
    def scd_type1_col(self) -> []:
        cm: ColumnMapping
        if self.column_mapping:
            return [cm for cm in self.column_mapping if cm.scd_type == 1]

    @property
    def scd_type2_col(self) -> []:
        cm: ColumnMapping
        if self.column_mapping:
            return [cm for cm in self.column_mapping if cm.scd_type == 2]

    @property
    def group_by(self) -> []:
        return [gb for gb in GroupBy.get_all_instances() if gb.pipeline.id == self.id]

    @property
    def group_by_col_names(self) -> []:
        gb: GroupBy
        return [f"{gb.column.column_name}" for gb in GroupBy.get_all_instances() if gb.pipeline.id == self.id]

    def _tgt_col_dic(self) -> dict:
        col_m: ColumnMapping
        _dic = {}
        for col_m in self.column_mapping:
            if not col_m.tgt_col.is_technical_col:
                if col_m.tgt_col not in _dic.keys():
                    _dic[col_m.tgt_col] = []
                _dic[col_m.tgt_col].append(col_m.src_col_trx)

        return _dic

    @property
    def _filters(self) -> []:
        f: Filter
        return [f.filter_expr for f in Filter.get_all_instances() if f.pipeline.id == self.id]

    @property
    def join_with(self) -> []:
        j: JoinWith
        return [j for j in JoinWith.get_all_instances() if j.pipeline.id == self.id]

    @property
    def technical_cols(self) -> []:
        col: Column
        if self.src_lyr_table:
            return [col for col in self.src_lyr_table.table.columns if col.is_technical_col]

    def add_new_alias(self, alias: str, table: Table) -> None:
        self._aliases[alias.lower()] = table.id

    def get_table_by_alias(self, alias: str) -> Table:
        try:
            table_id = self._aliases[alias.lower()]
            return Table.get_instance(_id=table_id)
        except KeyError:
            return None

    @property
    def query(self):
        distinct = 'distinct' if self.distinct else ''
        col_mapping = ''
        with_clause = ''
        from_clause = ''
        join_clause = ''
        where_clause = ''
        group_by_clause = ''
        having_clause = ''

        if not self.all_cols_constant:
            from_clause = FROM_TEMPLATE.format(schema_name=self.src_lyr_table.table.schema.schema_name
                                               , table_name=self.src_lyr_table.table.table_name
                                               , alias=self.src_table_alias)

            join_clause = ''
            jw: JoinWith
            for jw in self.join_with:
                with_lyr_table = jw.with_lyr_table.table
                with_table = with_lyr_table.schema.schema_name + '.' + with_lyr_table.table_name
                join_clause += JOIN_CLAUSE_TEMPLATE.format(join_type=jw.join_type.name
                                                           , with_table=with_table
                                                           , with_alias=jw.with_alias
                                                           , on_clause=jw.join_on.complete_join_on_expr
                                                           )

            where_clause = WHERE_TEMPLATE.format(conditions=list_to_string(self._filters, ' and ')) if self._filters else ''
            group_by_clause = GROUP_BY_TEMPLATE.format(columns=list_to_string(self.group_by_col_names, ',')) if self.group_by_col_names else ''
            having_clause = ''

        #############
        tgt_col: Column
        src_cols: str
        src_col: Column
        technical_cols: Column
        if not self.select_asterisk:
            for index, dic_items in enumerate(self._tgt_col_dic().items()):
                tgt_col, _src_cols = dic_items
                src_cols = list_to_string(_src_cols, '||')
                comma = '\n,' if index > 0 else ' '
                precise = ''
                cast_dtype = ''
                alias = tgt_col.column_name
                if tgt_col.table.table_kind == 'T':
                    dtype_name = tgt_col.data_type.dt_name
                    if tgt_col.dt_precision:
                        precise = '(' + str(tgt_col.dt_precision) + ')'
                    cast_dtype = CAST_DTYPE_TEMPLATE.format(dtype_name=dtype_name, precise=precise)

                col_mapping += COL_MAPPING_TEMPLATE.format(comma=comma
                                                           , col_name=src_cols
                                                           , cast_dtype=cast_dtype
                                                           , alias=alias
                                                           )
            if self.lyr_view.layer.layer_type.type_name != 'SK':  # avoid adding technical columns for BKEY input views
                if col_mapping:
                    for technical_cols in self.technical_cols:
                        comma = '\n,'
                        src_cols = f"{self.src_table_alias}.{technical_cols.column_name}"
                        cast_dtype = ""
                        alias = ""
                        col_mapping += COL_MAPPING_TEMPLATE.format(comma=comma
                                                                   , col_name=src_cols
                                                                   , cast_dtype=cast_dtype
                                                                   , alias=alias
                                                                   )

        else:
            col_mapping = " * "

        query = QUERY_TEMPLATE.format(with_clause=with_clause
                                      , distinct=distinct
                                      , col_mapping=col_mapping
                                      , from_clause=from_clause
                                      , join_clause=join_clause
                                      , where_clause=where_clause
                                      , group_by_clause=group_by_clause
                                      , having_clause=having_clause
                                      )

        return re.sub(r'\n+', '\n', query)


class JoinType(MyID):
    def __init__(self, code: str, name: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.code = code
        self.name = name


class JoinWith(MyID):
    def __init__(self, pipeline_id: int,
                 master_lyr_table_id: int, master_alias: str,
                 join_type_id: int,
                 with_lyr_table_id: int, with_alias: str,
                 *args, **kwargs):
        self._pipeline_id = pipeline_id
        self._master_lyr_table_id = master_lyr_table_id
        self._master_alias = master_alias
        self._join_type_id = join_type_id
        self._with_lyr_table_id = with_lyr_table_id
        self._with_alias = with_alias

        assert with_lyr_table_id, "join table can't be empty!"
        assert self.with_alias, "join table's alias can't be empty!"
        self.pipeline.add_new_alias(self.with_alias, self.with_lyr_table.table)
        super().__init__(*args, **kwargs)

    @functools.cached_property
    def pipeline(self) -> Pipeline:
        return Pipeline.get_instance(_id=self._pipeline_id)

    @functools.cached_property
    def master_lyr_table(self) -> LayerTable:
        return LayerTable.get_instance(_id=self._master_lyr_table_id)

    @functools.cached_property
    def with_lyr_table(self) -> LayerTable:
        return LayerTable.get_instance(_id=self._with_lyr_table_id)

    @functools.cached_property
    def join_type(self) -> JoinType:
        return JoinType.get_instance(_id=self._join_type_id)

    @functools.cached_property
    def with_alias(self) -> str:
        return self._with_alias if self._with_alias else self.with_lyr_table.table.table_name

    @functools.cached_property
    def master_alias(self) -> str:
        return self._master_alias if self._master_alias else self.master_lyr_table.table.table_name

    @property
    def join_on(self):
        join_on: JoinOn
        for join_on in JoinOn.get_all_instances():
            if join_on.join_with.id == self.id:
                return join_on


class JoinOn(MyID):
    def __init__(self, join_with_id: int
                 , complete_join_on_expr: str = None
                 , *args, **kwargs):
        self._join_with_id = join_with_id
        self._complete_join_on_expr = complete_join_on_expr

        super().__init__(*args, **kwargs)

    @functools.cached_property
    def join_with(self) -> JoinWith:
        return JoinWith.get_instance(_id=self._join_with_id)

    @property
    def complete_join_on_expr(self):
        return self._complete_join_on_expr

class ColumnMapping(MyID):
    """
    scd_type:
        Type 0 – Fixed Dimension. No changes allowed, dimension never changes.
        Type 1 – No History.
        Type 2 – Row Versioning.
    """

    def __init__(self
                 , pipeline_id: int
                 , tgt_col_id: int
                 , col_seq: int = 0
                 , src_col_id: int | None = None
                 , src_table_alias: int | None = None
                 , src_col_trx: str = None
                 , constant_value: bool = False
                 , fn_value_if_null=None
                 , scd_type: int = 1
                 , *args, **kwargs):

        self._pipeline_id = pipeline_id
        self._tgt_col_id = tgt_col_id
        self.col_seq = col_seq
        self._src_col_id = src_col_id
        self.src_table_alias = src_table_alias
        self.fn_value_if_null = fn_value_if_null
        self._src_col_trx = src_col_trx
        self.scd_type = scd_type
        self.constant_value = constant_value

        if src_col_id:
            alias_error_msg = f'Table alias for {self.src_col.column_name} is missing!'
            if not self.src_table_alias:
                self.src_table_alias = self.pipeline.src_table_alias

            assert self.src_table_alias, alias_error_msg
        assert self.valid_tgt_col, "Invalid target column, make sure all target columns are related to one table!"

        if not self.constant_value:
            self.pipeline.all_cols_constant = False
        super().__init__(*args, **kwargs)

    @property
    def pipeline(self) -> Pipeline:
        return Pipeline.get_instance(_id=self._pipeline_id)

    @property
    def tgt_col(self) -> Column:
        return Column.get_instance(_id=self._tgt_col_id)

    @property
    def src_col(self) -> Column:
        return Column.get_instance(_id=self._src_col_id)

    @property
    def src_col_trx(self):
        if self._src_col_trx:
            _src_col_trx = '(' + str(self._src_col_trx) + ')'
        elif self.src_col:
            _src_col_trx = f"{self.src_table_alias}.{self.src_col.column_name}"
        else:
            _src_col_trx = 'NULL'
        if self.pipeline.domain or self.tgt_col.domain:
            _src_col_trx = f"trim({_src_col_trx})"

        if self.tgt_col.domain:

            if self.tgt_col.domain.data_set.data_set_type.name == DS_BKEY:
                source_key_datatype = self.tgt_col.domain.data_set.surrogate_table.get_column_datatype('source_key')
                source_key_cast = "({data_type} ({precision}))".format(data_type=source_key_datatype[0], precision=source_key_datatype[1])
                _src_col_trx = SRCI_V_BKEY_TEMPLATE_QUERY.format(bkey_db=self.tgt_col.domain.data_set.surrogate_table.schema.schema_name
                                                                 , bkey_table_name=self.tgt_col.domain.data_set.surrogate_table.table_name
                                                                 , src_key=_src_col_trx
                                                                 , cast=source_key_cast
                                                                 , domain_id=self.tgt_col.domain.domain_code
                                                                 )
            elif self.tgt_col.domain.data_set.data_set_type.name == DS_BMAP:
                source_code_datatype = self.tgt_col.domain.data_set.surrogate_table.get_column_datatype('source_code')
                source_key_cast = "({data_type} ({precision}))".format(data_type=source_code_datatype[0], precision=source_code_datatype[1])
                _src_col_trx = SRCI_V_BMAP_TEMPLATE_QUERY.format(bmap_db=self.tgt_col.domain.data_set.surrogate_table.schema.schema_name
                                                                 , bmap_table_name=self.tgt_col.domain.data_set.surrogate_table.table_name
                                                                 , code_set_id=self.tgt_col.domain.data_set.set_code
                                                                 , source_code=_src_col_trx
                                                                 , cast=source_key_cast
                                                                 , domain_id=self.tgt_col.domain.domain_code)


        return str(_src_col_trx)

    @property
    def valid_tgt_col(self) -> bool:
        return True if self.pipeline.tgt_lyr_table.table.id == self.tgt_col.table.id else False


class Filter(MyID):
    def __init__(self
                 , pipeline_id: int
                 , filter_seq: int
                 , col_id: int = None
                 , complete_filter_expr: str = None
                 , *args, **kwargs):
        self._pipeline_id = pipeline_id
        self.filter_seq = filter_seq
        self._complete_filter_expr = complete_filter_expr
        self._col_id = col_id
        super().__init__(*args, **kwargs)

    @functools.cached_property
    def pipeline(self) -> Pipeline:
        return Pipeline.get_instance(_id=self._pipeline_id)

    @property
    def column(self) -> Column:
        return Column.get_instance(_id=self._col_id)

    @property
    def filter_expr(self):
        return self._complete_filter_expr


class OrFilter(MyID):
    def __init__(self
                 , filter_id: int
                 , col_id: int
                 , *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._filter_id = filter_id
        self._col_id = col_id

    @property
    def filter(self) -> Filter:
        return Filter.get_instance(_id=self._filter_id)

    @property
    def column(self) -> Column:
        return Column.get_instance(_id=self._col_id)


class GroupBy(MyID):
    def __init__(self, pipeline_id: int, col_id: int, *args, **kwargs):
        self._pipeline_id = pipeline_id
        self._col_id = col_id
        assert self.column.table.id == self.pipeline.tgt_lyr_table.table.id, "Invalid group by column, must be one of the pipeline's target table!"
        super().__init__(*args, **kwargs)

    @property
    def pipeline(self) -> Pipeline:
        return Pipeline.get_instance(_id=self._pipeline_id)

    @property
    def column(self) -> Column:
        return Column.get_instance(_id=self._col_id)


if __name__ == '__main__':
    x = DataSetType(name='xxxX')
    y = DataSet(set_type_id=x.id, set_code="ztztztz", set_table_id=11, surrogate_table_id=99)

    print(y.id, y._set_table_id)

    y = DataSet(set_type_id=x.id, set_code="ztztztz", set_table_id=33, surrogate_table_id=99, _override=1)
    print(y.id, y._set_table_id)

    y = DataSet(set_type_id=x.id, set_code="qaqaqaqa", set_table_id=66, surrogate_table_id=99, _override=1)
    print(y.id, y._set_table_id)

    z = DataSet.get_instance(_key=(x.id, "ztztztz"))
    print(z.id, z._set_table_id)
