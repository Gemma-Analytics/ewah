from airflow.hooks.base_hook import BaseHook

from ewah.constants import EWAHConstants as EC

import json
from copy import deepcopy
from collections import OrderedDict
from psycopg2.extras import RealDictCursor

class EWAHBaseDWHook(BaseHook):
    """Extension of airflow's native Base Hook.

    Contains a few functions used by the EWAH Hooks for data cleaning prior
    to uploading. Each implemented DWH Engine has its own Ewah hook, which is
    always a child of this class and contains logic that is DWH specific.
    """

    def __init__(self, dwh_engine, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dwh_engine = dwh_engine
        self.credentials = self.get_connection(args[0])
        self._init_conn(first_call=True)

    def __del__(self, *args, **kwargs):
        self.close()
        if hasattr(super(), '__del__'):
            super().__del__(*args, **kwargs)

    def _init_conn(self, first_call=False, commit=False):
        if not first_call:
            if commit:
                self.commit()
            self.close()
        self.conn = self._create_conn()
        self.cur = self.conn.cursor()

        if self.dwh_engine == EC.DWH_ENGINE_POSTGRES:
            self.dict_cur = self.conn.cursor(
                cursor_factory=RealDictCursor
            )

        if self.dwh_engine == EC.DWH_ENGINE_SNOWFLAKE:
            self.cur.execute("BEGIN;")

    def _get_column_type(self, column_definition):
        return column_definition.get(
            EC.QBC_FIELD_TYPE,
            EC.QBC_TYPE_MAPPING[self.dwh_engine].get(
                EC.QBC_TYPE_MAPPING_DEFAULT,
                EC.QBC_TYPE_MAPPING_INCONSISTENT,
            ),
        )

    def close(self):
        if hasattr(self, 'cur') and self.cur:
            self.cur.close()
        if hasattr(self, 'dcur') and self.dcur:
            self.cur.close()
        if self.conn:
            self.conn.close()

    def execute(self, sql, params=None, commit=False, cursor=None):
        for statement in sql.split(';'):
            if statement.strip():
                kwargs = {}
                if params:
                    kwargs.update({'vars': params})
                (cursor or self.cur).execute(statement.strip(), **kwargs)
        if commit:
            self.commit()

    def execute_and_return_result(self, sql, params=None, return_dict=False):
        if ';' in sql.strip()[:-1]:
            raise Exception(
                'This function only executes a single statement! This query'
                + ' consists of more than one statement:\n' + sql
            )

        self.execute(
            sql=sql,
            params=params,
            commit=False,
            cursor=self.dict_cur if return_dict else None,
        )

        if self.dwh_engine == EC.DWH_ENGINE_POSTGRES:
            if return_dict:
                return self.dict_cur.fetchall()
            return self.cur.fetchall()

        if self.dwh_engine == EC.DWH_ENGINE_SNOWFLAKE:
            return [row for row in self.cur]

        raise Exception(
            'Function not implemented for this DWH Engine: {0}'.format(
                self.dwh_engine,
            )
        )

    def get_cursor(self, dict_cursor=False):
        if dict_cursor:
            if not hasattr(self, 'dict_cur'):
                raise Exception(
                    'Dictionary cursor is not yet implemented for {0}!'
                    .format(self.dwh_engine)
                )
            return self.dict_cur
        return self.cur

    def detect_and_apply_schema_changes(
        self,
        new_schema_name,
        new_table_name,
        new_columns_dictionary,
        drop_missing_columns=False,
        database=None,
        commit=False,
    ):
        params = {
            'schema_name': new_schema_name,
            'table_name': new_table_name,
        }
        if self.dwh_engine == EC.DWH_ENGINE_SNOWFLAKE:
            database = database or self.database
            params.update({'database_name': database})
        if not self.test_if_table_exists(**params):
            return ([], []) # Table did not previously exist, so there is nothing to do

        list_of_old_columns = [
            col[0].strip() for col in self.execute_and_return_result(
                sql=self._QUERY_SCHEMA_CHANGES_COLUMNS.format(**params),
                params=params,
                return_dict=False,
            )]
        list_of_columns = [
            col_name.strip() for col_name in list(new_columns_dictionary.keys())
        ]

        dropped_columns = []
        if drop_missing_columns:
            for old_column in list_of_old_columns:
                if not (old_column in list_of_columns):
                    drop_params = deepcopy(params)
                    drop_params.update({'column_name': old_column})
                    dropped_columns += [old_column]
                    self.execute(
                        sql=self._QUERY_SCHEMA_CHANGES_DROP_COLUMN.format(
                            **drop_params,
                        ),
                        commit=False,
                    )

        new_columns = []
        for column in list_of_columns:
            if not (column in list_of_old_columns):
                new_columns += [column]
                add_params = deepcopy(params)
                add_params.update({
                    'column_name': column,
                    'column_type': self._get_column_type(
                        new_columns_dictionary[column],
                    )
                })
                self.execute(
                    sql=self._QUERY_SCHEMA_CHANGES_ADD_COLUMN.format(
                        **add_params,
                    ),
                    commit=False,
                )

        if commit:
            self.commit()

        return (new_columns, dropped_columns)

    def create_or_update_table(
        self,
        data,
        columns_definition,
        table_name,
        schema_name,
        database_name=None,
        drop_and_replace=True,
        update_on_columns=None,
        commit=False,
        logging_function=None,
    ):
        logging_function = logging_function or (lambda *args: None)
        database_name = database_name or \
            (getattr(self, 'database') if hasattr(self, 'database') else None)

        raw_row = {} # Used as template for params at execution
        sql_part_columns = [] # Used for CREATE and INSERT / UPDATE query
        jsonb_columns = [] # JSON columns require special treatment
        create_update_on_columns = (not (drop_and_replace or update_on_columns))
        update_on_columns = update_on_columns or []
        if not (type([]) == type (update_on_columns)):
            raise Exception('"update_on_columns" must be a list!' + \
                'Is currently: type {0}'.format(str(type(update_on_columns))))
        field_constraints_mapping = EC.QBC_FIELD_CONSTRAINTS_MAPPING.get(
            self.dwh_engine,
        )

        for column_name in columns_definition.keys():
            raw_row.update({column_name: None})
            definition = columns_definition[column_name]
            if not type({}) == type(definition):
                raise Exception('Column {0} is not properly defined!'.format(
                    column_name,
                ))
            sql_part_columns += ['"{0}"\t{1}\t{2}'.format(
                column_name, # Field name
                self._get_column_type(definition), # Type: text, int etc.
                ' '.join([ # Get all additional field properties (unique etc.)
                    field_constraints_mapping[addon] if definition.get(addon) else ''
                    for addon in list(field_constraints_mapping.keys())
                ])
            )]
            if self._get_column_type(definition) == 'jsonb':
                jsonb_columns += [column_name]
            if definition.get(field_constraints_mapping[EC.QBC_FIELD_PK]):
                update_on_columns += [column_name]
        sql_part_columns = ',\n\t'.join(sql_part_columns)

        logging_function('Cleaning data for upload...')
        upload_data = []
        for _ in range(len(data)):
            datum = data.pop(0)
            for column_name in datum.keys():
                if datum[column_name] == '\0':
                    datum[column_name] = None
                elif column_name in jsonb_columns:
                    if datum[column_name]:
                        datum[column_name] = json.dumps(datum[column_name])
                    else:
                        datum[column_name] = None
                elif type(datum[column_name]) in [type({}), OrderedDict]:
                    datum[column_name] = json.dumps(datum[column_name])

            # Make sure that each dict in upload_data has all keys, even if None
            row = deepcopy(raw_row)
            row.update(datum)
            upload_data += [row]

        logging_function('Uploading {0} rows of data...'.format(
            str(len(upload_data)),
        ))
        if database_name:
            kwargs = {'database_name': database_name}
        else:
            kwargs = {}
        kwargs.update({
            'data': upload_data,
            'table_name': table_name,
            'schema_name': schema_name,
            'columns_definition': columns_definition,
            'columns_partial_query': sql_part_columns,
            'update_on_columns': update_on_columns,
            'drop_and_replace': drop_and_replace,
            'logging_function': logging_function,
        })
        self._create_or_update_table(**kwargs)

        if commit:
            self.commit()
