from airflow.models import BaseOperator
from airflow.hooks.base_hook import BaseHook
from airflow.utils.decorators import apply_defaults

from ewah.dwhooks import get_dwhook
from ewah.constants import EWAHConstants as EC

import hashlib

class EWAHBaseOperator(BaseOperator):
    """Extension of airflow's native Base Operator.

    EWAH operators always work in the same way: They extract raw data from a
    source and load it into a relational database aka data warehouse aka DWH.
    Transformations only happen insofar as they are required to bring data
    into a relational format.

    *How to use*
    The child class extracts data from the source. At the end of the child
    class' execute() function, it calls self.update(). Optional: The child can
    also repeatedly call the self.update() class for chunking.

    *Arguments of self.update():*
    Data is a list of dictionaries, where each list element will become
    one row of data and the contained dict is fieldname:value. Value can be
    None, or a fieldname can be missing in a particular dictionary, in which
    case the value is assumed to be None.

    columns_definition may be supplied at the self.update() function call,
    at initialization, or not at all, depending on the usecase. The use order is
    - If available, take the columns_definition from the self.update() call
    - If None, check if columns_definition was supplied to the operator at init
    - If neither, create a columns_definition on the fly using (all) the data

    columns_definition is a dictionary with fieldname:properties. Properties
    is None or a dictionary of option:value where option can be one of the
    following:
    - EC.QBC_FIELD_TYPE -> String: Field type (default text)
    - EC.QBC_FIELD_PK -> Boolean: Is this field the primary key? (default False)
    - EC.QBC_FIELD_NN -> Boolean: Not Null constraint for this field? (False)
    - EC.QBC_FIELD_UQ -> Boolean: Unique constraint for this field? (False)

    Note that the value of EC.QBC_FIELD_TYPE is DWH-engine specific!

    columns_definition is case sensitive.

    Implemented DWH engines:
    - PostgreSQL
    - Snowflake

    Next up:
    - BigQuery
    - Redshift
    """

    _IS_INCREMENTAL = False # Child class must update these values accordingly.
    _IS_FULL_REFRESH = False # Defines whether operator is usable in factories.

    _REQUIRES_COLUMNS_DEFINITION = False # raise error if true an none supplied

    _INDEX_QUERY = '''
        CREATE INDEX IF NOT EXISTS {0}
        ON "{1}"."{2}" ({3})
    '''

    upload_call_count = 0

    _metadata = {} # to be updated by operator, if applicable

    def __init__(
        self,
        source_conn_id,
        dwh_engine,
        dwh_conn_id,
        target_table_name,
        target_schema_name,
        target_schema_suffix='_next',
        target_database_name=None, # Only for Snowflake
        columns_definition=None,
        drop_and_replace=True,
        update_on_columns=None,
        primary_key_column_name=None,
        clean_data_before_upload=True,
        add_metadata=True, # adds columns with metadata to all rows
        # Note: that metadata is specified by a dict on operator level!
        # metadata can only be added if no columns definition is given
        exclude_columns=[], # list of columns to exclude, if no
        # columns_definition was supplied (e.g. for select * with sql)
        index_columns=[], # list of columns to create an index on. can be
        # an expression, must be quoted in list of quoting is required.
    *args, **kwargs):
        super().__init__(*args, **kwargs)

        if columns_definition and exclude_columns:
            raise Exception('Must not supply both columns_definition and ' \
                + 'exclude_columns!')

        if not dwh_engine or not dwh_engine in EC.DWH_ENGINES:
            raise Exception('Invalid DWH Engine: {0}\n\nAccapted Engines:{1}'
                .format(
                str(dwh_engine),
                '\n'.join(EC.DWH_ENGINES),
            ))

        if index_columns and not dwh_engine == EC.DWH_ENGINE_POSTGRES:
            raise Exception('Indices are only allowed for PostgreSQL DWHs!')

        if dwh_engine == EC.DWH_ENGINE_SNOWFLAKE:
            if not target_database_name:
                conn_db_name = BaseHook.get_connection(dwh_conn_id)
                conn_db_name = conn_db_name.extra_dejson.get('database')
                if conn_db_name:
                    target_database_name = conn_db_name
                else:
                    raise Exception('If using DWH Engine {0}, must provide {1}!'
                        .format(
                            dwh_engine,
                            '"target_database_name" to specify the Database',
                        )
                    )
        else:
            if target_database_name:
                raise Exception('Received argument for "target_database_name"!')

        if self._REQUIRES_COLUMNS_DEFINITION:
            if not columns_definition:
                raise Exception('This operator requires the argument ' \
                    + 'columns_definition!')

        if primary_key_column_name and update_on_columns:
            raise Exception('Cannot supply BOTH primary_key_column_name AND' + \
                ' update_on_columns!')

        if not drop_and_replace:
            if not (
                update_on_columns
                or primary_key_column_name
                or (columns_definition and (0 < sum([
                        bool(columns_definition[col].get(
                            EC.QBC_FIELD_PK
                        )) for col in list(columns_definition.keys())
                    ])))
                ):
                raise Exception("If this is incremental loading of a table, "
                    + "one of the following is required:"
                    + "\n- List of columns to update on (update_on_columns)"
                    + "\n- Name of the primary key (primary_key_column_name)"
                    + "\n- Column definition (columns_definition) that includes"
                    + " the primary key(s)"
                )

        self.source_conn_id = source_conn_id
        self.dwh_engine = dwh_engine
        self.dwh_conn_id = dwh_conn_id
        self.target_table_name = target_table_name
        self.target_schema_name = target_schema_name
        self.target_schema_suffix = target_schema_suffix
        self.target_database_name = target_database_name
        self.columns_definition = columns_definition
        self.drop_and_replace = drop_and_replace
        if (not update_on_columns) and primary_key_column_name:
            if type(primary_key_column_name) == str:
                update_on_columns = [primary_key_column_name]
            elif type(primary_key_column_name) in (list, tuple):
                update_on_columns = primary_key_column_name
        self.update_on_columns = update_on_columns
        self.clean_data_before_upload = clean_data_before_upload
        self.primary_key_column_name = primary_key_column_name # may be used ...
        #   ... by a child class at execution!
        self.add_metadata = add_metadata
        self.exclude_columns = exclude_columns
        self.index_columns = index_columns

        self.hook = get_dwhook(self.dwh_engine)

    def execute(self, context):
        """ Why this method is defined here:
            When executing a task, airflow calls this method. Generally, this
            method contains the "business logic" of the individual operator.
            However, EWAH may want to do some actions for all operators. Thus,
            the child operators shall have an ewah_execute() function which is
            called by this general execute() method.
        """
        # the upload hook is used in the self.upload_data() function
        # which is called by the child's ewah_execute function whenever there is
        # data to upload.
        self.upload_hook = self.hook(self.dwh_conn_id)
        # execute operator
        result = self.ewah_execute(context)
        # if PostgreSQL and arg given: create indices
        for column in self.index_columns:
            # Use hashlib to create a unique 63 character string as index
            # name to avoid breaching index name length limits and accidental
            # duplicates / missing indices due to name truncation leading to
            # identical index names.
            self.hook.execute(self._INDEX_QUERY.format(
                '__ewah_' + hashlib.blake2b(
                    (self.target_schema_name
                        + self.target_schema_suffix
                        + '.'
                        + self.target_table_name
                        + '.'
                        + column
                    ).encode(),
                    digest_size=28,
                ).hexdigest(),
                self.target_schema_name + self.target_schema_suffix,
                self.target_table_name,
                column,
            ))
        # commit only at the end, so that no data may be committed before an
        # error occurs.
        self.log.info('Now committing changes!')
        self.upload_hook.commit()
        self.upload_hook.close()
        return result

    def test_if_target_table_exists(self):
        hook = self.hook(self.dwh_conn_id)
        if self.dwh_engine == EC.DWH_ENGINE_POSTGRES:
            result = hook.test_if_table_exists(
                table_name=self.target_table_name,
                schema_name=self.target_schema_name + self.target_schema_suffix,
            )
            hook.close()
            return result
        elif self.dwh_engine == EC.DWH_ENGINE_SNOWFLAKE:
            result = hook.test_if_table_exists(
                table_name=self.target_table_name,
                schema_name=self.target_schema_name + self.target_schema_suffix,
                database_name=self.target_database_name,
            )
            hook.close()
            return result
        raise Exception('Function not implemented for DWH {0}!'.format(
            dwh_engine
        ))

    def _create_columns_definition(self, data):
        "Create a columns_definition from data (list of dicts)."
        inconsistent_data_type = EC.QBC_TYPE_MAPPING[self.dwh_engine].get(
            EC.QBC_TYPE_MAPPING_INCONSISTENT
        )
        def get_field_type(value):
            return EC.QBC_TYPE_MAPPING[self.dwh_engine].get(
                type(value)
            ) or inconsistent_data_type

        result = {}
        for datum in data:
            if self.add_metadata and self._metadata:
                datum.update(self._metadata)
            for field in datum.keys():
                if field in self.exclude_columns:
                    datum[field] = None
                elif not (result.get(field, {}).get(EC.QBC_FIELD_TYPE) \
                    == inconsistent_data_type) and (not datum[field] is None):
                    if result.get(field):
                        # column has been added in a previous iteration.
                        # If not default column: check if new and old column
                        #   type identification agree.
                        if not (result[field][EC.QBC_FIELD_TYPE] \
                            == get_field_type(datum[field])):
                            self.log.info(
                                'WARNING! Data types are inconsistent.'
                                + ' Affected column: {0}'.format(field)
                            )
                            result[field][EC.QBC_FIELD_TYPE] = \
                                inconsistent_data_type

                    else:
                        # First iteration with this column. Add to result.
                        result.update({field:{
                            EC.QBC_FIELD_TYPE: get_field_type(datum[field])
                        }})
        return result

    def upload_data(self, data=None, columns_definition=None):
        """Upload data, no matter the source. Call this functions in the child
            operator whenever data is available for upload, as often as needed.
        """
        if not data:
            self.log.info('No data to upload!')
            return
        self.upload_call_count += 1
        self.log.info('Chunk {1}: Uploading {0} rows of data.'.format(
            str(len(data)),
            str(self.upload_call_count),
        ))

        columns_definition = columns_definition or self.columns_definition
        if not columns_definition:
            self.log.info('Creating table schema on the fly based on data.')
            # Note: This is also where metadata is added, if applicable
            columns_definition = self._create_columns_definition(data)

        if self.update_on_columns:
            pk_list = self.update_on_columns # is a list already
        elif self.primary_key_column_name:
            pk_list = [self.primary_key_column_name]
        else:
            pk_list = []

        if pk_list:
            for pk_name in pk_list:
                if not pk_name in columns_definition.keys():
                    raise Exception(('Column {0} does not exist but is ' + \
                        'expected!').format(pk_name))
                columns_definition[pk_name][EC.QBC_FIELD_PK] = True

        hook = self.upload_hook

        if (not self.drop_and_replace) or (self.upload_call_count > 1):
            self.log.info('Checking for, and applying schema changes.')
            new_cols, del_cols = hook.detect_and_apply_schema_changes(
                new_schema_name=self.target_schema_name+self.target_schema_suffix,
                new_table_name=self.target_table_name,
                new_columns_dictionary=columns_definition,
                # When introducing a feature utilizing this, remember to
                #  consider multiple runs within the same execution
                drop_missing_columns=False and self.upload_call_count==1,
                database=self.target_database_name,
                commit=False, # Commit only when / after uploading data
            )
            self.log.info('Added fields:\n\t{0}\nDeleted fields:\n\t{1}'.format(
                '\n\t'.join(new_cols) or '\n',
                '\n\t'.join(del_cols) or '\n',
            ))

        self.log.info('Uploading data now.')
        hook.create_or_update_table(
            data=data,
            columns_definition=columns_definition,
            table_name=self.target_table_name,
            schema_name=self.target_schema_name,
            schema_suffix=self.target_schema_suffix,
            database_name=self.target_database_name,
            drop_and_replace=self.drop_and_replace and \
                (self.upload_call_count == 1), # In case of chunking of uploads
            update_on_columns=self.update_on_columns,
            commit=False,
            logging_function=self.log.info,
            clean_data_before_upload=self.clean_data_before_upload,
        )
        """ Note on commiting changes:
            The hook used for data uploading is created at the beginning of the
            execute function and automatically committed and closed at the end.
            DO NOT commit in this function, as multiple uploads may be required,
            and any intermediate commit may be subsequently followed by an
            error, which would then result in incomplete data committed.
        """

class EWAHEmptyOperator(EWAHBaseOperator):
    _IS_INCREMENTAL = True
    _IS_FULL_REFRESH = True
    def __init__(self, *args, **kwargs):
        raise Exception('Failed to load operator! Probably missing' \
            + ' requirements for the operator in question.\n\nSupplied args:' \
            + '\n\t' + '\n\t'.join(args) + '\n\nSupplied kwargs:\n\t' \
            + '\n\t'.join(['{0}: {1}'.format(k, v) for k, v in kwargs.items()])
        )
