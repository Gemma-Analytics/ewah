from airflow.models import BaseOperator
from airflow.hooks.base_hook import BaseHook

from ewah.dwhooks.dwhook_snowflake import EWAHDWHookSnowflake
from ewah.dwhooks.dwhook_postgres import EWAHDWHookPostgres
from ewah.constants import EWAHConstants as EC


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

    upload_call_count = 0

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
    *args, **kwargs):

        if not dwh_engine or not dwh_engine in EC.DWH_ENGINES:
            raise Exception('Invalid DWH Engine: {0}\n\nAccapted Engines:{1}'
                .format(
                str(dwh_engine),
                '\n'.join(EC.DWH_ENGINES),
            ))

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

        super().__init__(*args, **kwargs)

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
            update_on_columns = [primary_key_column_name]
        self.update_on_columns = update_on_columns
        self.primary_key_column_name = primary_key_column_name # may be used ...
        #   ... by a child class at execution!

        self.hook = {
            EC.DWH_ENGINE_POSTGRES: EWAHDWHookPostgres,
            EC.DWH_ENGINE_SNOWFLAKE: EWAHDWHookSnowflake,
            # DWH_ENGINE_BIGQUERY: bq_hook,
            # DWH_ENGINE_REDSHIFT: rs_hook,
        }[self.dwh_engine]

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
            for field in datum.keys():
                if not (result.get(field, {}).get(EC.QBC_FIELD_TYPE) \
                    == inconsistent_data_type) and datum[field]:
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

    def upload_data(
        self,
        data=None,
        columns_definition=None,
    ):
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
            columns_definition = self._create_columns_definition(data)

        hook = self.hook(self.dwh_conn_id)

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
            schema_name=self.target_schema_name+self.target_schema_suffix,
            database_name=self.target_database_name,
            drop_and_replace=self.drop_and_replace and \
                (self.upload_call_count == 1), # In case of chunking of uploads
            update_on_columns=self.update_on_columns,
            commit=True,
            logging_function=self.log.info,
        )
        #hook.commit()
        hook.close()

class EWAHEmptyOperator(EWAHBaseOperator):
    def __init__(self, *args, **kwargs):
        raise Exception('Failed to load operator! Probably missing' \
            + ' requirements for the operator in question.')
