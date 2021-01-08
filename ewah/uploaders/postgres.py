from ewah.uploaders.base import EWAHBaseUploader
from ewah.hooks.postgres import EWAHPostgresHook
from ewah.constants import EWAHConstants as EC
from psycopg2.extras import execute_values


class EWAHPostgresUploader(EWAHBaseUploader):

    _QUERY_SCHEMA_CHANGES_COLUMNS = """
        SELECT
        	f.attname AS "name"
        FROM pg_attribute f
        	JOIN pg_class cl ON cl.OID = f.attrelid
        	LEFT JOIN pg_namespace n ON n.OID = cl.relnamespace
        WHERE cl.relkind = 'r'::CHAR
        	AND n.nspname = %(schema_name)s
        	AND cl.relname = %(table_name)s
        	AND f.attnum > 0;
    """
    _QUERY_SCHEMA_CHANGES_DROP_COLUMN = """
        ALTER TABLE "{schema_name}"."{table_name}"
        DROP COLUMN IF EXISTS "{column_name}" CASCADE;
    """
    _QUERY_SCHEMA_CHANGES_ADD_COLUMN = """
        ALTER TABLE "{schema_name}"."{table_name}"
        ADD COLUMN "{column_name}" {column_type};
    """
    _QUERY_TABLE = 'SELECT * FROM "{schema_name}"."{table_name}"'

    _COPY_TABLE = """
        -- Drop a previous version of the table if it exists
        DROP TABLE IF EXISTS "{new_schema}"."{new_table}";
        CREATE TABLE "{new_schema}"."{new_table}"
            (LIKE "{old_schema}"."{old_table}"
            INCLUDING CONSTRAINTS INCLUDING INDEXES INCLUDING DEFAULTS);
        INSERT INTO "{new_schema}"."{new_table}"
            SELECT * FROM "{old_schema}"."{old_table}";
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(EC.DWH_ENGINE_POSTGRES, *args, **kwargs)

    def commit(self):
        self.dwh_hook.commit()

    def rollback(self):
        self.dwh_hook.commit()

    def close(self):
        self.dwh_hook.close()

    def _create_or_update_table(
        self,
        data,
        table_name,
        schema_name,
        schema_suffix,
        columns_definition,
        columns_partial_query,
        update_on_columns,
        drop_and_replace,
        upload_chunking=100000,
        pk_columns=[],
    ):
        self.log.info("Preparing DWH Tables...")
        schema_name += schema_suffix
        if drop_and_replace or (
            not self.test_if_table_exists(
                table_name=table_name,
                schema_name=schema_name,
            )
        ):
            self.dwh_hook.execute(
                sql="""
                    DROP TABLE IF EXISTS "{schema_name}"."{table_name}" CASCADE;
                    CREATE TABLE "{schema_name}"."{table_name}" ({columns});
                """.format(
                    schema_name=schema_name,
                    table_name=table_name,
                    columns=columns_partial_query,
                ),
                commit=False,
            )
            if pk_columns:
                self.dwh_hook.execute(
                    sql="""
                        ALTER TABLE ONLY "{schema_name}"."{table_name}"
                        ADD PRIMARY KEY ("{columns}");
                    """.format(
                        schema_name=schema_name,
                        table_name=table_name,
                        columns='","'.join(pk_columns),
                    )
                )

        if not drop_and_replace and update_on_columns:
            # make sure there is a unique constraint for update_on_columns
            self.dwh_hook.execute(
                sql="""
                    ALTER TABLE "{schema_name}"."{table_name}"
                    DROP CONSTRAINT IF EXISTS "{constraint}";
                    ALTER TABLE "{schema_name}"."{table_name}"
                    ADD CONSTRAINT "{constraint}" UNIQUE ("{columns}");
                """.format(
                    schema_name=schema_name,
                    table_name=table_name,
                    constraint="ufu_{0}_{1}".format(schema_name, table_name),
                    columns='", "'.join(update_on_columns),
                ),
                commit=False,
            )

        set_columns = []
        for column in columns_definition.keys():
            if not (column in update_on_columns):
                set_columns += [column]

        cols_list = list(columns_definition.keys())
        sql = (
            """
            INSERT INTO "{schema_name}"."{table_name}"
            ("{column_names}") VALUES {placeholder}
            ON CONFLICT {do_on_conflict};
        """.format(
                **{
                    "schema_name": schema_name,
                    "table_name": table_name,
                    "placeholder": "{placeholder}",
                    "column_names": '", "'.join(cols_list),
                    "do_on_conflict": "DO NOTHING"
                    if drop_and_replace or not update_on_columns
                    else """
                ("{update_on_columns}") DO UPDATE SET\n\t{sets}
            """.format(
                        update_on_columns='", "'.join(update_on_columns),
                        sets="\n\t,".join(
                            [
                                '"{column}" = EXCLUDED."{column}"'.format(column=column)
                                for column in set_columns
                            ]
                        ),
                    ),
                }
            )
            .replace("%", "%%")
            .format(
                **{
                    "placeholder": "%s",
                }
            )
        )
        self.log.info("Now Uploading! Using SQL:\n\n{0}".format(sql))
        # escape crappy column names by using aliases in the psycopg2 template
        cols_map = {cols_list[i]: "col_" + str(i) for i in range(len(cols_list))}
        template = "(%(" + ")s, %(".join([val for key, val in cols_map.items()]) + ")s)"
        cur = self.dwh_hook.cursor
        while data:
            upload_data = [
                {cols_map[key]: val for key, val in row.items()}
                for row in data[:upload_chunking]
            ]
            del data[:upload_chunking]  # Free up memory ASAP
            execute_values(
                cur=cur,
                sql=sql,
                argslist=upload_data,
                template=template,
            )
        self.log.info("Upload done.")

        self.dwh_hook.execute(
            sql='ANALYZE "{0}"."{1}";'.format(schema_name, table_name),
            commit=False,
        )

    def test_if_table_exists(self, table_name, schema_name):
        return bool(
            self.dwh_hook.execute_and_return_result(
                sql="SELECT to_regclass(%(name)s);",
                params={"name": '"{0}"."{1}"'.format(schema_name, table_name)},
            )[0][0]
        )
