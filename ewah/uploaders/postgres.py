from ewah.uploaders.base import EWAHBaseUploader
from ewah.hooks.postgres import EWAHPostgresHook
from ewah.constants import EWAHConstants as EC
from ewah.utils.airflow_utils import PGO

from psycopg2.extras import execute_values
from copy import deepcopy

import hashlib
import re


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
        CREATE TABLE "{new_schema}"."{new_table}" AS (
            SELECT * FROM "{old_schema}"."{old_table}"
        );
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(EC.DWH_ENGINE_POSTGRES, *args, **kwargs)

    @classmethod
    def get_schema_tasks(
        cls,
        dag,
        dwh_engine,
        dwh_conn_id,
        target_schema_name,
        target_schema_suffix="_next",
        target_database_name=None,
        read_right_users=None,  # Only for PostgreSQL
        **additional_task_args,
    ):
        sql_kickoff = """
            DROP SCHEMA IF EXISTS "{schema_name}{schema_suffix}" CASCADE;
            CREATE SCHEMA "{schema_name}{schema_suffix}";
        """.format(
            schema_name=target_schema_name,
            schema_suffix=target_schema_suffix,
        )
        sql_final = """
            DROP SCHEMA IF EXISTS "{schema_name}" CASCADE;
            ALTER SCHEMA "{schema_name}{schema_suffix}"
                RENAME TO "{schema_name}";
        """.format(
            schema_name=target_schema_name,
            schema_suffix=target_schema_suffix,
        )

        # Don't fail final task just because a user or role that should
        # be granted read rights does not exist!
        grant_rights_sql = """
            DO $$
            BEGIN
              GRANT USAGE ON SCHEMA "{target_schema_name}" TO {user};
              GRANT SELECT ON ALL TABLES
                IN SCHEMA "{target_schema_name}" TO {user};
              EXCEPTION WHEN OTHERS THEN -- catches any error
                RAISE NOTICE 'not granting rights - user does not exist!';
            END
            $$;
        """
        if read_right_users:
            if not isinstance(read_right_users, list):
                raise Exception("Arg read_right_users must be of type List!")
            for user in read_right_users:
                if re.search(r"\s", user) or (";" in user):
                    _msg = "No whitespace or semicolons allowed in usernames!"
                    raise ValueError(_msg)
                sql_final += grant_rights_sql.format(
                    target_schema_name=target_schema_name,
                    user=user,
                )

        task_1_args = deepcopy(additional_task_args)
        task_2_args = deepcopy(additional_task_args)
        task_1_args.update(
            {
                "sql": sql_kickoff,
                "task_id": "kickoff",
                "dag": dag,
                "postgres_conn_id": dwh_conn_id,
            }
        )
        task_2_args.update(
            {
                "sql": sql_final,
                "task_id": "final",
                "dag": dag,
                "postgres_conn_id": dwh_conn_id,
            }
        )
        return (PGO(**task_1_args), PGO(**task_2_args))

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
        load_strategy,
        upload_call_count,
        primary_key=None,
    ):
        self.log.info("Preparing DWH Tables...")
        schema_name += schema_suffix
        if (upload_call_count == 1 and load_strategy == EC.LS_INSERT_REPLACE) or (
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
                    columns=",\n\t".join(
                        [
                            '"{0}"\t{1}'.format(col, self._get_column_type(defi))
                            for col, defi in columns_definition.items()
                        ]
                    ),
                ),
                commit=False,
            )
            if primary_key:
                self.dwh_hook.execute(
                    sql="""
                        ALTER TABLE ONLY "{schema_name}"."{table_name}"
                        ADD PRIMARY KEY ("{columns}");
                    """.format(
                        schema_name=schema_name,
                        table_name=table_name,
                        columns='","'.join(primary_key),
                    )
                )

        if primary_key:
            # make sure there is a unique constraint for primary_key
            self.dwh_hook.execute(
                sql="""
                    ALTER TABLE "{schema_name}"."{table_name}"
                    DROP CONSTRAINT IF EXISTS "{constraint}";
                    ALTER TABLE "{schema_name}"."{table_name}"
                    ADD CONSTRAINT "{constraint}" UNIQUE ("{columns}");
                """.format(
                    schema_name=schema_name,
                    table_name=table_name,
                    # max length for constraint is 63 chars - make sure it's unique!
                    constraint="__ewah_{0}".format(
                        hashlib.blake2b(
                            "ufu_{0}_{1}".format(schema_name, table_name).encode(),
                            digest_size=28,
                        ).hexdigest()
                    ),
                    columns='", "'.join(primary_key),
                ),
                commit=False,
            )

        set_columns = []
        for column in columns_definition.keys():
            if not (column in (primary_key or [])):
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
                    if not primary_key
                    else """
                ("{primary_key}") DO UPDATE SET\n\t{sets}
            """.format(
                        primary_key='", "'.join(primary_key),
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
        upload_data = [
            {cols_map[key]: row.get(key) for key in cols_list} for row in data
        ]
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

    def get_max_value_of_column(self, column_name):
        return self.dwh_hook.execute_and_return_result(
            sql='SELECT MAX("{0}") FROM {1}'.format(
                column_name,
                f'"{self.schema_name}{self.schema_suffix}"."{self.table_name}"',
            ),
            return_dict=False,
        )[0][0]
