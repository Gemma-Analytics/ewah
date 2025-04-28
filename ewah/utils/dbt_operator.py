from airflow.models import BaseOperator
from airflow.utils.file import TemporaryDirectory

from ewah.hooks.base import EWAHBaseHook
from ewah.constants import EWAHConstants as EC
from ewah.utils.run_commands import run_cmd

try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader as Loader

import re
import os
import venv
import yaml
import json
from tempfile import NamedTemporaryFile
from distutils.dir_util import copy_tree


class EWAHdbtOperator(BaseOperator):
    """
    Runs dbt tasks, given a git repository and a target data warehouse.
    """

    # allowed commands (sans flags)
    _commands = [
        "run",
        "test",
        "snapshot",
        "seed",
        "docs generate",
        "docs serve",
        "source freshness",
    ]
    # note: dbt deps is always executed first!

    def __init__(
        self,
        repo_type,
        dwh_engine,
        dwh_conn_id,
        git_conn_id=None,
        local_path=None,
        dbt_commands=["run"],  # string or list of strings - dbt commands
        dbt_version=None,
        subfolder=None,  # optional: supply if dbt project is in a subfolder
        threads=4,  # see https://docs.getdbt.com/dbt-cli/configure-your-profile/#understanding-threads
        schema_name="analytics",  # see https://docs.getdbt.com/dbt-cli/configure-your-profile/#understanding-target-schemas
        database_name=None,  # Snowflake & BigQuery only - name of the database / project
        keepalives_idle=0,  # see https://docs.getdbt.com/reference/warehouse-profiles/postgres-profile/
        dataset=None,  # BigQuery alias for schema_name
        project=None,  # BigQuery alias for database_name
        metabase_conn_id=None,  # Push docs to Metabase if exists
        env_var_conn_ids=None,  # Name of list of names of connections to use as env vars
        *args,
        **kwargs
    ):
        schema_name = dataset or schema_name
        database_name = project or database_name

        assert repo_type in ("git", "local")
        assert dbt_commands
        assert threads
        assert schema_name

        if repo_type == "local":
            assert git_conn_id is None
            assert local_path
        else:
            assert local_path is None

        assert dwh_engine in (
            EC.DWH_ENGINE_POSTGRES,
            EC.DWH_ENGINE_SNOWFLAKE,
            EC.DWH_ENGINE_BIGQUERY,
        )

        # only Snowflake is allowed to have the database_name kwarg
        assert (dwh_engine in (EC.DWH_ENGINE_SNOWFLAKE, EC.DWH_ENGINE_BIGQUERY)) or (
            not database_name
        )

        if dwh_engine == EC.DWH_ENGINE_BIGQUERY:
            assert schema_name and database_name

        if isinstance(dbt_commands, str):
            dbt_commands = [dbt_commands]

        # check if all commands are allowed -- note that flags are allowed!
        if (
            min(
                [
                    max([1 if cmd.startswith(_c) else 0 for _c in self._commands])
                    for cmd in dbt_commands
                ]
            )
            == 0
        ):
            raise Exception(
                "dbt commands must be one of:\n\n\t{0}{1}".format(
                    "\n\t".join(self._commands),
                    "\n\ncurrently: {0}".format(dbt_commands),
                )
            )

        # Make sure no process command tries to sneak in extra commands
        if not max([1 if "&" in cmd else 0 for cmd in dbt_commands]) == 0:
            raise Exception("Ampersand (&) is an invalid character in dbt_commands!")

        assert isinstance(
            env_var_conn_ids, (type(None), str, list)
        ), "env_var_conn_ids must be a string or list of strings!"
        if env_var_conn_ids and isinstance(env_var_conn_ids, str):
            env_var_conn_ids = [env_var_conn_ids]

        super().__init__(*args, **kwargs)

        self.repo_type = repo_type
        self.git_conn_id = git_conn_id
        self.local_path = local_path
        self.dwh_engine = dwh_engine
        self.dwh_conn_id = dwh_conn_id
        self.dbt_commands = dbt_commands
        self.dbt_version = dbt_version
        self.subfolder = subfolder
        self.threads = threads
        self.schema_name = schema_name
        self.keepalives_idle = keepalives_idle
        self.database_name = database_name
        self.metabase_conn_id = metabase_conn_id
        self.env_var_conn_ids = env_var_conn_ids

    def execute(self, context):
        # env to be used in processes later
        env = os.environ.copy()
        env["PIP_USER"] = "no"

        if self.env_var_conn_ids:
            for env_var_conn_id in self.env_var_conn_ids:
                # get name, secret via hook and add to environment
                # note that the type doesn't matter; just takes login and password
                hook = EWAHBaseHook.get_hook_from_conn_id(conn_id=env_var_conn_id)
                env[hook.conn.login] = hook.conn.password

        # create a new temp folder, all action happens in here
        with TemporaryDirectory(prefix="__ewah_dbt_operator_") as tmp_dir:
            # clone repo into temp directory
            repo_dir = tmp_dir + os.path.sep + "repo"
            if self.repo_type == "git":
                # Clone repo into temp folder
                git_hook = EWAHBaseHook.get_hook_from_conn_id(conn_id=self.git_conn_id)
                git_hook.clone_repo(repo_dir, env)
            elif self.repo_type == "local":
                # Copy local version of the repository into temp folder
                copy_tree(self.local_path, repo_dir)
            else:
                raise Exception("Not Implemented!")

            # create a virual environment in temp folder
            venv_folder = tmp_dir + os.path.sep + "venv"
            self.log.info(
                "creating a new virtual environment in {0}...".format(
                    venv_folder,
                )
            )
            venv.create(venv_folder, with_pip=True)
            dbt_dir = repo_dir
            if self.subfolder:
                if not self.subfolder[:1] == os.path.sep:
                    self.subfolder = os.path.sep + self.subfolder
                dbt_dir += self.subfolder

            dwh_hook = EWAHBaseHook.get_hook_from_conn_id(self.dwh_conn_id)
            # in case of SSH: execute a query to create the connection and tunnel
            dwh_hook.execute("SELECT 1 AS a -- Testing the connection")
            dwh_conn = dwh_hook.conn

            # read profile name and dbt version & create temporary profiles.yml
            project_yml_file = dbt_dir
            if not project_yml_file[-1:] == os.path.sep:
                project_yml_file += os.path.sep
            project_yml_file += "dbt_project.yml"
            project_yml = yaml.load(open(project_yml_file, "r"), Loader=Loader)
            profile_name = project_yml["profile"]
            dbt_version = self.dbt_version or project_yml.get("require-dbt-version")
            del self.dbt_version  # Make sure it can't accidentally be used below
            assert dbt_version, "Must supply dbt_version or set require-dbt-version!"
            if isinstance(dbt_version, str):
                if not dbt_version.startswith(("=", "<", ">")):
                    dbt_version = "==" + dbt_version
            elif isinstance(dbt_version, list):
                dbt_version = ",".join(dbt_version)
            else:
                raise Exception("dbt_version must be a string or a list of strings!")
            self.log.info('Creating temp profile "{0}"'.format(profile_name))
            profiles_yml = {
                "config": {
                    "send_anonymous_usage_stats": False,
                    "use_colors": False,  # colors won't be useful in logs
                },
            }
            if self.dwh_engine == EC.DWH_ENGINE_POSTGRES:
                mb_database = dwh_conn.schema
                profiles_yml[profile_name] = {
                    "target": "prod",  # same as the output defined below
                    "outputs": {
                        "prod": {  # for postgres
                            "type": "postgres",
                            "host": dwh_hook.local_bind_address[0],
                            "port": dwh_hook.local_bind_address[1],
                            "user": dwh_conn.login,
                            "pass": dwh_conn.password,
                            "dbname": dwh_conn.schema,
                            "schema": self.schema_name,
                            "threads": self.threads,
                            "keepalives_idle": self.keepalives_idle,
                        },
                    },
                }
            elif self.dwh_engine == EC.DWH_ENGINE_SNOWFLAKE:
                mb_database = self.database_name or dwh_conn.database
                profiles_yml[profile_name] = {
                    "target": "prod",  # same as the output defined below
                    "outputs": {
                        "prod": {  # for snowflake
                            "type": "snowflake",
                            "account": dwh_conn.account,
                            "user": dwh_conn.user,
                            "password": dwh_conn.password,
                            "role": dwh_conn.role,
                            "database": self.database_name or dwh_conn.database,
                            "warehouse": dwh_conn.warehouse,
                            "schema": self.schema_name or dwh_conn.schema,
                            "threads": self.threads,
                            "keepalives_idle": self.keepalives_idle,
                        },
                    },
                }
            elif self.dwh_engine == EC.DWH_ENGINE_BIGQUERY:
                mb_database = self.database_name
                profiles_yml[profile_name] = {
                    "target": "prod",  # same as the output defined below
                    "outputs": {
                        "prod": {
                            "type": "bigquery",
                            "method": "service-account-json",
                            "project": self.database_name,
                            "dataset": self.schema_name,
                            "threads": self.threads,
                            "timeout_seconds": self.keepalives_idle or 300,
                            "priority": "interactive",
                            "keyfile_json": json.loads(dwh_conn.service_account_json),
                        },
                    },
                }
                if dwh_conn.location:
                    profiles_yml[profile_name]["outputs"]["prod"][
                        "location"
                    ] = dwh_conn.location
            else:
                raise Exception("DWH Engine not implemented!")

            # install dbt into created venv
            cmd = []
            cmd.append("source {0}/bin/activate".format(venv_folder))
            cmd.append("pip install --quiet --upgrade pip setuptools")
            if re.search("[^0-9\.]0(\.[0-9]+)?(\.[0-9]+)?$", dbt_version):
                # regex checks whether the (last) version start with 0
                # if true, version <1.0.0 required
                cmd.append(
                    'pip install --quiet --upgrade "MarkupSafe<=2.0.1" "dbt{0}"'.format(
                        dbt_version
                    )
                )
            else:
                # Different pip behavior since dbt 1.0.0
                cmd.append(
                    "pip install --quiet --upgrade "
                    '"MarkupSafe<=2.0.1" '
                    '"certifi==2025.1.31" '  # Workaround for Snowflake certificate errors
                    '"dbt-core{1}" '
                    '"dbt-{0}{1}"'.format(
                        {
                            EC.DWH_ENGINE_POSTGRES: "postgres",
                            EC.DWH_ENGINE_SNOWFLAKE: "snowflake",
                            EC.DWH_ENGINE_BIGQUERY: "bigquery",
                        }[self.dwh_engine],
                        dbt_version,
                    )
                )

            cmd.append("dbt --version")
            cmd.append("deactivate")
            assert run_cmd(cmd, env, self.log.info) == 0

            # run commands with correct profile in the venv in the temp folder
            profiles_yml_name = tmp_dir + os.path.sep + "profiles.yml"
            env["DBT_PROFILES_DIR"] = os.path.abspath(tmp_dir)
            with open(profiles_yml_name, "w") as profiles_file:
                # write profile into profiles.yml file
                yaml.dump(profiles_yml, profiles_file, default_flow_style=False)

                # run dbt commands
                self.log.info("Now running commands dbt!")
                cmd = []
                cmd.append("cd {0}".format(dbt_dir))
                cmd.append("source {0}/bin/activate".format(venv_folder))
                if self.repo_type == "local":
                    cmd.append("dbt clean")
                cmd.append("dbt deps")
                [cmd.append("dbt {0}".format(dc)) for dc in self.dbt_commands]
                cmd.append("deactivate")
                assert run_cmd(cmd, env, self.log.info) == 0

                if self.metabase_conn_id:
                    # Push docs to Metabase at the end of the run!
                    metabase_hook = EWAHBaseHook.get_hook_from_conn_id(
                        conn_id=self.metabase_conn_id
                    )
                    metabase_hook.push_dbt_docs_to_metabase(
                        dbt_project_path=dbt_dir,
                        dbt_database_name=mb_database,
                    )
