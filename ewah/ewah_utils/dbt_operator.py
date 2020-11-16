from airflow.models import BaseOperator
from airflow.hooks.base_hook import BaseHook
from airflow.utils.file import TemporaryDirectory

from ewah.constants import EWAHConstants as EC
from ewah.ewah_utils.yml_loader import Loader, Dumper
from ewah.ewah_utils.ssh_tunnel import start_ssh_tunnel

import re
import os
import venv
import yaml
import subprocess
from tempfile import NamedTemporaryFile

class EWAHdbtOperator(BaseOperator):
    """
        Runs dbt tasks, given a git repository and a target data warehouse.
    """

    # allowed commands (sans flags)
    _commands = [
        'run',
        'test',
        'snapshot',
        'seed',
        'docs generate',
    ]
    # note: dbt deps is always executed first!

    def __init__(self,
        repo_type,
        dwh_engine,
        dwh_conn_id,
        git_conn_id=None,
        dbt_commands=['run'], # string or list of strings - dbt commands
        dbt_version='0.18.1',
        git_link=None, # SSH link to a remote git repository, if using git repo
        git_branch=None, # optional: branch to check out
        subfolder=None, # optional: supply if dbt project is in a subfolder
        threads=4, # see https://docs.getdbt.com/dbt-cli/configure-your-profile/#understanding-threads
        schema_name='analytics', # see https://docs.getdbt.com/dbt-cli/configure-your-profile/#understanding-target-schemas
        database_name=None, # Snowflake only - name of the database
        keepalives_idle=0, # see https://docs.getdbt.com/reference/warehouse-profiles/postgres-profile/
        ssh_tunnel_id=None,
    *args, **kwargs):

        assert repo_type in ('git')
        assert dbt_commands
        assert dbt_version
        assert threads
        assert schema_name

        assert dwh_engine in (EC.DWH_ENGINE_POSTGRES, EC.DWH_ENGINE_SNOWFLAKE)

        # only Snowflake is allowed to have the database_name kwarg
        assert (dwh_engine == EC.DWH_ENGINE_SNOWFLAKE) or (not database_name)


        if isinstance(dbt_commands, str):
            dbt_commands = [dbt_commands]

        # check if all commands are allowed -- note that flags are allowed!
        if min([
                max([1 if cmd.startswith(_c) else 0 for _c in self._commands])
                for cmd in dbt_commands
            ]) == 0:
            raise Exception('dbt commands must be one of:\n\n\t{0}{1}'.format(
                '\n\t'.join(self._commands),
                '\n\ncurrently: {0}'.format(dbt_commands),
            ))

        if repo_type == 'git':
            assert git_link
            assert re.search('@(.*):', git_link) # only SSH links currently work

        super().__init__(*args, **kwargs)

        self.repo_type = repo_type
        self.git_conn_id = git_conn_id
        self.dwh_engine = dwh_engine
        self.dwh_conn_id = dwh_conn_id
        self.dbt_commands = dbt_commands
        self.dbt_version = dbt_version
        self.git_link = git_link
        self.git_branch = git_branch
        self.subfolder = subfolder
        self.threads = threads
        self.schema_name = schema_name
        self.keepalives_idle = keepalives_idle
        self.ssh_tunnel_id = ssh_tunnel_id
        self.database_name = database_name

    def execute(self, context):
        def run_cmd(cmd, env):
            # run a bash command (cmd) with the environment variables env
            self.log.info('running:\n\n\t{0}\n\n'.format(cmd))
            with subprocess.Popen(
                cmd,
                executable='/bin/bash',
                shell=True, # required when parsing string as command
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                env=env,
            ) as p:
                # write output to log without delay
                for stdout_line in p.stdout:
                    self.log.info(stdout_line.decode().strip())
            assert p.returncode == 0 # fail task if process failed

        # env to be used in processes later
        env = os.environ.copy()

        # create a new temp folder, all action happens in here
        with TemporaryDirectory(prefix='__ewah_dbt_operator_') as tmp_dir:
            # clone repo into temp directory
            repo_dir = tmp_dir + os.path.sep + 'repo'
            if self.repo_type == 'git':
                git_repo_link = ''
                if self.git_branch:
                    git_repo_link = '-b {0} '.format(self.git_branch)
                git_repo_link += self.git_link
                with NamedTemporaryFile(dir=tmp_dir) as f:
                    # temporarily have a file with the SSH key when cloning
                    cmd = 'eval `ssh-agent`'
                    if self.git_conn_id:
                        git_conn = BaseHook.get_connection(self.git_conn_id)
                        ssh_key_text = git_conn.extra
                        file_name = os.path.abspath(f.name)
                        f.write(ssh_key_text.encode())
                        f.seek(0)
                        # if there is a password set in the git conn, it's
                        # the password for the SSH key! supply it!
                        # use SSH_ASKPASS env var & a temp file to do so
                        ssh_pw = ''
                        if git_conn.password:
                            f_pw_path = tmp_dir + os.path.sep + 'ssh_key_pw'
                            with open(f_pw_path, 'w+') as f_pw:
                                # create file in temp folder -> ssh-add cannot
                                # open a NamedTemporaryFile in context manager
                                f_pw.write('#!/bin/bash\necho "{0}"'
                                        .format(git_conn.password)
                                )
                                f_pw.seek(0)
                            ssh_pw = 'DISPLAY=":0.0" SSH_ASKPASS="{0}" '
                            ssh_pw = ssh_pw.format(f_pw_path)
                            cmd += ' && chmod 777 {0}'.format(f_pw_path)
                        cmd += ' && {1}ssh-add {0}'.format(file_name, ssh_pw)
                    else:
                        cmd += 'echo "cloning without credentials!"'
                    ssh_domain = re.search('@(.*):', self.git_link).group(1)
                    cmd += ' && mkdir -p $HOME/.ssh'
                    cmd += (' && ssh-keyscan -H {0} >> $HOME/.ssh/known_hosts'
                    .format(ssh_domain))
                    cmd += ' && git clone {0} {1}'.format(
                        git_repo_link,
                        repo_dir,
                    )
                    run_cmd(cmd, env) # cloning repo
            else:
                raise Exception('Not Implemented!')

            # create a virual environment in temp folder
            venv_folder = tmp_dir + os.path.sep + 'venv'
            self.log.info('creating a new virtual environment in {0}...'.format(
                venv_folder,
            ))
            venv.create(venv_folder, with_pip=True)

            # install dbt into created venv
            self.log.info('installing dbt=={0}'.format(self.dbt_version))
            cmd = 'source {0}/bin/activate'.format(venv_folder)
            cmd += (' && pip install --quiet --upgrade dbt=={0}'
                    .format(self.dbt_version))
            cmd += ' && dbt --version'
            cmd += ' && deactivate'
            run_cmd(cmd, env)

            dbt_dir = repo_dir
            if self.subfolder:
                if not self.subfolder[:1] == os.path.sep:
                    self.subfolder = os.path.sep + self.subfolder
                dbt_dir += self.subfolder

            # if applicable: open SSH tunnel
            if self.ssh_tunnel_id:
                self.ssh_tunnel_forwarder, dwh_conn = start_ssh_tunnel(
                    ssh_conn_id=self.ssh_tunnel_id,
                    remote_conn_id=self.dwh_conn_id,
                )
            else:
                dwh_conn = BaseHook.get_connection(self.dwh_conn_id)


            # read profile name & create temporary profiles.yml
            project_yml_file = dbt_dir
            if not project_yml_file[-1:] == os.path.sep:
                project_yml_file += os.path.sep
            project_yml_file += 'dbt_project.yml'
            project_yml = yaml.load(open(project_yml_file, 'r'), Loader=Loader)
            profile_name = project_yml['profile']
            self.log.info('Creating temp profile "{0}"'.format(profile_name))
            profiles_yml = {
                'config': {
                    'send_anonymous_usage_stats': False,
                    'use_colors': False, # colors won't be useful in logs
                },
            }
            if self.dwh_engine == EC.DWH_ENGINE_POSTGRES:
                profiles_yml[profile_name] = {
                    'target': 'prod', # same as the output defined below
                    'outputs': {
                        'prod': { # for postgres
                            'type': 'postgres',
                            'host': dwh_conn.host,
                            'port': dwh_conn.port or '5432',
                            'user': dwh_conn.login,
                            'pass': dwh_conn.password,
                            'dbname': dwh_conn.schema,
                            'schema': self.schema_name,
                            'threads': self.threads,
                            'keepalives_idle': self.keepalives_idle,
                        },
                    },
                }
            elif self.dwh_engine == EC.DWH_ENGINE_SNOWFLAKE:
                extra = dwh_conn.extra_dejson
                _db = extra.get('database', self.database_name)
                _db = _db or dwh_conn.schema
                profiles_yml[profile_name] = {
                    'target': 'prod', # same as the output defined below
                    'outputs': {
                        'prod': { # for postgres
                            'type': 'snowflake',
                            'account': extra.get('account', dwh_conn.host),
                            'user': extra.get('user', dwh_conn.login),
                            'password': extra.get('password', dwh_conn.password),
                            'role': extra.get('role'),
                            'database': _db,
                            'warehouse': extra.get('warehouse'),
                            'schema': self.schema_name,
                            'threads': self.threads,
                            'keepalives_idle': self.keepalives_idle,
                        },
                    },
                }
            else:
                raise Exception('DWH Engine not implemented!')

            # run commands with correct profile in the venv in the temp folder
            profiles_yml_name = tmp_dir + os.path.sep + 'profiles.yml'
            env['DBT_PROFILES_DIR'] = os.path.abspath(tmp_dir)
            with open(profiles_yml_name, 'w') as profiles_file:
                # write profile into profiles.yml file
                yaml.dump(profiles_yml, profiles_file, default_flow_style=False)

                # run dbt commands
                self.log.info('Now running commands dbt!')
                cmd = 'cd {0}'.format(dbt_dir)
                cmd += ' && source {0}/bin/activate'.format(venv_folder)
                cmd += ' && dbt '.join(['', 'deps'] + self.dbt_commands)
                cmd += ' && deactivate'
                run_cmd(cmd, env)

            # if applicable: close SSH tunnel
            if hasattr(self, 'ssh_tunnel_forwarder'):
                self.log.info('Stopping!')
                self.ssh_tunnel_forwarder.stop()
                del self.ssh_tunnel_forwarder
