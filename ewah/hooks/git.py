from ewah.hooks.base import EWAHBaseHook
from ewah.utils.run_commands import run_cmd
from airflow.utils.file import TemporaryDirectory
from tempfile import NamedTemporaryFile
from typing import Optional, Dict, Any

import os
import re


class EWAHRemoteGitHook(EWAHBaseHook):

    _ATTR_RELABEL = {
        "git_link": "host",
        "branch": "schema",
    }

    conn_name_attr = "ewah_git_conn_id"
    default_conn_name = "ewah_git_default"
    conn_type = "ewah_git"
    hook_name = "EWAH Git Remote Connection"

    @staticmethod
    def get_connection_form_widgets() -> Dict[str, Any]:
        """Returns connection widgets to add to connection form"""
        from wtforms import StringField
        from ewah.utils.widgets import EWAHTextAreaWidget

        return {
            "extra__ewah_git__private_key": StringField(
                "Private SSH Key (RSA Format)",
                widget=EWAHTextAreaWidget(rows=12),
            ),
        }

    @staticmethod
    def get_ui_field_behaviour() -> Dict:
        return {
            "hidden_fields": ["extra", "port", "login"],
            "relabeling": {
                "host": "Git SSH Link",
                "schema": "Branch (optional)",
                "password": "SSH Key Password (optional)",
            },
        }

    def clone_repo(self, clone_to: str, env: Optional[Dict[str, str]] = None) -> None:
        """Clone the repository into a specific location.

        :param clone_to: Directory to clone to.
        :param env: Dictionary of environment variables to use. Defaults to
            os.environ.copy(). Requires a $HOME environment variable to work with SSH.
        """

        env = env or os.environ.copy()

        cmd = []
        cmd.append("eval `ssh-agent`")
        with TemporaryDirectory(prefix="__ewah_git_") as tmp_dir:
            # temporarily save SSH key, if applicable, in this folder
            with NamedTemporaryFile(dir=tmp_dir) as ssh_key_file:
                if self.conn.private_key:
                    ssh_key_filepath = os.path.abspath(ssh_key_file.name)
                    ssh_key_file.write(self.conn.private_key.encode())
                    ssh_key_file.seek(0)
                    if self.conn.password:
                        passfile_path = os.path.abspath(
                            tmp_dir + os.path.sep + "ssh_key_pw"
                        )
                        with open(passfile_path, "w+") as passfile:
                            passfile.write(
                                '#!/bin/bash\necho "{0}"'.format(self.conn.password)
                            )
                            passfile.seek(0)
                        cmd.append("chmod 777 {0}".format(passfile_path))
                        cmd.append(
                            'DISPLAY=":0.0" SSH_ASKPASS="{0}" ssh-add {1}'.format(
                                passfile_path, ssh_key_filepath
                            )
                        )
                    else:
                        cmd.append("ssh-add {0}".format(ssh_key_filepath))

                # Add actual clone commands
                git_link = self.conn.git_link
                cmd.append("mkdir -p $HOME/.ssh")

                # Must add the host to the known hosts or it will ask for confirmation
                ssh_domain = re.search("@(.*):", git_link).group(1)
                cmd.append(
                    "ssh-keyscan -H {0} >> $HOME/.ssh/known_hosts".format(ssh_domain)
                )
                if self.conn.branch:
                    cmd.append(
                        "git clone -b {0} {1} {2}".format(
                            self.conn.branch, git_link, clone_to
                        )
                    )
                else:
                    cmd.append("git clone {0} {1}".format(git_link, clone_to))

                # Execute commands!
                assert run_cmd(cmd, env, self.log.info) == 0
