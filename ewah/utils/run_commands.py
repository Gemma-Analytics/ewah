import subprocess

from typing import Optional, Callable, Dict, Union, List


def run_cmd(
    cmd: Union[str, List[str]], env: Dict[str, str], logger: Optional[Callable] = None
) -> int:
    """Run a bash command and return the returncode.

    :param cmd: Command as string or list of strings.
    :param env: Environment variables to use.
    :param logger: Optional callable, stdout will be sent there.
    :return: Returncode.
    """
    if isinstance(cmd, list):
        for command in cmd:
            # Avoid users supplying invalid commands via injection
            assert (
                not "&" in command
            ), "Ampersand is not allowed in individual commands!"
        cmd = " && ".join(cmd)
    else:
        assert isinstance(cmd, str)
        assert (
            not "&" in cmd
        ), "Ampersand is not allowed if supplying command as string!"
    no_func = lambda x: None
    logger = logger or no_func
    logger("Running command:\n\n\t{0}\n\n".format(cmd))
    with subprocess.Popen(
        cmd,
        executable="/bin/bash",
        shell=True,  # required when parsing string as command
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        env=env,
    ) as p:
        # write output to log without delay
        for stdout_line in p.stdout:
            logger(stdout_line.decode().strip())
    return p.returncode
