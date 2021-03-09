import subprocess
import typing


def run_process(args: typing.List[str], env=None):
    run = subprocess.run(
        args,
        stderr=subprocess.PIPE,
        stdout=subprocess.PIPE,
        env=env,
    )
    if run.returncode:
        raise Exception(
            f"Exited with code {run.returncode}:\n{run.stderr.decode('utf8')}"
        )
    return run.stdout
