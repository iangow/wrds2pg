from __future__ import annotations

import io
import subprocess
from contextlib import contextmanager
from typing import Iterator, TextIO

import paramiko

from .codegen import get_wrds_sas
from .preamble import with_stdout_preamble

@contextmanager
def get_process_stream(
    sas_code: str,
    wrds_id: str | None = None,
    fpath: str | None = None,
    encoding: str = "utf-8",
) -> Iterator[TextIO]:
    """
    Yield a text stream containing SAS stdout (typically CSV / listing).

    Intended usage:
        with get_process_stream(sas_code, wrds_id=..., fpath=...) as stream:
            ...
    """
    sas_code = with_stdout_preamble(sas_code)

    if fpath is not None:
        proc = subprocess.Popen(
            ["sas", "-stdio", "-noterminal"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding=encoding,
        )
        try:
            assert proc.stdin is not None
            proc.stdin.write(sas_code)
            proc.stdin.close()

            assert proc.stdout is not None
            yield proc.stdout

            rc = proc.wait()
            if rc != 0:
                err = proc.stderr.read() if proc.stderr else ""
                raise RuntimeError(f"SAS exited with code {rc}.\n{err}")
        finally:
            if proc.stdout:
                proc.stdout.close()
            if proc.stderr:
                proc.stderr.close()
            if proc.poll() is None:
                proc.terminate()

    elif wrds_id is not None:
        client = paramiko.SSHClient()
        client.load_system_host_keys()
        client.set_missing_host_key_policy(paramiko.WarningPolicy())
        client.connect(
            "wrds-cloud-sshkey.wharton.upenn.edu",
            username=wrds_id,
            compress=False,
        )
        try:
            stdin, stdout, stderr = client.exec_command("qsas -stdio -noterminal")

            stdin.write(sas_code)
            stdin.close()

            text_stdout = io.TextIOWrapper(stdout, encoding=encoding)
            text_stderr = io.TextIOWrapper(stderr, encoding=encoding)
            try:
                yield text_stdout
            finally:
                # make sure the remote command finished and surface errors if any
                exit_status = stdout.channel.recv_exit_status()
                if exit_status > 4:
                    text_stdout.close()
                    text_stderr.close()
                    raise RuntimeError(f"Remote SAS exited with code {exit_status}.\n{err}")

                text_stdout.close()
                text_stderr.close()
        finally:
            client.close()

    else:
        raise ValueError("Either `wrds_id` or `fpath` must be provided.")


def get_wrds_process_stream(
    table_name,
    schema,
    wrds_id=None,
    fpath=None,
    drop=None,
    keep=None,
    fix_cr=False,
    fix_missing=False,
    obs=None,
    rename=None,
    where=None,
    encoding=None,
    sas_encoding=None,
    stream_encoding="utf-8",
):
    sas_code = get_wrds_sas(
        table_name=table_name,
        schema=schema,
        wrds_id=wrds_id,
        fpath=fpath,
        drop=drop,
        keep=keep,
        fix_cr=fix_cr,
        fix_missing=fix_missing,
        obs=obs,
        rename=rename,
        where=where,
        encoding=encoding,
        sas_encoding=sas_encoding,
    )

    return get_process_stream(
        sas_code=sas_code,
        wrds_id=wrds_id,
        fpath=fpath,
        encoding=stream_encoding,
    )
