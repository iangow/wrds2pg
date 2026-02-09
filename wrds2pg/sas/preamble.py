from __future__ import annotations

SAS_STDOUT_PREAMBLE = """\
ods listing;
ods html close;
ods pdf close;
ods results off;

options nodate nonumber nocenter;
"""

def with_stdout_preamble(sas_code: str) -> str:
    return SAS_STDOUT_PREAMBLE + "\n" + sas_code
