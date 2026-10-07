# Administration tools

These source-only utilities are separate from the installed Arcus client package.
Use Python 3.10 to 3.12 because `arcus_util.py` imports `telnetlib`,
which was removed in Python 3.13.
Install their dependencies in a dedicated environment:

```sh
python3.10 -m venv .venv-admin
. .venv-admin/bin/activate
python -m pip install kazoo scramp paramiko
python tools/arcus_cmd.py --help
python tools/arcus_zk_cmd.py --help
```

Run scripts by their path so Python can find the neighboring utility modules.
`arcus_cmd.py` and `arcus_zk_cmd.py` provide command-line entry points;
`arcus_util.py` and `zk_util.py` contain their support code. `zk_sync.py` is the
legacy synchronization script. Inspect command options and use an isolated
service before running administrative mutations.
