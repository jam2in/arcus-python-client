# Administration tools

These source-only utilities are separate from the installed Arcus client package.
Use Python 3.11 or 3.12 because `arcus_util.py` imports `telnetlib`.
Install their dependencies in a dedicated environment:

```sh
python3.11 -m venv .venv-admin
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
