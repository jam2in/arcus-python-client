"""Library diagnostics must respect application logging and avoid eager output."""

import io
import logging
import os
from pathlib import Path
import subprocess
import sys
import warnings

import pytest

from arcus import arcuslog, enable_log


@pytest.fixture
def arcus_logger():
    logger = logging.getLogger("arcus")
    previous = logger.level, logger.disabled, logger.propagate, logger.handlers[:]
    logger.setLevel(logging.NOTSET)
    logger.disabled = False
    logger.propagate = True
    try:
        yield logger
    finally:
        level, disabled, propagate, handlers = previous
        logger.setLevel(level)
        logger.disabled = disabled
        logger.propagate = propagate
        logger.handlers[:] = handlers


def test_import_and_compatibility_helper_do_not_configure_root_or_write_output():
    # A fresh interpreter detects import-time configuration even when pytest has
    # installed its own root handlers in the parent process.
    script = """
import logging
import warnings
root = logging.getLogger()
root.setLevel(logging.ERROR)
assert root.handlers == []
import arcus
logger = logging.getLogger('arcus')
assert logger.level == logging.NOTSET
assert logger.propagate
assert len(logger.handlers) == 1
assert isinstance(logger.handlers[0], logging.NullHandler)
with warnings.catch_warnings():
    warnings.simplefilter('ignore', DeprecationWarning)
    arcus.enable_log()
arcus.arcuslog(None, 'silent without application handlers')
assert root.level == logging.ERROR
assert root.handlers == []
"""
    # Reuse the import path of the package under test, including wheel tests
    # running outside the checkout, rather than forcing a source-tree import.
    import arcus

    environment = dict(os.environ)
    environment["PYTHONPATH"] = str(Path(arcus.__file__).parent.parent)
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        env=environment,
        check=True,
    )
    assert result.stdout == ""
    assert result.stderr == ""


def test_application_logger_configuration_enables_debug_records(
    arcus_logger, caplog, capsys
):
    caller = object()
    with caplog.at_level(logging.DEBUG, logger="arcus"):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            arcuslog(caller, "sent: ", b"payload")
    assert not caught
    record = caplog.records[-1]
    assert record.name == "arcus"
    assert record.levelno == logging.DEBUG
    assert record.getMessage() == f"[object({id(caller):#x})] 'sent: 'b'payload'"
    assert (
        record.funcName == "test_application_logger_configuration_enables_debug_records"
    )
    assert capsys.readouterr().out == ""


def test_enable_log_only_changes_arcus_level_and_reports_deprecation(
    arcus_logger, caplog
):
    with caplog.at_level(logging.DEBUG):
        root = logging.getLogger()
        root_state = root.level, root.handlers[:]
        handlers = arcus_logger.handlers[:]
        with pytest.warns(DeprecationWarning, match="logging.getLogger"):
            enable_log()
        arcuslog(None, "enabled")
        with pytest.warns(DeprecationWarning, match="logging.getLogger"):
            enable_log(False)
        arcuslog(None, "disabled")
        assert arcus_logger.level == logging.WARNING
        assert arcus_logger.handlers == handlers
        assert (root.level, root.handlers) == root_state
    assert [record.getMessage() for record in caplog.records] == ["'enabled'"]
    assert "logging.getLogger('arcus')" in enable_log.__deprecated__


class Unformattable:
    def __repr__(self):
        raise AssertionError("disabled logging must not format diagnostic values")


def test_disabled_debug_does_not_format_diagnostic_values(arcus_logger):
    arcus_logger.setLevel(logging.WARNING)
    arcuslog(None, Unformattable())


def test_filtered_handler_does_not_format_debug_values(arcus_logger):
    output = io.StringIO()
    handler = logging.StreamHandler(output)
    handler.setLevel(logging.WARNING)
    arcus_logger.addHandler(handler)
    arcus_logger.setLevel(logging.DEBUG)
    arcus_logger.propagate = False
    arcuslog(None, Unformattable())
    assert output.getvalue() == ""
