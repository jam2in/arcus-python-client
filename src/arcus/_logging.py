#
# arcus-python-client - Arcus python client drvier
# Copyright 2014 NAVER Corp.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


"""DEBUG diagnostics controlled by the application's ``arcus`` logger settings."""

import logging

from ._deprecation import deprecated


_logger = logging.getLogger("arcus")
_logger.addHandler(logging.NullHandler())


@deprecated(
    "enable_log() is deprecated; use logging.getLogger('arcus').setLevel(logging.DEBUG) "
    "or logging.WARNING instead."
)
def enable_log(flag=True):
    """Set the diagnostic level to DEBUG, or WARNING when disabled.

    This compatibility helper never installs output handlers. Applications can
    configure the named logger directly instead; its initial level is inherited.
    """
    _logger.setLevel(logging.DEBUG if flag else logging.WARNING)


def arcuslog(caller, *param):
    """Emit caller context and diagnostic parts without eager value formatting."""
    if not _logger.isEnabledFor(logging.DEBUG):
        return
    message = "%r" * len(param)
    if caller is not None:
        message = "[%s(%#x)] " + message
        param = (type(caller).__name__, id(caller), *param)
    _logger.debug(message, *param, stacklevel=2)
