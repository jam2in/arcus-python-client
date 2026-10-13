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


"""Optional diagnostic logging shared by client components."""

import logging

_logger = logging.getLogger("arcus")
_logger.addHandler(logging.NullHandler())


def enable_log(flag=True):
    _logger.setLevel(logging.DEBUG if flag else logging.WARNING)


def arcuslog(caller, *param):
    if not _logger.isEnabledFor(logging.DEBUG):
        return

    message = " ".join(str(value) for value in param)
    if caller is None:
        _logger.debug("%s", message, stacklevel=2)
    else:
        _logger.debug(
            "[%s(%s)] %s",
            type(caller).__name__,
            hex(id(caller)),
            message,
            stacklevel=2,
        )
