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


"""Existing node administration command construction."""

from ..request import CommandRequest, CommandSubmitter


class AdminCommands:
    def __init__(self, executor: CommandSubmitter, responses):
        self._executor = executor
        self._responses = responses

    def flush_all(self):
        full_cmd = b"flush_all"
        return self._executor.submit(
            CommandRequest("flush_all", full_cmd, self._responses.ok)
        )

    def get_stats(self, stat_args=None):
        if stat_args == None:
            full_cmd = b"stats"
        else:
            full_cmd = bytes("stats " + stat_args, "utf-8")
        return self._executor.submit(
            CommandRequest("stats", full_cmd, self._responses.stats)
        )
