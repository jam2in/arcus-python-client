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

import datetime

g_log = False


def enable_log(flag=True):
    global g_log
    g_log = flag


def arcuslog(caller, *param):
    global g_log

    if g_log:
        str = ""
        if caller:
            str = "[%s - %s(%s)] " % (
                datetime.datetime.now(),
                caller.__class__.__name__,
                hex(id(caller)),
            )

        for p in param:
            str += repr(p)

        print(str)
