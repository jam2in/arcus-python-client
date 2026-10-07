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


"""B+Tree element-flag filter parsing."""

import re

from ..exceptions import FilterInvalid


class EflagFilter:
    def __init__(self, expr=None):
        self.lhs_offset = 0
        self.bit_op = None
        self.bit_rhs = None
        self.comp_op = None
        self.comp_rhs = None

        if expr != None:
            self._parse(expr)

    def get_expr(self):
        expr = ""
        if self.lhs_offset != None:
            expr += "%d" % self.lhs_offset

            if self.bit_op and self.bit_rhs:
                expr += " %s %s" % (self.bit_op, self.bit_rhs)

            if self.comp_op and self.comp_rhs:
                expr += " %s %s" % (self.comp_op, self.comp_rhs)

        return expr

    def _parse(self, expr):
        re_expr = re.compile(
            r"EFLAG[ ]*(\[[ ]*([0-9]*)[ ]*\:[ ]*\])?[ ]*(([\&\|\^])[ ]*(0x[0-9a-fA-F]+))?[ ]*(==|\!=|<|>|<=|>=)[ ]*(0x[0-9a-fA-F]+)"
        )

        match = re_expr.match(expr)
        if match == None:
            raise FilterInvalid()

        # ( dummy, lhs_offset, dummy, bit_op, bit_rhs, comp_op, comp_rhs )
        g = match.groups()
        (
            dummy_1,
            self.lhs_offset,
            dummy_2,
            self.bit_op,
            self.bit_rhs,
            self.comp_op,
            self.comp_rhs,
        ) = g

        if self.lhs_offset == None:
            self.lhs_offset = 0
        else:
            self.lhs_offset = int(self.lhs_offset)

        if self.comp_op == "==":
            self.comp_op = "EQ"
        elif self.comp_op == "!=":
            self.comp_op = "NE"
        elif self.comp_op == "<":
            self.comp_op = "LT"
        elif self.comp_op == "<=":
            self.comp_op = "LE"
        elif self.comp_op == ">":
            self.comp_op = "GT"
        elif self.comp_op == ">=":
            self.comp_op = "GE"
