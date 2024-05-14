# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from congruity.json_conversion import to_json_conversion
from congruity.rdd_adapter import adapt_to_rdd

_monkey_patch_complete = False


def monkey_patch_spark():
    global _monkey_patch_complete
    if _monkey_patch_complete:
        return
    from pyspark.sql.connect.dataframe import DataFrame

    # Register all the monkey patches here.
    DataFrame.toJSON = to_json_conversion

    # Patch properties.
    setattr(DataFrame, "rdd", property(adapt_to_rdd))

    _monkey_patch_complete = True
    return


monkey_patch_spark()
