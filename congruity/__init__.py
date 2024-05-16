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
from pyspark.errors import PySparkAttributeError, PySparkNotImplementedError
from pyspark.sql.connect.session import SparkSession

from congruity.json_conversion import to_json_conversion
from congruity.rdd_adapter import adapt_to_rdd
from congruity.spark_context_adapter import adapt_to_spark_context

_monkey_patch_complete = False


def adapt_session_getattr(self, name):
    if name in ["_jsc", "_jconf", "_jvm", "_jsparkSession"]:
        raise PySparkAttributeError(
            error_class="JVM_ATTRIBUTE_NOT_SUPPORTED", message_parameters={"attr_name": name}
        )
    elif name in ["newSession"]:
        raise PySparkNotImplementedError(
            error_class="NOT_IMPLEMENTED", message_parameters={"feature": f"{name}()"}
        )


def monkey_patch_spark():
    global _monkey_patch_complete
    if _monkey_patch_complete:
        return
    from pyspark.sql.connect.dataframe import DataFrame

    # Register all the monkey patches here.
    DataFrame.toJSON = to_json_conversion

    # Patch properties.
    setattr(DataFrame, "rdd", property(adapt_to_rdd))
    setattr(DataFrame, "sparkContext", property(adapt_to_spark_context))

    # Spark Session
    setattr(SparkSession, "sparkContext", property(adapt_to_spark_context))
    SparkSession.__getattr__ = adapt_session_getattr

    _monkey_patch_complete = True
    return


monkey_patch_spark()
