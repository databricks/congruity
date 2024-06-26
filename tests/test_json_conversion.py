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

from congruity.helper.patch import monkey_patch_spark
from pyspark.sql import SparkSession


def test_json_conversion(spark_session: "SparkSession"):
    monkey_patch_spark()
    data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]
    df = spark_session.createDataFrame(data)
    assert df.count() == 3

    rows = df.toJSON().collect()
    assert len(rows) == 3
    assert rows[0] == '{"_1":"Java","_2":"20000"}'
