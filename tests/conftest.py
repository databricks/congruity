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

import pytest
import subprocess

import socket
import time


def check_port_availability(host: str, port: int, timeout: int):
    start_time = time.time()
    while True:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(1)
        result = sock.connect_ex((host, port))
        if result == 0:
            sock.close()
            break
        else:
            sock.close()
            if time.time() - start_time > timeout:
                raise Exception(
                    "Could not connect to port {} after {} seconds".format(port, timeout)
                )
        time.sleep(1)


def spark_connect_starter() -> subprocess.Popen:
    pid = subprocess.Popen(
        ["pyspark", "--remote", "local", "--packages", "org.apache.spark:spark-connect_2.12:3.5.0"],
        stderr=subprocess.PIPE,
        stdout=subprocess.PIPE,
    )
    # Try to connect on port 15002 until it is ready:
    check_port_availability("localhost", 15002, 90)
    time.sleep(1)
    assert pid.poll() is None
    return pid


@pytest.fixture(scope="session", params=["classic", "connect"])
def spark_session(request):
    if request.param == "classic":
        import os

        if "SPARK_CONNECT_MODE_ENABLED" in os.environ:
            del os.environ["SPARK_CONNECT_MODE_ENABLED"]
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.master("local").appName("test").getOrCreate()
        yield spark
        spark.sparkContext.stop()
    else:
        pid = spark_connect_starter()
        from pyspark.sql.connect.session import SparkSession as RSS

        spark = RSS.builder.remote("sc://localhost").create()
        yield spark
        spark.stop()
        pid.terminate()
