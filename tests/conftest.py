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
import tempfile

import pytest
import subprocess

import socket
import time


def _check_port_availability(port: int, timeout: int):
    """
    Check if a port is available for connection. If it is not available after the timeout,
    raise an exception.

    Parameters
    ----------
    port : int
        The port to connect to.
    timeout : int
        The maximum time to wait for the port to become available.
    """
    start_time = time.time()
    while True:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(1)
        result = sock.connect_ex(("localhost", port))
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


def _spark_connect_starter() -> subprocess.Popen:
    log_file = tempfile.NamedTemporaryFile(prefix="spark_connect_", suffix=".log", delete=False)
    pid = subprocess.Popen(
        [
            "spark-submit --class org.apache.spark.sql.connect.service.SparkConnectServer"
            " --packages org.apache.spark:spark-connect_2.12:3.5.0"
        ],
        shell=True,
        stderr=log_file,
        stdout=log_file,
    )
    # Try to connect on port 15002 until it is ready:
    _check_port_availability(15002, 90)
    time.sleep(1)
    assert pid.poll() is None
    return pid, log_file.name


@pytest.fixture(
    scope="session",
    params=[
        # Classic Spark Tests
        "classic",
        # Only with Connect
        "connect",
    ],
)
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
        pid, name = _spark_connect_starter()
        print(name)
        from pyspark.sql.connect.session import SparkSession as RSS

        spark = RSS.builder.remote("sc://localhost").create()
        yield spark
        time.sleep(1)
        spark.stop()
        pid.terminate()
