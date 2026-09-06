import os

LIB_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "lib")
KAFKA_CONNECTOR_JAR = os.path.join(LIB_DIR, "flink-sql-connector-kafka.jar")
JDBC_CONNECTOR_JAR = os.path.join(LIB_DIR, "flink-connector-jdbc.jar")
POSTGRES_DRIVER_JAR = os.path.join(LIB_DIR, "postgresql-driver.jar")


def _jar_url(path):
    return "file:///" + path.replace("\\", "/")


def create_streaming_env():
    from pyflink.table import EnvironmentSettings, TableEnvironment

    t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
    t_env.get_config().set("parallelism.default", "2")
    jar_paths = (KAFKA_CONNECTOR_JAR, JDBC_CONNECTOR_JAR, POSTGRES_DRIVER_JAR)
    jars = ";".join(_jar_url(p) for p in jar_paths)
    t_env.get_config().set("pipeline.jars", jars)
    return t_env


def create_batch_env():
    from pyflink.table import EnvironmentSettings, TableEnvironment

    t_env = TableEnvironment.create(EnvironmentSettings.in_batch_mode())
    t_env.get_config().set("parallelism.default", "1")
    return t_env
