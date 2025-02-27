import sys

import lib.Utils
from lib.ConfigLoader import get_conf, get_spark_conf
from lib.logger import Log4j

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: sbdl {local, qa, prod} {load date}: Arguments are missing")
        sys.exit(-1)

    job_run_env = sys.argv[1].upper()
    load_date = sys.argv[2]

    spark = lib.Utils.get_spark_session(job_run_env)
    logger = Log4j(spark)

    conf = get_conf(job_run_env)
    spark_conf = get_spark_conf(job_run_env)

    kafka_topic = conf["account.filter"]
    print(kafka_topic)

    logger.info("Finished creating Spark Session")