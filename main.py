import sys
import uuid

from pyspark.sql.functions import col, to_json, struct

from lib import ConfigLoader, Utils, DataLoader, Transformations
from lib.ConfigLoader import get_conf, get_spark_conf
from lib.logger import Log4j

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: sbdl {local, qa, prod} {load date}: Arguments are missing")
        sys.exit(-1)

    job_run_env = sys.argv[1].upper()
    load_date = sys.argv[2]
    job_run_id = "SBDL-" + str(uuid.uuid4())


    print("Initializing SBDL Job in " + job_run_env + " Job ID: " + job_run_id)
    conf = ConfigLoader.get_conf(job_run_env)
    enable_hive = True if conf["enable.hive"] == "true" else False
    hive_df = conf["hive.database"]

    print(f"enable_hive : {enable_hive}")
    print(f"hive_df: {hive_df}")


    print("Creating Spark Session")
    spark = Utils.get_spark_session(job_run_env)

    logger = Log4j(spark)


    logger.info("Reading and Transforming SBDL Account DF")
    accounts_df = DataLoader.read_account_data(spark, 'LOCAL', False, "demo_db")
    # accounts_df.show(truncate=False)
    transformed_accounts_df = Transformations.get_contract(accounts_df)
    # transformed_accounts_df.show(truncate=False)
    # accounts_df.printSchema()


    logger.info("Reading and Transforming SBDL Party DF")
    parties_df = DataLoader.read_party_data(spark, 'LOCAL', False, "demo_db")
    # parties_df.show(truncate=False)
    transformed_parties_df = Transformations.get_party(parties_df)
    # transformed_parties_df.show(truncate=False)
    # party_df.printSchema()


    logger.info("Reading and Transforming SBDL Address DF")
    addresses_df = DataLoader.read_address_data(spark, 'LOCAL', False, "demo_db")
    # addresses_df.show(truncate=False)
    transformed_address_df = Transformations.get_address(addresses_df)
    # transformed_address_df.show(truncate=False)
    # address_df.printSchema()


    logger.info("Join Party Relations and Address")
    party_address_df = Transformations.join_party_address(transformed_parties_df, transformed_address_df)
    # party_address_df.show(truncate=False)


    logger.info("Join Account and Party")
    data_df = Transformations.join_contract_party(transformed_accounts_df, party_address_df)
    # data_df.show(truncate=False)

    final_df = Transformations.apply_header(spark, data_df)
    logger.info("Preparing send data to Kafka")

    # final_df.show(truncate=False)
    final_df.printSchema()
    kafka_kv_df = final_df.select(col("payload.contractIdentifier.newValue").alias("key"),
                                  to_json(struct("*")).alias("value"))

    api_key = conf["kafka.api_key"]
    api_secret = conf["kafka.api_secret"]

    kafka_kv_df.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", conf["kafka.bootstrap.servers"]) \
        .option("topic", conf["kafka.topic"]) \
        .option("kafka.security.protocol", conf["kafka.security.protocol"]) \
        .option("kafka.sasl.jaas.config", conf["kafka.sasl.jaas.config"].format(api_key, api_secret)) \
        .option("kafka.sasl.mechanism", conf["kafka.sasl.mechanism"]) \
        .option("kafka.client.dns.lookup", conf["kafka.client.dns.lookup"]) \
        .save()

    logger.info("Finished sending data to Kafka")
