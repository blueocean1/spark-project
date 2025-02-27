import sys
import uuid

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
    accounts_df.show(truncate=False)
    transformed_accounts_df = Transformations.get_contract(accounts_df)
    transformed_accounts_df.show(truncate=False)
    # accounts_df.printSchema()


    logger.info("Reading and Transforming SBDL Party DF")
    parties_df = DataLoader.read_party_data(spark, 'LOCAL', False, "demo_db")
    parties_df.show(truncate=False)
    transformed_parties_df = Transformations.get_party(parties_df)
    transformed_parties_df.show(truncate=False)
    # party_df.printSchema()


    logger.info("Reading and Transforming SBDL Address DF")
    addresses_df = DataLoader.read_address_data(spark, 'LOCAL', False, "demo_db")
    addresses_df.show(truncate=False)
    transformed_address_df = Transformations.get_address(addresses_df)
    transformed_address_df.show(truncate=False)
    # address_df.printSchema()


    logger.info("Join Party Relations and Address")
    party_address_df = Transformations.join_party_address(transformed_parties_df, transformed_address_df)
    party_address_df.show(truncate=False)


    logger.info("Join Account and Party")
    contract_party_df = Transformations.join_contract_party(transformed_accounts_df, transformed_parties_df)
    contract_party_df.show(truncate=False)

