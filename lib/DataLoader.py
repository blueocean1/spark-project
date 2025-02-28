from pyspark.sql.types import StructType, StructField, DateType, IntegerType, StringType, TimestampType
from lib.ConfigLoader import get_data_filter
from lib.Utils import get_spark_session

# spark = get_spark_session('LOCAL')

# DDL
# def get_account_schema_old():
#     schema = """load_date date,active_ind int ,account_id string,source_sys string,account_start_date timestamp,
#                 legal_title_1 string,legal_title_2 string,tax_id_type string,tax_id string,branch_code string,country string"""
#     return schema

def get_account_schema():
    schema = StructType([StructField("load_date", DateType()),
                         StructField("active_ind", IntegerType()),
                         StructField("account_id", StringType()),
                         StructField("source_sys", StringType()),
                         StructField("account_start_date", TimestampType()),
                         StructField("legal_title_1", StringType()),
                         StructField("legal_title_2", StringType()),
                         StructField("tax_id_type", StringType()),
                         StructField("tax_id", StringType()),
                         StructField("branch_code", StringType()),
                         StructField("country", StringType())])
    return schema

# DDL
# def get_party_schema_old():
#     schema = """load_date date,account_id string,party_id string,relation_type string,relation_start_date timestamp"""
#     return schema

def get_party_schema():
    schema = StructType([StructField("load_date", DateType()),
                         StructField("account_id", StringType()),
                         StructField("party_id", StringType()),
                         StructField("relation_type", StringType()),
                         StructField("relation_start_date", TimestampType())])
    return schema

# DDL
# def get_address_schema_old():
#     schema = """load_date date,party_id string,address_line_1 string,address_line_2 string,city string,postal_code int,
#                 country_of_address string,address_start_date date"""
#     return schema

def get_address_schema():
    schema = StructType([StructField("load_date", DateType()),
                         StructField("party_id", StringType()),
                         StructField("address_line_1", StringType()),
                         StructField("address_line_2", StringType()),
                         StructField("city", StringType()),
                         StructField("postal_code", StringType()),
                         StructField("country_of_address", StringType()),
                         StructField("address_start_date", DateType())])
    return schema


def read_account_data(spark, env, enable_hive, hive_db):
    runtime_filter = get_data_filter(env, "account.filter")
    if enable_hive:
        return spark.sql("SELECT * FROM " + hive_db + ".accounts").where(runtime_filter)
    else:
        return spark.read \
                .format("csv") \
                .option("header", "true") \
                .schema(get_account_schema()) \
                .load("/Users/Andriy_Bezpaliuk/Spark-Udemy/spark-project/test_data/accounts/account_samples.csv") \
                .where(runtime_filter)


def read_party_data(spark, env, enable_hive, hive_db):
    runtime_filter = get_data_filter(env, "party.filter")
    if enable_hive:
        return spark.sql("SELECT * FROM " + hive_db + ".parties").where(runtime_filter)
    else:
        return spark.read \
                .format("csv") \
                .option("header", "true") \
                .schema(get_party_schema()) \
                .load("/Users/Andriy_Bezpaliuk/Spark-Udemy/spark-project/test_data/parties/party_samples.csv") \
                .where(runtime_filter)


def read_address_data(spark, env, enable_hive, hive_db):
    runtime_filter = get_data_filter(env, "address.filter")
    if enable_hive:
        return spark.sql("SELECT * FROM " + hive_db + ".addresses").where(runtime_filter)
    else:
        return spark.read \
                .format("csv") \
                .option("header", "true") \
                .schema(get_address_schema()) \
                .load("/Users/Andriy_Bezpaliuk/Spark-Udemy/spark-project/test_data/party_address/address_samples.csv") \
                .where(runtime_filter)


# print("accounts data")
# accounts_df = read_account_data(spark, 'LOCAL', False, "demo_db")
# accounts_df.show(5, truncate=False)
#
# print("parties data")
# parties_df = read_party_data(spark, 'LOCAL', False, "demo_db")
# parties_df.show(5, truncate=False)
#
# print("addresses data")
# addresses_df = read_address_data(spark, 'LOCAL', False, "demo_db")
# addresses_df.show(5, truncate=False)
