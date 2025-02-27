from pyspark.sql.functions import struct, lit, col, array, when, isnull, filter, collect_list, expr, current_timestamp, \
    date_format

from lib.DataLoader import read_address_data, read_party_data, read_account_data
from lib.Utils import get_spark_session

# spark = get_spark_session('LOCAL')

def get_insert_operation(column, alias):
    return struct(lit("INSERT").alias("operation"),
                  column.alias("newValue"),
                  lit(None).alias("oldValue")).alias(alias)

# data = [(1, "Alice"), (2, "Bob")]
# df = spark.createDataFrame(data, ["id", "name"])
#
# transformed_df = df.withColumn("insert_info", get_insert_operation(col("name"), "insert_data"))
# transformed_df.show(truncate=False)
#
# extracted_df = transformed_df.select(
#         col("id"),
#         col("name"),
#         col("insert_info.operation"),
#         col("insert_info.newValue"),
#         col("insert_info.oldValue"))
# extracted_df.show(truncate=False)


def get_contract(df):
    contract_title = array(when(~isnull("legal_title_1"),
                                 struct(lit("lgl_ttl_ln_1").alias("contractTitleLineType"),
                                        col("legal_title_1").alias("contractTitleLine")).alias("contractTitle")),
                            when(~isnull("legal_title_2"),
                                 struct(lit("lgl_ttl_ln_2").alias("contractTitleLineType"),
                                        col("legal_title_2").alias("contractTitleLine")).alias("contractTitle"))
                            )
    contract_title_nl = filter(contract_title, lambda x: ~isnull(x))
    tax_identifier = struct(col("tax_id_type").alias("taxIdType"),
                            col("tax_id").alias("taxId"))
    return df.select("account_id", get_insert_operation(col("account_id"), "contractIdentifier"),
                     get_insert_operation(col("source_sys"), "sourceSystemIdentifier"),
                     get_insert_operation(col("account_start_date"), "contractStartDateTime"),
                     get_insert_operation(contract_title_nl, "contractTitle"),
                     get_insert_operation(tax_identifier, "taxIdentifier"),
                     get_insert_operation(col("branch_code"), "contractBranchCOde"),
                     get_insert_operation(col("country"), "contractCountry"))


def get_party(df):
    return df.select("account_id", "party_id",
                     get_insert_operation(col("party_id"), "partyIdentifier"),
                     get_insert_operation(col("relation_type"), "partyRelationshipType"),
                     get_insert_operation(col("relation_start_date"), "partyRelationStartDate"))


def get_address(df):
    address = struct(col("address_line_1").alias("addressLine1"),
                     col("address_line_2").alias("addressLine2"),
                     col("city").alias("addressCity"),
                     col("postal_code").alias("addressPostalCode"),
                     col("country_of_address").alias("addressCountry"),
                     col("address_start_date").alias("addressStartDate"))
    return df.select("party_id", get_insert_operation(address, "partyAddress"))


# accounts_df = read_account_data(spark, 'LOCAL', False, "demo_db")
# accounts_df = get_contract(accounts_df)
# accounts_df.show(truncate=False)
# accounts_df.printSchema()
#
#
# parties_df = read_party_data(spark, 'LOCAL', False, "demo_db")
# party_df = get_party(parties_df)
# party_df.show(truncate=False)
# party_df.printSchema()
#
#
# addresses_df = read_address_data(spark, 'LOCAL', False, "demo_db")
# address_df = get_address(addresses_df)
# address_df.show(truncate=False)
# address_df.printSchema()


def join_party_address(p_df, a_df):
    return p_df.join(a_df, "party_id", "left_outer") \
            .groupBy("account_id") \
            .agg(collect_list(struct("partyIdentifier",
                                     "partyRelationshipType",
                                     "partyRelationStartDate",
                                     "partyAddress").alias("partyDetails")
                              ).alias("partyRelations"))


def join_contract_party(c_df, p_df):
    return c_df.join(p_df, "account_id", "left_outer")


def apply_header(spark, df):
    header_info = [("SBDL", 1, 0), ]
    header_df = spark.createDataFrame(header_info) \
                  .toDF("eventType", "majorSchemaVersion", "minorSchemaVersion")

    event_df = header_df.hint("broadcast").crossJoin(df) \
                 .select(struct(expr("uuid()").alias("eventIdentifier"),
                                col("eventType"), col("majorSchemaVersion"), col("minorSchemaVersion"),
                                lit(date_format(current_timestamp(), "yyyy-MM-dd'T'HH:mm:ssZ")).alias("eventDateTime")
                                ).alias("eventHeader"),
                         array(struct(lit("contractIdentifier").alias("keyField"),
                                      col("account_id").alias("keyValue")
                                      )).alias("keys"),
                         struct(col("contractIdentifier"),
                                col("sourceSystemIdentifier"),
                                col("contractStartDateTime"),
                                col("contractTitle"),
                                col("taxIdentifier"),
                                col("contractBranchCode"),
                                col("contractCountry"),
                                col("partyRelations")
                                ).alias("payload")
                         )
    return event_df