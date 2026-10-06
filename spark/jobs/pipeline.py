from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    from_unixtime,
    lit,
    coalesce
)
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    LongType,
    StringType
)

from elasticsearch import Elasticsearch


# ============================================================
# CONFIG
# ============================================================

KAFKA_BOOTSTRAP = "kafka:9092"
KAFKA_TOPIC = "foodflow.public.orders"

MINIO_ENDPOINT = "http://minio:9000"
MINIO_BUCKET = "foodflow"

MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"

ELASTICSEARCH_URL = "http://elasticsearch:9200"
ELASTICSEARCH_INDEX = "foodflow_orders"

POSTGRES_URL = "jdbc:postgresql://postgres:5432/foodflow"
POSTGRES_USER = "foodflow"
POSTGRES_PASSWORD = "foodflow"


# ============================================================
# SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("FoodFlowStreamingPipeline")

    # --------------------------------------------------------
    # MinIO / S3
    # --------------------------------------------------------

    .config(
        "spark.hadoop.fs.s3a.endpoint",
        MINIO_ENDPOINT
    )
    .config(
        "spark.hadoop.fs.s3a.access.key",
        MINIO_ACCESS_KEY
    )
    .config(
        "spark.hadoop.fs.s3a.secret.key",
        MINIO_SECRET_KEY
    )
    .config(
        "spark.hadoop.fs.s3a.path.style.access",
        "true"
    )
    .config(
        "spark.hadoop.fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem"
    )
    .config(
        "spark.hadoop.fs.s3a.connection.ssl.enabled",
        "false"
    )

    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


print("========================================")
print(" FoodFlow Streaming Pipeline")
print("========================================")


# ============================================================
# KAFKA
# ============================================================

print("Connecting to Kafka...")

kafka_df = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        KAFKA_BOOTSTRAP
    )
    .option(
        "subscribe",
        KAFKA_TOPIC
    )
    .option(
        "startingOffsets",
        "earliest"
    )
    .option(
        "failOnDataLoss",
        "false"
    )
    .load()
)


# ============================================================
# RAW JSON
# ============================================================

raw_df = (
    kafka_df
    .select(
        col("value")
        .cast("string")
        .alias("json")
    )
)


# ============================================================
# DEBEZIUM ORDER SCHEMA
#
# Matches your actual Kafka message:
#
# payload:
# {
#     before: {...},
#     after: {...},
#     source: {...},
#     transaction: null,
#     op: "c",
#     ts_ms: ...
# }
# ============================================================

order_schema = StructType([

    StructField(
        "id",
        IntegerType(),
        True
    ),

    StructField(
        "customer_id",
        IntegerType(),
        True
    ),

    StructField(
        "restaurant_id",
        IntegerType(),
        True
    ),

    StructField(
        "menu_item_id",
        IntegerType(),
        True
    ),

    StructField(
        "quantity",
        IntegerType(),
        True
    ),

    StructField(
        "description",
        StringType(),
        True
    ),

    # IMPORTANT:
    # Debezium says MicroTimestamp.
    StructField(
        "created_at",
        LongType(),
        True
    )
])


source_schema = StructType([

    StructField(
        "version",
        StringType(),
        True
    ),

    StructField(
        "connector",
        StringType(),
        True
    ),

    StructField(
        "name",
        StringType(),
        True
    ),

    StructField(
        "ts_ms",
        LongType(),
        True
    ),

    StructField(
        "snapshot",
        StringType(),
        True
    ),

    StructField(
        "db",
        StringType(),
        True
    ),

    StructField(
        "sequence",
        StringType(),
        True
    ),

    StructField(
        "ts_us",
        LongType(),
        True
    ),

    StructField(
        "ts_ns",
        LongType(),
        True
    ),

    StructField(
        "schema",
        StringType(),
        True
    ),

    StructField(
        "table",
        StringType(),
        True
    ),

    StructField(
        "txId",
        LongType(),
        True
    ),

    StructField(
        "lsn",
        LongType(),
        True
    ),

    StructField(
        "xmin",
        LongType(),
        True
    )
])


payload_schema = StructType([

    StructField(
        "before",
        order_schema,
        True
    ),

    StructField(
        "after",
        order_schema,
        True
    ),

    StructField(
        "source",
        source_schema,
        True
    ),

    StructField(
        "transaction",
        StringType(),
        True
    ),

    StructField(
        "op",
        StringType(),
        True
    ),

    StructField(
        "ts_ms",
        LongType(),
        True
    ),

    StructField(
        "ts_us",
        LongType(),
        True
    ),

    StructField(
        "ts_ns",
        LongType(),
        True
    )
])


envelope_schema = StructType([

    StructField(
        "schema",
        StringType(),
        True
    ),

    StructField(
        "payload",
        payload_schema,
        True
    )
])


# ============================================================
# PROCESS BATCH
# ============================================================

def process_batch(batch_df, batch_id):

    print("")
    print("========================================")
    print("Processing batch:", batch_id)
    print("========================================")

    if batch_df.rdd.isEmpty():

        print("No new Kafka records.")

        return


    # ========================================================
    # RAW KAFKA
    # ========================================================

    print("")
    print("RAW KAFKA RECORDS:")

    batch_df.select(
        "json"
    ).show(
        truncate=False,
        n=20
    )


    # ========================================================
    # BRONZE
    # ========================================================

    print("")
    print("=== BRONZE ===")

    bronze = (
        batch_df
        .withColumn(
            "processed_at",
            lit(
                spark.sql(
                    "SELECT current_timestamp()"
                ).first()[0]
            )
        )
    )

    bronze_path = (
        f"s3a://{MINIO_BUCKET}/bronze/orders"
    )

    (
        bronze
        .write
        .mode("append")
        .parquet(bronze_path)
    )

    print(
        "Bronze written:",
        bronze_path
    )


    # ========================================================
    # PARSE DEBEZIUM
    # ========================================================

    print("")
    print("=== PARSING DEBEZIUM ===")

    parsed = (
        batch_df
        .select(
            from_json(
                col("json"),
                envelope_schema
            ).alias("event")
        )
    )


    # ========================================================
    # EXTRACT CDC EVENT
    #
    # For CREATE / UPDATE / READ:
    #     payload.after
    #
    # For DELETE:
    #     payload.after == null
    #     payload.before contains the old row
    # ========================================================

    events = (
        parsed
        .select(

            col("event.payload.op")
            .alias("op"),

            coalesce(
                col("event.payload.after.id"),
                col("event.payload.before.id")
            )
            .alias("order_id"),

            coalesce(
                col("event.payload.after.customer_id"),
                col("event.payload.before.customer_id")
            )
            .alias("customer_id"),

            coalesce(
                col("event.payload.after.restaurant_id"),
                col("event.payload.before.restaurant_id")
            )
            .alias("restaurant_id"),

            coalesce(
                col("event.payload.after.menu_item_id"),
                col("event.payload.before.menu_item_id")
            )
            .alias("menu_item_id"),

            coalesce(
                col("event.payload.after.quantity"),
                col("event.payload.before.quantity")
            )
            .alias("quantity"),

            coalesce(
                col("event.payload.after.description"),
                col("event.payload.before.description")
            )
            .alias("description"),

            coalesce(
                col("event.payload.after.created_at"),
                col("event.payload.before.created_at")
            )
            .alias("created_at"),

            col("event.payload.ts_ms")
            .alias("event_ts_ms")
        )
    )


    print("")
    print("=== PARSED CDC EVENTS ===")

    events.show(
        truncate=False,
        n=50
    )


    # ========================================================
    # CDC COUNTS
    # ========================================================

    print("")
    print("=== CDC OPERATIONS ===")

    events.groupBy(
        "op"
    ).count().show()


    # ========================================================
    # VALID EVENTS
    # ========================================================

    valid_events = (
        events
        .filter(
            col("order_id").isNotNull()
        )
    )


    if valid_events.rdd.isEmpty():

        print("")
        print("No valid order events found.")

        return


    # ========================================================
    # REFERENCE TABLES
    #
    # NO psycopg2.
    #
    # Spark JDBC reads PostgreSQL directly.
    # ========================================================

    print("")
    print("=== LOADING REFERENCE DATA ===")


    jdbc_properties = {
        "user": POSTGRES_USER,
        "password": POSTGRES_PASSWORD,
        "driver": "org.postgresql.Driver"
    }


    # --------------------------------------------------------
    # CUSTOMERS
    # --------------------------------------------------------

    customers = (
        spark.read
        .jdbc(
            url=POSTGRES_URL,
            table="customers",
            properties=jdbc_properties
        )
        .select(
            col("id").alias("customer_id"),
            col("name").alias("customer")
        )
    )


    # --------------------------------------------------------
    # RESTAURANTS
    # --------------------------------------------------------

    restaurants = (
        spark.read
        .jdbc(
            url=POSTGRES_URL,
            table="restaurants",
            properties=jdbc_properties
        )
        .select(
            col("id").alias("restaurant_id"),
            col("name").alias("restaurant"),
            col("category").alias("category")
        )
    )


    # --------------------------------------------------------
    # MENU ITEMS
    # --------------------------------------------------------

    menu_items = (
        spark.read
        .jdbc(
            url=POSTGRES_URL,
            table="menu_items",
            properties=jdbc_properties
        )
        .select(
            col("id").alias("menu_item_id"),
            col("name").alias("menu_item")
        )
    )


    # ========================================================
    # SILVER
    # ========================================================

    print("")
    print("=== SILVER ===")


    # Only CREATE / UPDATE / READ produce a current row.
    #
    # DELETE is handled separately below.
    current_events = (
        valid_events
        .filter(
            col("op").isin(
                "c",
                "u",
                "r",
                "create",
                "update",
                "read"
            )
        )
    )


    silver = (
        current_events

        .join(
            customers,
            "customer_id",
            "left"
        )

        .join(
            restaurants,
            "restaurant_id",
            "left"
        )

        .join(
            menu_items,
            "menu_item_id",
            "left"
        )

        .select(
            "order_id",
            "customer_id",
            "customer",
            "restaurant_id",
            "restaurant",
            "category",
            "menu_item_id",
            "menu_item",
            "quantity",
            "description",
            "created_at",
            "event_ts_ms"
        )

        # Prevent duplicate records inside the same batch.
        .dropDuplicates([
            "order_id"
        ])
    )


    # ========================================================
    # CREATED_AT
    #
    # Debezium:
    #
    # io.debezium.time.MicroTimestamp
    #
    # Therefore:
    #
    # microseconds -> seconds
    #
    # divide by 1,000,000
    #
    # NOT 1,000.
    # ========================================================

    silver = (
        silver
        .withColumn(
            "created_at_ts",
            from_unixtime(
                col("created_at") / 1000000
            ).cast("timestamp")
        )
        .drop("created_at")
        .withColumnRenamed(
            "created_at_ts",
            "created_at"
        )
    )


    print("")
    print("SILVER DATA:")

    silver.show(
        truncate=False,
        n=50
    )


    # ========================================================
    # WRITE SILVER
    # ========================================================

    silver_path = (
        f"s3a://{MINIO_BUCKET}/silver/orders"
    )


    (
        silver
        .write
        .mode("append")
        .parquet(silver_path)
    )


    print(
        "Silver written:",
        silver_path
    )


    # ========================================================
    # GOLD
    # ========================================================

    print("")
    print("=== GOLD ===")


    gold_restaurant = (
        silver
        .groupBy(
            "restaurant"
        )
        .count()
        .withColumnRenamed(
            "count",
            "total_orders"
        )
        .orderBy(
            col("total_orders").desc()
        )
    )


    print("")
    print("Orders by restaurant:")

    gold_restaurant.show()


    gold_category = (
        silver
        .groupBy(
            "category"
        )
        .count()
        .withColumnRenamed(
            "count",
            "total_orders"
        )
        .orderBy(
            col("total_orders").desc()
        )
    )


    print("")
    print("Orders by category:")

    gold_category.show()


    # ========================================================
    # GOLD STORAGE
    # ========================================================

    gold_restaurant_path = (
        f"s3a://{MINIO_BUCKET}/gold/orders_by_restaurant"
    )

    gold_category_path = (
        f"s3a://{MINIO_BUCKET}/gold/orders_by_category"
    )


    (
        gold_restaurant
        .write
        .mode("overwrite")
        .parquet(gold_restaurant_path)
    )


    (
        gold_category
        .write
        .mode("overwrite")
        .parquet(gold_category_path)
    )


    # ========================================================
    # ELASTICSEARCH
    # ========================================================

    print("")
    print("=== ELASTICSEARCH ===")


    es = Elasticsearch(
        ELASTICSEARCH_URL
    )


    # ========================================================
    # CREATE INDEX
    # ========================================================

    if not es.indices.exists(
        index=ELASTICSEARCH_INDEX
    ):

        es.indices.create(
            index=ELASTICSEARCH_INDEX,

            mappings={
                "properties": {

                    "order_id": {
                        "type": "integer"
                    },

                    "customer_id": {
                        "type": "integer"
                    },

                    "customer": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword"
                            }
                        }
                    },

                    "restaurant_id": {
                        "type": "integer"
                    },

                    "restaurant": {
                        "type": "keyword",
                    },

                    "category": {
                        "type": "keyword"
                    },

                    "menu_item_id": {
                        "type": "integer"
                    },

                    "menu_item": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword"
                            }
                        }
                    },

                    "quantity": {
                        "type": "integer"
                    },

                    "description": {
                        "type": "text"
                    },

                    "created_at": {
                        "type": "date"
                    }
                }
            }
        )

        print(
            "Created Elasticsearch index:",
            ELASTICSEARCH_INDEX
        )


    # ========================================================
    # DELETE EVENTS
    #
    # If PostgreSQL deletes order #7:
    #
    # Debezium:
    #
    # op = d
    # after = null
    # before = {... id: 7 ...}
    #
    # We extracted order_id from BEFORE.
    # ========================================================

    delete_events = (
        valid_events
        .filter(
            col("op") == "d"
        )
        .select(
            "order_id"
        )
        .dropDuplicates([
            "order_id"
        ])
    )


    delete_records = delete_events.collect()


    for row in delete_records:

        order_id = row["order_id"]

        try:

            es.delete(
                index=ELASTICSEARCH_INDEX,
                id=order_id
            )

            print(
                "Deleted Elasticsearch order:",
                order_id
            )

        except Exception as e:

            # Ignore 404 because the document may not
            # exist in Elasticsearch yet.
            if "404" not in str(e):

                print(
                    "Elasticsearch delete error:",
                    order_id,
                    str(e)
                )


    # ========================================================
    # INDEX CREATE / UPDATE / READ
    # ========================================================

    records = silver.collect()

    indexed = 0


    for row in records:

        document = {

            "order_id":
                row["order_id"],

            "customer_id":
                row["customer_id"],

            "customer":
                row["customer"],

            "restaurant_id":
                row["restaurant_id"],

            "restaurant":
                row["restaurant"],

            "category":
                row["category"],

            "menu_item_id":
                row["menu_item_id"],

            "menu_item":
                row["menu_item"],

            "quantity":
                row["quantity"],

            "description":
                row["description"]
        }


        # ----------------------------------------------------
        # Timestamp
        # ----------------------------------------------------

        if row["created_at"] is not None:

            document["created_at"] = (
                row["created_at"].isoformat()
            )


        # ----------------------------------------------------
        # Same order_id = same Elasticsearch document.
        #
        # Therefore:
        #
        # INSERT -> creates
        # UPDATE -> replaces
        # SNAPSHOT -> creates/replaces
        # ----------------------------------------------------

        es.index(
            index=ELASTICSEARCH_INDEX,
            id=row["order_id"],
            document=document
        )

        indexed += 1


    # ========================================================
    # REFRESH
    # ========================================================

    es.indices.refresh(
        index=ELASTICSEARCH_INDEX
    )


    # ========================================================
    # VERIFY
    # ========================================================

    result = es.count(
        index=ELASTICSEARCH_INDEX
    )


    print("")
    print(
        f"Indexed/updated {indexed} orders"
    )

    print(
        "Elasticsearch document count:",
        result["count"]
    )

    print("")
    print("Batch completed successfully.")


# ============================================================
# START STREAMING
# ============================================================

print("")
print("Starting streaming query...")
print("Kafka -> Bronze -> Silver -> Gold -> Elasticsearch")
print("Micro-batch interval: 10 seconds")


query = (
    raw_df
    .writeStream
    .foreachBatch(process_batch)

    .option(
        "checkpointLocation",
        "s3a://foodflow/checkpoints/orders"
    )

    .trigger(
        processingTime="10 seconds"
    )

    .start()
)


query.awaitTermination()
