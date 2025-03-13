package com.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_json;

public class SparkProcessor {
    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession.builder()
                .appName("KafkaSparkCassandraProcessor")
                .master("local[*]")
                .config("spark.cassandra.connection.host", "127.0.0.1")
                .config("spark.cassandra.connection.port", "9042")
                .config("spark.sql.timestampType", "TIMESTAMP_NTZ")
                .getOrCreate();

        Dataset<Row> kafkaDF = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "172.24.97.59:9092")
                .option("subscribe", "stock-data")
//                .option("startingOffsets", "earliest")
                .load();

        Dataset<Row> stockDF = kafkaDF
                .selectExpr("CAST(value AS STRING) as message")
                .select(from_json(col("message"), schema()).as("data"))
                .select("data.*");

        StreamingQuery query = stockDF.writeStream()
                .foreachBatch((batchDF, batchId) -> {
                    batchDF.write()
                            .format("org.apache.spark.sql.cassandra")
                            .option("keyspace", "financial_data")
                            .option("table", "stock_prices")
                            .mode("append")
                            .save();
                })
                .trigger(Trigger.ProcessingTime("30 seconds"))
                .start();

        query.awaitTermination();
    }

    private static org.apache.spark.sql.types.StructType schema() {

        return new StructType(new StructField[]{
                    new StructField("id", DataTypes.StringType, true, Metadata.empty()),  // UUID as String
                    new StructField("symbol", DataTypes.StringType, true, Metadata.empty()),
                    new StructField("timestamp", DataTypes.TimestampType, true, Metadata.empty()),  // Timestamp field
                    new StructField("open", DataTypes.DoubleType, true, Metadata.empty()),
                    new StructField("high", DataTypes.DoubleType, true, Metadata.empty()),
                    new StructField("low", DataTypes.DoubleType, true, Metadata.empty()),
                    new StructField("close", DataTypes.DoubleType, true, Metadata.empty()),
                    new StructField("volume", DataTypes.LongType, true, Metadata.empty())
                });
    }
}
