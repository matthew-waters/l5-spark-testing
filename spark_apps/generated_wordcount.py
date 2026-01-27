#!/usr/bin/env python3
import argparse

from pyspark.sql import SparkSession, functions as F


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Word count over generated data.")
    parser.add_argument("--rows", type=int, default=100000, help="Number of rows to generate.")
    parser.add_argument("--vocab-size", type=int, default=100, help="Number of unique words.")
    parser.add_argument("--words-per-row", type=int, default=5, help="Words per generated row.")
    parser.add_argument(
        "--output-s3",
        type=str,
        default="",
        help="Optional S3 URI to write output data (e.g., s3://bucket/prefix/).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    spark = SparkSession.builder.appName("generated-wordcount").getOrCreate()

    base = spark.range(0, args.rows).withColumn(
        "base_word",
        F.concat(F.lit("word_"), (F.col("id") % F.lit(args.vocab_size)).cast("string")),
    )

    repeated = base.select(
        F.expr(f"array_repeat(base_word, {args.words_per_row}) as words")
    )

    words = repeated.select(F.explode("words").alias("word"))
    counts = words.groupBy("word").count().orderBy(F.desc("count"))

    counts.show(20, truncate=False)

    if args.output_s3:
        counts.coalesce(1).write.mode("overwrite").parquet(args.output_s3)

    spark.stop()


if __name__ == "__main__":
    main()
