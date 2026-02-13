import argparse
from pyspark.sql import SparkSession


def main():
    parser = argparse.ArgumentParser(description="Convert Parquet to CSV using Spark")

    parser.add_argument(
        "--input",
        type=str,
        required=True,
        help="Input parquet directory path"
    )

    parser.add_argument(
        "--output",
        type=str,
        required=True,
        help="Output CSV directory path"
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional row limit for sampling"
    )

    args = parser.parse_args()

    spark = SparkSession.builder.appName("ParquetToCSV").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    print(f"Reading from: {args.input}")
    df = spark.read.parquet(args.input)

    if args.limit:
        print(f"Limiting to {args.limit} rows")
        df = df.limit(args.limit)

    print(f"Writing CSV to: {args.output}")

    (
        df.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(args.output)
    )

    print("Done.")
    spark.stop()


if __name__ == "__main__":
    main()
