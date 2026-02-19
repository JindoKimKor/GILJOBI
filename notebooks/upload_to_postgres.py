# upload_to_postgres.py
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

# ===== Configuration =====
TABLE_NAME = "job_postings"
# DB_HOST = "postgres"
# DB_PORT = "5432"
# DB_NAME = "giljobi"
# DB_USER = "postgres"
# DB_PASSWORD = "postgres"

DB_HOST = "ep-autumn-rice-aj6zkfzb.c-3.us-east-2.aws.neon.tech"
DB_PORT = "5432"
DB_NAME = "neondb"
DB_USER = "neondb_owner"
DB_PASSWORD = "npg_Oy5pZmqt0VnG"

# Define schema
SCHEMA = StructType([
    StructField("id", StringType(), nullable=False),
    StructField("title", StringType(), nullable=True),
    StructField("seniority", StringType(), nullable=True),
    StructField("seniority_removed_title", StringType(), nullable=True),
    StructField("primary_tag", StringType(), nullable=True),
    StructField("match_type", StringType(), nullable=True)
])

def upload_parquet_to_postgres(parquet_path, mode="append"):
    """Upload parquet file to PostgreSQL"""
    
    spark = SparkSession.builder \
        .appName("ParquetToPostgreSQL") \
        .config("spark.jars", "/opt/spark/jars/custom/postgresql-42.7.1.jar") \
        .getOrCreate()
    
    try:
        print(f"Reading parquet file: {parquet_path}")
        df = spark.read.schema(SCHEMA).parquet(parquet_path)
        
        # Rename 'id' to 'job_id'
        df = df.withColumnRenamed("id", "job_id")
        
        record_count = df.count()
        print(f"✓ Loaded {record_count} records")
        print("\nSample data:")
        df.show(5, truncate=False)
        
        # PostgreSQL connection
        jdbc_url = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
        connection_properties = {
            "user": DB_USER,
            "password": DB_PASSWORD,
            "driver": "org.postgresql.Driver"
        }
        
        print(f"\nUploading to PostgreSQL table: {TABLE_NAME}")
        print(f"Mode: {mode}")
        
        df.write.jdbc(
            url=jdbc_url,
            table=TABLE_NAME,
            mode=mode,
            properties=connection_properties
        )
        
        print(f"✓ Successfully uploaded {record_count} records to {TABLE_NAME}")
        
    except Exception as e:
        print(f"✗ Error: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Upload parquet to PostgreSQL')
    parser.add_argument('--input', '-i', required=True, help='Input parquet file path')
    parser.add_argument('--mode', '-m', default='append', 
                        choices=['append', 'overwrite'],
                        help='Write mode (default: append)')
    args = parser.parse_args()
    
    upload_parquet_to_postgres(args.input, args.mode)