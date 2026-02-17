"""
Step 1: Select Required Columns with Flexible Column Matching

Automatically finds and standardizes columns:
- ID columns (job_id, posting_id, id, etc.) → 'id'
- Title columns (title, job_title, position, etc.) → 'title'
- Description columns (description, desc, job_description, etc.) → 'description' [COMMENTED OUT - uncomment if needed]

Input: Raw CSV
Output: Parquet with standardized columns [id, title]  # description removed to reduce file size
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, length, trim
from pyspark.sql.types import StringType
from typing import Dict, Optional


def find_column(columns: list, keywords: list) -> Optional[str]:
    """
    Find first column that contains any of the keywords (case-insensitive).
    
    Args:
        columns: List of column names
        keywords: List of keywords to search for
        
    Returns:
        Matched column name or None
    """
    columns_lower = [c.lower() for c in columns]
    
    for keyword in keywords:
        for original_col, lower_col in zip(columns, columns_lower):
            if keyword.lower() in lower_col:
                return original_col
    return None


def detect_columns(df: DataFrame) -> Dict[str, str]:
    """
    Detect required columns from DataFrame.
    
    Args:
        df: Input DataFrame
        
    Returns:
        Dictionary mapping target column name to source column name
    """
    columns = df.columns
    
    mapping = {}
    
    # Find ID column
    id_col = find_column(columns, ['job_id', 'posting_id', 'id'])
    if id_col:
        mapping['id'] = id_col
    
    # Find Title column
    title_col = find_column(columns, ['title', 'position'])
    if title_col:
        mapping['title'] = title_col
    
    # Find Description column [COMMENTED OUT - uncomment if needed]
    desc_col = find_column(columns, ['description', 'desc'])
    if desc_col:
        mapping['description'] = desc_col
    
    return mapping


def run_step1(spark: SparkSession, input_path: str, output_path: str) -> DataFrame:
    """
    Step 1: Select and standardize required columns
    
    Args:
        spark: SparkSession
        input_path: Raw CSV path
        output_path: Output Parquet path
        
    Returns:
        Processed DataFrame with columns [id, title]  # description excluded
    """
    print(f"\n{'='*80}")
    print("STEP 1: Column Selection & Standardization")
    print(f"{'='*80}")
    
    # 1. Load data (read all columns as String to avoid type inference issues)
    print(f"\n[1/4] Reading from: {input_path}")
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .option("multiLine", True)
        .option("quote", '"')
        .option("escape", '"')
        .option("mode", "PERMISSIVE")
        .csv(input_path)
    )
    
    initial_count = df.count()
    print(f"      Total records: {initial_count:,}")
    print(f"      Available columns: {len(df.columns)} columns")
    
    # 2. Auto-detect required columns
    print(f"\n[2/4] Detecting required columns...")
    column_mapping = detect_columns(df)
    
    # Check for missing required columns
    # required = ['id', 'title']  # description removed to reduce file size
    required = ['id', 'title', 'description']  # [UNCOMMENT to include description]
    missing = [col_name for col_name in required if col_name not in column_mapping]
    
    if missing:
        print(f"      Available columns: {df.columns}")
        raise ValueError(f"Could not find columns for: {missing}")
    
    print(f"      Column mapping:")
    for target, source in column_mapping.items():
        print(f"        {target:15} ← {source}")
    
    # 3. Select columns and rename (with explicit type casting)
    print(f"\n[3/4] Selecting and renaming columns...")
    selected_df = df.select(
        col(column_mapping['id']).cast(StringType()).alias('id'),
        col(column_mapping['title']).cast(StringType()).alias('title'),
        col(column_mapping['description']).cast(StringType()).alias('description')  # [UNCOMMENT to include description]
    )
    
    # Filter out null and empty strings (safe method)
    cleaned_df = selected_df.filter(
        col('id').isNotNull() & 
        col('title').isNotNull() &
        (length(trim(col('title'))) > 0)  # Check for empty strings
    )
    
    final_count = cleaned_df.count()
    removed = initial_count - final_count
    
    print(f"      Records after filtering: {final_count:,}")
    print(f"      Removed (null/empty): {removed:,}")
    
    # 4. Save to Parquet
    print(f"\n[4/4] Saving to: {output_path}")
    cleaned_df.write.mode('overwrite').parquet(output_path)
    print(f"      ✓ Saved as Parquet")
    
    # Show sample output
    print(f"\nSample Output (first 5 rows):")
    print("-" * 80)
    cleaned_df.show(5, truncate=True)
    
    print(f"\n{'='*80}")
    print("STEP 1 COMPLETED")
    print(f"{'='*80}")
    
    return cleaned_df


if __name__ == "__main__":
    """Standalone execution for testing."""
    spark = SparkSession.builder \
        .appName("Step1_ColumnSelection") \
        .getOrCreate()
    
    try:
        result_df = run_step1(
            spark,
            input_path="/opt/spark/data/raw/linkedin_postings_2023-2024.csv",
            output_path="/opt/spark/data/processed/step1_selected"
        )
        
        print(f"\n✓ Step 1 completed successfully")
        print(f"  Final schema:")
        result_df.printSchema()
        print(f"  Column count: {len(result_df.columns)}")
        print(f"  Record count: {result_df.count():,}")
        
    except Exception as e:
        print(f"\n✗ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        
    finally:
        spark.stop()