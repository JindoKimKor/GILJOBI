"""
Step 2: Seniority Extraction

Enriches data by adding:
- seniority: Detected seniority level (entry_level, mid_level, senior, executive, not_specified)
- seniority_removed_title: Title with seniority keywords removed

Input: Parquet from Step 1 [id, title, description]
Output: Parquet with additional columns [id, title, description, seniority, seniority_removed_title]
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import udf, col, count, desc
from pyspark.sql.types import StringType, StructType, StructField
import json
import re
from typing import Dict, Tuple


def load_seniority_config(config_path: str) -> Dict:
    """Load seniority keyword configuration from JSON file."""
    with open(config_path, 'r') as f:
        return json.load(f)


def extract_seniority_from_title(title: str, config: Dict) -> Tuple[str, str]:
    """
    Extract seniority level from a single job title.
    
    Args:
        title: Job title string
        config: Seniority configuration dictionary
        
    Returns:
        Tuple of (seniority_level, seniority_removed_title)
    """
    if not title:
        return ('not_specified', '')
    
    title_lower = title.lower().strip()
    detected_level = 'not_specified'
    
    # Sort by priority (Executive > Senior > Mid > Entry)
    seniority_levels = sorted(
        config.items(), 
        key=lambda x: x[1]['priority'], 
        reverse=True
    )
    
    # Check each title for seniority keywords
    for level_name, level_data in seniority_levels:
        for keyword in level_data['keywords']:
            # Use word boundary for accurate matching
            pattern = r'\b' + re.escape(keyword) + r'\b'
            if re.search(pattern, title_lower):
                detected_level = level_name
                break
        
        if detected_level != 'not_specified':
            break
    
    # Remove seniority keywords from title
    cleaned = title
    if detected_level != 'not_specified':
        for keyword in config[detected_level]['keywords']:
            pattern = r'\b' + re.escape(keyword) + r'\b'
            cleaned = re.sub(pattern, '', cleaned, flags=re.IGNORECASE)
    
    # Remove extra whitespace
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    # Remove leading/trailing hyphens and commas
    cleaned = cleaned.strip('-,').strip()
    
    return (detected_level, cleaned)


def add_seniority_columns(
    df: DataFrame, 
    config: Dict, 
    title_column: str = 'title'
) -> DataFrame:
    """
    Add seniority and seniority_removed_title columns to DataFrame.
    All existing columns are preserved.
    
    Args:
        df: Input DataFrame
        config: Seniority configuration dictionary
        title_column: Name of the column containing job titles
        
    Returns:
        DataFrame with added columns:
        - seniority: Detected seniority level
        - seniority_removed_title: Title with seniority keywords removed
    """
    # Capture config in closure
    def extract_with_config(title: str) -> Tuple[str, str]:
        return extract_seniority_from_title(title, config)
    
    # Create UDF
    extract_udf = udf(
        extract_with_config, 
        StructType([
            StructField("seniority", StringType(), True),
            StructField("seniority_removed_title", StringType(), True)
        ])
    )
    
    # Apply UDF
    df_with_struct = df.withColumn(
        'seniority_result',
        extract_udf(col(title_column))
    )
    
    # Split struct columns
    result_df = df_with_struct \
        .withColumn('seniority', col('seniority_result.seniority')) \
        .withColumn('seniority_removed_title', col('seniority_result.seniority_removed_title')) \
        .drop('seniority_result')
    
    return result_df


def run_step2(spark: SparkSession, input_path: str, output_path: str) -> DataFrame:
    """
    Step 2: Extract seniority and add columns
    
    Args:
        spark: SparkSession
        input_path: Parquet from Step 1
        output_path: Output Parquet path
        
    Returns:
        Processed DataFrame with seniority columns added
    """
    print(f"\n{'='*80}")
    print("STEP 2: Seniority Extraction")
    print(f"{'='*80}")
    
    # 1. Load config
    config_path = "/opt/spark/config/seniority.json"
    print(f"\n[1/4] Loading config from: {config_path}")
    config = load_seniority_config(config_path)
    print(f"      Loaded {len(config)} seniority levels:")
    for level, data in sorted(config.items(), key=lambda x: x[1]['priority'], reverse=True):
        print(f"        - {level}: {len(data['keywords'])} keywords")
    
    # 2. Load data
    print(f"\n[2/4] Reading from: {input_path}")
    df = spark.read.parquet(input_path)
    
    record_count = df.count()
    print(f"      Total records: {record_count:,}")
    print(f"      Input columns: {df.columns}")
    
    # 3. Extract seniority
    print(f"\n[3/4] Extracting seniority levels...")
    result_df = add_seniority_columns(df, config, title_column='title')
    
    print(f"      ✓ Added columns: seniority, seniority_removed_title")
    print(f"      ✓ Preserved all original columns")
    
    # 4. Save to Parquet
    print(f"\n[4/4] Saving to: {output_path}")
    result_df.write.mode('overwrite').parquet(output_path)
    print(f"      ✓ Saved as Parquet")
    
    # Show distribution
    print(f"\nSeniority Distribution:")
    print("-" * 80)
    result_df.groupBy("seniority") \
        .agg(count("*").alias("count")) \
        .orderBy(desc("count")) \
        .show(truncate=False)
    
    # Show sample output
    print(f"\nSample Output (first 10 rows):")
    print("-" * 80)
    result_df.select("title", "seniority", "seniority_removed_title").show(10, truncate=True)
    
    print(f"\n{'='*80}")
    print("STEP 2 COMPLETED")
    print(f"{'='*80}")
    
    return result_df


if __name__ == "__main__":
    """Standalone execution for testing."""
    spark = SparkSession.builder \
        .appName("Step2_SeniorityExtraction") \
        .getOrCreate()
    
    try:
        result_df = run_step2(
            spark,
            input_path="/opt/spark/data/processed/step1_selected",
            output_path="/opt/spark/data/processed/step2_with_seniority"
        )
        
        print(f"\n✓ Step 2 completed successfully")
        print(f"  Output columns: {result_df.columns}")
        
    except Exception as e:
        print(f"\n✗ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        
    finally:
        spark.stop()