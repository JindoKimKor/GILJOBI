"""
Step 3: Rule-Based Primary Tag Assignment

Uses job taxonomy JSON (e.g., base_with_updated_tech.json) for matching.
Matches seniority_removed_title against all job titles and their sub_titles.

Input: Parquet from Step 2 [id, title, description, seniority, seniority_removed_title]
Output: Parquet with additional column [id, ..., primary_tag]

Usage:
    # Default paths
    docker exec -it spark-master python /opt/spark/notebooks/step3_primary_tag.py
    
    # Custom paths
    docker exec -it spark-master python /opt/spark/notebooks/step3_primary_tag.py \
        --config /opt/spark/config/onet_with_updated_tech.json \
        --input /opt/spark/data/processed/step2_with_seniority \
        --output /opt/spark/data/processed/step3_with_primary_tag
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import udf, col, count, desc
from pyspark.sql.types import StringType
import json
import argparse
from typing import Dict, Tuple


def load_job_taxonomy(config_path: str) -> Tuple[Dict[str, str], Dict]:
    """
    Load job taxonomy and create lookup dictionary.
    
    Args:
        config_path: Path to job taxonomy JSON file
        
    Returns:
        Tuple of (lookup_dict, stats)
        - lookup_dict: normalized_sub_title -> main_title
        - stats: Statistics about the taxonomy
    """
    with open(config_path, 'r', encoding='utf-8') as f:
        all_jobs = json.load(f)
    
    lookup = {}
    tech_count = 0
    non_tech_count = 0
    total_variations = 0
    
    for job in all_jobs:
        main_title = job['title']
        source = job.get('source', 'onet')
        
        # Count job types
        if source == 'custom' or job.get('code', '').startswith(('15-1', '15-2', '11-3021')):
            tech_count += 1
        else:
            non_tech_count += 1
        
        # Add all sub_titles to lookup
        for sub_title in job.get('sub_titles', [main_title]):
            normalized = sub_title.lower().strip()
            total_variations += 1
            
            # If duplicate, keep the first one
            if normalized not in lookup:
                lookup[normalized] = main_title
    
    stats = {
        'total_jobs': len(all_jobs),
        'tech_jobs': tech_count,
        'non_tech_jobs': non_tech_count,
        'total_variations': total_variations,
        'unique_titles': len(set(lookup.values()))
    }
    
    return lookup, stats


def match_job_tag(title: str, lookup: Dict[str, str]) -> str:
    """
    Match title against job taxonomy (case-insensitive, exact match).
    
    Args:
        title: Job title with seniority removed
        lookup: Dictionary mapping normalized title -> main job title
        
    Returns:
        Matched main job title or empty string
    """
    if not title:
        return ""
    
    # Normalize for matching
    normalized = title.lower().strip()
    
    # Lookup
    return lookup.get(normalized, "")


def run_step3(
    spark: SparkSession, 
    input_path: str, 
    output_path: str,
    config_path: str
) -> DataFrame:
    """
    Step 3: Rule-based job tagging using job taxonomy
    
    Args:
        spark: SparkSession
        input_path: Parquet from Step 2
        output_path: Output Parquet path
        config_path: Path to job taxonomy JSON
        
    Returns:
        Processed DataFrame with primary_tag column added
    """
    print(f"\n{'='*80}")
    print("STEP 3: Rule-Based Job Tagging")
    print(f"{'='*80}")
    
    # 1. Load job taxonomy
    print(f"\n[1/4] Loading job taxonomy from: {config_path}")
    
    lookup, stats = load_job_taxonomy(config_path)
    
    print(f"      Total job titles: {stats['total_jobs']:,}")
    print(f"      - Tech jobs: {stats['tech_jobs']:,}")
    print(f"      - Non-tech jobs: {stats['non_tech_jobs']:,}")
    print(f"      Total matching variations: {stats['total_variations']:,}")
    print(f"      Unique normalized titles: {stats['unique_titles']:,}")
    
    # 2. Load data from Step 2
    print(f"\n[2/4] Reading from: {input_path}")
    df = spark.read.parquet(input_path)
    
    record_count = df.count()
    print(f"      Total records: {record_count:,}")
    print(f"      Input columns: {df.columns}")
    
    # 3. Apply rule-based matching
    print(f"\n[3/4] Applying rule-based matching...")
    
    # Create UDF with captured lookup dictionary
    def match_with_lookup(title: str) -> str:
        return match_job_tag(title, lookup)
    
    match_udf = udf(match_with_lookup, StringType())
    
    # Add primary_tag column
    result_df = df.withColumn(
        'primary_tag',
        match_udf(col('seniority_removed_title'))
    )
    
    # Calculate matching statistics
    matched_count = result_df.filter(
        col('primary_tag').isNotNull() & 
        (col('primary_tag') != "")
    ).count()
    unmatched_count = record_count - matched_count
    match_rate = (matched_count / record_count * 100) if record_count > 0 else 0
    
    print(f"      ✓ Matched: {matched_count:,} ({match_rate:.1f}%)")
    print(f"      ✓ Unmatched (for LLM Step 4): {unmatched_count:,} ({100-match_rate:.1f}%)")
    print(f"      ✓ Added column: primary_tag")
    
    # 4. Save to Parquet
    print(f"\n[4/4] Saving to: {output_path}")
    result_df.write.mode('overwrite').parquet(output_path)
    print(f"      ✓ Saved as Parquet")
    
    # Show top matched jobs
    print(f"\nTop 40 Matched Jobs by Frequency:")
    print("-" * 80)
    result_df.filter(col('primary_tag') != "") \
        .groupBy('primary_tag') \
        .agg(count('*').alias('count')) \
        .orderBy(desc('count')) \
        .show(40, truncate=False)
    
    # Show sample matched records
    print(f"\nSample Matched Records (first 20):")
    print("-" * 80)
    result_df.filter(col('primary_tag') != "") \
        .select('seniority_removed_title', 'primary_tag') \
        .show(20, truncate=False)
    
    # Show sample unmatched records
    print(f"\nSample Unmatched Records (for LLM processing, first 20):")
    print("-" * 80)
    result_df.filter((col('primary_tag').isNull()) | (col('primary_tag') == "")) \
        .select('seniority_removed_title', 'primary_tag') \
        .show(20, truncate=False)
    
    print(f"\n{'='*80}")
    print("STEP 3 COMPLETED")
    print(f"{'='*80}")
    
    return result_df


def main():
    parser = argparse.ArgumentParser(
        description='Step 3: Rule-Based Primary Tag Assignment',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Default paths
  python /opt/spark/notebooks/step3_primary_tag.py
  
  # Custom paths
  python /opt/spark/notebooks/step3_primary_tag.py \\
      --config /opt/spark/config/onet_with_updated_tech.json \\
      --input /opt/spark/data/processed/step2_with_seniority \\
      --output /opt/spark/data/processed/step3_with_primary_tag
        """
    )
    
    parser.add_argument(
        '--config',
        default='/opt/spark/config/onet_with_updated_tech.json',
        help='Path to job taxonomy JSON file (default: /opt/spark/config/onet_with_updated_tech.json)'
    )
    parser.add_argument(
        '--input',
        default='/opt/spark/data/processed/step2_with_seniority',
        help='Input Parquet path from Step 2 (default: /opt/spark/data/processed/step2_with_seniority)'
    )
    parser.add_argument(
        '--output',
        default='/opt/spark/data/processed/step3_with_primary_tag',
        help='Output Parquet path (default: /opt/spark/data/processed/step3_with_primary_tag)'
    )
    
    args = parser.parse_args()
    
    # Create Spark session
    spark = SparkSession.builder \
        .appName("Step3_RuleBasedTagging") \
        .getOrCreate()
    
    try:
        result_df = run_step3(
            spark,
            input_path=args.input,
            output_path=args.output,
            config_path=args.config
        )
        
        print(f"\n{'='*80}")
        print("STEP 3 EXECUTION SUMMARY")
        print(f"{'='*80}")
        print(f"  Output columns: {result_df.columns}")
        
        # Calculate final statistics
        total = result_df.count()
        matched = result_df.filter(col('primary_tag') != "").count()
        unmatched = total - matched
        
        print(f"\n  Overall Statistics:")
        print(f"    Total records: {total:,}")
        print(f"    Rule-based matched: {matched:,} ({matched/total*100:.1f}%)")
        print(f"    Unmatched (for LLM): {unmatched:,} ({unmatched/total*100:.1f}%)")
        
        # Show schema
        print(f"\n  Output Schema:")
        result_df.printSchema()
        
        print(f"\n{'='*80}")
        print("✓ STEP 3 COMPLETED SUCCESSFULLY")
        print(f"{'='*80}")
        
    except Exception as e:
        print(f"\n{'='*80}")
        print("✗ STEP 3 FAILED")
        print(f"{'='*80}")
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()