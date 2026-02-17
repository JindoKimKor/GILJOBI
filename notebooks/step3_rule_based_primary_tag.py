"""
Step 3: Rule-Based Primary Tag Assignment (Improved with Safe Partial Matching)

Usage:
    # Default (exact match only)
    docker exec -it spark-master python /opt/spark/notebooks/step3_primary_tag_improved_safe.py
    
    # With partial matching enabled
    docker exec -it spark-master python /opt/spark/notebooks/step3_primary_tag_improved_safe.py \
        --enable-partial-match
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import udf, col, count, desc, split
from pyspark.sql.types import StringType
import json
import argparse
from typing import Dict, Tuple, List

# ------------------------------
# Taxonomy Loading
# ------------------------------
def load_job_taxonomy_improved(config_path: str) -> Tuple[Dict, Dict, List, Dict]:
    """
    Load job taxonomy with improved structure.
    Returns main_lookup, sub_lookup, partial_match_list, stats
    """
    with open(config_path, 'r', encoding='utf-8') as f:
        all_jobs = json.load(f)
    
    main_lookup = {}      
    sub_lookup = {}       
    partial_match_list = []  

    tech_count = 0
    non_tech_count = 0
    total_main_titles = 0
    total_sub_variations = 0
    
    for job in all_jobs:
        main_title = job['title']
        source = job.get('source', 'onet')
        
        # Count tech/non-tech jobs
        if source == 'custom' or job.get('code', '').startswith(('15-1','15-2','11-3021')):
            tech_count += 1
        else:
            non_tech_count += 1
        
        # Exact match dict
        normalized_main = main_title.lower().strip()
        if normalized_main not in main_lookup:
            main_lookup[normalized_main] = main_title
            total_main_titles += 1
        
        # Sub-title exact match
        sub_titles = job.get('sub_titles', [])
        normalized_variants = [normalized_main]  # Include main title for partial match
        for sub_title in sub_titles:
            normalized_sub = sub_title.lower().strip()
            if normalized_sub not in sub_lookup:
                sub_lookup[normalized_sub] = main_title
                total_sub_variations += 1
            normalized_variants.append(normalized_sub)
        
        # Partial match list (sorted by length desc)
        normalized_variants.sort(key=len, reverse=True)
        partial_match_list.append((main_title, normalized_variants))
    
    stats = {
        'total_jobs': len(all_jobs),
        'tech_jobs': tech_count,
        'non_tech_jobs': non_tech_count,
        'main_titles': total_main_titles,
        'sub_variations': total_sub_variations,
        'total_variations': total_main_titles + total_sub_variations
    }
    
    return main_lookup, sub_lookup, partial_match_list, stats

# ------------------------------
# Matching Function
# ------------------------------
def match_job_tag_improved(
    title: str, 
    main_lookup: Dict[str, str],
    sub_lookup: Dict[str, str],
    partial_match_list: List[Tuple[str, List[str]]],
    enable_partial: bool = False
) -> Tuple[str, str]:
    """
    Improved matching: exact main -> exact sub -> partial (taxonomy words ⊂ DB title words)
    """
    if not title:
        return "", "no_match"
    
    normalized_title = title.lower().strip()
    title_words = set(normalized_title.split())

    # 1. Exact match main title
    if normalized_title in main_lookup:
        return main_lookup[normalized_title], "exact_main"

    # 2. Exact match sub-title
    if normalized_title in sub_lookup:
        return sub_lookup[normalized_title], "exact_sub"

    # 3. Partial match
    if enable_partial:
        stopwords = {
            'engineer', 'developer', 'manager', 'analyst', 'specialist',
            'coordinator', 'assistant', 'director', 'associate', 'lead',
            'senior', 'junior', 'staff', 'principal', 'chief'
        }

        for main_title, variants in partial_match_list:
            for variant in variants:
                if variant in stopwords or len(variant) < 8:
                    continue

                variant_words = set(variant.split())
                if variant_words.issubset(title_words):
                    return main_title, "partial"

    return "", "no_match"

# ------------------------------
# Step 3 Execution
# ------------------------------
def run_step3_improved(
    spark: SparkSession, 
    input_path: str, 
    output_path: str,
    config_path: str,
    enable_partial_match: bool = False
) -> DataFrame:
    print(f"\n{'='*80}")
    print("STEP 3: Rule-Based Job Tagging (Improved Safe Partial Matching)")
    print(f"{'='*80}")
    
    # Load taxonomy
    main_lookup, sub_lookup, partial_match_list, stats = load_job_taxonomy_improved(config_path)
    
    print(f"\nLoaded taxonomy: {stats['total_jobs']} titles, {stats['tech_jobs']} tech, {stats['non_tech_jobs']} non-tech")
    
    # Load Step2 data
    df = spark.read.parquet(input_path)
    record_count = df.count()
    print(f"Loaded {record_count} records from Step2")
    
    # UDF for matching
    def match_with_improved_logic(title: str) -> str:
        tag, match_type = match_job_tag_improved(
            title, main_lookup, sub_lookup, partial_match_list, enable_partial_match
        )
        return f"{tag}|{match_type}"
    
    match_udf = udf(match_with_improved_logic, StringType())

    # Apply UDF
    result_df = df.withColumn('match_result', match_udf(col('seniority_removed_title')))
    result_df = result_df.withColumn('primary_tag', split(col('match_result'), '\\|').getItem(0)) \
                         .withColumn('match_type', split(col('match_result'), '\\|').getItem(1)) \
                         .drop('match_result')

    # Matching statistics
    match_stats = result_df.groupBy('match_type').count().orderBy(desc('count')).collect()
    match_dict = {row['match_type']: row['count'] for row in match_stats}
    total_matched = match_dict.get('exact_main',0) + match_dict.get('exact_sub',0) + match_dict.get('partial',0)

    print(f"\nMatching Summary:")
    for mt in ['exact_main','exact_sub','partial','no_match']:
        count = match_dict.get(mt,0)
        pct = count / record_count * 100 if record_count>0 else 0
        print(f"  {mt:<12} : {count:,} ({pct:.1f}%)")
    print(f"  TOTAL MATCHED : {total_matched:,} ({total_matched/record_count*100:.1f}%)")
    
    # Save output
    result_df.write.mode('overwrite').parquet(output_path)
    print(f"\nSaved result to {output_path}")
    
    return result_df

# ------------------------------
# Main
# ------------------------------
def main():
    parser = argparse.ArgumentParser(description='Step 3: Rule-Based Primary Tag Assignment (Improved Safe Partial Matching)')
    parser.add_argument('--config', default='/opt/spark/config/onet_with_updated_tech.json', help='Taxonomy JSON path')
    parser.add_argument('--input', default='/opt/spark/data/processed/step2_with_seniority', help='Step2 Parquet input')
    parser.add_argument('--output', default='/opt/spark/data/processed/step3_with_primary_tag', help='Output Parquet path')
    parser.add_argument('--enable-partial-match', action='store_true', help='Enable partial matching')
    args = parser.parse_args()

    spark = SparkSession.builder.appName("Step3_ImprovedSafePartialTagging").getOrCreate()
    try:
        run_step3_improved(
            spark,
            input_path=args.input,
            output_path=args.output,
            config_path=args.config,
            enable_partial_match=args.enable_partial_match
        )
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
