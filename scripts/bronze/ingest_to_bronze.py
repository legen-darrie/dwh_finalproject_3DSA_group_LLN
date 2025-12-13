import os
import pandas as pd
import argparse
from datetime import datetime
import uuid
import re
import time

# --- Constants ---
SOURCE_DATA_PATH = "/app/source_data"
LANDING_ZONE_PATH = "/app/data_zone"

# --- Validation Logging ---
validation_log = []

def log_validation(stage, file, issue_type, details, severity="WARNING"):
    """Logs validation issues."""
    validation_log.append({
        'timestamp': datetime.now().isoformat(),
        'stage': stage,
        'file': file,
        'issue_type': issue_type,
        'details': details,
        'severity': severity
    })
    print(f"    [{severity}] {issue_type}: {details}")

# --- Stage 1: Identify All Sources ---
def identify_sources(raw_folder: str) -> dict:
    """Lists all datasets from each department. Notes path and format."""
    print("\n" + "="*70)
    print("STAGE 1: IDENTIFY ALL SOURCES (DATASETS)")
    print("="*70)
    
    sources = {}
    
    for department in os.listdir(raw_folder):
        dept_path = os.path.join(raw_folder, department)
        
        if not os.path.isdir(dept_path):
            continue
        
        sources[department] = []
        print(f"\nDepartment: {department}")
        
        for file in os.listdir(dept_path):
            file_path = os.path.join(dept_path, file)
            
            if not os.path.isfile(file_path):
                continue
            
            ext = file.split(".")[-1].lower()
            size = os.path.getsize(file_path)
            
            sources[department].append({
                'filename': file,
                'path': file_path,
                'format': ext,
                'size_bytes': size
            })
            
            print(f"  [OK] {file} ({ext}, {size} bytes)")
    
    return sources

# --- Stage 2: Extract All Sources ---
def load_dataset(path: str, max_retries=3) -> pd.DataFrame:
    """
    Detects file format and reads using pandas.
    This step will NOT clean the data yet.
    Includes retry logic on failure (3x max).
    """
    ext = path.split(".")[-1].lower()

    for attempt in range(max_retries):
        try:
            if ext == "csv":
                return pd.read_csv(path, low_memory=False)
            elif ext == "parquet":
                return pd.read_parquet(path)
            elif ext in ["pickle", "pkl"]:
                return pd.read_pickle(path)
            elif ext in ["xlsx", "xls"]:
                return pd.read_excel(path) 
            elif ext == "json":
                return pd.read_json(path)
            elif ext == "html":
                tables = pd.read_html(path)
                return tables[0] if tables else pd.DataFrame()
            else:
                raise ValueError(f"Unsupported file type: {ext}")
                
        except Exception as e:
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt  # Exponential backoff
                log_validation("EXTRACT", os.path.basename(path), "RETRY_EXTRACT", 
                             f"Attempt {attempt + 1}/{max_retries} failed: {str(e)}. Retrying in {wait_time}s...", 
                             "WARNING")
                time.sleep(wait_time)
            else:
                log_validation("EXTRACT", os.path.basename(path), "EXTRACTION_FAILED", 
                             f"All {max_retries} attempts exhausted: {str(e)}", "ERROR")
                raise

# --- FIRST VALIDATION (After Extract, Before Loading) ---
def first_validation(df: pd.DataFrame, file: str, file_path: str) -> bool:
    """
    First Validation Checkpoint:
    - Check and log extraction errors
    - Retry on fail (3x)
    """
    print(f"\n  FIRST VALIDATION: {file}")
    
    # Check 1: File exists and readable
    if not os.path.exists(file_path):
        log_validation("FIRST_VALIDATION", file, "FILE_NOT_FOUND", 
                      f"File does not exist: {file_path}", "ERROR")
        return False
    
    # Check 2: File size validation
    size = os.path.getsize(file_path)
    if size < 10:
        log_validation("FIRST_VALIDATION", file, "EMPTY_FILE", 
                      f"File size is {size} bytes (too small)", "ERROR")
        return False
    
    # Check 3: DataFrame validation
    if df is None:
        log_validation("FIRST_VALIDATION", file, "NULL_DATAFRAME", 
                      "Extraction returned None", "ERROR")
        return False
    
    if df.empty:
        log_validation("FIRST_VALIDATION", file, "EMPTY_DATAFRAME", 
                      "DataFrame has no rows", "WARNING")
    
    if len(df.columns) == 0:
        log_validation("FIRST_VALIDATION", file, "NO_COLUMNS", 
                      "DataFrame has no columns", "ERROR")
        return False
    
    print(f"    [OK] Validation passed: {len(df)} rows, {len(df.columns)} columns")
    return True

# --- Stage 3: Raw Loading Zone ---
def standardize_filename(file_name: str, department: str) -> str:
    """Creates a standardized Parquet output filename."""
    base = file_name.rsplit(".", 1)[0]
    base = re.sub(r'[\s\-\.]+', '_', base).lower()
    dept = department.replace(" ", "_").lower()
    return f"{dept}_{base}.parquet"

def raw_loading_zone(sources: dict, bronze_folder: str, batch_id: str, ingest_ts: datetime):
    """Converts raw tables into .parquet files. Saves in the data warehouse."""
    print("\n" + "="*70)
    print("STAGE 3: RAW LOADING ZONE (BRONZE LAYER)")
    print("="*70)
    
    os.makedirs(bronze_folder, exist_ok=True)
    
    successful = 0
    failed = 0
    
    for department, files in sources.items():
        print(f"\nLoading Department: {department}")
        
        for file_info in files:
            file = file_info['filename']
            file_path = file_info['path']
            
            print(f"\n  Processing: {file}")
            
            try:
                # Extract
                df = load_dataset(file_path, max_retries=3)
                
                # === FIRST VALIDATION CHECKPOINT ===
                if not first_validation(df, file, file_path):
                    failed += 1
                    continue
                
                # Add audit metadata
                df['ingest_ts'] = ingest_ts
                df['batch_id'] = batch_id
                df['source_department'] = department
                df['source_filename'] = file
                
                # Save to Bronze
                output_name = standardize_filename(file, department)
                output_path = os.path.join(bronze_folder, output_name)
                df.to_parquet(output_path, index=False)
                
                print(f"    [OK] Saved to Bronze: {output_name}")
                successful += 1
                
            except Exception as e:
                failed += 1
                log_validation("RAW_LOADING", file, "PROCESSING_ERROR", 
                             f"Unexpected error: {str(e)}", "ERROR")
    
    print(f"\n{'='*70}")
    print(f"Bronze Layer Complete: {successful} successful | {failed} failed")
    print(f"{'='*70}")

# --- Main Ingestion ---
def ingestion(raw_folder: str, bronze_folder: str):
    """Main Bronze pipeline orchestrator."""
    batch_id = str(uuid.uuid4())
    ingest_ts = datetime.now()
    
    print("\n" + "="*70)
    print(f"BRONZE LAYER INGESTION PIPELINE")
    print(f"Batch ID: {batch_id}")
    print(f"Timestamp: {ingest_ts.isoformat()}")
    print("="*70)
    
    # Stage 1: Identify sources
    sources = identify_sources(raw_folder)
    
    # Stage 2 & 3: Extract and Load with First Validation
    raw_loading_zone(sources, bronze_folder, batch_id, ingest_ts)
    
    # Save validation log
    if validation_log:
        log_df = pd.DataFrame(validation_log)
        log_path = os.path.join(bronze_folder, "_bronze_validation_log.csv")
        log_df.to_csv(log_path, index=False)
        print(f"\nValidation log saved: {log_path}")
        
        errors = len([x for x in validation_log if x['severity'] == 'ERROR'])
        warnings = len([x for x in validation_log if x['severity'] == 'WARNING'])
        print(f"Validation Summary: {errors} errors, {warnings} warnings")

# --- Execution ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Bronze Layer Ingestion with First Validation Checkpoint"
    )
    
    parser.add_argument("--raw-folder", type=str, default=SOURCE_DATA_PATH)
    parser.add_argument("--bronze-folder", type=str, default=LANDING_ZONE_PATH)
    
    args = parser.parse_args()
    os.makedirs(args.bronze_folder, exist_ok=True)
    
    ingestion(args.raw_folder, args.bronze_folder)