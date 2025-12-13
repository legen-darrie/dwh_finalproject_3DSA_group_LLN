import os
import time
import re
import uuid
import argparse
from datetime import datetime
import pandas as pd

# --- Default Paths (Inside Container) ---
SOURCE_DATA_PATH = "/app/source_data"
LANDING_ZONE_PATH = "/app/data_zone"

# --- Validation Logging ---
validation_log: list[dict] = []

def log_validation(stage, file, issue_type, details, severity="WARNING"):
    validation_log.append({
        "timestamp": datetime.now().isoformat(),
        "stage": stage,
        "file": file,
        "issue_type": issue_type,
        "details": details,
        "severity": severity,
    })
    print(f"      [{severity}] {stage} - {file} - {issue_type}: {details}")

def save_validation_log(bronze_folder):
    if not validation_log:
        return
    log_df = pd.DataFrame(validation_log)
    # Save log to the root of the data_zone
    log_path = os.path.join(os.path.dirname(bronze_folder), "_bronze_validation_log.csv")
    log_df.to_csv(log_path, index=False)
    print(f"\nValidation log saved: {log_path}")

# ===================
# Stage 1: Discovery
# ===================
def discover_sources(raw_folder):
    print("\n" + "=" * 70)
    print("STAGE 1: IDENTIFY ALL SOURCES")
    print("=" * 70)
    
    if not os.path.exists(raw_folder):
        print(f"[CRITICAL ERROR] Source path not found: {raw_folder}")
        return {}

    sources = {}
    for department in os.listdir(raw_folder):
        dept_path = os.path.join(raw_folder, department)
        if not os.path.isdir(dept_path):
            continue

        dept_files = []
        print(f"\nChecking Department: {department}")
        for file in os.listdir(dept_path):
            file_path = os.path.join(dept_path, file)
            if os.path.isfile(file_path):
                ext = file.split(".")[-1].lower()
                dept_files.append({"filename": file, "path": file_path, "format": ext})
                print(f"  - Found: {file}")
        
        if dept_files:
            sources[department] = dept_files
    return sources

# ====================
# Stage 2: Extraction (The corrected function is here)
# ====================
def load_dataset(path):
    ext = path.split(".")[-1].lower()
    file_name = os.path.basename(path).lower() # Get the file name for conditional check
    try:
        if ext == "csv": 
            # Use tab delimiter ONLY for the known tab-separated Marketing files
            if "campaign" in file_name:
                print(f"      [INFO] Reading {file_name} with TAB delimiter.")
                return pd.read_csv(path, sep='\t', low_memory=False)
            # Use default comma delimiter for all other CSV files (Operations, Customer, etc.)
            else:
                return pd.read_csv(path, low_memory=False)
                
        if ext == "parquet": return pd.read_parquet(path)
        if ext in {"pickle", "pkl"}: return pd.read_pickle(path)
        if ext in {"xlsx", "xls"}: return pd.read_excel(path)
        if ext == "json": return pd.read_json(path)
        if ext == "html": 
            tables = pd.read_html(path)
            return tables[0] if tables else pd.DataFrame()
        raise ValueError(f"Unknown type: {ext}")
    except Exception as e:
        print(f"      [!] Error loading {os.path.basename(path)}: {e}")
        raise

# ====================
# Stage 3: Loading
# ====================
def raw_loading_zone(raw_folder, bronze_folder):
    # CRITICAL: Create the subfolder inside data_zone
    if not os.path.exists(bronze_folder):
        print(f"Creating Bronze subdirectory: {bronze_folder}")
        os.makedirs(bronze_folder, exist_ok=True)

    sources = discover_sources(raw_folder)
    
    print("\n" + "=" * 70)
    print("STAGE 2: RAW LOADING TO BRONZE PARQUET")
    print("=" * 70)

    for department, files in sources.items():
        for meta in files:
            file = meta["filename"]
            path = meta["path"]
            
            try:
                df = load_dataset(path)
                
                # Add Audit Columns
                df["_loaded_ts"] = datetime.now().isoformat()
                df["_src_dept"] = department
                
                # Standardize Bronze Name
                clean_name = re.sub(r"[\s\-]+", "_", os.path.splitext(file)[0].lower())
                out_name = f"{department.lower()}_{clean_name}_bronze.parquet"
                out_path = os.path.join(bronze_folder, out_name)

                df.to_parquet(out_path, index=False)
                print(f"  [OK] Saved Bronze: {out_name}")
                
            except Exception as e:
                log_validation("BRONZE_LOAD", file, "FAILED", str(e), "ERROR")

def run_bronze_pipeline(source_data, data_zone):
    print("\n" + "=" * 70)
    print("BRONZE LAYER INGESTION PIPELINE")
    print("=" * 70)
    
    # Force path to subfolder
    bronze_subfolder = os.path.join(data_zone, "bronze_files")
    
    raw_loading_zone(source_data, bronze_subfolder)
    save_validation_log(bronze_subfolder)
    print(f"\n[COMPLETE] Files should be in: {bronze_subfolder}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-data", type=str, default=SOURCE_DATA_PATH)
    parser.add_argument("--landing-zone", type=str, default=LANDING_ZONE_PATH)
    args = parser.parse_args()
    
    run_bronze_pipeline(args.source_data, args.landing_zone)