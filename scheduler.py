# scheduler.py

import schedule
import time
import subprocess
import sys
from datetime import datetime


def run_etl_pipeline():
    """Execute the main ETL pipeline."""
    print(f"\n{'='*60}")
    print(f"[{datetime.now()}] Starting scheduled ETL pipeline...")
    print(f"{'='*60}\n")
    
    try:
        # Run main.py using the same Python interpreter
        result = subprocess.run(
            [sys.executable, "main.py"],
            capture_output=True,
            text=True,
            cwd="."
        )
        
        # Print output
        if result.stdout:
            print(result.stdout)
        if result.stderr:
            print(f"[STDERR] {result.stderr}")
        
        if result.returncode == 0:
            print(f"\n[{datetime.now()}] ETL pipeline completed successfully!")
        else:
            print(f"\n[{datetime.now()}] ETL pipeline failed with return code: {result.returncode}")
            
    except Exception as e:
        print(f"\n[{datetime.now()}] Error running ETL pipeline: {e}")


def main():
    print("="*60)
    print("Weather Data Warehouse - ETL Scheduler")
    print("="*60)
    print(f"Started at: {datetime.now()}")
    print("Scheduled to run daily at 11:00 AM")
    print("Press Ctrl+C to stop the scheduler")
    print("="*60)
    
    # Schedule the job to run every day at 11:00 AM
    schedule.every().day.at("11:00").do(run_etl_pipeline)
    
    # Option: Run immediately on startup (uncomment if needed)
    # run_etl_pipeline()
    
    # Keep the scheduler running
    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute


if __name__ == "__main__":
    main()
