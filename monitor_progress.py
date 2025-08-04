"""
Database Processing Monitor
==========================

Monitor the progress of the full database processing and show current status.
"""

import os
import glob
import time
from datetime import datetime

def monitor_progress():
    """Monitor the progress of database processing"""
    
    print("🔍 DATABASE PROCESSING MONITOR")
    print("=" * 50)
    
    while True:
        try:
            # Check for output files
            output_files = glob.glob("webhook_ready_contacts_FULL_*.csv")
            intermediate_files = glob.glob("webhook_contacts_intermediate_*.csv")
            
            print(f"\n⏰ Status Check: {datetime.now().strftime('%H:%M:%S')}")
            
            if output_files:
                # Final file exists - processing complete!
                latest_file = max(output_files, key=os.path.getctime)
                file_size = os.path.getsize(latest_file)
                
                print(f"🎉 PROCESSING COMPLETE!")
                print(f"📁 Final file: {latest_file}")
                print(f"📏 File size: {file_size:,} bytes")
                
                # Count lines in the file
                try:
                    with open(latest_file, 'r') as f:
                        line_count = sum(1 for line in f) - 1  # Subtract header
                    print(f"📊 Contacts processed: {line_count:,}")
                    print(f"✅ Success! Database processing completed.")
                except:
                    print(f"📊 File created successfully")
                
                break
            
            elif intermediate_files:
                # Show intermediate progress
                latest_intermediate = max(intermediate_files, key=os.path.getctime)
                file_size = os.path.getsize(latest_intermediate)
                
                # Extract contact count from filename
                try:
                    parts = latest_intermediate.split('_')
                    contact_count = int(parts[3])  # Should be the count
                    percentage = (contact_count / 2075) * 100
                    
                    print(f"⏳ IN PROGRESS...")
                    print(f"📁 Latest intermediate: {latest_intermediate}")
                    print(f"📊 Contacts processed: {contact_count:,}/2,075 ({percentage:.1f}%)")
                    print(f"📏 File size: {file_size:,} bytes")
                except:
                    print(f"⏳ IN PROGRESS...")
                    print(f"📁 Intermediate file found: {latest_intermediate}")
                    print(f"📏 File size: {file_size:,} bytes")
            
            else:
                print(f"⏳ PROCESSING STARTED...")
                print(f"📊 No output files yet - processing in progress")
                print(f"💡 Check terminal output for detailed progress")
            
            # Check for any CSV files that might indicate progress
            all_csv_files = glob.glob("webhook*.csv")
            if all_csv_files:
                print(f"\n📂 CSV files in directory:")
                for file in sorted(all_csv_files):
                    size = os.path.getsize(file)
                    modified = datetime.fromtimestamp(os.path.getmtime(file))
                    print(f"  - {file} ({size:,} bytes, {modified.strftime('%H:%M:%S')})")
            
            print(f"\n💤 Waiting 30 seconds before next check...")
            print("-" * 50)
            
            time.sleep(30)  # Check every 30 seconds
            
        except KeyboardInterrupt:
            print(f"\n⚠️  Monitoring stopped by user")
            break
        except Exception as e:
            print(f"\n❌ Error during monitoring: {str(e)}")
            time.sleep(10)

if __name__ == "__main__":
    monitor_progress()
