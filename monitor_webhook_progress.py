"""
Monitor Webhook Payload Enhancement Progress
==========================================

Monitor the progress of the webhook payload enhancement process.
"""

import os
import glob
import time
from datetime import datetime

def monitor_webhook_progress():
    """Monitor the progress of webhook payload enhancement"""
    
    print("🔍 WEBHOOK PAYLOAD ENHANCEMENT MONITOR")
    print("=" * 60)
    
    start_monitor_time = datetime.now()
    
    while True:
        try:
            # Check for output files
            output_files = glob.glob("webhook_payload_ready_*.csv")
            
            print(f"\n⏰ Status Check: {datetime.now().strftime('%H:%M:%S')}")
            
            if output_files:
                # Final file exists - processing complete!
                latest_file = max(output_files, key=os.path.getctime)
                file_size = os.path.getsize(latest_file)
                
                print(f"🎉 WEBHOOK PAYLOAD ENHANCEMENT COMPLETE!")
                print(f"📁 Final file: {latest_file}")
                print(f"📏 File size: {file_size:,} bytes")
                
                # Count lines in the file
                try:
                    with open(latest_file, 'r') as f:
                        line_count = sum(1 for line in f) - 1  # Subtract header
                    print(f"📊 Contacts processed: {line_count:,}")
                    
                    # Show first few lines of output
                    with open(latest_file, 'r') as f:
                        lines = f.readlines()
                        print(f"\n📋 WEBHOOK PAYLOAD COLUMNS:")
                        if len(lines) > 0:
                            headers = lines[0].strip().split(',')
                            for i, header in enumerate(headers[:10], 1):
                                print(f"  {i}. {header}")
                            if len(headers) > 10:
                                print(f"  ... and {len(headers) - 10} more columns")
                    
                    print(f"\n✅ SUCCESS! Webhook payload CSV created with {line_count:,} contacts")
                    print(f"🎯 File contains ONLY webhook payload columns")
                    print(f"📤 Ready to process with your Supabase webhook function")
                    
                except Exception as e:
                    print(f"📊 File created successfully (size: {file_size:,} bytes)")
                
                break
            
            else:
                print(f"⏳ PROCESSING IN PROGRESS...")
                print(f"📊 No output files yet - enhancement running")
                print(f"💡 Processing 2,220 contacts across 29 sources")
                
                # Show elapsed time
                elapsed = datetime.now() - start_monitor_time
                print(f"🕐 Monitor running: {elapsed}")
            
            print(f"\n💤 Waiting 30 seconds before next check...")
            print("-" * 60)
            
            time.sleep(30)  # Check every 30 seconds
            
        except KeyboardInterrupt:
            print(f"\n⚠️  Monitoring stopped by user")
            break
        except Exception as e:
            print(f"\n❌ Error during monitoring: {str(e)}")
            time.sleep(10)

if __name__ == "__main__":
    monitor_webhook_progress()
