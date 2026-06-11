import time
import random

def fetch_machine_logs():
    machines = ["Machine-A", "Machine-B", "Machine-C"]
    statuses = ["OPERATIONAL", "OPERATIONAL", "OVERHEATING", "UNKNOWN"]
    
    logs = []
    for i in range(1, 11):
        logs.append({
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "machine_id": random.choice(machines),
            "temperature_c": random.randint(150, 360),
            "status": random.choice(statuses)
        })
    return logs

def run_etl_pipeline():
    print("=== [START] Industrial ETL Data Pipeline ===")
    raw_data = fetch_machine_logs()
    print(f"[INFO] Ingested {len(raw_data)} raw log records from production line.")
    
    cleaned_data = []
    alert_logs = []
    
    for record in raw_data:
        if record["status"] == "UNKNOWN" or record["temperature_c"] < 160:
            continue
            
        if record["temperature_c"] > 300 or record["status"] == "OVERHEATING":
            record["alert_level"] = "CRITICAL"
            alert_logs.append(record)
        else:
            record["alert_level"] = "NORMAL"
            
        cleaned_data.append(record)
        
    print("\n--- [LOAD] Processed Active Records ---")
    for row in cleaned_data:
        print(f"[{row['timestamp']}] {row['machine_id']} | Temp: {row['temperature_c']}C | Status: {row['alert_level']}")
        
    print("\n--- [ALERT] System Anomalies Detected ---")
    if not alert_logs:
        print("No anomalies detected. Plant operating safely.")
    for alert in alert_logs:
        print(f"⚠️ ALERT! {alert['machine_id']} requires immediate check! Temp: {alert['temperature_c']}C")
        
    print("=== [END] ETL Pipeline Execution Completed ===")

if __name__ == "__main__":
    run_etl_pipeline()
