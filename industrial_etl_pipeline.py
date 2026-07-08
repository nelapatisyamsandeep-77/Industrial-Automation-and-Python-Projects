import math
import random
import time
import uuid

TARGET_MACHINES_POOL = ["Machine-A", "Machine-B", "Machine-C"]
OPERATIONAL_STATUS_POOL = ["OPERATIONAL", "OPERATIONAL", "OVERHEATING", "UNKNOWN"]

ABSOLUTE_MIN_TEMPERATURE_C = 150
INGESTION_SKIP_TEMPERATURE_C = 160
CRITICAL_ALERT_TEMPERATURE_C = 300
ABSOLUTE_MAX_TEMPERATURE_C = 360

INGESTION_STREAM_BATCH_SIZE = 120
MEMORY_PROCESSING_CHUNK_SIZE = 15


class PipelineCoreException(Exception):
    pass


class MissingPayloadKeyException(PipelineCoreException):
    pass


class InvalidDataTypeException(PipelineCoreException):
    pass


class UnregisteredMachineException(PipelineCoreException):
    pass


class MachineTelemetryExtractor:

    def __init__(self, target_batch_size: int = INGESTION_STREAM_BATCH_SIZE):
        self.packet_limit = target_batch_size

    def generate_live_stream_packet(self) -> list:
        raw_stream_buffer = []

        for _ in range(self.packet_limit):
            generated_time = time.strftime("%Y-%m-%d %H:%M:%S")
            selected_machine = random.choice(TARGET_MACHINES_POOL)
            measured_temp = random.randint(ABSOLUTE_MIN_TEMPERATURE_C, ABSOLUTE_MAX_TEMPERATURE_C)
            current_status = random.choice(OPERATIONAL_STATUS_POOL)

            payload = {
                "transaction_uuid": str(uuid.uuid4()),
                "timestamp": generated_time,
                "machine_id": selected_machine,
                "temperature_c": measured_temp,
                "status": current_status
            }
            raw_stream_buffer.append(payload)

            if random.random() < 0.10:
                duplicate_payload = payload.copy()
                raw_stream_buffer.append(duplicate_payload)

        if random.random() < 0.05:
            raw_stream_buffer.append({
                "transaction_uuid": str(uuid.uuid4()),
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "machine_id": random.choice(TARGET_MACHINES_POOL),
                "temperature_c": "CORRUPTED_STRING_VALUE",
                "status": "OPERATIONAL"
            })

        if random.random() < 0.04:
            raw_stream_buffer.append({
                "transaction_uuid": str(uuid.uuid4()),
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "machine_id": "Machine-Z_UNREGISTERED",
                "temperature_c": 210,
                "status": "OPERATIONAL"
            })

        if random.random() < 0.03:
            raw_stream_buffer.append({
                "transaction_uuid": str(uuid.uuid4()),
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "machine_id": "Machine-A",
                "status": "OPERATIONAL"
            })

        return raw_stream_buffer


class MachineETLPipelineEngine:

    def __init__(self):
        self.raw_ingestion_buffer = []
        self.cleaned_data_sink = []
        self.alert_logs_sink = []
        self.dead_letter_queue = []

        self.deduplication_registry = set()

        self.machine_partition_matrix = {
            machine: {
                "records_processed": 0,
                "running_temperature_sum": 0.0,
                "critical_incident_count": 0,
                "skipped_records_count": 0
            } for machine in TARGET_MACHINES_POOL
        }

        self.global_processed_counter = 0
        self.global_dropped_counter = 0
        self.global_cumulative_temperature_sum = 0.0

    def load_raw_ingestion_package(self, data_packet: list) -> None:
        self.raw_ingestion_buffer = data_packet

    def enforce_schema_validation(self, record: dict) -> None:
        mandatory_structural_keys = ["transaction_uuid", "timestamp", "machine_id", "temperature_c", "status"]

        for structural_key in mandatory_structural_keys:
            if structural_key not in record:
                raise MissingPayloadKeyException(f"Mandatory data property missing: '{structural_key}'")

        if record["machine_id"] not in TARGET_MACHINES_POOL:
            raise UnregisteredMachineException(f"Asset identifier code not recognized: '{record['machine_id']}'")

        if not isinstance(record["temperature_c"], (int, float)):
            raise InvalidDataTypeException(
                f"Data type mutation caught. Value '{record['temperature_c']}' is not numeric."
            )

    def run_transformation_pipeline(self) -> None:
        if not self.raw_ingestion_buffer:
            print("[SYSTEM FAULT] Ingestion cache empty. Aborting pipeline operation.")
            return

        total_records_ingested = len(self.raw_ingestion_buffer)
        
        for pointer in range(0, total_records_ingested, MEMORY_PROCESSING_CHUNK_SIZE):
            memory_chunk = self.raw_ingestion_buffer[pointer:pointer + MEMORY_PROCESSING_CHUNK_SIZE]

            for row in memory_chunk:
                try:
                    self.enforce_schema_validation(row)
                except PipelineCoreException as structural_anomaly:
                    self.dead_letter_queue.append({
                        "error_type": structural_anomaly.__class__.__name__,
                        "error_message": str(structural_anomaly),
                        "compromised_payload": row
                    })
                    self.global_dropped_counter += 1
                    continue

                packet_uuid = row["transaction_uuid"]
                if packet_uuid in self.deduplication_registry:
                    self.dead_letter_queue.append({
                        "error_type": "DeduplicationEnforcementException",
                        "error_message": f"Duplicate transaction fingerprint blocked: {packet_uuid}",
                        "compromised_payload": row
                    })
                    self.global_dropped_counter += 1
                    continue

                self.deduplication_registry.add(packet_uuid)

                target_machine = row["machine_id"]
                current_temperature = row["temperature_c"]
                current_status = row["status"]

                if current_status == "UNKNOWN" or current_temperature < INGESTION_SKIP_TEMPERATURE_C:
                    self.machine_partition_matrix[target_machine]["skipped_records_count"] += 1
                    continue

                if current_temperature > CRITICAL_ALERT_TEMPERATURE_C or current_status == "OVERHEATING":
                    row["alert_level"] = "CRITICAL"
                    self.alert_logs_sink.append(row)
                    self.machine_partition_matrix[target_machine]["critical_incident_count"] += 1
                else:
                    row["alert_level"] = "NORMAL"

                self.cleaned_data_sink.append(row)

                self.global_processed_counter += 1
                self.global_cumulative_temperature_sum += current_temperature

                self.machine_partition_matrix[target_machine]["records_processed"] += 1
                self.machine_partition_matrix[target_machine]["running_temperature_sum"] += current_temperature


class DownstreamDataDispatcher:

    @staticmethod
    def commit_to_historical_timeseries(cleaned_records: list) -> None:
        print("\n--- [LOAD] Loading Cleaned Records into Production Database ---")
        if not cleaned_records:
            print("[INFO] Staging layer clear. No logs generated for timeline append action.")
            return

        for record in cleaned_records:
            print(f"  [TS_DB_WRITE] [{record['timestamp']}] {record['machine_id']} | "
                  f"Temp: {record['temperature_c']}C | Metric Tier: {record['alert_level']}")

    @staticmethod
    def dispatch_emergency_scada_alerts(alert_records: list) -> None:
        print("\n--- [ALERT] Routing System Anomalies To Plant Control SCADA ---")
        if not alert_records:
            print("  [INFO] No thermal anomalies detected. All manufacturing blocks operating cleanly.")
            return

        for alert in alert_records:
            print(f"  [WARNING] Alert: {alert['machine_id']} requires immediate field engineering check! "
                  f"Current Thermal Footprint: {alert['temperature_c']}C [Status Flags: {alert['status']}]")

    @staticmethod
    def serialize_dead_letter_queue(dlq_records: list) -> None:
        print("\n--- [AUDIT] Archiving Compromised Data Payloads into Dead Letter Store ---")
        if not dlq_records:
            print("  [AUDIT_CLEAN] Zero network packet corruptions occurred during stream slice windows.")
            return

        print(f"  [AUDIT] Isolated {len(dlq_records)} malformed log elements from the main thread execution.")
        for anomaly_log in dlq_records:
            print(f"    -> Trace Fault: {anomaly_log['error_type']} | Info: {anomaly_log['error_message']} | "
                  f"Raw Stub: {str(anomaly_log['compromised_payload'])[:65]}...")


class PipelineAnalyticsReporter:

    @staticmethod
    def compile_and_render_report(engine: MachineETLPipelineEngine) -> None:
        print("\n" + "=" * 80)
        print("                 INDUSTRIAL SYSTEMS PERFORMANCE INFRASTRUCTURE REPORT          ")
        print("=" * 80)
        print(f"Total Telemetry Log Traces Cleaned & Processed : {engine.global_processed_counter}")
        print(f"Total Dropped, Duplicated, or Mutated Records  : {engine.global_dropped_counter}")

        if engine.global_processed_counter > 0:
            global_average_thermal = engine.global_cumulative_temperature_sum / engine.global_processed_counter
            print(f"Global Plant Operations Mean Thermal Baseline  : {global_average_thermal:.2f} C")

        print("\n--- PER-MACHINE ASSET PROFILING SUB-MATRICES ---")
        for asset_id, matrix in engine.machine_partition_matrix.items():
            print(f"Asset System Profile: {asset_id}")
            print(f"  -> Valid Operational Samples Captured   : {matrix['records_processed']}")
            
            if matrix["records_processed"] > 0:
                mean_thermal_signature = matrix["running_temperature_sum"] / matrix["records_processed"]
                print(f"  -> Monitored Average Operating Heat     : {mean_thermal_signature:.2f} C")
            else:
                print("  -> Monitored Average Operating Heat     : 0.00 C (Offline / Idle)")

            print(f"  -> Critical Anomaly Threshold Overruns  : {matrix['critical_incident_count']}")
            print(f"  -> Skipped Records (Below 160C/Unknown) : {matrix['skipped_records_count']}")
        print("=" * 80 + "\n")


def run_comprehensive_etl_pipeline() -> None:
    print("=== [START] Industrial ETL Data Pipeline System Initialization ===")
    start_execution_timestamp = time.time()

    telemetry_extractor_node = MachineTelemetryExtractor(target_batch_size=100)
    pipeline_processing_core = MachineETLPipelineEngine()

    print("[PIPELINE_ORCHESTRATOR] Activating physical production plant log sockets...")
    raw_production_logs = telemetry_extractor_node.generate_live_stream_packet()
    print(f"[PIPELINE_ORCHESTRATOR] Successfully extracted {len(raw_production_logs)} raw records to intake array.")

    print("[PIPELINE_ORCHESTRATOR] Flushing telemetry frames to in-memory staging caches...")
    pipeline_processing_core.load_raw_ingestion_package(data_packet=raw_production_logs)

    print("[PIPELINE_ORCHESTRATOR] Deploying schema validation rules & context analysis scripts...")
    pipeline_processing_core.run_transformation_pipeline()

    print("[PIPELINE_ORCHESTRATOR] Initiating multi-sink data routing jobs...")
    DownstreamDataDispatcher.commit_to_historical_timeseries(pipeline_processing_core.cleaned_data_sink)
    DownstreamDataDispatcher.dispatch_emergency_scada_alerts(pipeline_processing_core.alert_logs_sink)
    DownstreamDataDispatcher.serialize_dead_letter_queue(pipeline_processing_core.dead_letter_queue)

    print("[PIPELINE_ORCHESTRATOR] Gathering computational running metrics for analytics compilation...")
    PipelineAnalyticsReporter.compile_and_render_report(pipeline_processing_core)

    total_compute_runtime = time.time() - start_execution_timestamp
    print("=== [END] ETL Pipeline Process Completed Successfully ===")
    print(f"Total Computation Job Clock Duration: {total_compute_runtime:.6f} seconds\n")


if __name__ == "__main__":
    run_comprehensive_etl_pipeline()
