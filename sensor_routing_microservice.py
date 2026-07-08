import datetime
import json
import math
import uuid
from typing import Dict, List, Optional, Any, Tuple


class RouterException(Exception):
    pass


class InvalidPacketException(RouterException):
    pass


class UnknownRouteException(RouterException):
    pass


class DataCorruptionException(RouterException):
    pass


class DeviceAuthException(RouterException):
    pass


class StorageBufferFullException(RouterException):
    pass


class SystemMutedException(RouterException):
    pass


class MetricPipelineException(RouterException):
    pass


class PacketMetadata:

    def __init__(self, packet_id: str, section: str, priority: int = 1, origin: str = "DEFAULT_GATEWAY"):
        self.packet_id = packet_id
        self.section = section
        self.priority = priority
        self.origin_node = origin
        self.arrival_time = datetime.datetime(2026, 7, 8, 17, 56, 0)
        self.signature = str(uuid.uuid5(uuid.NAMESPACE_DNS, packet_id + section))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "packet_id": self.packet_id,
            "section": self.section,
            "priority": self.priority,
            "origin_node": self.origin_node,
            "arrival_time": self.arrival_time.strftime("%Y-%m-%d %H:%M:%S"),
            "cryptographic_signature": self.signature
        }


class DataTransformationEngine:

    @staticmethod
    def scale_voltage_reading(raw_reading: float) -> float:
        return round(raw_reading * 1.045, 3)

    @staticmethod
    def calculate_thermal_variance(current_temp: float) -> float:
        baseline = 180.0
        return round(abs(current_temp - baseline), 2)

    @staticmethod
    def normalize_metrics(payload: Dict[str, Any]) -> Dict[str, Any]:
        processed_payload = {}
        for key, value in payload.items():
            if key == "core_temperature_celsius" and isinstance(value, (int, float)):
                processed_payload[key] = value
                processed_payload["variance_from_baseline"] = DataTransformationEngine.calculate_thermal_variance(value)
            elif key == "raw_voltage_draw_amperes" and isinstance(value, (int, float)):
                processed_payload["scaled_voltage_amperes"] = DataTransformationEngine.scale_voltage_reading(value)
            else:
                processed_payload[key] = value
        return processed_payload


class DeviceRegistryValidator:

    def __init__(self):
        self.whitelisted_devices = {"DEV_01", "DEV_02", "DEV_03", "DEV_04", "DEV_05"}

    def authenticate_source(self, device_id: Optional[str]) -> bool:
        if not device_id:
            return False
        return device_id in self.whitelisted_devices


class RouterStorageMetrics:

    def __init__(self):
        self.total_processed = 0
        self.total_routed = 0
        self.total_dropped = 0
        self.total_warned = 0
        self.critical_priority_count = 0
        self.standard_priority_count = 0

    def increment_processed(self) -> None:
        self.total_processed += 1

    def increment_routed(self) -> None:
        self.total_routed += 1

    def increment_dropped(self) -> None:
        self.total_dropped += 1

    def increment_warned(self) -> None:
        self.total_warned += 1

    def evaluate_priority_metrics(self, level: int) -> None:
        if level > 2:
            self.critical_priority_count += 1
        else:
            self.standard_priority_count += 1

    def fetch_summary(self) -> Dict[str, int]:
        return {
            "processed": self.total_processed,
            "routed": self.total_routed,
            "dropped": self.total_dropped,
            "warned": self.total_warned,
            "critical_priority": self.critical_priority_count,
            "standard_priority": self.standard_priority_count
        }


class AdvancedSensorRouter:

    def __init__(self, capacity_limit: int = 100):
        self.routing_table: Dict[str, List[Dict[str, Any]]] = {
            "EXTRUSION_LINE": [],
            "ARMOURING_LINE": [],
            "QUALITY_QC": []
        }
        self.metrics = RouterStorageMetrics()
        self.validator = DeviceRegistryValidator()
        self.required_fields = ["packet_id", "section", "sensor_reading"]
        self.execution_id = str(uuid.uuid4())
        self.max_buffer_capacity = capacity_limit

    def verify_packet_structure(self, packet: Dict[str, Any]) -> None:
        if not packet or not isinstance(packet, dict):
            raise InvalidPacketException("Packet raw data structure is completely null or invalid")
        missing_fields = [field for field in self.required_fields if field not in packet]
        if missing_fields:
            fields_str = ", ".join(missing_fields)
            raise InvalidPacketException(f"Packet integrity compromised. Missing required attributes: {fields_str}")

    def evaluate_target_route(self, section: str) -> bool:
        return section in self.routing_table

    def check_buffer_saturation(self, section: str) -> bool:
        if len(self.routing_table[section]) >= self.max_buffer_capacity:
            return True
        return False

    def process_single_packet(self, packet: Dict[str, Any]) -> bool:
        self.metrics.increment_processed()
        try:
            self.verify_packet_structure(packet)
            device_id = packet.get("device_id", "DEV_01")
            if not self.validator.authenticate_source(device_id):
                raise DeviceAuthException(f"Unauthorized source hardware device detected: {device_id}")
            packet_id = packet["packet_id"]
            section = packet["section"]
            reading = packet["sensor_reading"]
            if not isinstance(reading, dict):
                raise DataCorruptionException(f"Payload context type mismatch on packet sequence {packet_id}")
            if not self.evaluate_target_route(section):
                self.metrics.increment_warned()
                print(f"[WARN] Destination mismatch. No registered channel link for area: {section}")
                return False
            if self.check_buffer_saturation(section):
                raise StorageBufferFullException(f"Target register allocations saturated for sector allocation: {section}")
            priority_value = packet.get("priority", 1)
            self.metrics.evaluate_priority_metrics(priority_value)
            metadata = PacketMetadata(packet_id, section, priority_value, device_id)
            transformed_payload = DataTransformationEngine.normalize_metrics(reading)
            optimized_payload = {
                "system_backbone_id": self.execution_id,
                "metadata": metadata.to_dict(),
                "payload": transformed_payload
            }
            self.routing_table[section].append(optimized_payload)
            self.metrics.increment_routed()
            print(f"[ROUTE] Packet {packet_id} successfully synchronized and committed to {section}")
            return True
        except InvalidPacketException as packet_error:
            self.metrics.increment_dropped()
            p_id = packet.get("packet_id", "UNKNOWN_ID") if isinstance(packet, dict) else "INVALID_STRUCTURE"
            print(f"[DROP] Packet validation failure on identifier {p_id}. Error stack: {str(packet_error)}")
            return False
        except DeviceAuthException as auth_error:
            self.metrics.increment_dropped()
            print(f"[DROP] Security infrastructure mitigation triggered. Trace info: {str(auth_error)}")
            return False
        except DataCorruptionException as corruption_error:
            self.metrics.increment_dropped()
            print(f"[DROP] Operational compliance error during normalization: {str(corruption_error)}")
            return False
        except StorageBufferFullException as buffer_error:
            self.metrics.increment_dropped()
            print(f"[DROP] Infrastructure capacity ceiling reached: {str(buffer_error)}")
            return False


class PerformanceEvaluationEngine:

    def __init__(self, monitored_router: AdvancedSensorRouter):
        self.monitored_router = monitored_router

    def run_efficiency_pass(self) -> float:
        summary = self.monitored_router.metrics.fetch_summary()
        inbound = summary["processed"]
        if inbound == 0:
            return 0.0
        routed_successfully = summary["routed"]
        efficiency_percentage = (routed_successfully / inbound) * 100.0
        return round(efficiency_percentage, 2)

    def verify_alert_thresholds(self) -> List[str]:
        triggered_alerts = []
        summary = self.monitored_router.metrics.fetch_summary()
        if summary["dropped"] > 5:
            triggered_alerts.append("CRITICAL_DROP_THRESHOLD_EXCEEDED")
        if summary["warned"] > 3:
            triggered_alerts.append("HIGH_UNMAPPED_ROUTE_COUNT_DETECTED")
        return triggered_alerts


class EngineeringAuditInspector:

    def __init__(self, router_instance: AdvancedSensorRouter):
        self.router = router_instance
        self.evaluator = PerformanceEvaluationEngine(router_instance)

    def render_system_report(self) -> None:
        summary = self.router.metrics.fetch_summary()
        operational_efficiency = self.evaluator.run_efficiency_pass()
        active_alerts = self.evaluator.verify_alert_thresholds()
        print("\n==========================================================================================")
        print("                      INDUSTRIAL SENSOR ROUTING ARCHITECTURE REPORT               ")
        print("==========================================================================================")
        print(f"Router Microservice Identifier   : {self.router.execution_id}")
        print(f"Total Inbound Sensor Packets     : {summary['processed']}")
        print(f"Successfully Transmitted Streams : {summary['routed']}")
        print(f"Dropped Corrupted Allocations    : {summary['dropped']}")
        print(f"Unrouted Destination Warnings    : {summary['warned']}")
        print(f"High Priority Stream Allocations : {summary['critical_priority']}")
        print(f"Standard Operational Packets     : {summary['standard_priority']}")
        print(f"Calculated Data Throughput Yield : {operational_efficiency}%")
        print(f"Active Infrastructure Fault Tags : {', '.join(active_alerts) if active_alerts else 'NONE'}")
        print("\n--- DETAILED SEGMENT MEMORY MATRIX ---")
        for section_name, targeted_packets in self.router.routing_table.items():
            print(f"Segment Area: {section_name} | Active Buffer Count: {len(targeted_packets)}")
            for stored_item in targeted_packets:
                meta = stored_item["metadata"]
                print(f"  -> [ID: {meta['packet_id']}] [Priority: {meta['priority']}] [Device: {meta['origin_node']}]")
                print(f"     [Hash: {meta['cryptographic_signature']}]")
                print(f"     [Data Matrix: {json.dumps(stored_item['payload'])}]")
        print("==========================================================================================\n")


class IndustrialSimulationHarness:

    @staticmethod
    def generate_packet_stream() -> List[Dict[str, Any]]:
        return [
            {
                "packet_id": "P01",
                "section": "EXTRUSION_LINE",
                "priority": 3,
                "device_id": "DEV_01",
                "sensor_reading": {"line_velocity_meters_per_min": 45.5, "core_temperature_celsius": 182.0}
            },
            {
                "packet_id": "P02",
                "section": "QUALITY_QC",
                "priority": 1,
                "device_id": "DEV_02",
                "sensor_reading": {"laser_thickness_tolerance_mm": 2.41, "defects_detected": 0}
            },
            {
                "packet_id": "P03",
                "section": "UNKNOWN_ZONE",
                "priority": 2,
                "device_id": "DEV_03",
                "sensor_reading": {"raw_voltage_draw_amperes": 14.2}
            },
            {
                "packet_id": "P04",
                "section": "ARMOURING_LINE",
                "priority": 1,
                "device_id": "DEV_04"
            },
            {
                "packet_id": "P05",
                "section": "ARMOURING_LINE",
                "priority": 4,
                "device_id": "DEV_01",
                "sensor_reading": {"tension_coefficient_newtons": 340.0, "rpm_counter": 1200}
            },
            {
                "packet_id": "P06",
                "section": "EXTRUSION_LINE",
                "priority": 2,
                "device_id": "DEV_99",
                "sensor_reading": {"line_velocity_meters_per_min": 12.1}
            },
            {
                "packet_id": "P07",
                "section": "QUALITY_QC",
                "priority": 1,
                "device_id": "DEV_02",
                "sensor_reading": "INVALID_NESTED_DATATYPE"
            },
            {
                "packet_id": "P08",
                "section": "QUALITY_QC",
                "priority": 2,
                "device_id": "DEV_03",
                "sensor_reading": {"laser_thickness_tolerance_mm": 2.39, "defects_detected": 1}
            }
        ]

    @staticmethod
    def launch_subsystem() -> None:
        print("=== Industrial Sensor Microservice Routing Started ===")
        router_engine = AdvancedSensorRouter(capacity_limit=15)
        packets = IndustrialSimulationHarness.generate_packet_stream()
        for telemetry_packet in packets:
            router_engine.process_single_packet(telemetry_packet)
        inspector = EngineeringAuditInspector(router_engine)
        inspector.render_system_report()
        print("=== Industrial Sensor Microservice Routing Finished ===")


if __name__ == "__main__":
    IndustrialSimulationHarness.launch_subsystem()
