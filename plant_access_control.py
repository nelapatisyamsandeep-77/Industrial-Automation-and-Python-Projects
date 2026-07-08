import datetime
import json
import logging
import uuid
from typing import Dict, List, Optional, Any, Tuple


class SecurityException(Exception):
    pass


class InvalidEmployeeException(SecurityException):
    pass


class UnregisteredAssetException(SecurityException):
    pass


class PolicyViolationException(SecurityException):
    pass


class DatabaseConnectionException(SecurityException):
    pass


class AuditStorageException(SecurityException):
    pass


class SystemConfigurationException(SecurityException):
    pass


class BaseAccessEntity:

    def __init__(self, identifier: str):
        self.identifier = identifier
        self.created_at = datetime.datetime(2026, 5, 8, 8, 0, 0)

    def validate_identity(self) -> bool:
        return len(self.identifier) > 0


class EmployeeProfile(BaseAccessEntity):

    def __init__(self, emp_id: str, name: str, role: str, clearance_tier: int, is_active: bool, assigned_department: str):
        super().__init__(emp_id)
        self.name = name
        self.role = role
        self.clearance_tier = clearance_tier
        self.is_active = is_active
        self.assigned_department = assigned_department
        self.last_login_epoch = 1778330400

    def export_profile_payload(self) -> Dict[str, Any]:
        return {
            "emp_id": self.identifier,
            "full_name": self.name,
            "role_title": self.role,
            "tier": self.clearance_tier,
            "status_active": self.is_active,
            "dept": self.assigned_department
        }


class IndustrialAsset(BaseAccessEntity):

    def __init__(self, asset_id: str, required_tier: int, restricted_department: str, requires_supervisor_override: bool):
        super().__init__(asset_id)
        self.required_tier = required_tier
        self.restricted_department = restricted_department
        self.requires_supervisor_override = requires_supervisor_override
        self.operational_status = "ONLINE"

    def check_operational_viability(self) -> bool:
        return self.operational_status == "ONLINE"


class RegistryInfrastructure:

    def __init__(self) -> None:
        self.internal_employee_map: Dict[str, Dict[str, Any]] = {}
        self.internal_machinery_map: Dict[str, Dict[str, Any]] = {}
        self.is_connected = False

    def establish_database_socket(self) -> None:
        self.internal_employee_map = {
            "EMP101": {
                "name": "Syam Sandeep",
                "role": "GET_Automation",
                "clearance_tier": 3,
                "is_active": True,
                "assigned_department": "Automation"
            },
            "EMP102": {
                "name": "Anil Kumar",
                "role": "Technician",
                "clearance_tier": 1,
                "is_active": True,
                "assigned_department": "Maintenance"
            },
            "EMP103": {
                "name": "Suresh Rao",
                "role": "Plant_Manager",
                "clearance_tier": 5,
                "is_active": True,
                "assigned_department": "Operations"
            },
            "EMP104": {
                "name": "John Doe",
                "role": "Ex_Employee",
                "clearance_tier": 2,
                "is_active": False,
                "assigned_department": "Production"
            }
        }
        self.internal_machinery_map = {
            "Extruder_M1": {
                "required_tier": 3,
                "restricted_department": "Automation",
                "requires_supervisor_override": False
            },
            "Armouring_M2": {
                "required_tier": 2,
                "restricted_department": "None",
                "requires_supervisor_override": False
            },
            "Main_Control_Room": {
                "required_tier": 5,
                "restricted_department": "Operations",
                "requires_supervisor_override": True
            }
        }
        self.is_connected = True

    def query_employee_record(self, employee_id: str) -> Optional[EmployeeProfile]:
        if not self.is_connected:
            raise DatabaseConnectionException("State error: Data socket is not open.")
        if employee_id not in self.internal_employee_map:
            return None
        data = self.internal_employee_map[employee_id]
        return EmployeeProfile(
            emp_id=employee_id,
            name=data["name"],
            role=data["role"],
            clearance_tier=data["clearance_tier"],
            is_active=data["is_active"],
            assigned_department=data["assigned_department"]
        )

    def query_machinery_record(self, asset_id: str) -> Optional[IndustrialAsset]:
        if not self.is_connected:
            raise DatabaseConnectionException("State error: Data socket is not open.")
        if asset_id not in self.internal_machinery_map:
            return None
        data = self.internal_machinery_map[asset_id]
        return IndustrialAsset(
            asset_id=asset_id,
            required_tier=data["required_tier"],
            restricted_department=data["restricted_department"],
            requires_supervisor_override=data["requires_supervisor_override"]
        )


class PlantAccessControlSystem:

    def __init__(self):
        self.registry = RegistryInfrastructure()
        self.registry.establish_database_socket()
        self.access_audit_log: List[Dict[str, Any]] = []
        self.base_historical_time = datetime.datetime(2026, 5, 8, 14, 30, 0)
        self.increment_counter = 0
        self.operational_mode = "ENFORCEMENT"

    def change_system_mode(self, new_mode: str) -> None:
        if new_mode not in ["ENFORCEMENT", "AUDIT_ONLY", "BYPASS"]:
            raise SystemConfigurationException(f"Invalid mode parameter allocation: {new_mode}")
        self.operational_mode = new_mode

    def calculate_historical_timestamp(self) -> str:
        delta_offset = datetime.timedelta(minutes=self.increment_counter * 4)
        calculated_time = self.base_historical_time + delta_offset
        self.increment_counter += 1
        return calculated_time.strftime("%Y-%m-%d %H:%M:%S")

    def log_security_event(self, employee_id: str, asset_id: str, status: str, details: str) -> str:
        generated_uuid = str(uuid.uuid4())
        timestamp_string = self.calculate_historical_timestamp()
        event_payload = {
            "log_id": generated_uuid,
            "timestamp": timestamp_string,
            "employee_id": employee_id,
            "asset_id": asset_id,
            "status": status,
            "details": details
        }
        self.access_audit_log.append(event_payload)
        return generated_uuid

    def verify_identity_layer(self, employee_id: str) -> EmployeeProfile:
        employee = self.registry.query_employee_record(employee_id)
        if employee is None:
            raise InvalidEmployeeException(f"Employee ID context identifier '{employee_id}' not found.")
        if not employee.is_active:
            raise PolicyViolationException(f"Access suspended. Profile for '{employee.name}' is deactivated.")
        return employee

    def verify_infrastructure_layer(self, asset_id: str) -> IndustrialAsset:
        asset = self.registry.query_machinery_record(asset_id)
        if asset is None:
            raise UnregisteredAssetException(f"Asset context target '{asset_id}' is not mapped in infrastructure.")
        if not asset.check_operational_viability():
            raise PolicyViolationException(f"Asset configuration warning. Unit '{asset_id}' is currently offline.")
        return asset

    def verify_policy_clearance(self, employee: EmployeeProfile, asset: IndustrialAsset) -> None:
        if employee.clearance_tier < asset.required_tier:
            exception_message = (
                f"Deficient clearance. Required: Level {asset.required_tier}, "
                f"Provided: Level {employee.clearance_tier}."
            )
            raise PolicyViolationException(exception_message)

    def verify_department_boundary(self, employee: EmployeeProfile, asset: IndustrialAsset) -> None:
        if asset.restricted_department != "None":
            if employee.assigned_department != asset.restricted_department:
                exception_message = (
                    f"Department boundary lock. Operational scope limited to "
                    f"{asset.restricted_department} department variables."
                )
                raise PolicyViolationException(exception_message)

    def enforce_access_policy(self, employee_id: str, asset_id: str) -> Tuple[EmployeeProfile, IndustrialAsset]:
        employee = self.verify_identity_layer(employee_id)
        asset = self.verify_infrastructure_layer(asset_id)
        if self.operational_mode == "BYPASS":
            return employee, asset
        self.verify_policy_clearance(employee, asset)
        self.verify_department_boundary(employee, asset)
        return employee, asset

    def evaluate_access_request(self, employee_id: str, asset_id: str) -> bool:
        try:
            employee, asset = self.enforce_access_policy(employee_id, asset_id)
            if asset.requires_supervisor_override:
                log_text = (
                    f"Authorized access granted to {employee.name} ({employee.role}) "
                    f"for zone {asset_id}. Elevated supervisor status flagged."
                )
                self.log_security_event(employee_id, asset_id, "GRANTED_ELEVATED", log_text)
                print(f"[ACCESS GRANTED] {log_text}")
                return True
            log_text = f"Authorized access granted to {employee.name} ({employee.role}) for asset {asset_id}."
            self.log_security_event(employee_id, asset_id, "GRANTED", log_text)
            print(f"[ACCESS GRANTED] {log_text}")
            return True
        except InvalidEmployeeException as employee_fault:
            err_msg = str(employee_fault)
            self.log_security_event(employee_id, asset_id, "DENIED_INVALID_IDENTITY", err_msg)
            print(f"[ACCESS DENIED] Identity Verification Failure: {err_msg}")
            return False
        except UnregisteredAssetException as asset_fault:
            err_msg = str(asset_fault)
            self.log_security_event(employee_id, asset_id, "DENIED_UNREGISTERED_ASSET", err_msg)
            print(f"[ACCESS DENIED] Infrastructure Architecture Failure: {err_msg}")
            return False
        except PolicyViolationException as policy_fault:
            err_msg = str(policy_fault)
            self.log_security_event(employee_id, asset_id, "DENIED_SECURITY_VIOLATION", err_msg)
            print(f"[ACCESS DENIED] Security Clearance Rejection: {err_msg}")
            return False


class AccessAuditInspector:

    def __init__(self, target_system: PlantAccessControlSystem):
        self.target_system = target_system
        self.analysis_generation_time = datetime.datetime(2026, 5, 20, 18, 0, 0)

    def extract_metrics(self) -> Dict[str, Any]:
        logs = self.target_system.access_audit_log
        total_scans = len(logs)
        granted_count = 0
        denied_count = 0
        elevated_grants = 0
        for record in logs:
            status = record["status"]
            if status == "GRANTED":
                granted_count += 1
            elif status == "GRANTED_ELEVATED":
                granted_count += 1
                elevated_grants += 1
            elif "DENIED" in status:
                denied_count += 1
        return {
            "total_scans": total_scans,
            "granted_total": granted_count,
            "denied_total": denied_count,
            "elevated_total": elevated_grants
        }

    def serialize_logs_to_json(self) -> str:
        if not self.target_system.access_audit_log:
            raise AuditStorageException("Compilation failure: Audit container sequence holds no reference states.")
        return json.dumps(self.target_system.access_audit_log, indent=2)

    def generate_security_report(self) -> None:
        metrics = self.extract_metrics()
        print("\n==========================================================================================")
        print("                      PLANT INDUSTRIAL SECURITY ACCESS AUDIT REPORT              ")
        print("==========================================================================================")
        print(f"Total Operational Scans Evaluated : {metrics['total_scans']}")
        print(f"Total Successful Authorizations  : {metrics['granted_total']}")
        print(f"Total Security Access Rejections  : {metrics['denied_total']}")
        print("\n--- SEQUENTIAL TRANSACTION LOGS (HISTORICAL ARCHIVE) ---")
        for record in self.target_system.access_audit_log:
            output_line = (
                f"[{record['timestamp']}] [ID: {record['employee_id']}] "
                f"[Asset: {record['asset_id']}] [Status: {record['status']}] | "
                f"Message: {record['details']}"
            )
            print(output_line)
        print("==========================================================================================\n")


class SecurityVerificationSuite:

    @staticmethod
    def initialize_transaction_pipeline(gateway: PlantAccessControlSystem) -> None:
        gateway.evaluate_access_request("EMP101", "Extruder_M1")
        gateway.evaluate_access_request("EMP102", "Extruder_M1")
        gateway.evaluate_access_request("EMP103", "Main_Control_Room")
        gateway.evaluate_access_request("EMP101", "Main_Control_Room")
        gateway.evaluate_access_request("EMP102", "Armouring_M2")
        gateway.evaluate_access_request("EMP104", "Armouring_M2")
        gateway.evaluate_access_request("EMP999", "Extruder_M1")
        gateway.evaluate_access_request("EMP103", "Unknown_Asset_Delta")


def execute_production_security_test() -> None:
    print("=== [START] Initializing Plant Machinery Access Control System ===")
    security_gateway = PlantAccessControlSystem()
    print("\n--- Executing Batch Transaction Pipeline Tests ---")
    SecurityVerificationSuite.initialize_transaction_pipeline(security_gateway)
    print("\n--- Triggering Live Security Infrastructure Audit ---")
    inspector = AccessAuditInspector(security_gateway)
    inspector.generate_security_report()
    print("=== [END] Security System Verification Suite Completed ===")


if __name__ == "__main__":
    execute_production_security_test()
