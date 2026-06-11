class PlantAccessControl:
    def __init__(self):
        self.employee_database = {
            "EMP101": {"name": "Syam Sandeep", "role": "GET_Automation", "access_level": 3},
            "EMP102": {"name": "Anil Kumar", "role": "Technician", "access_level": 1},
            "EMP103": {"name": "Suresh Rao", "role": "Plant_Manager", "access_level": 5}
        }
        
        self.machinery_clearance = {
            "Extruder_M1": 3,
            "Armouring_M2": 2,
            "Main_Control_Room": 5
        }

    def verify_machine_access(self, emp_id, machine_name):
        if emp_id not in self.employee_database:
            print(f"🛑 [DENIED] Access Revoked. Employee ID {emp_id} not found in database.")
            return False
            
        employee = self.employee_database[emp_id]
        required_level = self.machinery_clearance.get(machine_name, 99)
        
        if employee["access_level"] >= required_level:
            print(f"🔒 [GRANTED] {employee['name']} ({employee['role']}) is authorized to operate {machine_name}.")
            return True
        else:
            print(f"❌ [DENIED] {employee['name']} does not have sufficient clearance for {machine_name}.")
            return False

if __name__ == "__main__":
    system = PlantAccessControl()
    
    print("=== Plant Machinery Access Control Verification ===")
    system.verify_machine_access("EMP101", "Extruder_M1")
    system.verify_machine_access("EMP102", "Extruder_M1")
    system.verify_machine_access("EMP103", "Main_Control_Room")
    system.verify_machine_access("EMP999", "Armouring_M2")
