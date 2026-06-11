class SensorDataRouter:
    def __init__(self):
        self.routing_table = {
            "EXTRUSION_LINE": [],
            "ARMOURING_LINE": [],
            "QUALITY_QC": []
        }
        
    def validate_packet(self, packet):
        required_keys = ["packet_id", "section", "sensor_reading"]
        for key in required_keys:
            if key not in packet:
                return False
        return True

    def route_packet(self, packet):
        if not self.validate_packet(packet):
            print(f"❌ [DROP] Invalid data packet detected. Dropping.")
            return False
            
        section = packet["section"]
        if section in self.routing_table:
            self.routing_table[section].append(packet)
            print(f"✅ [ROUTE] Packet {packet['packet_id']} successfully sent to {section}.")
            return True
        else:
            print(f"⚠️ [WARN] No route found for section: {section}")
            return False

if __name__ == "__main__":
    router = SensorDataRouter()
    
    incoming_packets = [
        {"packet_id": "P01", "section": "EXTRUSION_LINE", "sensor_reading": {"speed": "45m/m"}},
        {"packet_id": "P02", "section": "QUALITY_QC", "sensor_reading": {"thickness_mm": 2.4}},
        {"packet_id": "P03", "section": "UNKNOWN_ZONE", "sensor_reading": {"val": 0}},
        {"packet_id": "P04", "section": "ARMOURING_LINE"}
    ]
    
    print("=== Industrial Sensor Microservice Routing Started ===")
    for packet in incoming_packets:
        router.route_packet(packet)
