"""
This configuration file is used to define the expected structure of the data packets

Contents are defined in order of appearance and have a number of bytes associated with them
"""

from collections import OrderedDict

packet_structure = OrderedDict()

packet_structure["rats_gds_protocol_version"] = 1
packet_structure["payload_size"] = 1
packet_structure["packet_count"] = 2
packet_structure["time"] = 6
packet_structure["rats_sample_rate"] = 2
packet_structure["llc_trigger_count"] = 4
packet_structure["function_number"] = 2
packet_structure["sample_number"] = 2
packet_structure["barcode_hash"] = 4
packet_structure["retention_time"] = 4
packet_structure["reserved"] = 2
packet_structure["rats_capture_enable"] = 2
packet_structure["data"] = 0 # LEAVE ME ALONE - SHOULD BE 0

# These will either get hard coded into the parser later or will be updated from some kind of datasheet read. Steve and
# I don't seem to be aligned on the requirements for the protocol designation bit
rats_input = {}
rats_input['edb'] = 1
rats_input['protocol'] = 0
rats_input['llc_input'] = 0
rats_input['llc_bit'] = 10
rats_input['bufiss_bit'] = 12


