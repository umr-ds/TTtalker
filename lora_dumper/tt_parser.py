import struct

with open("packets.csv") as f:
    for line in f:
        packet = {}
        input_string = line.split(",")[1]
        if input_string[0:8] == "4a4a4a4a":
            packet["receiver"] = struct.unpack("=I", bytes.fromhex(input_string[:8]))[0]
            packet["sender"] = struct.unpack("=I", bytes.fromhex(input_string[8:16]))[0]
            packet["packet_type"] = input_string[16:18]
            packet["packet_number"] = struct.unpack(
                "=B", bytes.fromhex(input_string[18:20])
            )[0]
            print(packet)
            continue
        packet["receiver"] = struct.unpack("=I", bytes.fromhex(input_string[:8]))[0]
        packet["sender"] = struct.unpack("=I", bytes.fromhex(input_string[8:16]))[0]
        packet["packet_type"] = input_string[16:18]
        if packet["packet_type"] == "49" or packet["packet_type"] == "4d":
            packet["packet_number"] = struct.unpack(
                "=B", bytes.fromhex(input_string[18:20])
            )[0]
            packet["time"] = struct.unpack("=I", bytes.fromhex(input_string[20:28]))[0]
            if packet["packet_type"] == "49":
                packet["AS7263_610"] = struct.unpack(
                    "=f", bytes.fromhex(input_string[28:36])
                )[0]
                packet["AS7263_680"] = struct.unpack(
                    "=f", bytes.fromhex(input_string[36:44])
                )[0]
                packet["AS7263_730"] = struct.unpack(
                    "=f", bytes.fromhex(input_string[44:52])
                )[0]
                packet["AS7263_760"] = struct.unpack(
                    "=f", bytes.fromhex(input_string[52:60])
                )[0]
                packet["AS7263_810"] = struct.unpack(
                    "=f", bytes.fromhex(input_string[60:68])
                )[0]
                packet["AS7263_860"] = struct.unpack(
                    "=f", bytes.fromhex(input_string[68:76])
                )[0]
                packet["AS7262_450"] = struct.unpack(
                    "=f", bytes.fromhex(input_string[76:84])
                )[0]
                packet["AS7262_500"] = struct.unpack(
                    "=f", bytes.fromhex(input_string[84:92])
                )[0]
                packet["AS7262_550"] = struct.unpack(
                    "=f", bytes.fromhex(input_string[92:100])
                )[0]
                packet["AS7262_570"] = struct.unpack(
                    "=f", bytes.fromhex(input_string[100:108])
                )[0]
                packet["AS7262_600"] = struct.unpack(
                    "=f", bytes.fromhex(input_string[108:116])
                )[0]
                packet["AS7262_650"] = struct.unpack(
                    "=f", bytes.fromhex(input_string[116:124])
                )[0]
                packet["integration_time"] = struct.unpack(
                    "=B", bytes.fromhex(input_string[124:126])
                )[0]
                packet["gain"] = struct.unpack(
                    "=B", bytes.fromhex(input_string[126:128])
                )[0]
            elif packet["packet_type"] == "4d":
                packet["temperature_reference_0"] = struct.unpack(
                    "=I", bytes.fromhex(input_string[-92:-84])
                )[0]
                packet["temperature_heat_0"] = struct.unpack(
                    "=I", bytes.fromhex(input_string[-84:-76])
                )[0]
                packet["growth_sensor"] = struct.unpack(
                    "=I", bytes.fromhex(input_string[-76:-68])
                )[0]
                packet["adc_bandgap"] = struct.unpack(
                    "=I", bytes.fromhex(input_string[-68:-60])
                )[0]
                packet["number of bits"] = struct.unpack(
                    "=B", bytes.fromhex(input_string[-60:-58])
                )[0]
                packet["air_relative_humidity"] = struct.unpack(
                    "=B", bytes.fromhex(input_string[-58:-56])
                )[0]
                packet["air_temperature"] = struct.unpack(
                    "=h", bytes.fromhex(input_string[-56:-52])
                )[0]
                packet["gravity_z_mean"] = struct.unpack(
                    "=h", bytes.fromhex(input_string[-52:-48])
                )[0]
                packet["gravity_z_derivation"] = struct.unpack(
                    "=h", bytes.fromhex(input_string[-48:-44])
                )[0]
                packet["gravity_y_mean"] = struct.unpack(
                    "=h", bytes.fromhex(input_string[-44:-40])
                )[0]
                packet["gravity_y_derivation"] = struct.unpack(
                    "=h", bytes.fromhex(input_string[-40:-36])
                )[0]
                packet["gravity_x_mean"] = struct.unpack(
                    "=h", bytes.fromhex(input_string[-36:-32])
                )[0]
                packet["gravity_x_derivation"] = struct.unpack(
                    "=h", bytes.fromhex(input_string[-32:-28])
                )[0]
                packet["temperature_reference_1"] = struct.unpack(
                    "=I", bytes.fromhex(input_string[-28:-20])
                )[0]
                packet["temperature_heat_1"] = struct.unpack(
                    "=I", bytes.fromhex(input_string[-20:-12])
                )[0]
                packet["StWC"] = struct.unpack(
                    "=H", bytes.fromhex(input_string[108:112])
                )[0]
                packet["adc_volt_bat"] = struct.unpack(
                    "=I", bytes.fromhex(input_string[112:120])
                )[0]
        else:
            packet["packet_type"] = input_string[16:18]
            if len(input_string) >= 28:
                if packet["packet_type"] == "41":
                    packet["comand"] = struct.unpack(
                        "=B", bytes.fromhex(input_string[18:20])
                    )[0]
                    packet["time"] = struct.unpack(
                        "=I", bytes.fromhex(input_string[20:28])
                    )[0]
                elif packet["packet_type"] == "4a":
                    packet["comand"] = struct.unpack(
                        "=B", bytes.fromhex(input_string[18:20])
                    )[0]
                    packet["time"] = struct.unpack(
                        "=I", bytes.fromhex(input_string[20:28])
                    )[0]
                    packet["integration_time"] = struct.unpack(
                        "=B", bytes.fromhex(input_string[28:30])
                    )[0]
                    packet["gain"] = struct.unpack(
                        "=B", bytes.fromhex(input_string[30:32])
                    )[0]
                else:
                    packet["comand"] = struct.unpack(
                        "=B", bytes.fromhex(input_string[18:20])
                    )[0]
                    packet["time"] = struct.unpack(
                        "=I", bytes.fromhex(input_string[20:28])
                    )[0]
                    packet["sleep_intervall"] = struct.unpack(
                        "=H", bytes.fromhex(input_string[28:32])
                    )[0]
                    packet["unknown"] = struct.unpack(
                        "=H", bytes.fromhex(input_string[32:36])
                    )[0]
                    packet["heating"] = struct.unpack(
                        "=H", bytes.fromhex(input_string[36:40])
                    )[0]
                    packet["unknown2"] = struct.unpack(
                        "=B", bytes.fromhex(input_string[40:42])
                    )[0]
                    packet["unknown3"] = struct.unpack(
                        "=B", bytes.fromhex(input_string[42:44])
                    )[0]

            else:
                packet["succes"] = struct.unpack(
                    "=?", bytes.fromhex(input_string[18:20])
                )[0]
        print(line.split(",")[0] + str(packet))
