import socket  # noqa: F401
import struct
import asyncio
import os
import uuid

def parse_metadata_log(log_path, topic_name):
    """
    Reads the __cluster_metadata log file to find metadata for a specific topic.
    """
    try:
        with open(log_path, "rb") as f:
            while True:
                base_offset_data = f.read(8)
                
                base_offset = struct.unpack(">q", base_offset_data)[0]
                
                batch_length_data = f.read(4)
                
                batch_length = struct.unpack(">i", batch_length_data)[0]

                record_batch_data = f.read(batch_length)


                metadata = parse_record_batch(record_batch_data, topic_name)
                if metadata:
                    print(f"Found metadata for topic: {metadata}")
                    return metadata

    except FileNotFoundError:
        print(f"Metadata log file {log_path} not found.")
        return None
    except Exception as e:
        print(f"Error parsing metadata log: {e}")
        return None
    
def parse_record_batch(data, topic_name):
    """
    Parses a single Record Batch and extracts topic metadata.
    """
    offset = 0

    # Partition Leader Epoch (4 bytes)
    partition_leader_epoch = struct.unpack(">i", data[offset:offset + 4])[0]
    offset += 4

    # Magic Byte (1 byte)
    magic_byte = struct.unpack(">B", data[offset:offset + 1])[0]
    offset += 1

    # CRC (4 bytes)
    crc = struct.unpack(">i", data[offset:offset + 4])[0]
    offset += 4

    # Attributes (2 bytes)
    attributes = struct.unpack(">h", data[offset:offset + 2])[0]
    offset += 2

    # Last Offset Delta (4 bytes)
    last_offset_delta = struct.unpack(">i", data[offset:offset + 4])[0]
    offset += 4

    # Base Timestamp (8 bytes)
    base_timestamp = struct.unpack(">q", data[offset:offset + 8])[0]
    offset += 8

    # Max Timestamp (8 bytes)
    max_timestamp = struct.unpack(">q", data[offset:offset + 8])[0]
    offset += 8

    # Producer ID (8 bytes)
    producer_id = struct.unpack(">q", data[offset:offset + 8])[0]
    offset += 8

    # Producer Epoch (2 bytes)
    producer_epoch = struct.unpack(">h", data[offset:offset + 2])[0]
    offset += 2

    # Base Sequence (4 bytes)
    base_sequence = struct.unpack(">i", data[offset:offset + 4])[0]
    offset += 4

    # Records Length (4 bytes)
    records_length = struct.unpack(">i", data[offset:offset + 4])[0]
    offset += 4

    # Parse Records
    while offset < len(data):
        # Parse individual records for topic_name
        record = parse_record(data[offset:])
        if record and record["key"] == topic_name:
            return {
                "uuid": record["value"]["topic_uuid"],
                "partitions": [{
                    "partition_index": 0,
                    "error_code": 0,
                    "leader": 1,
                    "leader_epoch": 0,
                    "replicas": [1],
                    "isr": [1],
                    "eligible_leader_replicas": [],
                    "last_known_elr": [],
                    "offline_replicas": []
                }]
            }

        # Skip to the next record
        offset += record["record_length"]

    return None

def parse_varint(data):
    """
    Parses a variable-length integer (Varint) from the given binary data.
    
    Args:
        data (bytes): The binary data to parse.

    Returns:
        tuple: (parsed integer, number of bytes consumed)
    """
    result = 0
    shift = 0

    for i, byte in enumerate(data):
        # 하위 7비트를 결과에 추가
        result |= (byte & 0x7F) << shift

        # MSB가 0이면 마지막 바이트
        if (byte & 0x80) == 0:
            return result, i + 1  # 파싱된 값과 사용한 바이트 수 반환

        # 다음 바이트 처리를 위해 7비트씩 이동
        shift += 7

    raise ValueError("Varint parsing error: incomplete data")    


def parse_record(data):
    """
    Parses an individual Kafka Record.
    """
    offset = 0

    # Record Length (varint)
    record_length, bytes_consumed = parse_varint(data[offset:])
    offset += bytes_consumed

    # Attributes (1 byte)
    attributes = data[offset]
    offset += 1

    # Timestamp Delta (Varint)
    timestamp_delta, bytes_consumed = parse_varint(data[offset:])
    offset += bytes_consumed

    # Offset Delta (Varint)
    offset_delta, bytes_consumed = parse_varint(data[offset:])
    offset += bytes_consumed

    # Key Length (Varint)
    key_length, bytes_consumed = parse_varint(data[offset:])
    offset += bytes_consumed

    # Key (nullable, length = key_length bytes)
    key = None
    if key_length > 0:
        key = data[offset:offset + key_length].decode("utf-8")
        offset += key_length

    # Value Length (Varint)
    value_length, bytes_consumed = parse_varint(data[offset:])
    offset += bytes_consumed

    # Value (nullable, length = value_length bytes)
    value = None
    if value_length > 0:
        value = parse_value(data[offset:offset + value_length], offset)
        offset = value["next_offset"]  # Update offset after parsing

    # Headers Array Count (Varint)
    header_count, bytes_consumed = parse_varint(data[offset:])
    offset += bytes_consumed

    # Skip Headers (if any)
    for _ in range(header_count):
        # Header Key Length (Varint)
        header_key_length, bytes_consumed = parse_varint(data[offset:])
        offset += bytes_consumed

        # Header Key
        offset += header_key_length

        # Header Value Length (Varint)
        header_value_length, bytes_consumed = parse_varint(data[offset:])
        offset += bytes_consumed

        # Header Value
        offset += header_value_length

    return {
        "record_length": record_length,
        "attributes": attributes,
        "timestamp_delta": timestamp_delta,
        "offset_delta": offset_delta,
        "key": key,
        "value": value,
        "next_offset": offset  # For parsing the next record
    }

def parse_value(data, offset=0):
    """
    Parses the Value (Topic Record) from the Kafka Record.

    Args:
        data (bytes): The binary data containing the value.
        offset (int): The starting offset of the value in the data.

    Returns:
        dict: Parsed value with details.
    """
    # Frame Version (1 byte)
    frame_version = data[offset]
    offset += 1

    # Type (1 byte)
    record_type = data[offset]
    offset += 1

    # Version (1 byte)
    record_version = data[offset]
    offset += 1

    # Partition ID(4 byte)
    partition_id = struct.unpack(">i", data[offset:offset + 4])[0] # int32
    offset += 4

    # Topic UUID (16 bytes)
    topic_uuid_raw = data[offset:offset + 16]
    topic_uuid = str(uuid.UUID(bytes=topic_uuid_raw))  # UUID 객체로 변환 후 문자열로 반환
    offset += 16

    # Length of replica array (varint)
    replica_length, bytes_consumed = parse_varint(data[offset:])
    offset += bytes_consumed

    # Replica array(4 byte)
    replica_array = [] 
    for _ in range(replica_length - 1):
        replica_id = struct.unpack(">i", data[offset:offset + 4])[0]  # int32
        replica_array.append(replica_id)
        offset += 4

    # Length of In Sync Replica Array (Varint)
    isr_length, bytes_consumed = parse_varint(data[offset:])
    offset += bytes_consumed

    # In Sync Replica Array (4 bytes per replica)
    isr_array = []
    for _ in range(isr_length - 1):
        isr_id = struct.unpack(">i", data[offset:offset + 4])[0]  # int32
        isr_array.append(isr_id)
        offset += 4

    # In Sync replica array (4 bytes)
    isr_array = struct.unpack(">i", data[offset:offset + 4])[0] # int32
    offset += 4

    # Length of Removing Replicas Array (Varint)
    removing_length, bytes_consumed = parse_varint(data[offset:])
    offset += bytes_consumed

    # Removing Replicas Array (4 bytes per replica)
    removing_array = []
    for _ in range(removing_length - 1):
        removing_id = struct.unpack(">i", data[offset:offset + 4])[0]  # int32
        removing_array.append(removing_id)
        offset += 4

    # Length of Adding Replicas Array (Varint)
    adding_length, bytes_consumed = parse_varint(data[offset:])
    offset += bytes_consumed

    # Adding Replicas Array (4 bytes per replica)
    adding_array = []
    for _ in range(adding_length - 1):
        adding_id = struct.unpack(">i", data[offset:offset + 4])[0]  # int32
        adding_array.append(adding_id)
        offset += 4

    # Leader (4 bytes)
    leader = struct.unpack(">i", data[offset:offset + 4])[0]  # int32
    offset += 4

    # Leader Epoch (4 bytes)
    leader_epoch = struct.unpack(">i", data[offset:offset + 4])[0]  # int32
    offset += 4

    # Partition Epoch (4 bytes)
    partition_epoch = struct.unpack(">i", data[offset:offset + 4])[0]  # int32
    offset += 4

    # Length of Directories Array (Varint)
    directories_length, bytes_consumed = parse_varint(data[offset:])
    offset += bytes_consumed

    # Directories Array (Variable length strings)
    directories_array = []
    for _ in range(directories_length - 1):
        directory_uuid_raw = data[offset:offset + 16]
        directory_uuid = str(uuid.UUID(bytes=directory_uuid_raw))  # Raw bytes -> UUID 형식으로 변환
        directories_array.append(directory_uuid)
        offset += 16

    # Tagged Fields Count (Varint)
    tagged_fields_count, bytes_consumed = parse_varint(data[offset:])
    offset += bytes_consumed

    # Return parsed value as a dictionary
    return {
        "frame_version": frame_version,
        "record_type": record_type,
        "record_version": record_version,
        "partition_id": partition_id,
        "topic_uuid": topic_uuid,
        "replica_array": replica_array,
        "isr_array": isr_array,
        "removing_array": removing_array,
        "adding_array": adding_array,
        "leader": leader,
        "leader_epoch": leader_epoch,
        "partition_epoch": partition_epoch,
        "directories_array": directories_array,
        "tagged_fields_count": tagged_fields_count,
        "next_offset": offset  # Offset for further parsing
    }

def parse_header_request(header_request):
    """
    Parses the Kafka request header.
    """
    api_key = struct.unpack(">h", header_request[:2])[0] # int16
    api_version = struct.unpack(">h", header_request[2:4])[0] # int16
    correlation_id = struct.unpack(">i", header_request[4:8])[0] # int32

    return {
        "api_key": api_key, 
        "api_version": api_version, 
        "correlation_id": correlation_id
    }

def parse_describe_topic_request(body_request):
    """
    Parses the DescribeTopicPartitions request body.
    """
    
    # 리퀘스트 헤더
    length = struct.unpack(">h", body_request[:2])[0]
    client_id = body_request[2:2+length].decode("utf-8")
    offset = 2 + length

    # 토픽 배열 크기 읽기 
    # buffer = body_request[offset:offset+1]
    array_length = struct.unpack(">B", body_request[offset+1:offset+2])[0] - 1


    topic_name_length = struct.unpack(">B", body_request[offset+2:offset+3])[0] - 1
    offset += 3
    topic_name = body_request[offset:offset + topic_name_length].decode("utf-8")
    offset = offset + topic_name_length
    
    # 파티션 리밋, 그리고 커서 
    
    # buffer = request[offset:offset+1]
    partition_limit = body_request[offset+1:offset+5]
    cursor = struct.unpack(">B", body_request[offset+5:offset+6])[0]

    return {
        "array_length": array_length,
        "topic_name_length": topic_name_length,
        "topic_name": topic_name,
        "partition_limit": partition_limit,
        "cursor": cursor
    }

def create_describe_topic_response(parsed_request, metadata):
    # Extract data from parsed_request
    array_length = parsed_request["array_length"]
    topic_name_length = parsed_request["topic_name_length"]
    topic_name = parsed_request["topic_name"]
    partition_limit = parsed_request["partition_limit"]
    cursor = parsed_request["cursor"]    
    
    if metadata is None:
        # UNKNOWN_TOPIC_OR_PARTITION error
        throttle_time_ms = 0   
        error_code = 3
        uuid = "00000000-0000-0000-0000-000000000000"
        is_internal = 0
        partitions_array = 0
        topic_authorized_operations = int("00000df8", 16)

        body = struct.pack(">B", 0)
        body += struct.pack(">i", throttle_time_ms)
        body += struct.pack(">B", array_length + 1)
        body += struct.pack(">h", error_code)

        body += struct.pack(">B", topic_name_length + 1) # topicname
        body += topic_name.encode("utf-8") # contents
        body += bytes.fromhex(uuid.replace("-", "")) # topic id
        body += struct.pack(">B", is_internal)
        body += struct.pack(">B", partitions_array + 1)
        body += struct.pack(">i", topic_authorized_operations)
        body += struct.pack(">B", 0)
        body += struct.pack(">B", cursor)
        body += struct.pack(">B", 0)
    else:
        throttle_time_ms = 0
        error_code = 0
        is_internal = 0

        topic_uuid = metadata["uuid"]
        partitions = metadata["partitions"]
        partitions_count = len(partitions)

        topic_authorized_operations = 0 

        body = struct.pack(">B", 0)
        body += struct.pack(">i", throttle_time_ms)
        body += struct.pack(">B", array_length + 1)
        body += struct.pack(">h", error_code)

        body += struct.pack(">B", topic_name_length + 1) # topicname
        body += topic_name.encode("utf-8") # contents
        body += bytes.fromhex(uuid.replace("-", "")) # topic id
        body += struct.pack(">B", is_internal)
        body += struct.pack(">B", partitions_array + 1)
        body += struct.pack(">i", topic_authorized_operations)
        body += struct.pack(">B", 0)
        body += struct.pack(">B", cursor)
        body += struct.pack(">B", 0)


    return body

def create_api_version_response(api_version):
    error_code = 0 if api_version in [0, 1, 2, 3, 4] else 35
    
    api_keys = {18:[0, 4],
                75:[0, 0]}
    
    throttle_time_ms = 0
    tag_buffer = b"\x00"
    body = struct.pack(">h", error_code)  # error_code: 2 bytes
    number_api_key = len(api_keys) + 1
    print(number_api_key)
    body += struct.pack(">B", number_api_key) #api_version count
    
    for key, (min_version, max_version) in api_keys.items():
        body += struct.pack(">hhh", key, min_version, max_version)
        body += struct.pack(">B", 0)
    body += struct.pack(">i", throttle_time_ms)
    body += struct.pack(">B", 0)

    return body

def build_response_header(correlation_id, body):
    response_message_size = len(body) + 4  # 4 bytes for correlation_id
    header = struct.pack(">i", response_message_size)  # Response size (4 bytes)
    header += struct.pack(">i", correlation_id)        # Correlation ID (4 bytes)
    return header + body

async def handle_client(reader, writer):
    try:
        while True:
            message_size_data = await reader.readexactly(4)
            message_size = struct.unpack(">i", message_size_data)[0]
            request = await reader.readexactly(message_size)

            header_data = request[:8]  # 공통 헤더 부분
            body_data = request[8:]    # 나머지 본문 데이터

            common_data = parse_header_request(header_data)
            
            api_key = common_data["api_key"]
            api_version = common_data["api_version"]
            correlation_id = common_data["correlation_id"]
            print(api_key)

            if api_key == 75:
                parsed_request = parse_describe_topic_request(body_data)
                topic_name = parsed_request["topic_name"]
            
                metadata_log_path = "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log"
                metadata = parse_metadata_log(metadata_log_path, topic_name)

                body = create_describe_topic_response(parsed_request, metadata)
            
            elif api_key == 18:
                body = create_api_version_response(api_version)
            
            else:
                print(f"Unsupported API key: {api_key}")
                continue

            response = build_response_header(correlation_id, body)

            writer.write(response)
            await writer.drain()
        
    except asyncio.IncompleteReadError:
        print(f"Connection closed unexpectedly")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        writer.close()
        await writer.wait_closed()


async def main():
    # You can use print statements as follows for debugging,
    # they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage
    #
    
    
    server = await asyncio.start_server(handle_client, "localhost", 9092)
    # server = socket.create_server(("localhost", 9092), reuse_port=True 
    async with server:
        print("Kafka server running on port 9092.")
        await server.serve_forever()

    # while True:
    #     conn, addr = server.accept() # wait for client

    #     client_thread = threading.Thread(target=handle_client, args=(conn, addr))
    #     client_thread.start()



if __name__ == "__main__":
    # main()
    asyncio.run(main())
