import socket  # noqa: F401
import struct
import asyncio
import os
import struct

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
                batch_length = struct.unpack(">i", batch_length)[0]

                record_batch_data = f.read(batch_length)


                metadata = parse_record_batch(record_batch_data, topic_name)
                if metadata:
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
        if record and record["topic_name"] == topic_name:
            return {
                "uuid": record["topic_uuid"],
                "partitions": [{"partition_index": 0, "error_code": 0}]
            }

        # Skip to the next record
        offset += record["record_length"]

    return None


def parse_record(data):
    """
    Parses an individual Kafka Record.
    """
    offset = 0

    # Partition Leader Epoch. It is a monotonically increasing number that is incremented by 1 whenever the partition leader changes.
    partition_leader_epoch = struct.unpack(">i", data[offset:offset + 4])[0]
    offset += 4

    # Magic Byte. This value is used to evolve the record batch format in a backward-compatible way.
    magic_byte = struct.unpack(">b", data[offset])[0]
    offset += 1

    # Attributes.  2-byte big-endian integer indicating the attributes of the record batch.
    attributes = struct.unpack(">h", data[offset:offset+2])[0]
    offset += 2

    # Last Offset Delta (varint, assume 4 bytes for simplicity)
    last_offset_delta = struct.unpack(">i", data[offset:offset + 4])[0]
    offset += 4

    # Base Timestamp
    base_timestamp = struct.unpack(">q", data[offset:offset + 8])[0]
    offset += 8

    # Max Timestamp
    base_timestamp = struct.unpack(">q", data[offset:offset + 8])[0]
    offset += 8

    # producer_id
    producer_id = struct.unpack(">q", data[offset:offset + 8])[0]
    offset += 8

    # producer_epoch
    producer_epoch = struct.unpack(">h", data[offset:offset + 2])[0]
    offset += 2

    # base_sequence
    producer_epoch = struct.unpack(">i", data[offset:offset + 4])[0]
    offset += 4

    # records_length
    producer_epoch = struct.unpack(">i", data[offset:offset + 4])[0]
    offset += 4

    topic_name = value  # Assuming the value contains the topic name
    topic_uuid = "12345678-90ab-cdef-1234-567890abcdef"  # Placeholder for UUID

    return {
        "record_length": record_length,
        "topic_name": topic_name,
        "topic_uuid": topic_uuid
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

        body = struct.pack(">i", 0)  # throttle_time_ms
        body += struct.pack(">h", error_code)

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

            if api_key == 75:
                parsed_request = parse_describe_topic_request(body_data)
                topic_name = parsed_request["topic_name"]
            
                metadata_log_path = "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log"
                metadata = parse_metadata_log(metadata_log_path, topic_name)

                body = create_describe_topic_response(topic_name, metadata)
            
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
