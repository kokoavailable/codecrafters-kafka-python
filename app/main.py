import socket  # noqa: F401
import struct
import asyncio
import os
import struct

# def parse_metadata_log(log_path):
#     impo

def parse_describe_topic_request(request):

    # 리퀘스트 헤더
    length = struct.unpack(">h", request[8:10])[0]
    client_id = request[10:10+length].decode("utf-8")
    offset = 10 + length

    # 토픽 배열 크기 읽기 
    # buffer = request[offset:offset+1]
    array_length = struct.unpack(">B", request[offset+1:offset+2])[0] - 1
    topic_name_length = struct.unpack(">B", request[offset+2:offset+3])[0] - 1
    offset = offset + 3
    topic_name = request[offset:offset + topic_name_length].decode("utf-8")
    offset = offset + topic_name_length
    
    # 파티션 리밋, 그리고 커서 
    
    # buffer = request[offset:offset+1]
    partition_limit = request[offset+1:offset+5]
    cursor = struct.unpack(">B", request[offset+5:offset+6])[0]

    return array_length, topic_name_length, topic_name, partition_limit, cursor

def create_response(request):
    print(f"{request.hex()}")
    api_key = struct.unpack(">h", request[:2])[0] # int16
    api_version = struct.unpack(">h", request[2:4])[0] # int16
    correlation_id = struct.unpack(">i", request[4:8])[0] # int32


    if api_key == 75:
        array_length, topic_name_length, topic_name, partition_limit, cursor = parse_describe_topic_request(request)
        throttle_time_ms = 0   
        error_code = 0
        uuid = "00000000-0000-0000-0000-000000000000"
        is_internal = 0
        partitions_array = 0
        topic_authorized_operations = int("00000df8", 16)
        # cursor = 255 # null

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





    # body
    if api_key == 18:
        error_code = 0 if api_version in [0, 1, 2, 3, 4] else 35
        
        api_keys = {18:[0, 4],
                    75:[0, 0]}
        
        # api_key = 18
        # min_version, max_version = 0, 4
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

    response_message_size = len(body) + 4
    header = struct.pack(">i", response_message_size)
    header += struct.pack(">i", correlation_id)
    print(header, response_message_size, correlation_id)
    
    response = header + body

    print(f"Response (Hex): {response.hex()}")
    return response

# def handle_client(conn, addr):
#     print(f"Connected to {addr}")

#     try:
#         while True:
#             request = conn.recv(1024)
            
#             response = create_response(request)

#             conn.sendall(response)
#     except Exception as e:
#         print(f"{e}")
#     finally:
#         conn.close()

async def handle_client(reader, writer):
    try:
        while True:
            message_size_data = await reader.readexactly(4)
            message_size = struct.unpack(">i", message_size_data)[0]
            request = await reader.readexactly(message_size)
            
            metadata_log_path = "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log"

            response = create_response(request)
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
    # server = socket.create_server(("localhost", 9092), reuse_port=True)
    addr = server.sockets[0].getsockname()    
    async with server:
        await server.serve_forever()

    # while True:
    #     conn, addr = server.accept() # wait for client

    #     client_thread = threading.Thread(target=handle_client, args=(conn, addr))
    #     client_thread.start()



if __name__ == "__main__":
    # main()
    asyncio.run(main())
