import socket  # noqa: F401
import struct

def create_response(request):
    message_size = struct.unpack(">i", request[:4])[0]
    api_key = struct.unpack(">h", request[4:6])[0] # int16
    api_version = struct.unpack(">h", request[6:8])[0] # int16
    correlation_id = struct.unpack(">i", request[8:12])[0] # int32

    # body
    error_code = 0
    if api_key == 18:
        api_versions = [
        {"api_key": 18, "min_version": 0, "max_version": 4}
        ]
    throttle_time_ms = 0
    tag_buffer = 0
        
    body = struct.pack(">h", error_code)  # error_code: 2 bytes
    body += struct.pack(">B", len(api_versions)) #api_version count

    for api in api_versions:
        body += struct.pack(">hhhh", api["api_key"], api["min_version"], api["max_version"], tag_buffer)

    body += struct.pack(">i", throttle_time_ms)

    response_message_size = len(body) + 8
    header = struct.pack(">ii", response_message_size, correlation_id)
    
    response = header + body

    print(f"Response (Hex): {response.hex()}")
    return response



def main():
    # You can use print statements as follows for debugging,
    # they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage
    #
    server = socket.create_server(("localhost", 9092), reuse_port=True)
    
    
    while True:
        try:
            conn, addr = server.accept() # wait for client
            
            request = conn.recv(1024)
            
            response = create_response(request)

            conn.sendall(response)
        except Exception as e:
            print(f"{e}")
        finally:
            conn.close()




if __name__ == "__main__":
    main()
