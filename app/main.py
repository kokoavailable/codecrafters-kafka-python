import socket  # noqa: F401
import struct

def create_response(request):
    message_size = struct.unpack(">i", request[:4])[0]
    api_key = struct.unpack(">h", request[4:6])[0] # int16
    api_version = struct.unpack(">h", request[6:8])[0] # int16
    correlation_id = struct.unpack(">i", request[8:12])[0] # int32

    # body
    error_code = 0
    api_key = 18
    min_version, max_version = 0, 4
    throttle_time_ms = 0
    tag_buffer = b"\x00"
    body = struct.pack(">h", error_code)  # error_code: 2 bytes
    body += struct.pack(">B", 2) #api_version count
    body += struct.pack(">hhh", api_key, min_version, max_version)
    body += struct.pack(">B", 0)
    body += struct.pack(">i", throttle_time_ms)
    body += struct.pack(">B", 0)

    response_message_size = len(body) + 4
    header = struct.pack(">i", response_message_size)
    header += struct.pack(">i", correlation_id)
    
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
