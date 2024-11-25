import socket  # noqa: F401
import struct

def create_response():
    message_size = 4
    correlation_id = 7
    return struct.pack(">ii", message_size, correlation_id)

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

            response = create_response()

            conn.sendall(response)
        except Exception as e:
            print(f"{e}")
        finally:
            conn.close()




if __name__ == "__main__":
    main()
