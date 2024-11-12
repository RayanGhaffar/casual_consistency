# run as py Server.py 


import socket
import threading
import time
import sys

class Server:
    def __init__(self, host, port, server_id):
        self.host = host
        self.port = port
        self.server_id = server_id
        self.data_store = {}  # main data store for the server
        self.dependencies = {}  
        self.connections = []  # store client connections

    def start_server(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((self.host, self.port))
        server_socket.listen(5)
        print(f"Server {self.server_id} listening on port {self.port}")

        # Accept clients in separate threads
        while True:
            client_socket, addr = server_socket.accept()
            self.connections.append(client_socket)
            print(f"Client connected from {addr}")
            threading.Thread(target=self.handle_client, args=(client_socket,)).start()

    # Handle read or write requests from the client
    def handle_client(self, client_socket):
        while True:
            data = client_socket.recv(1024).decode()
            if not data:
                break
            cmd, key = data.split()[0], data.split()[1]
            if cmd == "write":
                value = data.split()[2]
                self.write_to_store(key, value, client_socket)
            elif cmd == "read":
                print("\ncmd is read")
                self.send_value_to_client(key, client_socket)

    def write_to_store(self, key, value, client_socket):
        timestamp = int(time.time())
        version = (timestamp, self.server_id)
        self.data_store[key] = (value, version)
        self.dependencies[key] = version
        print(f"Server {self.server_id} updated {key} with {value} at version {version}")

        # Propagate the update to all connected clients
        threading.Thread(target=self.propagate_update, args=(key, value, version)).start()

    def propagate_update(self, key, value, version):
        time.sleep(2)  # Simulate propagation delay
        for c in self.connections:
            c.send(f"replicate {key} {value} {version}".encode())

    def send_value_to_client(self, key, client_socket):
        if key in self.data_store:
            value, version = self.data_store[key]
            client_socket.send(f"value {key} {value} {version}".encode())
        else:
            client_socket.send(f"error Key {key} not found".encode())

# Start the server with command-line arguments for host, port, and server_id
if __name__ == "__main__":
    host = 'localhost'
    port = int(input("Server port: "))
    server_id = int(input("Server ID: "))
    server = Server(host, port, server_id)
    server.start_server()
