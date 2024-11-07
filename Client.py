# client.py

import socket
import threading
import time

class Client:
    def __init__(self, server_host, server_port, client_id):
        self.server_host = server_host
        self.server_port = server_port
        self.client_id = client_id
        self.dependency_list = {}  # Track dependencies for causal consistency

    def connect_to_server(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((self.server_host, self.server_port))
        print(f"Client {self.client_id} connected to server on port {self.server_port}")

    def write(self, key, value):
        # Send a write request to the server
        self.socket.send(f"write {key} {value}".encode())
        print(f"Client {self.client_id} requested write for {key} with value {value}")

    def read(self, key):
        # Send a read request to the server
        self.socket.send(f"read {key}".encode())
        response = self.socket.recv(1024).decode()
        print(f"Client {self.client_id} read response: {response}")

    def listen_for_updates(self):
        while True:
            data = self.socket.recv(1024).decode()
            print(f"Received data: {data}")  # Debugging line to show raw data received
            if data.startswith("replicate"):
                try:
                    parts = data.split(" ", 3)  # Split into 4 parts: "replicate", key, value, version
                    command, key, value, version = parts
                    # Parse the version string "(timestamp, id)" to a tuple of integers
                    version = tuple(map(int, version.strip("()").split(",")))
                    print(f"Client {self.client_id} received update: {key} -> {value} with version {version}")
                    self.dependency_list[key] = version
                except ValueError as e:
                    print(f"Error processing update message: {e}")

# Code to start the client
if __name__ == "__main__":
    client = Client('localhost', 8001, 1)
    client.connect_to_server()
    threading.Thread(target=client.listen_for_updates).start()
    client.write('x', 'lost')
    time.sleep(1)
    client.write('y', 'found')
    time.sleep(1)
    client.read('y')
