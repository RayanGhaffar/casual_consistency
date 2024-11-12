# run as py Client.py 

import socket
import threading
import sys

class Client:
    def __init__(self, server_host, server_port, client_id):
        self.server_host = server_host
        self.server_port = server_port
        self.client_id = client_id
        self.dependency_list = {}  # Track dependencies

    def connect_to_server(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((self.server_host, self.server_port))
        print(f"Client {self.client_id} connected to server on port {self.server_port}")
        threading.Thread(target=self.listen_for_updates).start()

    #sends a write request to the server
    def write(self, key, value):
        self.socket.send(f"write {key} {value}".encode())
        print(f"Client {self.client_id} requested write for {key} with value '{value}'")

    # sends a read request to the server
    def read(self, key):
        self.socket.send(f"read {key}".encode())
        response = self.socket.recv(1024).decode()
        print(f"Client {self.client_id} read response: '{response}'")
    
    #Sends connect command with ports to the server
    def connect_server_to_others(self, ports):
        command = f"connect {' '.join(ports)}"
        self.socket.send(command.encode())
        response = self.socket.recv(1024).decode()
        print(f"Server response: {response}")

    def listen_for_updates(self):
        while True:
            data = self.socket.recv(1024).decode()
            if data.startswith("replicate"):
                try:
                    parts = data.split(" ", 3)  # Split into 4 parts: "replicate", key, value, version
                    cmd, key, value, version = parts
                    version = tuple(map(int, version.strip("()").split(",")))  # Convert version to tuple
                    print(f"\n\tClient {self.client_id} received update: {key} -> {value} with version {version}")
                    self.dependency_list[key] = version
                except ValueError as e:
                    print(f"Error processing update message: {e}")

# Start client with command-line arguments
if __name__ == "__main__":
    server_host = 'localhost'
    server_port = int(input("Server Port: "))
    client_id =  int(input("Client ID: "))
    client = Client(server_host, server_port, client_id)
    client.connect_to_server()

    #gets input from user for read/write
    while True:
        command = input("\nEnter command 'write key_value message', 'read key_value', or 'connect port1,port2,...': ")
        cmd_parts = command.split()
        if len(cmd_parts) >= 2:
            cmd = cmd_parts[0]
            key = cmd_parts[1]
            if cmd == "write" and len(cmd_parts) == 3:
                value = cmd_parts[2]
                client.write(key, value)
            elif cmd == "read":
                client.read(key)
            elif cmd == "connect":
                ports = key.split(',') #ports of other server to connect to
                client.connect_server_to_others(ports)
            else:
                print("Invalid command. Please try again.")
        else:
            print("Invalid command format.")
