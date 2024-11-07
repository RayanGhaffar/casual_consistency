# server.py

import socket
import threading
import time

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

    #takes the data fro the client and performs read or write
    def handle_client(self, client_socket):
        while True:
            data = client_socket.recv(1024).decode()
            #print(f"data is {data}")
            if not data:
                break
            cmd, key = data.split()[0], data.split()[1]
            #print(f"cmd is {cmd}")
            if cmd == "write":
                value =data[2] 
                self.write_to_store(key, value, client_socket)
            elif cmd== "read":
                print('cmd == read')
                self.send_value_to_client(key, client_socket)

    def write_to_store(self, key, value, client_socket):
        timestamp = int(time.time())  
        version = (timestamp, self.server_id)
        self.data_store[key] = (value, version)
        self.dependencies[key] = version
        print(f"Server {self.server_id} updated {key} with {value} at version {version}")

        # multiple threads for propagation delay
        threading.Thread(target=self.propagate_update, args=(key, value, version)).start()

    def propagate_update(self, key, value, version):
        time.sleep(2)  
        for c in self.connections:
            c.send(f"replicate {key} {value} {version}".encode())

    def send_value_to_client(self, key, client_socket):
        if key in self.data_store:
            print('key in data store')
            value, version = self.data_store[key]
            client_socket.send(f"value {key} {value} {version}".encode())
        else:
            print('key not in data store')
            client_socket.send(f"error Key {key} not found".encode())

    def receive_replicated_update(self, key, value, version):
        #check dependencies before applying the update
        if self.check_dependencies(key, version):
            self.data_store[key] = (value, version)
            self.dependencies[key] = version
            print(f"Server {self.server_id} applied replicated update for {key} with value '{value}'")
        else: #dependencies aren't met
            print(f"Server {self.server_id} delaying update for {key}")

    def check_dependencies(self, key, version):
        #check if dependencies are met
        if key in self.dependencies and self.dependencies[key] >= version:
            return True
        return False

# Code to start the server
if __name__ == "__main__":
    server = Server('localhost', 8001, 1)
    server.start_server()
