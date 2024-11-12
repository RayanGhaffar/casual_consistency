# run as py Server.py 

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
        self.server_sockets = []# store sockets to other servers

    #starts server by binding a socket and allowing for threading
    def start_server(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((self.host, self.port))
        server_socket.listen(5)
        print(f"Server {self.server_id} listening on port {self.port}")
        
        # start listening for client connections
        threading.Thread(target=self.listen_for_clients, args=(server_socket,)).start()

    #allows the server to listen for clients to connect to it
    def listen_for_clients(self, server_socket):
        while True:
            client_socket, addr = server_socket.accept()
            self.connections.append(client_socket)
            print(f"Client connected from {addr}")
            threading.Thread(target=self.handle_client, args=(client_socket,)).start()

    #allows for other servers to connect to it
    def connect_to_server(self, other_host, other_port):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((other_host, other_port))
            self.server_sockets.append(s)
            print(f"Connected to server at {other_host}:{other_port}")
            threading.Thread(target=self.handle_server_updates, args=(s,)).start()
        except ConnectionRefusedError:
            print(f"Connection to server at {other_host}:{other_port} failed")

    # based on the user's input, will either read, write, or connect to another server
    def handle_client(self, client_socket):
        while True:
            data = client_socket.recv(1024).decode()
            if not data:
                break
            command_parts = data.split()
            cmd = command_parts[0]
            
            if cmd == "write":
                key = command_parts[1]
                value = command_parts[2]
                self.write_to_store(key, value, client_socket)
            elif cmd == "read":
                key = command_parts[1]
                self.send_value_to_client(key, client_socket)
            elif cmd == "connect":
                ports = command_parts[1:]
                self.connect_to_other_servers_by_ports(ports, client_socket)

    def connect_to_other_servers_by_ports(self, ports, client_socket):
        response = ""
        for port in ports:
            try:
                # Connect to each server on localhost using the specified port
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect(('localhost', int(port)))
                self.server_sockets.append(s)
                response += f"Connected to server on port {port}\n"
                print(f"Server {self.server_id} connected to server on port {port}")
            except ConnectionRefusedError:
                response += f"Failed to connect to server on port {port}\n"
                print(f"Connection to server on port {port} failed")

        # Send the connection results back to the client
        print('going back to client side: ', response)
        client_socket.send(response.encode())

    def handle_server_updates(self, server_socket):
        while True:
            data = server_socket.recv(1024).decode()
            if data.startswith("replicate"):
                parts = data.split(" ", 3)
                cmd, key, value, version = parts
                version = tuple(map(int, version.strip("()").split(",")))
                self.receive_replicated_update(key, value, version)

    def write_to_store(self, key, value, client_socket):
        timestamp = int(time.time())
        version = (timestamp, self.server_id)
        self.data_store[key] = (value, version)
        self.dependencies[key] = version
        print(f"Server {self.server_id} updated {key} with {value} at version {version}")

        for c in self.connections:
            c.send(f"replicate {key} {value} {version}".encode())
        for s in self.server_sockets:
            s.send(f"replicate {key} {value} {version}".encode())

    def send_value_to_client(self, key, client_socket):
        if key in self.data_store:
            value, version = self.data_store[key]
            client_socket.send(f"value {key} {value} {version}".encode())
        else:
            client_socket.send(f"error Key {key} not found".encode())

    def receive_replicated_update(self, key, value, version):
        if key in self.dependencies and self.dependencies[key] >= version:
            self.data_store[key] = (value, version)
            self.dependencies[key] = version
            print(f"Server {self.server_id} applied replicated update for {key} with value '{value}'")
        else:
            print(f"Server {self.server_id} delaying update for {key}")


# Start the server
if __name__ == "__main__":
    host = 'localhost'  
    port = int(input("Server port: "))
    server_id = int(input("Server ID: "))

    server = Server(host, port, server_id)
    server.start_server()
