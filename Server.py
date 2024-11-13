import socket
import threading
import time

class Server:
    def __init__(self, host, port, server_id, other_servers=[]):
        self.host = host
        self.port = port
        self.server_id = server_id
        self.data_store = {}  # main data store for the server
        self.dependencies = {}  
        self.connections = []  # store client connections
        self.other_servers = other_servers  # list of other server addresses
        self.server_sockets = []  # store sockets for connected servers

    def start_server(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((self.host, self.port))
        server_socket.listen(5)
        print(f"Server {self.server_id} listening on port {self.port}")
        
        # Start listening for client and server connections
        threading.Thread(target=self.listen_for_clients, args=(server_socket,)).start()

        # Connect to other servers and start listening for server updates
        self.connect_to_other_servers()

    def listen_for_clients(self, server_socket):
        while True:
            client_socket, addr = server_socket.accept()
            print(f"Connection received from {addr}")

            # Determine if connection is from another server or client
            identifier_message = client_socket.recv(1024).decode()
            identifier = identifier_message.split()[0]
            
            if identifier == "server":  # Server connection
                intended_port = int(identifier_message.split()[1])
                self.register_peer(client_socket, addr, intended_port)
                threading.Thread(target=self.handle_server_updates, args=(client_socket,)).start()
            else:  # Client connection
                self.connections.append(client_socket)
                print(f"Client connected from {addr}")
                threading.Thread(target=self.handle_client, args=(client_socket, identifier_message)).start()

    def register_peer(self, client_socket, addr, port):
        peer = (addr[0], port)
        if peer not in self.other_servers:
            self.other_servers.append(peer)
            self.server_sockets.append(client_socket)  # Add the socket to server_sockets
            print(f"Registered new server {addr[0]}:{port}")
        print(f"Connected Servers: {self.other_servers}")

    def connect_to_other_servers(self):
        for server in self.other_servers:
            host, port = server
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect((host, port))
                self.server_sockets.append(s)
                print(f"Connected Servers: {self.other_servers}")
                
                # Send registration info to the server
                s.send(f"server {self.port}".encode())
                
                # Start a thread to handle messages from this connected server
                threading.Thread(target=self.handle_server_updates, args=(s,)).start()
            except ConnectionRefusedError:
                print(f"Connection to server at {host}:{port} failed")

    def handle_server_updates(self, server_socket):
        while True:
            data = server_socket.recv(1024).decode()
            if data.startswith("register"):
                _, host, port = data.split()
                self.register_peer(server_socket, (host, int(port)))
            elif data.startswith("replicate"):
                # Handle replication
                parts = data.split(" ", 3)
                cmd, key, value, version = parts
                version = tuple(map(int, version.strip("()").split(",")))
                self.receive_replicated_update(key, value, version)
    
    def handle_client(self, client_socket, initial_message=None):
        if initial_message:
            self.process_client_command(client_socket, initial_message)

        while True:
            data = client_socket.recv(1024).decode()
            if not data:
                break
            self.process_client_command(client_socket, data)

    def process_client_command(self, client_socket, data):
        cmd, key = data.split()[0], data.split()[1]
        if cmd == "write":
            value = data.split()[2]
            self.write_to_store(key, value, client_socket)
        elif cmd == "read":
            self.send_value_to_client(key, client_socket)

    def write_to_store(self, key, value, client_socket):
        timestamp = int(time.time())
        version = (timestamp, self.server_id)
        self.data_store[key] = (value, version)
        self.dependencies[key] = version
        print(f"Server {self.server_id} updated {key} with {value} at version {version}")

        # Propagate the update to all clients and other servers
        for c in self.connections:
            c.send(f"replicate {key} {value} {version}".encode())
        print(f"\t{self.server_sockets} {self.other_servers}")
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

# Run server
if __name__ == "__main__":
    host = '127.0.0.1'
    port = int(input("Server port: "))
    server_id = int(input("Server ID: "))
    other_servers_input = input("Enter other servers' ports: ").split()

    other_servers = [(host, int(port)) for port in other_servers_input if port]

    server = Server(host, port, server_id, other_servers)
    server.start_server()
