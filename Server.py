# run as py Server.py 
# enter other servers as a space separated list

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

    def start_server(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((self.host, self.port))
        server_socket.listen(5)
        print(f"Server {self.server_id} listening on port {self.port}")
        
        # Start listening for client connections
        threading.Thread(target=self.listen_for_clients, args=(server_socket,)).start()

        # Connect to other servers and start listening for server updates
        self.connect_to_other_servers()

    def listen_for_clients(self, server_socket):
        while True:
            client_socket, addr = server_socket.accept()
            print(f"Connection received from {addr}")
            
            # Read identifier message from the connecting socket
            identifier_message = client_socket.recv(1024).decode()
            #print(f"\n\n{identifier_message}\n\n")
            identifier = identifier_message.split()[0]
            
            if identifier == "server": #connect servers together
                intended_port = identifier_message.split()[1]
                port= int(intended_port)
                self.register_peer('127.0.0.1', port)  # Register the connecting server
                threading.Thread(target=self.handle_server_updates, args=(client_socket,)).start()
            else: #adds a user client to the server and executes commands from user
                #print("else statement of listen for clients")
                #maybe an if else to see if client is already in connections list, if not add it, if so handle command
                self.connections.append(client_socket)
                print(f"Client connected from {addr}")
                threading.Thread(target=self.handle_client, args=(client_socket, identifier_message)).start()
    
    #registers servers with each other
    def register_peer(self, host, port):
        if (host, port) not in self.other_servers:
            self.other_servers.append((host, port))
            print(f"registered new server {host}:{port}")
        print(f"Connected Servers: {self.other_servers}")

    #connects a server to another server
    def connect_to_other_servers(self):
        self.server_sockets = []
        for server in self.other_servers:
            host, port = server
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect((host, port))
                self.server_sockets.append(s)
                print(f"Connected to server at {host}:{port}")
                
                # Send both the identifier and the intended port
                s.send(f"server {self.port}".encode())  # Include this server's listening port
                
                threading.Thread(target=self.handle_server_updates, args=(s,)).start()
            except ConnectionRefusedError:
                print(f"Connection to server at {host}:{port} failed")

    #when an update from another server is received, decode and break up the message, then determine order by timestamp
    def handle_server_updates(self, server_socket):
        while True:
            data = server_socket.recv(1024).decode()
            if data.startswith("register"):
                _, host, port = data.split()
                self.register_peer(host, int(port))
            elif data.startswith("replicate"):
                # existing replication handling code...
                parts = data.split(" ", 3)
                cmd, key, value, version = parts
                version = tuple(map(int, version.strip("()").split(",")))
                self.receive_replicated_update(key, value, version)
            print(f"inside handle server updates Connected Servers: {self.other_servers}")
    
    #takes message from client terminal and allows user commands
    def handle_client(self, client_socket, initial_message=None):
        if initial_message:
            self.process_client_command(client_socket, initial_message)

        while True:
            data = client_socket.recv(1024).decode()
            if not data:
                break
            self.process_client_command(client_socket, data)
    #helper function for handle_client to determine what to do with commands
    def process_client_command(self, client_socket, data):
        #print(f"\t{data}")

        cmd, key = data.split()[0], data.split()[1]
        if cmd == "write":
            value = data.split()[2]
            self.write_to_store(key, value, client_socket)
        elif cmd == "read":
            self.send_value_to_client(key, client_socket)

    # send write message to all clients in the server and other servers
    def write_to_store(self, key, value, client_socket):
        timestamp = int(time.time())
        version = (timestamp, self.server_id)
        self.data_store[key] = (value, version)
        self.dependencies[key] = version
        print(f"Server {self.server_id} updated {key} with {value} at version {version}")

        # propagate the update to all connected clients 
        #print(f"\tconnections: {self.connections} \t servers: {self.server_sockets}")
        for c in self.connections:
            c.send(f"replicate {key} {value} {version}".encode())
        #propagate update to all other servers so they can send to their clients
        for s in self.server_sockets:
            s.send(f"replicate {key} {value} {version}".encode())

    #sends an update to a specific client
    def send_value_to_client(self, key, client_socket):
        if key in self.data_store:
            value, version = self.data_store[key]
            client_socket.send(f"value {key} {value} {version}".encode())
        else:
            client_socket.send(f"error Key {key} not found".encode())

    #Takes the update from another server and if the timestamp is after the previous, add it to the chain, other wise delay it for later
    def receive_replicated_update(self, key, value, version):
        #check dependencies before applying the update
        if key in self.dependencies and self.dependencies[key] >= version:
            self.data_store[key] = (value, version)
            self.dependencies[key] = version
            print(f"Server {self.server_id} applied replicated update for {key} with value '{value}'")
        else:
            print(f"Server {self.server_id} delaying update for {key}")


# starts the server. 
if __name__ == "__main__":
    host = '127.0.0.1'  # hardcoded to localhost
    port = int(input("Server port: "))
    server_id = int(input("Server ID: "))
    #gets the other servers ports
    other_servers_input = input("Enter other servers' ports: ").split()

    # create list of other servers with localhost and their ports
    other_servers = [(host, int(port)) for port in other_servers_input if port]

    #initialize server object and starts the server
    server = Server(host, port, server_id, other_servers)
    server.start_server()