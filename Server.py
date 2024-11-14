import socket
import threading
import time

class Server:
    def __init__(self, host, port, server_id, other_servers=[]):
        self.host = host
        self.port = port
        self.server_id = server_id
        self.data_store = {}  # main data store for the server. key: (message, (timestamp, serverID))
        self.dependencies = {}  # key: (timestamp, serverID)
        self.connections = []  # store client connections
        self.other_servers = other_servers  # list of other server addresses
        self.server_sockets = []  # store sockets for connected servers
        self.delayed=[] # stores the delayed updates

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
            #print(f"\tidentifier message: {identifier_message}")
            identifier = identifier_message.split()[0]
            
            if identifier == "server":  # Server connection
                intended_port = int(identifier_message.split()[1])
                self.register_peer(client_socket, addr, intended_port)
                threading.Thread(target=self.handle_server_updates, args=(client_socket,)).start()
            else:  # Client connection
                self.connections.append(client_socket)
                print(f"Client connected from {addr}")
                threading.Thread(target=self.handle_client, args=(client_socket, identifier_message)).start()
    # adds peer to lists for sockets and servers to keep ttrack
    def register_peer(self, client_socket, addr, port):
        peer = (addr[0], port)
        if peer not in self.other_servers:
            self.other_servers.append(peer)
            self.server_sockets.append(client_socket) 
        print(f"Connected Servers: {self.other_servers}")

    #connects to servers together
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

    #handles messages from server to server
    def handle_server_updates(self, server_socket):
        while True:
            data = server_socket.recv(1024).decode()
            #print(f"\tserver data is {data}")
            if data.startswith("register"): # register servers together
                _, host, port = data.split()
                self.register_peer(server_socket, (host, int(port)))
            elif data.startswith("replicate"): #replicate data one server to another
                #print(f"\treplicating to other servers")
                parts = data.split(" ", 3)
                _, key, value, version = parts
                version = tuple(map(int, version.strip("()").split(",")))
                self.receive_replicated_update(key, value, version)
    
    def handle_client(self, client_socket, initial_message=None):
        if initial_message:
            self.process_client_command(client_socket, initial_message)

        while True:
            data = client_socket.recv(1024).decode()
            #print(f"\tclient data is {data}")
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

        # propagate the update to all clients and other servers
        for c in self.connections: #send to all clients
            c.send(f"replicate {key} {value} {version}".encode())
        #print(f"\t{self.server_sockets} {self.other_servers}")
        for s in self.server_sockets: #send to all servers
            s.send(f"replicate {key} {value} {version}".encode())

    #responds to a client read command by sending data for a given key
    def send_value_to_client(self, key, client_socket):
        #print(f"\tkey: {key} client_socket: {client_socket}")
        print(f"Current Data Store: {self.data_store}")
        if key in self.data_store:
            value, version = self.data_store[key]
            #print(f'key in data store. {value} - {version}')
            try:
                client_socket.send(f"replicate {key} {value} {version}".encode())
                #print(f"completed send: replicate {key} {value} {version}")
                
            except socket.error as e:
                print(f"\nSocket error when sending to client: {e}")

            #client_socket.send(f"value {key} {value} {version}".encode())
        else: 
            #print('key NOT in data store')
            client_socket.send(f"error Key {key} not found".encode())
        

    #handles updates from other servers
    def receive_replicated_update(self, key, value, version):
        # generates timestamp for when the server receives this replication, simulates transit delay
        time.sleep(1.5)
        ts = int(time.time())
        version = (ts, version[1])
        
        """
        test case for y found to be received after z glad causing inconsistency
        if A found, delay sending the update. 
        Server 3 should get B glad then A found, will output an error about inconsistencies.
        10 second delay for server 2 user to write "z glad". 
        the ts is the same stays the same which will cause the inconsistency
        """
        if key == "y" and value =="found" and self.server_id ==3:
            time.sleep(10) 

        #gets the latest time   
        last_key = list(self.dependencies)[-1] if len(self.dependencies) >0 else 0
        latest_ts = self.dependencies[last_key][0] if len(self.dependencies) >0 else 0

        #adds data to dictionaries if the timestamp of the new message is larger than the others      
        if ts >= latest_ts:
            print(f"Key {key} with timestamp {ts} occurs after {last_key} at timestamp {latest_ts}")
            self.data_store[key] = (value, version)
            self.dependencies[key] = version
            print(f"Server {self.server_id} applied replicated update for {key} {value} {version}'")
            
            # send the update to all connected clients
            #print(f"\tclient connections: {self.connections}")
            for c in self.connections:
                #print(f"\treplicate {key} {value} {version}")
                c.send(f"replicate {key} {value} {version}".encode())
        else: #Timestamps are out of order, delay and add to delayed list. Server 3 should have this run
            print(f"Server {self.server_id} delaying update for {key} at {ts} as it should occur before {last_key} at {latest_ts}")
            self.delayed.append((key, value, version))
            
        
# instantiate server
if __name__ == "__main__":
    host = '127.0.0.1' #local host
    port = int(input("Server port: "))
    server_id = int(input("Server ID: "))
    other_servers_input = input("Enter other servers' ports: ").split()
    other_servers = [(host, int(port)) for port in other_servers_input if port] # initialize sockets for all servers to conenct to

    #Create object and start server
    server = Server(host, port, server_id, other_servers)
    server.start_server()
