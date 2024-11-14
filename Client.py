# run as py Client.py 

import socket
import time
import threading
import sys

class Client:
    def __init__(self, server_host, server_port, client_id):
        self.server_host = server_host
        self.server_port = server_port
        self.client_id = client_id
        self.dependency_list = {} # tracks dependencies 

    #Conenction to server. Threading to allow for listening
    def connect_to_server(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
        self.socket.connect((self.server_host, self.server_port))
        print(f"Client {self.client_id} connected to server on port {self.server_port}")
        threading.Thread(target=self.listen_for_updates).start()

    # write a key, value to the server's data store
    def write(self, key, value):
        self.socket.send(f"write {key} {value}".encode())
        print(f"Client {self.client_id} requested write for {key} with value '{value}'")

    #read a key value from the server's data store
    def read(self, key):
        # Sends a read request to the server
        self.socket.send(f"read {key}".encode())
        #print('\tin read function')
        #response = self.socket.recv(1024).decode()
        print(f"Client {self.client_id} read response")

    # listen for messages from the server and add message into dependency list
    def listen_for_updates(self):
        while True:
            data = self.socket.recv(1024).decode()
            print(f"data from server: {data}")
            if data.startswith("replicate"):
                try:
                    parts = data.split(" ", 3)  # Split into 4 parts: "replicate", key, value, version
                    cmd, key, value, version = parts
                    version = tuple(map(int, version.strip("()").split(","))) 
                    print(f"\n\tClient {self.client_id} received update: {key} -> {value} with version {version}")
                    self.dependency_list[key] = version
                except ValueError as e:
                    print(f"Error processing update message: {e}")
            #print("\tend of listen_for_updates")

# instantiate client 
if __name__ == "__main__":
    server_host = 'localhost'
    server_port = int(input("Server Port: "))
    client_id =  int(input("Client ID: "))
    client = Client(server_host, server_port, client_id)
    client.connect_to_server()

    while True: # ask user for read or write commands 
        time.sleep(1) #1 sec delay to allow for response times 
        command = input("\nEnter command 'write key_value message' or 'read key_value': ")
        cmd_parts = command.split()
        if len(cmd_parts) >= 2:
            cmd = cmd_parts[0]
            key = cmd_parts[1]
            if cmd == "write" and len(cmd_parts) == 3:
                value = cmd_parts[2]
                client.write(key, value)
            elif cmd == "read":
                client.read(key)
            else:
                print("invalid command")
        else:
            print("invalid command") 