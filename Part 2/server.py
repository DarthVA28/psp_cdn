import socket
from socketserver import UDPServer
import threading 
import hashlib
import lrucache
import filehandler
import sys

## Initializing global lock
lock = threading.RLock()

class Server:
    def __init__(self, nclients, file):
        self.nclients = nclients
        self.filename = file
        self.cache = lrucache.LRUCache(nclients)
        self.chunks = filehandler.processFile(file)
        self.serviced = [False for i in range(nclients)]
        self.localIP = "127.0.0.1"
        ## Debug File and Mode 
        self.debug = False
        self.dump = open('server_dump.txt', 'w')
        ## Server Ports
        self.tcpPorts = self.generatePorts(39152)
        self.udpPorts = self.generatePorts(44152)
        self.bdcastPorts = self.generatePorts(49152)
        ## Server Sockets
        self.tcpsockets = [0 for i in range(nclients)]
        self.udpsockets = [0 for i in range(nclients)]
        self.broadcastsockets = [0 for i in range(nclients)]
        # Client Ports 
        clientports = self.generateClientPorts(55535)
        self.c_serviceports = clientports[0]
        self.c_requestports = clientports[1]
        self.c_tcpports = clientports[2]

    def generatePorts(self,base):
        return [(base + i) for i in range(self.nclients)]

    def generateClientPorts(self,base):
        return [[(base + (j*self.nclients) + i) for i in range(self.nclients)] for j in range(3)]

    def receiveUDPPacket(self,sock,buffer,addr,timeout):
        sock.settimeout(timeout)
        while (True):
            try:
                reply, address = sock.recvfrom(buffer)
                sock.settimeout(None)
                status = "200"
                sock.sendto(status.encode(),addr)
                break
            except socket.timeout:
                print("Possible Packet Drop: Requesting again.", file = self.dump)
                status = "408"
                sock.sendto(status.encode(),addr)
        return reply

    def sendUDPPacket(self, message, sock, buffer, addr):
        i = 0
        while (i < 3):
            sock.sendto(message.encode(),addr)
            sock.settimeout(0.01)
            try:
                response, address = sock.recvfrom(buffer)
                if (response.decode() == "200"):
                    break
                else: 
                    # print("Yha bhi hora kch toh life me") 
                    i += 1
            except socket.timeout:
                i += 1
        if (i >= 3):
            raise Exception("ERROR: Packet drop due to excessive network traffic.")
        return 

    def initializeSockets(self):
        tcpPorts = self.tcpPorts
        udpPorts = self.udpPorts
        bdcastPorts = self.bdcastPorts
        localIP = self.localIP
        for i in range(self.nclients):
            ## Initializing TCP Sockets
            self.tcpsockets[i] = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
            self.tcpsockets[i].setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.tcpsockets[i].setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            self.tcpsockets[i].bind((localIP, tcpPorts[i]))
            ## Initializing UDP Sockets
            self.udpsockets[i] = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
            self.udpsockets[i].setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 16384)
            self.udpsockets[i].bind((localIP, udpPorts[i]))
            ## Initializing Broadcast Ports
            self.broadcastsockets[i] = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
            self.broadcastsockets[i].setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 16384)
            self.broadcastsockets[i].bind((localIP, bdcastPorts[i]))
        return

    def sendInitialChunks(self, client_id, chunk_idx):
        if (self.debug):
            print("Waiting on connection...")
            print("Waiting on connection...", file=self.dump)
        bufferSize = 2048
        UDPServerSocket = self.udpsockets[client_id]

        ## Accept TCP Connection for exchange of control messages 
        self.tcpsockets[client_id].listen(1)
        connectionSocket, addr = self.tcpsockets[client_id].accept()
        self.tcpsockets[client_id] = connectionSocket

        n_distr = ((len(self.chunks)//self.nclients))
        buff_sz =  (len(self.chunks)//self.nclients) + (len(self.chunks)%self.nclients)
        req = connectionSocket.recv(bufferSize).decode()
        if (req == "init"):
            keepbuff = str(buff_sz)
            connectionSocket.send(keepbuff.encode())
            req = (connectionSocket.recv(bufferSize)).decode()
            if (req != "OK"):      
                connectionSocket.close()
        else: 
            connectionSocket.close()

        nsent = 0
        message = str(len(self.chunks)) + "$"
        while (nsent<n_distr and chunk_idx < len(self.chunks)):
            message += str(chunk_idx) + "$" + self.chunks[chunk_idx] + "$"
            chunk_idx += 1
            nsent += 1
        while (client_id == (self.nclients-1) and nsent==n_distr and chunk_idx < len(self.chunks)):
            message += str(chunk_idx) + "$" + self.chunks[chunk_idx] + "$"
            chunk_idx += 1
        client_addr = (self.localIP, self.c_requestports[client_id])
        try:
            self.sendUDPPacket(message, UDPServerSocket, bufferSize, client_addr)
        except Exception as e:
            print(client_id, e)
        print("Sent to client {}".format(client_id))
        return chunk_idx

    def distributeChunks(self):
        threads = []
        n_distr = ((len(self.chunks)//self.nclients))
        for i in range(self.nclients):
            x = threading.Thread(target=self.sendInitialChunks, args=(i, i*n_distr))
            threads.append(x)
            x.start()
        for t in threads:
            t.join()
        if (self.debug):
            print("Distributed all chunks.")
            print("Distributed all chunks.", file=self.dump)

    def serviceRequest(self, client_id):
        global lock 
        bufferSize = 1024
        connectionSocket = self.tcpsockets[client_id]
        request = connectionSocket.recv(bufferSize)
        if (request.decode() == "ALLOK"):
            self.serviced[client_id] = True
            return
        chunk_id = int(request.decode())
        if (self.debug):
            print("Received request from client {} for chunk {}".format(client_id, chunk_id), file=self.dump)
        lock.acquire()
        chunk = self.cache.get(chunk_id)
        lock.release()

        if (chunk != -1): 
            if (self.debug):
                print("Found cached chunk {1}, sending to client {0}".format(client_id, chunk_id), file=self.dump)
            UDPServerSocket = self.udpsockets[client_id]
            client_addr = (self.localIP, self.c_requestports[client_id])
            try:
                self.sendUDPPacket(chunk, UDPServerSocket, bufferSize, client_addr)
            except Exception as e:
                print(client_id, e)
        else:
            if (self.debug):
                print("Did not find cached chunk {1} for sending client {0}, will broadcast".format(client_id, chunk_id), file=self.dump)
            lock.acquire()
            chunk = self.cache.get(chunk_id)
            if (chunk == -1):
                self.getChunk(chunk_id)
                chunk = self.cache.get(chunk_id)
            lock.release()
            if (self.debug):
                print("Sending back the cached chunk {1} to client {0}".format(client_id, chunk_id), file=self.dump)
                # print("The chunk is:", chunk)
            UDPServerSocket = self.udpsockets[client_id]
            client_addr = (self.localIP, self.c_requestports[client_id])
            try:
                self.sendUDPPacket(chunk, UDPServerSocket, bufferSize, client_addr)
            except Exception as e:
                print(client_id, e)
        return

    def getChunk(self, chunk_id):
        threads = []
        for i in range(self.nclients):
            x = threading.Thread(target=self.broadcast, args=(chunk_id, i, ))
            threads.append(x)
            x.start()
        for t in threads:
            t.join()
        return

    def broadcast(self, chunk_id, id):
        global lock
        if (self.debug):
            print("Sending broadcast for chunk {1} to client {0}".format(id, chunk_id), file=self.dump)
        bufferSize = 2048
        broadcastSocket = self.broadcastsockets[id]
        message = "CREQ" + str(chunk_id) + "$$"

        connectionSocket = self.tcpsockets[id]
        connectionSocket.send(message.encode())       

        client_addr = (self.localIP,self.c_serviceports[id])
        chunk = self.receiveUDPPacket(broadcastSocket,bufferSize,client_addr,0.1).decode()
        
        if (chunk == "404"):
            if (self.debug):
                print("Client {0} did not find chunk {1}".format(id, chunk_id), file=self.dump)
            return
        else: 
            if (self.debug):
                    print("Client {0} found chunk {1} and going to add to cache".format(id, chunk_id), file=self.dump)
            if (self.cache.get(chunk_id) == -1):
                self.cache.put(chunk_id,chunk)
                if (self.debug):
                    print("Client {0} found chunk {1} and added to cache".format(id, chunk_id), file=self.dump)
        return 

    def serviceClient(self, client_id):
        while (not self.serviced[client_id]):
            self.serviceRequest(client_id)
        return 

    def operateServer(self):
        threads = []
        for i in range(self.nclients):
            x = threading.Thread(target=self.serviceClient, args=(i, ))
            threads.append(x)
            x.start()
        for t in threads:
            t.join()
        return  

if __name__ == "__main__":
    args = sys.argv[1:]
    server = Server(int(args[0]),"A2_small_file.txt")
    server.debug = True
    server.initializeSockets()
    server.distributeChunks()
    server.operateServer()
    print("Simulation complete")
    print("Simulation complete", file=server.dump)
