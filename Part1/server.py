import socket
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
        self.c_requestports = clientports[0]
        self.c_serviceports = clientports[1]
        self.c_udpports = clientports[2]
        self.flags = [False for j in range(nclients)]

    def generatePorts(self,base):
        return [(base + i) for i in range(self.nclients)]

    def generateClientPorts(self,base):
        return [[(base + (j*self.nclients) + i) for i in range(self.nclients)] for j in range(3)]

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
            self.broadcastsockets[i] = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
            self.broadcastsockets[i].setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.broadcastsockets[i].setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            self.broadcastsockets[i].bind((localIP, bdcastPorts[i]))

    def sendInitialChunks(self, client_id, chunk_idx):
        if (self.debug):
            print("Waiting on connection...")
            print("Waiting on connection...", file=self.dump)
        self.tcpsockets[client_id].listen(1)
        connectionSocket, addr = self.tcpsockets[client_id].accept()
        self.tcpsockets[client_id] = connectionSocket
        self.broadcastsockets[client_id].listen(1)
        # broadcastSocket, addr = self.broadcastsockets[client_id].accept()
        # self.broadcastsockets[client_id] = broadcastSocket
        bufferSize = 1024
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
        mlen = connectionSocket.send(message.encode())
        while (mlen < len(message)):
            mlen += connectionSocket.send(message[mlen:].encode())
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
        UDPServerSocket = self.udpsockets[client_id]
        request, addr = UDPServerSocket.recvfrom(bufferSize)
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
            TCPServerSocket = self.tcpsockets[client_id]
            TCPServerSocket.send(chunk.encode())
        else:
            if (self.debug):
                print("Did not find cached chunk {1} for sending client {0}, will broadcast".format(client_id, chunk_id), file=self.dump)
            lock.acquire()
            chunk = self.cache.get(chunk_id)
            if (chunk == -1):
                self.getChunk(chunk_id)
                chunk = self.cache.get(chunk_id)
            lock.release()
            TCPServerSocket = self.tcpsockets[client_id]
            if (self.debug):
                print("Sending back the cached chunk {1} to client {0}".format(client_id, chunk_id), file=self.dump)
                # print("The chunk is:", chunk)
            TCPServerSocket.send(chunk.encode())
        return

    def getChunk(self, chunk_id):
        threads = []
        for i in range(self.nclients):
            if (not self.flags[i]):
                broadcastSocket, addr = self.broadcastsockets[i].accept()
                self.broadcastsockets[i] = broadcastSocket
                self.flags[i] = True
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
        broadcastSocket.send(message.encode())
        chunk = broadcastSocket.recv(bufferSize).decode()
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
