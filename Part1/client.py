import socket
from sqlite3 import connect 
import threading
import time 
import filehandler
import hashlib
import sys

class Client:
    def __init__(self, nclients):
        self.nclients = nclients
        self.serviced = [False for i in range(nclients)]
        self.localIP = "127.0.0.1"
        self.chunks = [{} for i in range(nclients)] 
        self.missing_chunks = [[] for i in range(nclients)]
        self.nxt_chunk = [0 for i in range(nclients)]
        self.nchunks = 0
        ## Debug File and Mode 
        self.debug = False
        self.dump = open('client_dump.txt', 'w')
        ## Server Ports
        self.s_tcpPorts = self.generatePorts(39152)
        self.s_udpPorts = self.generatePorts(44152)
        self.s_bdcastPorts = self.generatePorts(49152)
        ## Client Ports 
        self.clientPorts = self.generateClientPorts(55535)
        ## Client Sockets
        self.servicesockets = [0 for i in range(nclients)]
        self.requestsockets = [0 for i in range(nclients)]
        self.udpsockets = [0 for i in range(nclients)]
        self.unserviced = nclients
        self.tdelta = 0
        self.instances = 0
        self.packrtt = []

    def generatePorts(self,base):
        return [(base + i) for i in range(self.nclients)]

    def generateClientPorts(self,base):
        return [[(base + (j*self.nclients) + i) for i in range(self.nclients)] for j in range(3)]

    def processPacket(self, packet, client_id):
        i = 0
        left_idx = 0
        chunk_id = 0
        chunk_data = ""
        get_tchunks = True
        reading_id = True
        ls = []
        while (i < len(packet)):
            if (get_tchunks):
                if (packet[i] != '$'):
                    i += 1
                else: 
                    self.nchunks = int(packet[left_idx:i])
                    get_tchunks = False
                    i += 1
                    left_idx = i
            elif (reading_id):
                if (packet[i] != '$'):
                    i += 1
                else: 
                    chunk_id = int(packet[left_idx:i])
                    ls.append(chunk_id)
                    reading_id = False
                    i += 1
                    left_idx = i
            else: 
                if (packet[i] != '$'):
                    i += 1
                else:   
                    chunk_data = packet[left_idx:i]
                    self.chunks[client_id][chunk_id] = chunk_data
                    reading_id = True
                    i += 1
                    left_idx = i
        if (self.chunks[client_id].get(0) != None):
            self.nxt_chunk[client_id] = chunk_id+1
        missing_ls = [i for i in range(self.nchunks)]
        for num in ls:
            missing_ls.remove(num)
        missing_ls.reverse()
        self.missing_chunks[client_id] = missing_ls
        return

    def initializeSockets(self):
        servicePorts = self.clientPorts[0]
        requestPorts = self.clientPorts[1]
        udpPorts     = self.clientPorts[2]
        localIP = "127.0.0.1"
        for i in range(self.nclients):
            ## Initializing Service Sockets
            self.servicesockets[i] = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
            self.servicesockets[i].setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.servicesockets[i].bind((localIP, servicePorts[i]))
            ## Initializing Request Sockets
            self.requestsockets[i] = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
            self.requestsockets[i].setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.requestsockets[i].bind((localIP, requestPorts[i]))
            ## Initializing UDP Sockets
            self.udpsockets[i] = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
            self.udpsockets[i].bind((localIP, udpPorts[i]))
        return
            
    def receiveInitialChunks(self, client_id):
        server_addr = (self.localIP, self.s_tcpPorts[client_id])
        self.requestsockets[client_id].connect(server_addr)
        # server_addr = (self.localIP, self.s_bdcastPorts[client_id])
        # self.servicesockets[client_id].connect(server_addr)

        TCPClientSocket = self.requestsockets[client_id]
        message = "init"
        TCPClientSocket.send(message.encode())
        npackets = int(TCPClientSocket.recv(1024).decode())
        message = "OK"
        TCPClientSocket.send(message.encode())
        bufferSize = (npackets+1)*1200
        recvd_packet = TCPClientSocket.recv(bufferSize).decode()
        self.processPacket(recvd_packet,client_id)
        # self.servicesockets[client_id] = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
        # self.servicesockets[client_id].setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # self.servicesockets[client_id].bind((self.localIP, self.clientPorts[0][client_id]))
        self.packrtt = [[0,0] for i in range(self.nchunks)]
        return 

    def getInitialChunks(self):
        threads = []
        for i in range(self.nclients):
            y = threading.Thread(target=self.receiveInitialChunks, args=(i,))
            threads.append(y)
            y.start()
        for t in threads:
            t.join()
        if (self.debug):
            print("Received all initial chunks.", file=self.dump)
        # print(self.missing_chunks)
        return 

    def sendRequest(self, id):
        bufferSize = 2048
        if (len(self.missing_chunks[id]) == 0):
            self.serviced[id] = True
            self.unserviced -= 1
            UDPClientSocket = self.udpsockets[id]
            request = "ALLOK"
            server_addr = (self.localIP, self.s_udpPorts[id])
            UDPClientSocket.sendto(request.encode(), server_addr)
            return 
        else: 
            chunk_id = self.missing_chunks[id][-1]
            UDPClientSocket = self.udpsockets[id]
            request = str(chunk_id)
            server_addr = (self.localIP, self.s_udpPorts[id])
            t1 = time.time()
            UDPClientSocket.sendto(request.encode(), server_addr)

            TCPClientSocket = self.requestsockets[id]
            chunk = TCPClientSocket.recv(bufferSize).decode()
            t2 = time.time()
            self.tdelta += (t2-t1)
            self.instances += 1
            self.packrtt[chunk_id][0] += (t2-t1)
            self.packrtt[chunk_id][1] += 1
            if (self.debug):
                print("Client {} received chunk {} and adding to known chunks".format(id,chunk_id), file=self.dump)
            self.chunks[id][chunk_id] = chunk
            self.missing_chunks[id].pop()
        return

    def serviceRequest(self,id):
        bufferSize = 2048
        TCPServiceSocket = self.servicesockets[id]
        req = TCPServiceSocket.recv(bufferSize).decode()
        if (req != ""):
            # print(id, req, file=self.dump)
            chunk_id = filehandler.getCIdx(req)
            chunk = self.chunks[id].get(chunk_id)
            if (chunk is not None):
                TCPServiceSocket.send(chunk.encode())
            else: 
                status = "404"
                TCPServiceSocket.send(status.encode())
                # self.servicesockets[id].close()
        return 

    def serviceBroadcasts(self, id):
        try:
            server_addr = (self.localIP, self.s_bdcastPorts[id])
            self.servicesockets[id].connect(server_addr)
        except Exception as e:
            print("Client", id, e)
        while(self.unserviced > 0):
            self.serviceRequest(id)
        return 

    def getChunks(self, id):
        while (not self.serviced[id]):
            self.sendRequest(id)
        return 
    
    def operateClients(self):
        requestthreads = []
        servicethreads = []
        for i in range(self.nclients):
            x = threading.Thread(target=self.serviceBroadcasts, args=(i, ))
            requestthreads.append(x)
            x.start()
        for i in range(self.nclients):
            y = threading.Thread(target=self.getChunks, args=(i, ))
            servicethreads.append(y)
            y.start()
        for t in requestthreads:
            t.join()
        for s in servicethreads:
            s.join()
        return

    def assembleFiles(self, id):
        # print(self.missing_chunks[id])
        # print(self.serviced)
        text = ""
        # for key in (self.chunks[id]):
        #     print(key)
        # print(self.chunks[id])
        for i in range(self.nchunks):
            text += self.chunks[id].get(i)
        # print(text)
        filename = "./clientfiles/A2_client_" + str(id) + ".txt"
        f = open(filename,"w")
        f.write(text)
        f.close()
        result = hashlib.md5(text.encode()).hexdigest()
        print("MD5 hash for client {} is: ".format(id), result)
        return

if __name__ == "__main__":
    args = sys.argv[1:]
    clients = Client(int(args[0]))
    clients.debug = True
    t_start = time.time()
    clients.initializeSockets()
    clients.getInitialChunks()
    print("Going to start operating clients...")
    # time.sleep(2)
    clients.operateClients()
    print("Simulation complete")
    print("Simulation complete", file=clients.dump)
    t_end = time.time()
    t_delta = t_end - t_start
    print("The simulation time is: {} seconds".format(t_delta))
    avg_delta = clients.tdelta/clients.instances
    print("The average RTT over all packets is: {}".format(avg_delta))
    for i in range(clients.nclients):
        clients.assembleFiles(i)
    for i in range(clients.nchunks):
        clients.packrtt[i] = clients.packrtt[i][0]/clients.packrtt[i][1]
    # print(clients.packrtt)
    print("Done.")