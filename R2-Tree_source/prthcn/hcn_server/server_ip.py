'''
Created on 2015/9/16

@author: tqw
'''

from SocketServer import ThreadingTCPServer, StreamRequestHandler 
from socket import *
import thread;
import threading;
import time;

def get_local_ip(ifname = 'eth0'):  
    import socket, fcntl, struct  
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)  
    inet = fcntl.ioctl(s.fileno(), 0x8915, struct.pack('256s', ifname[:15]))  
    ret = socket.inet_ntoa(inet[20:24])  
    return ret  

class MyRequestHandler(StreamRequestHandler):
    def handle(self):
        data=self.rfile.readline();
        data=data.strip();
        if (data):
            print (data);
            msg_split=data.split();
            fp=open('server_ip.txt','w');
            for i in range(len(msg_split)):
                fp.write(msg_split[i]);
                fp.write(' ');
            fp.write('\n');
            fp.close();

    
BUFSIZE=1024;
clientip='192.168.245.145'
PORT=1131;
bloom_filter=[];
data=get_local_ip();
tcpCliSock= socket(AF_INET, SOCK_STREAM);
ADDR=(clientip,PORT);
tcpCliSock.connect(ADDR);
tcpCliSock.send('%s\r\n'%data);
tcpCliSock.close();

ADDR=(data,PORT);
tcpServ = ThreadingTCPServer(ADDR, MyRequestHandler) 
print ('waiting for connection...') 
tcpServ.serve_forever();

