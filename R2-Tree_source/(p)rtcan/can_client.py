'''
Created on 2015/9/5

@author: tqw
'''

import random;
import time;
from SocketServer import ThreadingTCPServer, StreamRequestHandler 
from socket import *
import thread;
import threading;
import sys;

global sockets,sockets_use;
global h;
global dim;
global servers,total_server,total_query;
global query_existk,query_fail;
global last_feedback_time;
total_query=0;
dim=2;
h=2;
global max_load;
max_load=200000;
global max_query_load;
max_query_load=20000;

global prt_info;
prt_info=True;

servers=h**dim;
total_server=h**dim;

HOSTTABLE=[];
#HOSTTABLE = ['192.168.245.129','192.168.245.130','192.168.245.146','192.168.245.132','192.168.245.133','192.168.245.143','192.168.245.144','192.168.245.134','192.168.245.135','192.168.245.136','192.168.245.137','192.168.245.138','192.168.245.139','192.168.245.140','192.168.245.141','192.168.245.142'];
PORT = 1111

def cmp(a,b):
    return (a>b)-(a<b);

#get server ip
fp=open('client_ip.txt','r');
a=fp.readline();
a=a.strip();
a_split=a.split();
for i in range(total_server):
    HOSTTABLE.append(a_split[i]);
    data='server '+str(i)+' ip= '+a_split[i];
    print (data);
a=fp.readline();
a=a.strip();
PORT=int(a);
fp.close();

def init_socket(PORT):
    global total_server;
    global sockets,sockets_use,sockets_mutex,global_mutex;
    global HOSTTABLE;
    global_mutex=threading.Lock();
    sockets=[];
    for i in range(total_server):
        sockets.append([]);
    sockets_use=[[] for i in range(total_server)];
    sockets_mutex=[[] for i in range(total_server)];
    
def apply_socket(to_server,PORT):
    global prt_info;
    global total_server;
    global sockets,sockets_use,sockets_mutex,global_mutex;
    global HOSTTABLE;
    while (True):
        unused_socket_no=-1;
        for i in range(len(sockets_use[to_server])):
            if (sockets_use[to_server][i]==False):
                unused_socket_no=i;
                break;
        if (unused_socket_no==-1):
            global_mutex.acquire();
            unused_socket_no=len(sockets[to_server]);
            ADDR=(HOSTTABLE[to_server],PORT);
            tcpCliSock = socket(AF_INET, SOCK_STREAM);
           
            tcpCliSock.connect(ADDR);
            sockets[to_server].append(tcpCliSock);
            sockets_use[to_server].append(False);
            sockets_use[to_server][unused_socket_no]=True;
            if (prt_info):
                data_prt='socket['+str(to_server)+']['+str(unused_socket_no)+']in use';
                print(data_prt);
            sockets_mutex[to_server].append(threading.Lock());
            global_mutex.release();
            return ([sockets[to_server][unused_socket_no],unused_socket_no]);
        sockets_mutex[to_server][unused_socket_no].acquire();
        if (sockets_use[to_server][unused_socket_no]):
            sockets_mutex[to_server][unused_socket_no].release();
            continue;
        if (prt_info):
            data_prt='socket['+str(to_server)+']['+str(unused_socket_no)+']in use';
            print(data_prt);
        sockets_use[to_server][unused_socket_no]=True;
        sockets_mutex[to_server][unused_socket_no].release();
        return ([sockets[to_server][unused_socket_no],unused_socket_no]);
            
def dispose_socket(to_server,socket_no):
    global sockets,sockets_use;
    if (prt_info): 
        data_prt='socket['+str(to_server)+']['+str(socket_no)+']free';
        print(data_prt);
    sockets_use[to_server][socket_no]=False;

def query_thread(cnt,query_which,tcpCliSock,to_server,server_task,num):
    global prt_info,query_exist,total_query,query_fail;
    global last_feedback_time,max_process_time;
    global query_processing,query_timetable;
    global fp;
    start_query_time=time.time();
    data2=tcpCliSock.recv(BUFSIZE);
    if (time.time()-start_query_time>max_process_time):
        max_process_time=time.time()-start_query_time; 
    last_feedback_time=time.time();
    
    
    
    query_which=float(query_which);
    dispose_socket(to_server,num); 
    query_timetable[to_server][server_task]=-1;
    query_processing[to_server]-=1;
    
    query_exist=query_exist-1;
    total_query=total_query+1;
    if (prt_info):
        prt_data=str(cnt)+':'+data2+' remaining '+str(query_exist)+' done '+str(total_query);
        print (prt_data);

def del_socket():
    global total_server;
    global sockets,sockets_use;
    for i in range(total_server):
        for j in range(len(sockets[i])):
            sockets[i][j].close();

def update():
    global max_process_time,lst,query_dead,query_timetable,query_dead_cnt;
    print (query_processing);
    time.sleep(max_process_time*2);
    query_dead_cnt=0;
    print (query_processing);
    for i in range(total_server):
        query_dead[i]=0;
        for j in range(20):
            if (query_timetable[i][j]!=-1):
                query_dead[i]+=1;
                query_dead_cnt+=1;
                print ('update find a dead query at server '+str(i)+' : '+'data= '+str(query_timetable[i][j])+' key= '+str(lst[query_timetable[i][j]]));
    print ('total '+str(query_dead_cnt)+' dead query');

def test_build_thread(tcpCliSock,a):
    data2 = tcpCliSock.recv(BUFSIZE) 
    print (data2.strip()) 
    dispose_socket(a,num);

def get_free_server(cnt):
    global max_query_exist,query_processing,query_timetable;
    max_server_thread=int(max_query_exist/total_server)+2;
    while True:
        to_server=random.randint(0,total_server-1);
        if (query_processing[to_server]-query_dead[to_server]<=max_server_thread) and (query_processing[to_server]<20):
            query_processing[to_server]+=1;
            for i in range(20):
                if query_timetable[to_server][i]==-1:
                    query_timetable[to_server][i]=cnt;
                    return (to_server,i);
        time.sleep(0.0001);
  
BUFSIZE = 1024   

ADDR=(HOSTTABLE[0],PORT) 
init_socket(PORT);

query_exist=0;
max_query_exist=1;

while True: 
    data = input('> ') 
    if prt_info: print ('client is working');
    if not data: 
        break 
    command=data.split();
    if (not cmp(command[0],'store')):
        #initial data
        lst=[];
        fp=open('input.txt','r');
        a=fp.readline();
        while (a!=''):
            a=a.strip();
            if (len(lst)>=max_load*total_server):
                break;
            read_split=a.split();
            lst.append([]);
            loc=len(lst)-1;
            for i in range(len(read_split)):
                lst[loc].append(float(read_split[i]));
            a=fp.readline();
            a=a.strip();
        fp.close();
        
        for i in range(total_server):
            (tcpCliSock,num)=apply_socket(i,PORT);
            data='store';
            tcpCliSock.send('%s\r\n'%data);
            dispose_socket(i,num);
            
    if (not cmp(command[0],'test')) or (not cmp(command[0], 'build')): 
        for i in range(total_server):
            to_server=i;
            (tcpCliSock,num)=apply_socket(to_server,PORT);
            tcpCliSock.send( '%s\r\n' %data) ;
            thread.start_new_thread(test_build_thread,(tcpCliSock,i));
            
    if (not cmp(command[0],'display_global_index')):
        for i in range(total_server):
            to_server=i;
            (tcpCliSock,num)=apply_socket(to_server,PORT);
            tcpCliSock.send( '%s\r\n' %data) 
            dispose_socket(to_server,num);    
        for i in range(total_server):
            to_server=i; 
            (tcpCliSock,num)=apply_socket(to_server,PORT);
            data='get_global_index_num';
            tcpCliSock.send( '%s\r\n' %data);
            print ('server '+str(i)+' has '+tcpCliSock.recv(BUFSIZE)+' global index');
            dispose_socket(to_server,num);  
                
    if (command[0]=='query'):
        to_server=random.randint(0,servers-1);
        (tcpCliSock,num)=apply_socket(to_server,PORT); 
        tcpCliSock.send( '%s\r\n' %data) 
        print ('client send querypt request to ',to_server) 
        thread.start_new_thread(query_thread,(1,-1,tcpCliSock,to_server,num));
        
    
    if (command[0]=='query_local'):
        to_server=int(command[1]);
        data='query_local';
        for i in range(dim):
            data=data+' '+command[i+2];
        for i in range(dim):
            data=data+' '+command[dim+i+2];
        (tcpCliSock,num)=apply_socket(to_server,PORT);
        tcpCliSock.send( '%s\r\n' %data);
        print (data);
        data2=tcpCliSock.recv(BUFSIZE);
        dispose_socket(to_server,num);
        print ('%s\r\n'%data2);
    
    if (command[0]=='point_query') or (command[0]=='range_query') or (command[0]=='update_query'):
        #read query file
        file_name='point_query_'+command[1]+'.txt';
        if (command[0]=='range_query'):
            file_name='range_query_'+command[1]+'.txt';    
        fp=open(file_name,'r');
        a=fp.readline();
        a=a.strip();
        chosen_query=[];
        chosen_query_cnt=0;
        while (a!=''):
            if (len(chosen_query)>=max_query_load):
                break;
            chosen_query.append([]);
            a_split=a.strip().split();
            for i in range(dim):
                chosen_query[chosen_query_cnt].append(float(a_split[i]));
            if (command[0]=='point_query'):
                for i in range(dim):
                    chosen_query[chosen_query_cnt].append(float(a_split[i]));
            else:
                for i in range(dim):
                    chosen_query[chosen_query_cnt].append(float(a_split[i+dim]));
            chosen_query_cnt+=1;
            a=fp.readline();
            a=a.strip();
        print ('start query');    
        #how many queries are processing at server i
        query_processing=[0 for i in range(total_server)];
        #timetalbe[i][j]=server i no j query is the no. timetalbe[i][j] query
        query_timetable=[[-1 for i in range(20)] for j in range(total_server)];
        #how many queries will never return at server i
        query_dead=[0 for i in range(total_server)];
        #one server can only do one update at the same time
        update_inuse=[False for i in range(total_server)];
        
        max_process_time=0.001;
        
        start_time=time.time();
        total_query=0;    
        cnt=0;
        time_limit=int(command[2]);
        query_which=0;
        query_exist=0;
        query_fail=False;
        last_feedback_time=time.time();
        dead_query=max_query_exist;
        query_fail_cnt=0;
        update_time=10;
        while (total_query<time_limit):
            if (query_fail): 
                query_fail_cnt+=1;
                query_fail=False;
            if (time.time()-last_feedback_time>1): 
                print ('delay>1');        
                break;
            if (time.time()-start_time>update_time):
                update();
                update_time+=10;
            
            if (query_exist<max_query_exist) and (cnt<time_limit):
                cnt=cnt+1;
            
                (to_server,server_task)=get_free_server(cnt-1);
                data='query';
                for i in range(dim+dim):
                    data=data+' '+str(chosen_query[query_which][i]);
                query_which_copy=query_which;
                query_which=(query_which+1)%chosen_query_cnt;
                (tcpCliSock,num)=apply_socket(to_server,PORT); 
                query_exist=query_exist+1;
                tcpCliSock.send( '%s\r\n' %data) 
                if (prt_info):
                    print(cnt,': ',data,' at',to_server,' current',query_exist,' processing');
                thread.start_new_thread(query_thread,(cnt,query_which_copy,tcpCliSock,to_server,server_task,num));
            else:
                time.sleep(0.001);
        time.sleep(max_process_time*2);
        print (query_processing);
        print ('Program finish ',total_query,' in ',last_feedback_time-start_time,' seconds, total failure= ',query_fail_cnt);
    
    if (command[0]=='set_thread'):
        max_query_exist=int(command[1]);
        
    if (command[0]=='set_port'):
        PORT=int(command[1]);
        print('PORT set to '+str(PORT));
        init_socket(PORT);
        #get server ip
        fp=open('client_ip.txt','w');
        for i in range(total_server):
            fp.write(HOSTTABLE[i]);
            fp.write(' ');
        fp.write('\n');
        fp.write(str(PORT));
        fp.write('\n');
        fp.close();
        
    if (command[0]=='print'):
        prt_info=True;    
        for i in range(total_server):
            (tcpCliSock,num)=apply_socket(i,PORT);
            tcpCliSock.send('%s\r\n'%data);
            print(tcpCliSock.recv(BUFSIZE));
            dispose_socket(i,num);
    
    if (command[0]=='hide'):
        prt_info=False;
        for i in range(total_server):
            (tcpCliSock,num)=apply_socket(i,PORT);
            tcpCliSock.send('%s\r\n'%data);
            print(tcpCliSock.recv(BUFSIZE));
            dispose_socket(i,num);

    if (command[0]=='exit'):
        for i in range(total_server):
            (tcpCliSock,num)=apply_socket(i,PORT);
            tcpCliSock.send('%s\r\n'%data);
            print(tcpCliSock.recv(BUFSIZE));
            dispose_socket(i,num);
        del_socket();
        sys.exit();
