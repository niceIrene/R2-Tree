'''

Created on 2015/8/19



@author: tqw

'''



import random;
import time;
import math;
from SocketServer import ThreadingTCPServer, StreamRequestHandler 
from socket import *
import thread;
import threading;
import sys;

global lst;
global sockets,sockets_use;
global h,h_extra;
global dim;
global servers,total_server,total_server_extra,total_query;
global query_exist,query_fail;
global last_feedback_time;
global range_min,range_max;
global clientip;
clientip='192.168.245.145';
global max_process_time;
max_process_time=0;
global max_query_load;
max_query_load=20000;

range_min=[0,0];
range_max=[0.2,0.2];
total_query=0;
dim=2;
h=1;
h_extra=h+(dim-2);
total_server_extra=4**(h_extra);

global prt_info;
prt_info=True;
global max_load,update_rate,update_mutex;
max_load=200000;update_rate=5;update_mutex=threading.Lock();

servers=4**h;
total_server=4**h;

HOSTTABLE=[];

def cmp(a,b):
    return (a>b)-(a<b);


#get server ip
fp=open('client_ip.txt','r');
a=fp.readline();
a=a.strip();
a_split=a.split();
for i in range(total_server_extra):
    HOSTTABLE.append(a_split[i]);
    data='server '+str(i)+' ip= '+a_split[i];
    print (data);
a=fp.readline();
a=a.strip();
PORT=int(a);
fp.close();

def getmin(a,b):
    if (a>b): 
        return a; 
    else: 
        return b;

class pir_type:
    mins=[];
    maxs=[];
    def __init__(self,mins=[],maxs=[]):
        self.mins=mins;
        self.maxs=maxs;

def gen_pir_layer(metaserver,mins,maxs,layer):
    global pir,average_pir;
    pir[metaserver].mins[0]=average_pir[0][int(mins[0])];
    pir[metaserver].mins[1]=average_pir[1][int(mins[1])];
    pir[metaserver].maxs[0]=average_pir[0][int(maxs[0])];
    pir[metaserver].maxs[1]=average_pir[1][int(maxs[1])];
    if not(layer==0):
        step=4**(layer-1);
        starter=total_server*(layer-1)+(metaserver-total_server*layer);
        gen_pir_layer(starter,mins,[(mins[0]+maxs[0])/2,(mins[1]+maxs[1])/2],layer-1);
        gen_pir_layer(starter+step,[(mins[0]+maxs[0])/2,mins[1]],[maxs[0],(mins[1]+maxs[1])/2],layer-1);
        gen_pir_layer(starter+step+step,[mins[0],(mins[1]+maxs[1])/2],[(mins[0]+maxs[0])/2,maxs[1]],layer-1);
        gen_pir_layer(starter+step+step+step,[(mins[0]+maxs[0])/2,(mins[1]+maxs[1])/2],[maxs[0],maxs[1]],layer-1);

def gen_pir():
    global pir;   
    gen_average();
    pir=[pir_type([0,0],[0,0]) for i in range(total_server*(h+1))];
    gen_pir_layer(total_server*h,[0,0],[2**h,2**h],h);
    
def gen_average():
    global dim,h,range_min,range_max,lst;
    global average_pir;
    divisions=2**h;
    records=len(lst);
    part_num=getmin(records,int(5*(math.log(records)/math.log(2))));
    if (part_num<1):
        part_num=1;
    parts=[0];
    for i in range(part_num):
        parts.append(float(1)/part_num*(i+1));
    step=float(1)/part_num;
    sample_num=getmin(records,int(25*(math.log(records)/math.log(2))));
    if (sample_num<1):
        sample_num=1;
    average_pir=[];
    for d in range(dim):
        #choose dim d, random sample_num samples and make a list, then sort from min to max and map to 0~1
        num_this_dim=[];
        for i in range(sample_num):
            num_this_dim.append(float(lst[random.randint(0,records-1)][d]));
        for i in range(sample_num):
            num_this_dim[i]=(num_this_dim[i]-range_min[d])/(range_max[d]-range_min[d]);
        #if (prt_info):
            #print ('round ',0,' number:',num_this_dim);
        #no. cnt average operation
        #mirror contains each time the average list is mirrored to what number
        mirror=[];
        repeat_num=10;
        average_num=float(sample_num)/part_num;
        cnt=-1;
        while True:
            cnt+=1;
            mirror.append([]);    
            #number_in_part[i]=how many numbers are in part i
            #0~1 is evenly divided into part_num parts
            number_in_part=[0 for i in range(part_num)];
            for i in range(sample_num):
                part_belong=int(num_this_dim[i]/step);
                if (part_belong==part_num):
                    part_belong=part_belong-1;
                number_in_part[part_belong]+=1;
            square=0;
            for i in range(part_num):
                square+=(number_in_part[i]-average_num)*(number_in_part[i]-average_num);
            smaller_than_me=0;
            mirror[cnt].append(0);
            for i in range(part_num):
                smaller_than_me+=number_in_part[i];
                mirror[cnt].append(float(smaller_than_me)/sample_num);
            for i in range(sample_num):
                part_belong=int(num_this_dim[i]/step);
                if (part_belong==part_num):
                    part_belong=part_belong-1;
                mirrored_part_min=mirror[cnt][part_belong];
                mirrored_part_max=mirror[cnt][part_belong+1];
                if (mirrored_part_max==mirrored_part_min):
                    mirrored_part_max=mirrored_part_min+0.0001;
                num_this_dim[i]=(num_this_dim[i]-parts[part_belong])/step*(mirrored_part_max-mirrored_part_min)+mirrored_part_min;
            
            number_in_part=[0 for i in range(part_num)];
            for i in range(sample_num):
                part_belong=int(num_this_dim[i]/step);
                if (part_belong==part_num):
                    part_belong=part_belong-1;
                number_in_part[part_belong]+=1;
            square1=0;
            for i in range(part_num):
                square1+=(number_in_part[i]-average_num)*(number_in_part[i]-average_num);
            #if prt_info:
                #print ('round ',cnt+1,' mirror:',mirror[cnt]);
                #print ('round ',cnt+1,' number:',num_this_dim);
                #print ('square=',square1);
            print (square1,' ',square);
            if (cnt>2) and (square1>=0.95*square):
                break;
        
        repeat_num=cnt+1;
        average_pir.append([0]);
        for i in range(divisions):
            average_pir[d].append(float(i+1)/divisions);
        for cnt in range(repeat_num):
            part_belong=0;
            for i in range(divisions):
                while (mirror[repeat_num-1-cnt][part_belong+1]<average_pir[d][i]):
                    part_belong+=1;
                mirrored_part_min=mirror[repeat_num-1-cnt][part_belong];
                mirrored_part_max=mirror[repeat_num-1-cnt][part_belong+1];
                if (mirrored_part_max==mirrored_part_min):
                    mirrored_part_max=mirrored_part_min+0.0001;
                average_pir[d][i]=(average_pir[d][i]-mirrored_part_min)/(mirrored_part_max-mirrored_part_min)*step+parts[part_belong];
        for i in range(divisions+1):
            average_pir[d][i]=average_pir[d][i]*(range_max[d]-range_min[d])+range_min[d];
        if (prt_info):
            print('dim ',d,' :',average_pir[d]); 
        
def init_socket(PORT):
    global total_server_extra;
    global sockets,sockets_use,sockets_mutex,global_mutex;
    global HOSTTABLE;
    global_mutex=threading.Lock();
    sockets=[];
    for i in range(total_server_extra):
        sockets.append([]);
    sockets_use=[[] for i in range(total_server_extra)];
    sockets_mutex=[[] for i in range(total_server_extra)];
    
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
        
def query_knn_thread(cnt,query_which,tcpCliSock,to_server,server_task,num,knn_k):
    global prt_info,query_exist,total_query,query_fail;
    global last_feedback_time,max_process_time;
    global query_processing,query_timetable;
    global fp;
    global chosen_query;
    global lst;
    
    start_query_time=time.time();
    #square_r_rate=math.sqrt(float(knn_k)/len(lst));
    square_r_rate=math.sqrt(float(knn_k)/(max_load*total_server_extra));
    square_r=[];
    for i in range(dim):
        square_r.append(square_r_rate*(range_max[i]-range_min[i]));
    while True:
        data='query ';
        for i in range(dim):
            data=data+str(chosen_query[query_which][i]-square_r[i])+' ';
        for i in range(dim):
            data=data+str(chosen_query[query_which][i]+square_r[i])+' ';
        
        tcpCliSock.send('%s\r\n'%data);
        data2=tcpCliSock.recv(BUFSIZE);
        
        if (len(data2.split())>knn_k):
            break;
        for i in range(dim):
            square_r[i]*=2;
            
    if (time.time()-start_query_time>max_process_time):
        max_process_time=time.time()-start_query_time; 
    dispose_socket(to_server,num); 
    query_timetable[to_server][server_task]=-1;
    query_processing[to_server]-=1;
    
    last_feedback_time=time.time();
    query_exist=query_exist-1;
    total_query=total_query+1;
    if (prt_info):
        prt_data='square_r= '+str(square_r);
        print (prt_data);
        prt_data=str(cnt)+':'+str(data2)+' remaining '+str(query_exist)+' done '+str(total_query);
        print (prt_data);

def update():
    global max_process_time,lst,query_dead,query_timetable,query_dead_cnt;
    print (query_processing);
    time.sleep(max_process_time*2);
    query_dead_cnt=0;
    print (query_processing);
    for i in range(total_server_extra):
        query_dead[i]=0;
        for j in range(20):
            if (query_timetable[i][j]!=-1):
                query_dead[i]+=1;
                query_dead_cnt+=1;
                print ('update find a dead query at server '+str(i)+' : '+'data= '+str(query_timetable[i][j])+' key= '+str(lst[query_timetable[i][j]]));
    print ('total '+str(query_dead_cnt)+' dead query');

def get_free_server(cnt):
    global max_query_exist,query_processing,query_timetable;
    max_server_thread=int(max_query_exist/total_server_extra)+2;
    while True:
        to_server=random.randint(0,total_server_extra-1);
        if (query_processing[to_server]-query_dead[to_server]<=max_server_thread) and (query_processing[to_server]<20):
            query_processing[to_server]+=1;
            for i in range(20):
                if query_timetable[to_server][i]==-1:
                    query_timetable[to_server][i]=cnt;
                    return (to_server,i);
                
    
def del_socket():
    global total_server;
    global sockets,sockets_use;
    for i in range(total_server):
        for j in range(len(sockets[i])):
            sockets[i][j].close();

def test_build_thread(tcpCliSock,a):
    data2 = tcpCliSock.recv(BUFSIZE) 
    print (data2.strip()) 
    dispose_socket(a,num);
    
def update_thread(update_which,a):
    global total_server_extra,update_inuse,PORT,chosen_query,prt_info,cnt,BUFSIZE,query_exist,total_query,update_mutex,last_feedback_time;
    while True:
        to_server=random.randint(0,total_server_extra-1);
        update_mutex.acquire();
        if not (update_inuse[to_server]):
            update_inuse[to_server]=True;
            update_mutex.release();
            break;
        update_mutex.release();
        time.sleep(0.0001);
    data='update ';
    for i in range(dim+dim):
        data=data+str(chosen_query[update_which][i])+' ';
    if (prt_info):
        print (cnt,' : ',data+' at '+str(to_server));
    (tcpCliSock,num)=apply_socket(to_server, PORT);
    tcpCliSock.send('%s\r\n'%data);
    data2=tcpCliSock.recv(BUFSIZE);
    last_feedback_time=time.time();
    dispose_socket(to_server, num);
    update_inuse[to_server]=False;
    query_exist-=1;
    total_query+=1;
    if (prt_info):
        prt_data=str(cnt)+':'+data2+' remaining '+str(query_exist)+' done '+str(total_query);
        print (prt_data);

def query_direct_thread(to_server,data):
    global PORT,direct_result,BUFSIZE,last_feedback_time,direct_thread;
    (tcpCliSock,num)=apply_socket(to_server, PORT);
    tcpCliSock.send('%s\r\n'%data);
    data2=tcpCliSock.recv(BUFSIZE);
    dispose_socket(to_server, num);
    data2=data2.strip();
    direct_thread-=1;
    direct_result+=data2;
    last_feedback_time=time.time();
    

fp=open('client_ip.txt','r');
a=fp.readline();
while (a!=''):
    HOSTTABLE.append(a.strip());
    a=fp.readline();
fp.close();

BUFSIZE = 1024  

init_socket(PORT);

print ('client ip= '+clientip)

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
            if (len(lst)>=max_load*total_server_extra):
                break;
            read_split=a.split();
            lst.append([]);
            loc=len(lst)-1;
            for i in range(len(read_split)):
                lst[loc].append(float(read_split[i]));
            a=fp.readline();
            a=a.strip();
        fp.close();
        
        for i in range(total_server_extra):
            (tcpCliSock,num)=apply_socket(i,PORT);
            data='store';
            tcpCliSock.send('%s\r\n'%data);
            dispose_socket(i,num);

        gen_pir();
        for i in range(total_server_extra):
            (tcpCliSock,num)=apply_socket(i,PORT);
            if (prt_info):
                print('store pir to server ',i);
            for j in range(h*total_server+1):
                data='store_pir '+str(j);
                for k in range(2):
                    data=data+' '+str(pir[j].mins[k]);
                for k in range(2):
                    data=data+' '+str(pir[j].maxs[k]);
                tcpCliSock.send('%s\r\n'%data);
                tcpCliSock.recv(BUFSIZE);
                print('pir['+str(j)+']='+str(pir[j].mins)+' '+str(pir[j].maxs));
            if (prt_info):
                print('store pir to server ',i,' finish');
            dispose_socket(i,num);
            
    if (not cmp(command[0],'test')) or (not cmp(command[0], 'build')): 
        for i in range(total_server_extra):
            to_server=i;
            (tcpCliSock,num)=apply_socket(to_server,PORT);
            tcpCliSock.send( '%s\r\n' %data) ;
            thread.start_new_thread(test_build_thread,(tcpCliSock,i));
            
    if (not cmp(command[0],'display_global_index')):
        for i in range(total_server_extra):
            to_server=i;
            (tcpCliSock,num)=apply_socket(to_server,PORT);
            tcpCliSock.send( '%s\r\n' %data) 
            dispose_socket(to_server,num);    
        for i in range(total_server_extra):
            to_server=i; 
            (tcpCliSock,num)=apply_socket(to_server,PORT);
            data='get_global_index_num';
            tcpCliSock.send( '%s\r\n' %data);
            print ('server '+str(i)+' has '+tcpCliSock.recv(BUFSIZE)+' global index');
            dispose_socket(to_server,num);      
            
    if (command[0]=='query'):
        to_server=random.randint(0,total_server_extra-1);
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
            if (command[0]=='point_query') or (command[0]=='update_query'):
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
        query_processing=[0 for i in range(total_server_extra)];
        #timetalbe[i][j]=server i no j query is the no. timetalbe[i][j] query
        query_timetable=[[-1 for i in range(20)] for j in range(total_server_extra)];
        #how many queries will never return at server i
        query_dead=[0 for i in range(total_server_extra)];
        #one server can only do one update at the same time
        update_inuse=[False for i in range(total_server_extra)];
        
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
                if (command[0]=='update_query') and (cnt%update_rate==0):
                    query_exist=query_exist+1;
                    thread.start_new_thread(update_thread,(query_which,1))
                    query_which=(query_which+1)%chosen_query_cnt;
                    continue;
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
                time.sleep(0.0001);
        time.sleep(max_process_time*2);
        print (query_processing);
        print ('Program finish ',total_query,' in ',last_feedback_time-start_time,' seconds, total failure= ',query_fail_cnt);
    
    #knn_query file_number k query_cnt
    if (command[0]=='knn_query'):
        #read query file
        #file_name=command[0]+'_'+command[1]+'.txt';
        file_name='point_query_'+command[1]+'.txt';
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
            chosen_query_cnt+=1;
            a=fp.readline();
            a=a.strip();
        print ('start query');    
        
        knn_k=int(command[2]);
        #how many queries are processing at server i
        query_processing=[0 for i in range(total_server_extra)];
        #timetalbe[i][j]=server i no j query is the no. timetalbe[i][j] query
        query_timetable=[[-1 for i in range(20)] for j in range(total_server_extra)];
        #how many queries will never return at server i
        query_dead=[0 for i in range(total_server_extra)];
        
        max_process_time=0.001;
            
        start_time=time.time();
        total_query=0;    
        cnt=0;
        time_limit=int(command[3]);
        query_which=0;
        query_exist=0;
        query_fail=False;
        last_feedback_time=time.time();
        dead_query=max_query_exist;
        query_fail_cnt=0;
        update_time=10;
        while (total_query<time_limit):
            if (time.time()-last_feedback_time>1): 
                print ('delay>1');        
                break;
            if (time.time()-start_time>update_time):
                update();
                update_time+=10;
                
            if (query_exist<max_query_exist) and (cnt<time_limit):
                cnt=cnt+1;
                (to_server,server_task)=get_free_server(cnt-1);
                data='knn_query';
                for i in range(dim):
                    data=data+' '+str(chosen_query[query_which][i]);
                for i in range(dim):
                    data=data+' '+str(chosen_query[query_which][i]); 
                query_which_copy=query_which;
                query_which=(query_which+1)%chosen_query_cnt;
                (tcpCliSock,num)=apply_socket(to_server,PORT); 
                query_exist=query_exist+1;
                if (prt_info):
                    print(str(cnt)+' : '+str(data)+' at '+str(to_server)+' current '+str(query_exist)+' processing');
                thread.start_new_thread(query_knn_thread,(cnt,query_which_copy,tcpCliSock,to_server,server_task,num,knn_k));                
            else:
                time.sleep(0.001);
        time.sleep(max_process_time*2);
        print (query_processing);
        print ('Program finish ',total_query,' in ',last_feedback_time-start_time,' seconds');
    
    if (command[0]=='direct_query'):
        file_name='point_query_'+command[1]+'.txt';
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
            for i in range(dim):
                chosen_query[chosen_query_cnt].append(float(a_split[i]));
            chosen_query_cnt+=1;
            a=fp.readline();
            a=a.strip();
        print ('start query');   
        start_time=time.time();
         
        time_limit=int(command[2]);
        cnt=0;
        query_which=0;
        while (cnt<time_limit):
            cnt+=1;
            
            data='find_local ';
            for i in range(dim+dim):
                data=data+str(chosen_query[query_which][i])+' ';
            direct_result='';
            direct_thread=total_server_extra;
            for i in range(total_server_extra):
                thread.start_new_thread(query_direct_thread,(i,data));
            while (direct_thread>0):
                time.sleep(0.001);
            if (prt_info):
                print(str(cnt)+': '+direct_result);
                
            query_which=(query_which+1)%chosen_query_cnt;
        print ('Program finish ',cnt,' in ',last_feedback_time-start_time,' seconds');

    if (command[0]=='set_thread'):
        max_query_exist=int(command[1]);
        
    if (command[0]=='set_load'):
        max_load=int(command[1]);
        for i in range(total_server_extra):
            (tcpCliSock,num)=apply_socket(i,PORT);
            tcpCliSock.send('set_load '+str(max_load)+'\r\n');
            print (tcpCliSock.recv(BUFSIZE));
            dispose_socket(i, num)
    
    if (command[0]=='set_port'):
        PORT=int(command[1]);
        print('PORT set to '+str(PORT));
        init_socket(PORT);
        #get server ip
        fp=open('client_ip.txt','w');
        for i in range(total_server_extra):
            fp.write(HOSTTABLE[i]);
            fp.write(' ');
        fp.write('\n');
        fp.write(str(PORT));
        fp.write('\n');
        fp.close();
    
    if (command[0]=='set_bloom_filter'):
        bloom_filter_state=int(command[1]);
        for i in range(total_server_extra):
            (tcpCliSock,num)=apply_socket(i,PORT);
            tcpCliSock.send('set_bloom_filter '+str(bloom_filter_state)+'\r\n');
            print (tcpCliSock.recv(BUFSIZE));
            dispose_socket(i, num)
    
    if (command[0]=='set_m'):
        for i in range(total_server_extra):
            (tcpCliSock,num)=apply_socket(i,PORT);
            tcpCliSock.send(command[0]+' '+command[1]);
            print (tcpCliSock.recv(BUFSIZE));
        
    if (command[0]=='print'):
        prt_info=True;    
        for i in range(total_server_extra):
            (tcpCliSock,num)=apply_socket(i,PORT);
            tcpCliSock.send('%s\r\n'%data);
            print(tcpCliSock.recv(BUFSIZE));
            dispose_socket(i,num);
    
    if (command[0]=='hide'):
        prt_info=False;
        for i in range(total_server_extra):
            (tcpCliSock,num)=apply_socket(i,PORT);
            tcpCliSock.send('%s\r\n'%data);
            print(tcpCliSock.recv(BUFSIZE));
            dispose_socket(i,num);

    if (command[0]=='exit'):
        for i in range(total_server_extra):
            (tcpCliSock,num)=apply_socket(i,PORT);
            tcpCliSock.send('%s\r\n'%data);
            print(tcpCliSock.recv(BUFSIZE));
            dispose_socket(i,num);
        del_socket();
        sys.exit();

