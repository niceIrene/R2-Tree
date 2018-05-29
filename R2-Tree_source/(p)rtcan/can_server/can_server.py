'''
Created on 2015/9/5

@author: tqw
'''


from SocketServer import ThreadingTCPServer, StreamRequestHandler 
from time import ctime
from socket import *
import math;
import thread;
import time;
import threading;
import random;
import sys;

global prt_info;
prt_info=True; 

global global_index;
global HOSTTABLE;
HOSTTABLE=[];
global server_no;
server_no=0;
global PORT;
global route;
global dim;
dim=2;
global total_server;
global h;
global wait_for_build;
global wait_for_publish;
global data_for_build;
wait_for_publish=[];
global tree_node_list,tree_node_num;
tree_node_list=[];
tree_node_num=0;
h=2;
total_server=h**dim;
global range_min,range_max;
range_min=[0,0];
range_max=[0.2,0.2];
global thread_exist,max_thread;
thread_exist=0;
max_thread=100;
global rmax;
global M,m;
M=10;m=math.ceil(M/2);
global max_load;
max_load=200000;
rmax=(range_max[0]-range_min[0])*math.sqrt(float(M)**3/max_load);

def cmp(a,b):
    return (a>b)-(a<b);

class r_branch:
    dim=0;
    mins=[];
    maxs=[];
    link=0;
    size=0;
    data=-1;
    def __init__(self,d,min,max,l,data):
        self.dim=d;
        self.mins=min;
        self.maxs=max;
        self.link=l;
        self.data=data;
        self.update_size();
    def update_size(self):
        if (len(self.mins)==0):
            self.size=0;
        else:
            self.size=1;
            for i in range(len(self.mins)):
                self.size=self.size*(self.maxs[i]-self.mins[i]);
class r_node:
    branch=[];
    branch_num=0;
    no=0;
    mins=[];
    maxs=[];
    size=0;
    father=-1;
    level=0;
    def __init__(self,number):
        self.no=number;
        self.branch=[];
        self.mins=[];
        self.maxs=[];
        self.size=0;
        self.father=-1;
        self.level=0;
    def update_size(self):
        if (len(self.mins)==0):
            self.size=0;
        else:
            self.size=1;
            for i in range(len(self.mins)):
                self.size=self.size*(self.maxs[i]-self.mins[i]);

def add_tree_node():
    global tree_node_num,new_node,tree_node_list;
    tree_node_num=tree_node_num+1;
    new_node=r_node(tree_node_num-1);
    tree_node_list.append(new_node);
    return tree_node_num-1;

def get_min(min1,min2):
    min3=[];
    for i in range(len(min1)):
        if (min1[i]<min2[i]):
            min3.append(min1[i]);
        else:
            min3.append(min2[i]);
    return min3;

def get_max(max1,max2):
    max3=[];
    for i in range(len(max1)):
        if (max1[i]>max2[i]):
            max3.append(max1[i]);
        else:
            max3.append(max2[i]);
    return max3;

def get_size(min,max):
    global dim;
    size=1;
    for i in range(dim):
        size=size*(max[i]-min[i]);
    return size;

def insert(mins,maxs,data,branch,level):
    #if tree has no head
    global head,dim,M,m,tree_node_list;
    if (head==-1):
        #add a node into the tree as head
        head=add_tree_node();
        tree_node_list[0].branch.append(r_branch(dim,mins,maxs,-1,data));
        tree_node_list[0].branch_num=1;
        tree_node_list[0].mins=mins;
        tree_node_list[0].maxs=maxs;
    else:
        #find the leaf to insert new item
        p=tree_node_list[head];
        isleaf=(p.level==level);
        while (not isleaf):
            #choose the branch that insert the item will cause least extra cost
            for i in range(0,p.branch_num):
                #the boundary if the item insert to branch i 
                new_min=get_min(mins,p.branch[i].mins);
                new_max=get_max(maxs,p.branch[i].maxs);
                #newsize-oldsize=extra cost
                oldsize=get_size(p.branch[i].mins,p.branch[i].maxs);
                newsize=get_size(new_min,new_max);
                #for branch 0,choose this one first
                if (i==0):
                    minsize=newsize-oldsize;
                    choosebranch=i;
                #for other branches, if cost is lower, choose i
                elif (newsize-oldsize<minsize):
                    minsize=newsize-oldsize;
                    choosebranch=i;
            #p point to the linked node of the chosen branch
            tree_node_list[p.no].mins=get_min(p.mins,mins);
            tree_node_list[p.no].maxs=get_max(p.maxs,maxs);
            tree_node_list[p.no].update_size();
            tree_node_list[p.no].branch[choosebranch].mins=get_min(tree_node_list[p.no].branch[choosebranch].mins,mins);
            tree_node_list[p.no].branch[choosebranch].maxs=get_max(tree_node_list[p.no].branch[choosebranch].maxs,maxs);
            tree_node_list[p.no].branch[choosebranch].update_size();
            p=tree_node_list[p.branch[choosebranch].link];
            #until p is a leaf
            isleaf=(p.level==level);
        #add new item into p
        pno=p.no;
        if (level==0):
            tree_node_list[pno].branch.append(r_branch(dim,mins,maxs,-1,data));
        else:
            tree_node_list[pno].branch.append(branch);
        tree_node_list[pno].branch_num=p.branch_num+1;
        #update p's boundary and size
        new_min=get_min(mins,tree_node_list[pno].mins);
        new_max=get_max(maxs,tree_node_list[pno].maxs);
        tree_node_list[pno].mins=new_min;tree_node_list[pno].maxs=new_max;
        tree_node_list[pno].update_size();
        #tell p's father the boundary change
        father=tree_node_list[pno].father;
        if (not(father==-1)):
            for i in range(tree_node_list[father].branch_num):
                if (tree_node_list[father].branch[i].link==pno):
                    tree_node_list[father].branch[i].mins=new_min;
                    tree_node_list[father].branch[i].maxs=new_max;
                    tree_node_list[father].branch[i].size=tree_node_list[pno].size;
                    break;
        #p needs splitting
        p=tree_node_list[pno];
        while (p.branch_num==M+1):
            #choose the two branches will cause max extra cost if they are in one group
            max_waste=0;
            max_branch_1=-1;
            max_branch_2=-1;
            for i in range(p.branch_num):
                for j in range(i+1,p.branch_num):
                    #boundary of branch i and j
                    new_min=get_min(p.branch[i].mins,p.branch[j].mins);
                    new_max=get_max(p.branch[i].maxs,p.branch[j].maxs);
                    waste_size=get_size(new_min,new_max)-get_size(p.branch[i].mins,p.branch[i].maxs)-get_size(p.branch[j].mins,p.branch[j].maxs);
                    if (waste_size>max_waste):
                        max_branch_1=i;
                        max_branch_2=j;
                        max_waste=waste_size;
            #initialize group 1 node and group 2 node
            group_1_node=tree_node_list[add_tree_node()];
            group_1_node.branch_num=1;
            group_1_node.branch.append(p.branch[max_branch_1]);
            if (not (p.level==0)): tree_node_list[p.branch[max_branch_1].link].father=group_1_node.no;
            group_1_node.level=p.level;
            group_1_node.father=p.father;
            group_1_node.mins=p.branch[max_branch_1].mins;
            group_1_node.maxs=p.branch[max_branch_1].maxs;
            group_1_node.update_size();
            group_2_node=tree_node_list[add_tree_node()];
            group_2_node.branch_num=1;
            group_2_node.branch.append(p.branch[max_branch_2]);
            if (not (p.level==0)): tree_node_list[p.branch[max_branch_2].link].father=group_2_node.no;
            group_2_node.level=p.level;
            group_2_node.father=p.father;
            group_2_node.mins=p.branch[max_branch_2].mins;
            group_2_node.maxs=p.branch[max_branch_2].maxs;
            group_2_node.update_size();
            #move the chosen branch to location 0 and 1
            t=p.branch[0];p.branch[0]=p.branch[max_branch_1];p.branch[max_branch_1]=t;
            t=p.branch[1];p.branch[1]=p.branch[max_branch_2];p.branch[max_branch_2]=t;
            #for the rest branches, each time choose one branch and choose one group to add the branch
            for i in range(2,p.branch_num):
                #if the rest branches should all add to group x to satisfy the minimum m
                if (group_1_node.branch_num+p.branch_num-i==m):
                    choose_group=1;
                elif (group_2_node.branch_num+p.branch_num-i==m):
                    choose_group=2;
                else:
                    #choose the branch which cause greatest size difference when added to group 1 and 2 
                    max=-1;
                    for j in range(i,p.branch_num):
                        #boundary of group 1 and branch j;boundary of group 2 and branch j
                        new_1_min=get_min(group_1_node.mins,p.branch[i].mins);
                        new_1_max=get_max(group_1_node.maxs,p.branch[i].maxs);
                        new_2_min=get_min(group_2_node.mins,p.branch[j].mins);
                        new_2_max=get_max(group_2_node.maxs,p.branch[j].maxs);
                        #d1=extra size when add to group 1;d2
                        d1=get_size(new_1_min,new_1_max)-group_1_node.size;
                        d2=get_size(new_2_min,new_2_max)-group_2_node.size;
                        if (abs(d1-d2)>max):
                            max=abs(d1-d2);
                            choosebranch=j;
                            if (d1<d2):
                                choose_group=1;
                            else:
                                choose_group=2;
                    #move to i
                    t=p.branch[i];p.branch[i]=p.branch[choosebranch];p.branch[choosebranch]=t;
                if (choose_group==1):
                    group_1_node.branch.append(p.branch[i]);
                    group_1_node.branch_num=group_1_node.branch_num+1;
                    group_1_node.mins=get_min(group_1_node.mins,p.branch[i].mins);
                    group_1_node.maxs=get_max(group_1_node.maxs,p.branch[i].maxs);
                    group_1_node.update_size();
                    if (not (p.level==0)):
                        tree_node_list[p.branch[i].link].father=group_1_node.no;
                else:
                    group_2_node.branch.append(p.branch[i]);
                    group_2_node.branch_num=group_2_node.branch_num+1;
                    group_2_node.mins=get_min(group_2_node.mins,p.branch[i].mins);
                    group_2_node.maxs=get_max(group_2_node.maxs,p.branch[i].maxs);
                    group_2_node.update_size();
                    if (not (p.level==0)):
                        tree_node_list[p.branch[i].link].father=group_2_node.no;
            #split the head,add new node as head
            if (p.father==-1):
                head=add_tree_node();
                tree_node_list[head].level=p.level+1;
                tree_node_list[head].branch.append(r_branch(dim,group_1_node.mins,group_1_node.maxs,group_1_node.no,-1));
                tree_node_list[head].branch.append(r_branch(dim,group_2_node.mins,group_2_node.maxs,group_2_node.no,-1));
                tree_node_list[head].mins=get_min(group_1_node.mins,group_2_node.mins);
                tree_node_list[head].maxs=get_max(group_1_node.maxs,group_2_node.maxs);
                tree_node_list[head].update_size();
                tree_node_list[head].branch_num=2;
                tree_node_list[group_1_node.no].father=head;
                tree_node_list[group_2_node.no].father=head;
                p=tree_node_list[head];
            else:
                father=p.father;
                for i in range(tree_node_list[father].branch_num):
                    if (tree_node_list[father].branch[i].link==p.no):
                        tree_node_list[father].branch[i].mins=group_1_node.mins;
                        tree_node_list[father].branch[i].maxs=group_1_node.maxs;
                        tree_node_list[father].branch[i].link=group_1_node.no;
                        tree_node_list[father].branch[i].size=group_1_node.size;
                        new_branch=r_branch(dim,group_2_node.mins,group_2_node.maxs,group_2_node.no,-1);
                        tree_node_list[father].branch.append(new_branch);
                        tree_node_list[father].mins=get_min(tree_node_list[father].mins,group_2_node.mins);
                        tree_node_list[father].mins=get_min(tree_node_list[father].mins,group_2_node.mins);
                        tree_node_list[father].maxs=get_max(tree_node_list[father].maxs,group_2_node.maxs);
                        tree_node_list[father].maxs=get_max(tree_node_list[father].maxs,group_2_node.maxs);
                        tree_node_list[father].update_size();
                        tree_node_list[father].branch_num=tree_node_list[father].branch_num+1;
                        break;
                p=tree_node_list[father];

def find(node,mins,maxs):
    ser_node=[];ser_branch=[];
    for i in range(node.branch_num):
        if cross(node.branch[i].mins,node.branch[i].maxs,mins,maxs):
            if (node.level==0):
                if overlap(mins,maxs,node.branch[i].mins,node.branch[i].maxs):
                    ser_node.append(node.no);
                    ser_branch.append(i);
            else:
                [a,b]=find(tree_node_list[node.branch[i].link],mins,maxs);
                ser_node=ser_node+a;
                ser_branch=ser_branch+b;
    return [ser_node,ser_branch];
            
        
def overlap(min1,max1,min2,max2):
    flag=True;
    for i in range(dim):
        if (not((min1[i]<=min2[i]) and (max1[i]>=max2[i]))):
            flag=False;
            break;
    return flag;

def search(mins,maxs):
    global head;
    if (head==-1):
        return [[],[]];
    else:
        if (cross(tree_node_list[head].mins,tree_node_list[head].maxs,mins,maxs)):
            return find(tree_node_list[head],mins,maxs);
        else:
            return [[],[]];

def delete(mins,maxs):
    ser_node=-1;
    ser_branch=-1;
    search(mins,maxs);
    if (ser_node==-1):
        return -1;
    else:
        p=tree_node_list[ser_node];
        p.branch.remove(p.branch[ser_branch]);
        p.branch_num=p.branch_num-1;
        container=[];
        while (p.branch_num<m):
            if (not (p.no==head)):
                for i in range(p.branch_num):
                    container.append(p.branch[i]);
                father=tree_node_list[p.fa];
                for i in range(father.branch_num):
                    if (father.branch[i].link==p.no):
                        father.remove(father.branch[i]);
                        break;
                p=father;
        if (p.no==head):
            if (p.branch_num==1):
                head=p.branch[0];
            elif (p.branch_num==0):
                head=-1;
        return 0;

class globl():
    mins=[];
    maxs=[];
    ip=-1;
    id=-1;
    def __init__(self):
        self.mins=[];
        self.maxs=[];
        self.ip=-1;

def choose_publish_node(node_num,should_publish):
    global tree_node_list,wait_for_publish;
    if (should_publish==1):
        tree_node_list[node_num].publish_flag=True;
        publish_node=globl();
        publish_node.mins=tree_node_list[node_num].mins;
        publish_node.maxs=tree_node_list[node_num].maxs;
        publish_node.ip=node_num;
        wait_for_publish.append(publish_node);
    else:
        for i in range(tree_node_list[node_num].branch_num):
            if (tree_node_list[node_num].level<=2):
                choose_publish_node(tree_node_list[node_num].branch[i].link,1);
            else:
                choose_publish_node(tree_node_list[node_num].branch[i].link,0);
            
def buildIndex(): 
    #build local index
    global dim,M,m,tree_node_list,tree_node_num,head,wait_for_build,wait_for_publish,ser_node,ser_branch,data_for_build;
    global route;
    global range_min,range_max,pir;
    global PORT,HOSTTABLE;
    global prt_info,server_no;
    dim=2;
    tree_node_list=[];
    tree_node_num=0;
    head=-1;
    for i in range(len(wait_for_build)):
        insert(wait_for_build[i],wait_for_build[i],data_for_build[i],-1,0);
        if (i%3000==0):
            print (i);
    data='total '+str(len(tree_node_list))+' nodes';
    print (data);
    if (prt_info):
        for i in range(len(wait_for_build)):
            [ser_node,ser_branch]=search(wait_for_build[i],wait_for_build[i]);
            if (len(ser_node)==0):
                data='insert error';
                print ('%s\r\n' %data);
            else:
                data=str(tree_node_list[ser_node[0]].branch[ser_branch[0]].data);
                print ('%s\r\n' %data);
    wait_for_publish=[];
    if (head==-1):
        wait_for_publish=[];
    elif (tree_node_list[head].level==0):
        publish_node=globl();
        publish_node.mins=tree_node_list[head].mins;
        publish_node.maxs=tree_node_list[head].maxs;
        wait_for_publish.append(publish_node);
    else:
        choose_publish_node(head,0);

    #build global index 
    for i in range(len(wait_for_publish)):
        mins=wait_for_publish[i].mins;
        maxs=wait_for_publish[i].maxs;
        r_of_rect=0;
        for j in range(dim):
            r_of_rect=r_of_rect+(maxs[j]-mins[j])*(maxs[j]-mins[j]);
        r_of_rect=r_of_rect**0.5;
        r_of_rect=r_of_rect*0.5;
        if (r_of_rect<rmax):
            new_rmax=0.001;
        else:
            new_rmax=r_of_rect;
        center=[];
        for j in range(dim):
            center.append((mins[j]+maxs[j])/2);
        publish_to=[];
        for j in range(total_server):
            if cross_rect_sphere(pir[j].mins, pir[j].maxs, center, new_rmax):
                publish_to.append(j);
        for j in range(len(publish_to)):
            data='send ';
            data=data+str(publish_to[j])+' ';
            data=data+'publish ';
            for k in range(dim):
                data=data+str(mins[k])+' ';
            for k in range(dim):
                data=data+str(maxs[k])+' ';
            data=data+str(server_no);
            to_server=route[publish_to[j]];

            (tcpCliSock,num)=apply_socket(to_server,PORT);
            tcpCliSock.send( '%s\r\n' %data) 
            if (prt_info): print ('publish a node to ',to_server) 
            dispose_socket(to_server,num);
     

class pir_type:
    mins=[];
    maxs=[];
    def __init__(self,mins=[],maxs=[]):
        self.mins=mins;
        self.maxs=maxs;

def gen_pir():
    global pir,dim,h,range_min,range_max;   
    pir=[pir_type([0 for j in range(dim)],[0 for j in range(dim)]) for i in range(total_server*(h+1))];
    ser_no=0;
    loc=[0 for i in range(dim)];
    step=[];
    for i in range(dim):
        step.append((range_max[i]-range_min[i])/h);
    while (loc[0]<h):
        for i in range(dim):
            pir[ser_no].mins[i]=range_min[i]+step[i]*loc[i];
            pir[ser_no].maxs[i]=pir[ser_no].mins[i]+step[i];
        j=dim-1;
        while ((loc[j]==h-1) and (j>0)):
            j=j-1;
        loc[j]=loc[j]+1;
        for i in range(j+1,dim):
            loc[i]=0;
        ser_no=ser_no+1;

def cross(min1,max1,min2,max2):
    two_rect_cross=True;
    for i in range(len(min1)):
        if not ((min1[i]<=min2[i]) and (min2[i]<=max1[i]) and (max1[i]<=max2[i]) or (min2[i]<=min1[i]) and (min1[i]<=max2[i]) and (max2[i]<=max1[i])):
            if not ((min1[i]<=min2[i]) and (max2[i]<=max1[i]) or (min2[i]<=min1[i]) and (max1[i]<=max2[i])):
                two_rect_cross=False;
                break;
    return two_rect_cross;

def ms_to_loc(ms):
    loc=[];
    for i in range(dim):
        loc=[(ms%h)]+loc;
        ms=ms//h;
    return (loc);

def cross_rect_sphere(mins,maxs,center,r):
    global dim;
    cross_pt=0;
    distance=0;
    for i in range(dim):
        if (center[i]<mins[i]):
            cross_pt=mins[i];
        elif (center[i]>maxs[i]):
            cross_pt=maxs[i];
        else:
            cross_pt=center[i];
        distance=distance+(center[i]-cross_pt)*(center[i]-cross_pt);
    if (distance<=r*r): 
        return True;
    else:
        return False;
    
def routing():
    global server_no,h,pir,route;
    min_distance=[-1 for i in range(total_server)];
    route=[0 for i in range(total_server)];
    loc=ms_to_loc(server_no);
    for ser in range(total_server):
        if (ser==server_no):
            route[ser]=ser;
        else:
            ser_center=[];
            for i in range(dim):
                ser_center.append((pir[ser].mins[i]+pir[ser].maxs[i])/2);
            for i in range(dim):
                my_loc=loc[i];
                jump_pir=1;
                step_this_dim=h**(dim-i-1);
                while (jump_pir<=h):
                    if (my_loc-jump_pir>=0):
                        new_ser=server_no-jump_pir*step_this_dim;
                        dis=0;
                        for j in range(dim):
                            dis=dis+(ser_center[j]-(pir[new_ser].mins[j]+pir[new_ser].maxs[j])*0.5)*(ser_center[j]-(pir[new_ser].mins[j]+pir[new_ser].maxs[j])*0.5);
                        if (dis<min_distance[ser]) or (min_distance[ser]==-1):
                            min_distance[ser]=dis;
                            route[ser]=new_ser;
                    if (my_loc+jump_pir<h):
                        new_ser=server_no+jump_pir*step_this_dim;
                        dis=0;
                        for j in range(dim):
                            dis=dis+(ser_center[j]-(pir[new_ser].mins[j]+pir[new_ser].maxs[j])*0.5)*(ser_center[j]-(pir[new_ser].mins[j]+pir[new_ser].maxs[j])*0.5);
                        if (dis<min_distance[ser]) or (min_distance[ser]==-1):
                            min_distance[ser]=dis;
                            route[ser]=new_ser;
                    jump_pir=jump_pir*2;
        #print('route to ',ser,' = ', route[ser]);

def init_socket(PORT):
    global total_server_extra;
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

def query_thread(tcpCliSock,to_server,num):
    global prt_info,query_exist,total_query;
    data2 = tcpCliSock.recv(BUFSIZE) 
    dispose_socket(to_server,num); 
    print ('%s\r\n'%data2);
    query_exist=query_exist-1;
    total_query=total_query+1;

def del_socket():
    global total_server;
    global sockets,sockets_use;
    for i in range(total_server):
        for j in range(len(sockets[i])):
            sockets[i][j].close();

class MyRequestHandler(StreamRequestHandler):
    def handle(self):
        global thread_exist,max_thread,prt_info;        
        sleep_time=0.00001;        
        while True:            
            self.data=self.rfile.readline();   
            self.data=self.data.strip();            
            if (self.data):                
                sleep_time=sleep_time/16;
                if (prt_info): print (thread_exist);        
                if (thread_exist>=max_thread):            
                    if (self.data[0]=='q'):            
                        self.wfile.write('Fail');            
                    else:            
                        self.wfile.write(' ');      
                    continue;    
                thread_exist=thread_exist+1;                
                thread.start_new_thread(self.my_handler,(self.data,1));           
            else:                
                sleep_time=sleep_time*2;                
                time.sleep(sleep_time);
            
    def my_handler(self,data,tmp):
        BUFSIZE=1024;
        global server_no,route,wait_for_build;
        global global_index;
        global pir;
        global prt_info;
        global server_no;
        global thread_exist;
        global max_load;
        #if (prt_info): print ('...connected from:' , self.client_address);
        if (prt_info): print (data);
        if data:
            msg_split=data.split()
            should_handle=True;
            if (not cmp('send',msg_split[0])) or (not cmp('send_feedback',msg_split[0])):
                to_server=int(msg_split[1]);
                if (to_server==server_no):
                    should_handle=True;
                    msg_split.pop(0);
                    msg_split.pop(0);
                    if (prt_info): print (msg_split);
                else:
                    should_handle=False;
                    to_server=route[to_server];
                    (tcpCliSock,num)=apply_socket(to_server,PORT); 
                    tcpCliSock.send( '%s\r\n' %data) 
                    if (prt_info): print ('redirect to ',to_server,data) 
                    if (not cmp('send_feedback',msg_split[0])):
                        data2=tcpCliSock.recv(BUFSIZE);
                        if (prt_info): print('get feedback from ',to_server,' :',data2);
                        self.wfile.write(data2);
                    dispose_socket(to_server,num);
            if not(should_handle):
                thread_exist=thread_exist-1;
                return;
            
            if not cmp('publish',msg_split[0]):
                new_global_index=globl();
                for i in range(dim):
                    new_global_index.mins.append(float(msg_split[i+1]));
                for i in range(dim):
                    new_global_index.maxs.append(float(msg_split[dim+i+1]));
                new_global_index.ip=msg_split[dim+dim+1];
                global_index.append(new_global_index);
             
            if not cmp('test',msg_split[0]):
                self.wfile.write('server '+str(server_no)+':Received') 

            if not cmp('query_local',msg_split[0]):
                mins=[];
                maxs=[];
                for i in range(dim):
                    mins.append(float(msg_split[i+1]));
                for i in range(dim):
                    maxs.append(float(msg_split[dim+i+1]));
                ser_node=[];ser_branch=[];
                search(mins,maxs);
                if (prt_info): print (ser_node);
                if (len(ser_node)==0):
                    self.wfile.write('not found at server '+str(server_no));
                else:
                    data='';
                    for cnt in range(len(ser_node)):
                        data=data+' '+str(tree_node_list[ser_node[cnt]].branch[ser_branch[cnt]].data);
                    self.wfile.write(data);
            
            if not cmp('query',msg_split[0]):
                mins=[];maxs=[];
                for i in range(dim):
                    mins.append(float(msg_split[i+1]));
                for i in range(dim):
                    maxs.append(float(msg_split[dim+i+1]));
                
                r_of_rect=0;
                for i in range(dim):
                    r_of_rect=r_of_rect+(maxs[i]-mins[i])*(maxs[i]-mins[i]);
                r_of_rect=r_of_rect**0.5;
                r_of_rect=r_of_rect*0.5;
                r_of_rect=r_of_rect+rmax;
                
                center=[];
                for i in range(dim):
                    center.append((mins[i]+maxs[i])/2);
                
                get_global_servers=[];
                for i in range(total_server):
                    if cross_rect_sphere(pir[i].mins, pir[i].maxs, center, r_of_rect):
                        get_global_servers.append(i);
                back_ips=[];
                for i in range(len(get_global_servers)):
                    to_server=get_global_servers[i];
                    data='send_feedback '+str(to_server)+' ';
                    data=data+'get_global';
                    for j in range(dim):
                        data=data+' '+str(mins[j]);
                    for j in range(dim):
                        data=data+' '+str(maxs[j]);         
                    (tcpCliSock,num)=apply_socket(route[to_server],PORT);
                    tcpCliSock.send( '%s\r\n' %data);
                    if (prt_info): print (route[to_server],' ' ,data)
                    back_ips_str=tcpCliSock.recv(BUFSIZE);
                    back_ips=back_ips+back_ips_str.split();
                    dispose_socket(route[to_server],num);

                if (prt_info): print ('back_ips=',back_ips);
                ips_search=[False for i in range(total_server)];
                for i in range(len(back_ips)):
                    ips_search[int(back_ips[i])]=True;
                success=False;
                value='';
                for i in range(total_server):
                    server_ip=i;
                    if (ips_search[i]==False):
                        continue;
                    data='send_feedback '+str(server_ip)+' ';
                    data=data+'find_local';
                    for j in range(dim):
                        data=data+' '+str(mins[j]);
                    for j in range(dim):
                        data=data+' '+str(maxs[j]);
                    (tcpCliSock,num)=apply_socket(route[server_ip],PORT);
                    tcpCliSock.send( '%s\r\n' %data);
                    if (prt_info): print (data);
                    result=tcpCliSock.recv(BUFSIZE);
                    if (prt_info): print('Server ',server_ip,' return: ',result);
                    dispose_socket(route[server_ip],num);
                    if not(result==' '):
                        value=value+result;
                        success=True;
                if (success==True):
                    self.wfile.write('%s'%value[:1000]);
                else:
                    value='Fail to find the value of input key';
                    self.wfile.write('%s'%value);
                 
            if not cmp('find_local',msg_split[0]):
                mins=[];
                maxs=[];
                for i in range(dim):
                    mins.append(float(msg_split[i+1]));
                for i in range(dim):
                    maxs.append(float(msg_split[dim+i+1]));
                [ser_node,ser_branch]=search(mins,maxs);
                if (len(ser_node)==0):
                    data=' ';
                else:
                    data=' ';
                    for cnt in range(len(ser_node)):
                        data=data+' '+str(tree_node_list[ser_node[cnt]].branch[ser_branch[cnt]].data);   
                    if (prt_info): print ('found: ',data); 
                self.wfile.write(data[:1000]);
                    
            if not cmp('get_global',msg_split[0]):
                mins=[];
                maxs=[];
                for i in range(dim):
                    mins.append(float(msg_split[i+1]));
                for i in range(dim):
                    maxs.append(float(msg_split[i+dim+1]));
                back_ips=' ';
                if (prt_info): print ('This representative has ',len(global_index),' global index, Following cross(GI,query_rg):')
                for i in range(len(global_index)):
                    if cross(global_index[i].mins,global_index[i].maxs,mins,maxs):
                        if (prt_info): print ('GI=',global_index[i].mins,global_index[i].maxs);
                        if len(back_ips)<990:
                            back_ips=back_ips+' '+str(global_index[i].ip);
                if (prt_info): print ('GI end');
                if (prt_info): print ('back_ips return= ',back_ips);
                self.wfile.write(back_ips);        
                
            if not cmp('store',msg_split[0]):
                key=[];
                fp=open('../input.txt','r');
                a=fp.readline();
                a=a.strip();
                while (a!=''):
                    if (len(wait_for_build)>=max_load):
                        break;
                    read_split=a.split();
                    wait_for_build.append([]);
                    loc=len(wait_for_build)-1;
                    for i in range(dim):
                        wait_for_build[loc].append(float(read_split[i]));
                    a=fp.readline();
                    a=a.strip();
                    
                fp.close();
                data_num=len(wait_for_build);
                for i in range(data_num):
                    data_for_build.append(float(i+server_no*data_num));

                print ('store complete')
            
            if not cmp('set_load',msg_split[0]):
                max_load=int(msg_split[1]);
                self.wfile.write('server '+str(server_no)+' set load to '+msg_split[1]);
             
            if not cmp('build',msg_split[0]):
                if (prt_info): print ('start create r-tree and send publish request')
                buildIndex();
                if (prt_info): print ('build finish')
                self.wfile.write('server '+str(server_no)+' build r-tree and publish global index finish');
            
            if not cmp('display_global_index',msg_split[0]):
                for i in range(len(global_index)):
                    data='';
                    for j in range(dim):
                        data=data+str(global_index[i].mins[j])+' ';
                    for j in range(dim):
                        data=data+str(global_index[i].maxs[j])+' ';
                    data=data+'ip='+str(global_index[i].ip);
                    print ('%s\r\n'%data);
                    #print ('hash=',bloom_filter[i]);
                print ('total '+str(len(global_index))+' GIs');
            
            if not cmp('get_global_index_num',msg_split[0]):
                self.wfile.write(str(len(global_index)));
        
            if not cmp('print',msg_split[0]):
                prt_info=True;
                self.wfile.write('Server '+str(server_no)+' start to print info');
    
            if not cmp('hide',msg_split[0]):
                prt_info=False;
                self.wfile.write('Server '+str(server_no)+' start to hide info');   
          
            if not cmp('exit',msg_split[0]):
                del_socket();
                self.wfile.write('Server '+str(server_no)+' exit');          
                sys.exit();
        thread_exist=thread_exist-1;


def server_thread(a,b):
    a.serve_forever();

def get_local_ip(ifname = 'eth0'):  
    import socket, fcntl, struct  
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)  
    inet = fcntl.ioctl(s.fileno(), 0x8915, struct.pack('256s', ifname[:15]))  
    ret = socket.inet_ntoa(inet[20:24])  
    return ret  

def get_server_ip():
    global HOSTTABLE,server_no,PORT;
    fp=open('../public_ip.txt','r');
    local_ip=fp.readline();
    local_ip=local_ip.strip();
    fp.close();
    #local_ip=get_local_ip();
    #get server ip
    fp=open('../server_ip.txt','r');
    a=fp.readline();
    a=a.strip();
    a_split=a.split();
    for i in range(total_server):
        if (a_split[i]==local_ip):
            server_no=i;
        HOSTTABLE.append(a_split[i]);
        data='server '+str(i)+' ip= '+a_split[i];
        print (data);
    a=fp.readline();
    a=a.strip();
    PORT=int(a);
    fp.close();
    data='This is server '+str(server_no)+' PORT= '+str(PORT);
    print (data);
    fp=open('../server_ip.txt','w');
    for i in range(total_server):
        fp.write(HOSTTABLE[i]);
        if (i!=total_server-1):
            fp.write(' ');
        else:
            fp.write('\n');
    fp.write(str(PORT+1));
    fp.write('\n');
    fp.close();

if __name__ == "__main__":
    global global_index;
    global data_for_build;
    global pir;
    global PORT;
    get_server_ip();
    gen_pir();
    data_for_build=[];
    global_index=[];
    wait_for_build=[];
    routing();
    init_socket(PORT);
    ADDR=(get_local_ip(),PORT);
    tcpServ = ThreadingTCPServer(ADDR, MyRequestHandler) 
    if (prt_info): print ('waiting for connection...') 
    thread.start_new_thread(server_thread,(tcpServ,1));
    BUFSIZE = 1024   
 
    ADDR=(HOSTTABLE[0],PORT) 
    while True:
	data = 0; 
        #data = input('> ')
        #data = 'send_feedback '+str(server_no)+' '+data;
        #(tcpCliSock,num)=apply_socket(server_no,PORT);
        #tcpCliSock.send( '%s\r\n' %data);
        #if (prt_info): print('feedback=',tcpCliSock.recv(BUFSIZE));
        #dispose_socket(server_no,num);
  
