#include "ServerSocket.h"
#include "ClientSocket.h"
#include "SocketException.h"
#include <stdio.h>
#include <cstdlib>
#include <sys/time.h>
#include <pthread.h>
#include <iostream>
#include <string>
#include <sstream>
#include <fstream>
#include <cmath>
#include <cstring>
#include "RTree.h"

#include <sys/types.h>
#include <ifaddrs.h>
#include <netinet/in.h> 
#include <arpa/inet.h>

const int MAX_SOCKET_NUM = 50;
int BASE_PORT;
const int STORE_NUM = 100;
const int H = 0;
const int DIM = 2;
const int TWODIM_H = H - DIM + 2;
const bool BLOOM_FILTER_STATE=true;
int TOTAL_SERVER = pow(4,H);
int TWODIM_SERVER = pow(4, TWODIM_H);
int MAX_PUBLISH_NUM = 10000;
bool PRT_INFO = true;
bool TEST_FLAG = true;
bool TEST_MODE = false;
const float range_min[DIM] = { 0,0 };
const float range_max[DIM] = { 7500,7500 };
int SERVER_NO;
char** server_ip;

class my_clock
{
public:
	my_clock()
	{
		gettimeofday(&start_time, 0);
	}
	bool SubTimeval(timeval &result, timeval &begin, timeval &end)
	{
		if (begin.tv_sec>end.tv_sec) return false;

		if ((begin.tv_sec == end.tv_sec) && (begin.tv_usec > end.tv_usec))
			return false;

		result.tv_sec = (end.tv_sec - begin.tv_sec);
		result.tv_usec = (end.tv_usec - begin.tv_usec);

		if (result.tv_usec<0) {
			result.tv_sec--;
			result.tv_usec += 1000000;
		}

		return true;
	}
	double get_time()
	{
		timeval finish_time, result;
		gettimeofday(&finish_time, 0);
		SubTimeval(result, start_time, finish_time);
		return result.tv_sec + result.tv_usec*0.000001;
	}
private:
	timeval start_time;
};

my_clock global_clock;

pthread_mutex_t publish_mutex;
struct Socketpool {
	int num;
	ClientSocket* socket;
	bool* used;
	pthread_mutex_t mutex;
};
Socketpool* socketpool;

Socketpool* init_socket(int total_server, char** server_ip, int base_port)
{
	Socketpool* socketpool = new Socketpool[total_server];
	int i, j;
	for (i = 0; i < total_server; i++)
	{
		socketpool[i].socket = new ClientSocket[MAX_SOCKET_NUM];
		socketpool[i].used = new bool[MAX_SOCKET_NUM];
		socketpool[i].num = 0;
		pthread_mutex_init(&socketpool[i].mutex, NULL);
	}
	return socketpool;
}

int apply_socket(Socketpool* socketpool, int server_no)
{
	int i;
	int unused_socket_id = -1;
	pthread_mutex_lock(&socketpool[server_no].mutex);
	std::cout << "locked" << std::endl;
	for (i = 0; i < socketpool[server_no].num; i++)
	{
		if (not socketpool[server_no].used[i])
		{
			unused_socket_id = i;
			break;
		}
	}
	std::cout << "unused_socket_id=" << unused_socket_id << std::endl;
	if (unused_socket_id != -1)
	{
		if (PRT_INFO) std::cout << global_clock.get_time() << "socket[" << server_no << "][" << unused_socket_id << "] in use" << std::endl;
		socketpool[server_no].used[unused_socket_id] = true;
		pthread_mutex_unlock(&socketpool[server_no].mutex);
		return unused_socket_id;
	}
	else
	{
		std::string reply;
		std::cout << "connect to " << server_ip[server_no] << " port= " << BASE_PORT << std::endl;
		ClientSocket* new_sock = new ClientSocket(server_ip[server_no], BASE_PORT);
		socketpool[server_no].socket[socketpool[server_no].num] = *new_sock;
		socketpool[server_no].socket[socketpool[server_no].num] << "new_socket";
		socketpool[server_no].socket[socketpool[server_no].num] >> reply;
		socketpool[server_no].used[socketpool[server_no].num] = true;
		socketpool[server_no].num++;
		if (PRT_INFO) std::cout << global_clock.get_time() << "socket[" << server_no << "][" << socketpool[server_no].num - 1 << "] in use (new socket)" << std::endl;
		pthread_mutex_unlock(&socketpool[server_no].mutex);
		return socketpool[server_no].num - 1;
	}
}

void free_socket(Socketpool* socketpool, int server_no, int socket_id)
{
	socketpool[server_no].used[socket_id] = false;
	if (PRT_INFO) std::cout << global_clock.get_time() << " socket[" << server_no << "][" << socket_id << "] free" << std::endl;
}
std::string send_msg(Socketpool* socketpool, std::string data, int server_no)
{
	int socket_id;
	std::string reply;
	std::cout << "start_apply_socket" << std::endl;
	socket_id = apply_socket(socketpool, server_no);
	if (PRT_INFO) std::cout << global_clock.get_time() << " socket[" << server_no << "][" << socket_id << "]" << " send \"" << data << "\"" << std::endl;
	socketpool[server_no].socket[socket_id] << data;
	socketpool[server_no].socket[socket_id] >> reply;

	if (PRT_INFO) std::cout << global_clock.get_time() << " socket[" << server_no << "][" << socket_id << "]" << " receive \"" << reply << "\"" << std::endl;
	free_socket(socketpool, server_no, socket_id);
	return reply;
}

struct SomeThing
{
	float m_min[DIM];
	float m_max[DIM];
};

struct Rect {
	float mins[DIM];
	float maxs[DIM];
};

struct Rect2 {
	float mins[2];
	float maxs[2];
};
Rect2* pir=new Rect2[TOTAL_SERVER*H];

SomeThing query_file[STORE_NUM];



std::string itoa(const int &int_temp)
{
	std::stringstream stream;
	stream << int_temp;
	std::string st;
	stream >> st;
	return st;
}

std::string ftoa(const float &int_temp)
{
	std::stringstream stream;
	stream << int_temp;
	std::string st;
	stream >> st;
	return st;
}

std::string get_next_item_str(std::string &st)
{
	int spaceloc;
	spaceloc = st.find(" ");
	std::string first;
	if (spaceloc != -1)
	{
		first = st.substr(0, spaceloc);
		st.erase(0, spaceloc + 1);
	}
	else
	{
		first = st.substr(0, st.length());
		st = "";
	}
	return first;
}

const char* get_next_item(std::string &st)
{
	int spaceloc;
	spaceloc = st.find(" ");
	const char* cstr;
	if (spaceloc != -1)
	{
		cstr = st.substr(0, spaceloc).c_str();
		st.erase(0, spaceloc + 1);
	}
	else
	{
		cstr = st.substr(0,st.length()).c_str();
		st = "";
	}
	return cstr;
}

std::string display(float* a, int num)
{
	std::string data;
	data = "[";
	for (int i = 0; i < num; i++)
		if (i != num - 1)
		{
			data += ftoa(a[i]);
			data += ",";
		}
		else
			data += ftoa(a[i]);
	data += "]";
	return data;
}

std::string displayi(int* a, int num)
{
	std::string data;
	data = "[";
	for (int i = 0; i < num; i++)
		if (i != num - 1)
		{
			data += itoa(a[i]);
			data += ",";
		}
		else
			data += itoa(a[i]);
	data += "]";
	return data;
}

typedef RTree<SomeThing*, float, DIM> rtree_type;
rtree_type rtree;

struct publish_node_type
{
	float mins[DIM];
	float maxs[DIM];
	rtree_type::Node* ip;
	int key_num;
};
publish_node_type* wait_for_publish;
int publish_num = 0;

struct global_index_type
{
	float mins[DIM];
	float maxs[DIM];
	int ip;
	int key_num;
};
global_index_type* global_index = new global_index_type[1000];
int** bloom_filter = new int*[1000];
int* bloom_len = new int[1000];
int index_num = 0;
int max_index_num = 1000;
void choose_publish_node(rtree_type::Node* head, bool should_publish)
{
	int i = 0, cnt = 0;
	if (should_publish)
	{
		for (cnt = 0; cnt < head->m_count; cnt++)
		{
			for (i = 0; i < DIM; i++)
			{
				wait_for_publish[publish_num].mins[i] = head->m_branch[cnt].m_rect.m_min[i];
				wait_for_publish[publish_num].maxs[i] = head->m_branch[cnt].m_rect.m_max[i];
			}
			wait_for_publish[publish_num].ip = head->m_branch[cnt].m_child;
			publish_num++;
		}
	}
	else
		for (i = 0; i < head->m_count; i++)
			if (head->m_level <= 2)
				choose_publish_node(head->m_branch[i].m_child, true);
			else
				choose_publish_node(head->m_branch[i].m_child, false);
}

void count_publish_node(rtree_type::Node* head, bool should_publish)
{
	int i = 0;
	if (should_publish)
	{
		publish_num+=head->m_count;
	}
	else
		for (i = 0; i < head->m_count; i++)
			if (head->m_level <= 2)
			{
				count_publish_node(head->m_branch[i].m_child, true);
			}
			else
				count_publish_node(head->m_branch[i].m_child, false);
}

void ms_to_rep(int ms,int &repnum,int* rep)
{
	int four[TWODIM_H];
	int i,last,weight,no,j;
	for (i = 0; i < TWODIM_H; i++)
	{
		four[i] = ms % 4;
		ms /= 4;
	}
	if (ms == TWODIM_H)
		last = 0;
	else
		last = four[ms];
	repnum = 0;
	rep[0] = -1; rep[1] = -1; rep[2] = -1; rep[3] = -1;
	no = 0;
	for (i = 0; i < 4;i++)
		if (!(i == last) || (ms == TWODIM_H))
		{
			for (j = 0; j < ms; j++) four[j] = i;
			rep[repnum] = 0;
			repnum++;
			weight = 1;
			for (j = 0; j < TWODIM_H; j++)
			{
				rep[no] += weight*four[j];
				weight *= 4;
			}
			no++;
		}
	if (ms == 0) repnum = 1;
}

void ms_to_rep_high(int ms,int& repnum,int* rep)
{
	int four[H];
	int i, last, weight, no,j;
	for (i = 0; i < H; i++)
	{
		four[i] = ms % 4;
		ms /= 4;
	}
	if (ms == H)
		last = 0;
	else
		last = four[ms];
	repnum = 0;
	rep[0] = -1; rep[1] = -1; rep[2] = -1; rep[3] = -1;
	no = 0;
	for (i = 0; i < 4; i++)
		if (!(i == last) || (ms == H))
		{
			for (j = 0; j < ms; j++) four[j] = i;
			rep[repnum] = 0;
			repnum++;
			weight = 1;
			for (j = 0; j < H; j++)
			{
				rep[no] += weight*four[j];
				weight *= 4;
			}
			no++;
		}
	if (ms == 0) repnum = 1;
} 

bool cross(float* min1, float* max1, float* min2, float* max2)
{
	bool two_rect_cross = true;
	for (int i = 0; i < DIM;i++)
		if (!((min1[i]<=min2[i]) && (min2[i]<=max1[i]) && (max1[i]<=max2[i]) || (min2[i]<=min1[i]) && (min1[i]<=max2[i]) && (max2[i]<=max1[i])))
			if (!((min1[i] <= min2[i]) && (max2[i] <= max1[i]) || (min2[i] <= min1[i]) && (max1[i] <= max2[i])))
			{
				two_rect_cross = false;
				break;
			}
	return two_rect_cross;
}

bool cross2(float* min1, float* max1, float* min2, float* max2)
{
	bool two_rect_cross = true;
	for (int i = 0; i < 2; i++)
		if (!((min1[i] <= min2[i]) && (min2[i] <= max1[i]) && (max1[i] <= max2[i]) || (min2[i] <= min1[i]) && (min1[i] <= max2[i]) && (max2[i] <= max1[i])))
			if (!((min1[i] <= min2[i]) && (max2[i] <= max1[i]) || (min2[i] <= min1[i]) && (max1[i] <= max2[i])))
			{
				two_rect_cross = false;
				break;
			}
	return two_rect_cross;
}

bool cross1(float min1, float max1, float min2, float max2)
{
	bool two_rect_cross = true;
	if (!((min1 <= min2) && (min2 <= max1) && (max1 <= max2) || (min2 <= min1) && (min1 <= max2) && (max2 <= max1)))
		if (!((min1 <= min2) && (max2 <= max1) || (min2 <= min1) && (max1 <= max2)))
		{
			two_rect_cross = false;
		}
	return two_rect_cross;
}

bool overlap(float* min1, float* max1, float* min2, float* max2)
{
	for (int i = 0; i < DIM; i++)
		if (!((min1[i] <= min2[i]) && (max1[i] >= max2[i])))
			return false;
	return true;
}

bool overlap2(float* min1, float* max1, float* min2, float* max2)
{
	for (int i = 0; i < 2; i++)
		if (!((min1[i] <= min2[i]) && (max1[i] >= max2[i])))
		{
			return false;
		}
	return true;
}

bool overlap1(float min1, float max1, float min2, float max2)
{
	if (!((min1 <= min2) && (max1 >= max2)))
		return false;
	return true;
}

int just_cover(float* mins, float* maxs)
{
	int metaserver, layer, nowmetaserver,nextlayer_starter,step;
	bool nextlayer_overlap;
	metaserver = TWODIM_H*pow(4, TWODIM_H);
	layer = TWODIM_H;
	nowmetaserver = TWODIM_SERVER*TWODIM_H;
	nextlayer_overlap = true;
	while ((layer > 0) && (nextlayer_overlap))
	{
		nextlayer_starter =metaserver - TWODIM_SERVER;
		step = pow(4, layer - 1);
		nextlayer_overlap = false;
		if (overlap2(pir[nextlayer_starter].mins, pir[nextlayer_starter].maxs, mins, maxs))
		{
			metaserver = nextlayer_starter;
			nextlayer_overlap = true;
		}
		else if (overlap2(pir[nextlayer_starter + step].mins, pir[nextlayer_starter + step].maxs, mins, maxs))
		{
			metaserver = nextlayer_starter + step;
			nextlayer_overlap = true;

		}
		else if (overlap2(pir[nextlayer_starter + step + step].mins, pir[nextlayer_starter + step + step].maxs, mins, maxs))
		{
			metaserver = nextlayer_starter + step + step;
			nextlayer_overlap = true;
		}
		else if (overlap2(pir[nextlayer_starter + step + step + step].mins, pir[nextlayer_starter + step + step + step].maxs, mins, maxs))
		{
			metaserver = nextlayer_starter + step + step + step;
			nextlayer_overlap = true;
		}
		layer--;
	}
	return metaserver;
}

void get_rep_lower_layer(int metaserver, float* mins, float* maxs, int rep, int layer,int &rep_num,int* result)
{
	int meta_next_layer[4];
	int* repres=new int[4];
	int step, starter, i,repres_len,converted_meta,dim_to_judge,step_this_dim,which_rep;
	bool area_i;
	step = pow(4, layer - 1);
	starter = TOTAL_SERVER*(layer - 1) + (metaserver - TOTAL_SERVER*layer);
	for (i = 0; i < 4; i++)
		meta_next_layer[i] = starter + i*step;
	rep_num = 0;
	for (i = 0; i < 4; i++)
	{
		area_i = false;
		if (layer <= TWODIM_H)
		{
			converted_meta = meta_next_layer[i] % TWODIM_SERVER + TWODIM_SERVER*(layer - 1);
			area_i = cross2(mins, maxs, pir[converted_meta].mins, pir[converted_meta].maxs);
		}
		else
		{
			dim_to_judge = layer - TWODIM_H + 1;
			step_this_dim = float(range_max[dim_to_judge] - range_min[dim_to_judge]) / 4;
			area_i = cross1(mins[dim_to_judge], maxs[dim_to_judge], range_min[dim_to_judge] + step_this_dim*i, range_min[dim_to_judge] + step_this_dim*(i + 1));
		}
		if (area_i)
		{
			result[rep_num*3] = meta_next_layer[i];
			ms_to_rep_high(meta_next_layer[i],repres_len,repres);
			if (repres_len==1)
			{
				result[rep_num*3+1] = repres[0];
				result[rep_num*3+2] = rep;
			}
			else if (i > rep)
			{
				result[rep_num * 3 + 1] = repres[rep];
				result[rep_num * 3 + 2] = rep;
			}
			else if (i < rep)
			{
				result[rep_num * 3 + 1] = repres[rep - 1];
				result[rep_num * 3 + 2] = rep;
			}
			else
			{
				which_rep = rand() % repres_len;
				result[rep_num * 3 + 1] = repres[which_rep];
				if (which_rep >= i) result[rep_num * 3 + 2] = which_rep + 1; else result[rep_num * 3 + 2] = which_rep;
			}
			rep_num++;
		}
	}
	// result[]={meta,representative,relative rep location}
}



int* route;
void routing(int server_no)
{
	route = new int[TOTAL_SERVER];
	int i,j;
	int start_4[H], finish_4[H];
	for (i = 0; i < TOTAL_SERVER; i++) route[i] = 0;
	for (i = 0; i < H; i++)
	{
		start_4[H - 1 - i] = server_no % 4;
		server_no /= 4;
	}
	int finish_no_copy,pointer,first_dif_loc,orig_finish_4;
	bool dif_flag;
	for (i = 0; i < TOTAL_SERVER; i++)
	{
		if (i == server_no)
		{
			route[i] = i;
			continue;
		}
		finish_no_copy = i;
		for (j = 0; j < H; j++)
		{
			finish_4[H - 1 - j] = finish_no_copy % 4;
			finish_no_copy /= 4;
		}
		pointer = 0;
		first_dif_loc = -1;
		while (pointer != H - 1)
		{
			if (start_4[pointer] != finish_4[pointer])
			{
				if (first_dif_loc == -1)
				{
					first_dif_loc = pointer;
					orig_finish_4 = finish_4[pointer];
				}
				for (j = pointer + 1; j < H; j++) finish_4[j] = finish_4[pointer];
				finish_4[pointer] = start_4[pointer];
				pointer++;
			}
			else pointer++;
		}
		dif_flag = false;
		for (j = 0; j < H; j++)
			if (start_4[j] != finish_4[j]) dif_flag = true;
		if (!dif_flag)
		{
			for (j = first_dif_loc + 1; j < H; j++) finish_4[j] = start_4[first_dif_loc];
			finish_4[first_dif_loc] = orig_finish_4;
		}
		for (j = 0; j < H; j++) route[i] = route[i] * 4 + finish_4[j];
		if (TEST_FLAG) std::cout<<i<<" : "<<route[i]<<std::endl;
	}
}

struct key_type
{
	rtree_type::Node* node;
	int branch_num;
};
key_type* keys=new key_type[100];
int keycnt = 0;
int keymax = 100;
void get_keys(rtree_type::Node* head)
{
	for (int i = 0; i < head->m_count;i++)
		if (head->m_level == 0)
		{
			keys[keycnt].node = head;
			keys[keycnt].branch_num = i;
			keycnt++;
			if (keycnt >= keymax)
			{
				key_type* keys1 = new key_type[keymax * 2];
				for (int j = 0; j < keycnt; j++)
					keys1[j] = keys[j];
				delete []keys;
				keys = keys1;
				keymax *= 2;
			}
		}
		else
		{
			std::cout << head->m_level << " branch "<<i<<std::endl;
			get_keys(head->m_branch[i].m_child);
		}
}

#define ROT32(x, y) ((x << y) | (x >> (32 - y))) // avoid effor
int hash(const char *key, uint32_t seed,int capacity) {
	static const uint32_t c1 = 0xcc9e2d51;
	static const uint32_t c2 = 0x1b873593;
	static const uint32_t r1 = 15;
	static const uint32_t r2 = 13;
	static const uint32_t m = 5;
	static const uint32_t n = 0xe6546b64;

	uint32_t hash = seed;
	uint32_t len = strlen(key);
	const int nblocks = len / 4;
	const uint32_t *blocks = (const uint32_t *)key;
	int i;
	uint32_t k;
	for (i = 0; i < nblocks; i++) {
		k = blocks[i];
		k *= c1;
		k = ROT32(k, r1);
		k *= c2;

		hash ^= k;
		hash = ROT32(hash, r2) * m + n;
	}

	const uint8_t *tail = (const uint8_t *)(key + nblocks * 4);
	uint32_t k1 = 0;

	switch (len & 3) {
	case 3:
		k1 ^= tail[2] << 16;
	case 2:
		k1 ^= tail[1] << 8;
	case 1:
		k1 ^= tail[0];

		k1 *= c1;
		k1 = ROT32(k1, r1);
		k1 *= c2;
		hash ^= k1;
	}

	hash ^= len;
	hash ^= (hash >> 16);
	hash *= 0x85ebca6b;
	hash ^= (hash >> 13);
	hash *= 0xc2b2ae35;
	hash ^= (hash >> 16);

	return hash%capacity;
}

void test();

std::string perform(std::string command)
{
	int i,j;
	std::string data_ret,data;
	data_ret = ftoa(global_clock.get_time()) + " \"" + command + "\" received by server port " + itoa(BASE_PORT);
	if (PRT_INFO) std::cout << data_ret << std::endl;
	std::string data1;
	data1 = get_next_item_str(command);
	if (data1.compare("send_feedback") == 0)
	{
		int to_server = atoi(get_next_item(data));
		data_ret = send_msg(socketpool, data, to_server);
	}
	else if (data1.compare("store") == 0)
	{
		std::ifstream fin;
		fin.open("../input.txt", std::ios::in);

		std::string tmp;

		for (i = 0; i < STORE_NUM; i++)
		{
			if (PRT_INFO) std::cout << ftoa(global_clock.get_time()) << " load ";
			for (j = 0; j < DIM; j++)
			{
				fin >> tmp;
				query_file[i].m_min[j] = atof(tmp.c_str());
				query_file[i].m_max[j] = query_file[i].m_min[j];
				if (TEST_FLAG) std::cout << query_file[i].m_min[j] << " ";
			}
			std::cout << std::endl;
		}
		fin.close();
		std::cout << STORE_NUM << " point items loaded\n";
		data_ret=itoa(STORE_NUM);
	}
	else if (data1.compare("build") == 0)
	{
		float p;
		if (PRT_INFO) std::cout << global_clock.get_time() << " start build" << std::endl;
		for (i = 0; i < STORE_NUM; i++)
		{
			rtree.Insert(query_file[i].m_min, query_file[i].m_max, &query_file[i]);
			if (TEST_FLAG)
			{
				std::cout << "No. " << i << ' ';
				std::cout << display(query_file[i].m_min, DIM);
				std::cout << " ";
				std::cout << display(query_file[i].m_max, DIM);
				std::cout << " search result= " << rtree.Search(query_file[i].m_min, query_file[i].m_max) << std::endl;
			}
		}
		count_publish_node(rtree.m_root, false);
		std::cout << "Total publish node= " << publish_num << std::endl;
		wait_for_publish = new publish_node_type[publish_num];
		publish_num = 0;
		choose_publish_node(rtree.m_root, false);
		p = 0.001;
		int prime[12] = { 2,5,7,11,13,17,23,29,31,37,41,43 };
		int metaserver;
		int d, rep_step, len_new_rep, area_part, k;
		float area_step;
		int* representatives = new int[4];
		int* hashtable;
		int capacity, number_of_hash, key_len, len_rep, to_server, now_hash, global_index_no;
		std::string str_key;
		for (i = 0; i < publish_num; i++)
		{
			metaserver = just_cover(wait_for_publish[i].mins, wait_for_publish[i].maxs);
			std::cout << display(wait_for_publish[i].mins,DIM) << display(wait_for_publish[i].maxs,DIM) << "metaserver=" << metaserver << std::endl;
			ms_to_rep(metaserver, len_rep,representatives);
			std::cout << representatives[0] << std::endl;
			if (BLOOM_FILTER_STATE)
			{
				keycnt = 0;
				get_keys(wait_for_publish[i].ip);
				std::cout << "got keys" << std::endl;
				key_len = keycnt;
				capacity = int(key_len*(log(exp(1)) / log(2))*(log(1 / p) / log(2)));
				number_of_hash = int(log(2) / log(exp(1))*(log(1 / p) / log(2)));
				hashtable = new int[capacity];
				for (j = 0; j < capacity; j++) hashtable[j] = 0;
				for (j = 0; j < key_len; j++)
				{
					str_key = ftoa(keys[j].node->m_branch[keys[j].branch_num].m_rect.m_min[0]);
					for (d = 1; d < DIM; d++)
						str_key = str_key + " " + ftoa(keys[j].node->m_branch[keys[j].branch_num].m_rect.m_min[d]);
					for (k = 0; k < number_of_hash; k++)
						hashtable[hash(str_key.c_str(), prime[k], capacity)] = 1;
				}
				std::cout << "hash=" << displayi(hashtable, capacity) << std::endl;
				int* new_representatives;
				for (d = 2; d < DIM; d++)
				{
					rep_step = pow(4, TWODIM_H + d - 2);
					area_step = float(range_max[d] - range_min[d]) / 4;
					new_representatives = new int[TOTAL_SERVER];
					len_new_rep = 0;

					for (area_part = 0; area_part < 4; area_part++)
						if (cross1(range_min[d] + area_step*area_part, range_min[d] + area_step*(area_part + 1), wait_for_publish[i].mins[d], wait_for_publish[i].maxs[d]))
							for (k = 0; k < len_rep; k++)
							{
								new_representatives[len_new_rep] = representatives[k] + rep_step*area_part;
								len_new_rep++;
							}
					delete representatives;
					representatives = new_representatives;
					len_rep = len_new_rep;
				}
				std::cout << "publish to representatives=" << displayi(representatives, len_rep) << std::endl;
				for (j = 0; j < len_rep; j++)
				{
					data = "send_feedback " + itoa(representatives[j]) + " " + "publish ";
					for (k = 0; k < DIM; k++)
						data = data + ftoa(wait_for_publish[i].mins[k]) + " ";
					for (k = 0; k < DIM; k++)
						data = data + ftoa(wait_for_publish[i].maxs[k]) + " ";
					data += itoa(SERVER_NO);
					data += " ";
					data += itoa(key_len);
					to_server = route[representatives[j]];
					std::cout << "to_server=" << to_server << std::endl;
					global_index_no = atoi(send_msg(socketpool, data, to_server).c_str());

					if (BLOOM_FILTER_STATE)
					{
						now_hash = 0;
						while (now_hash < capacity)
						{
							data = "send_feedback " + itoa(representatives[j]) + " " + "hash " + itoa(global_index_no);
							for (k = 0; k < 960; k++)
							{
								if (now_hash > capacity)  break;
								data = data + itoa(hashtable[now_hash]);
								now_hash ++;
							}
							send_msg(socketpool, data, to_server);
						}
						if (PRT_INFO)
							std::cout << "published node to " << to_server << " " << display(wait_for_publish[i].mins, DIM) << " " << display(wait_for_publish[i].maxs, DIM)
							<< " hash(" << wait_for_publish[i].ip << ")=" << std::endl;
						//print(hashtable);
					}
				}
			}
		}
		if (PRT_INFO) std::cout << global_clock.get_time() << " build finished";
		data_ret="build_finished";
	}
	else if (data1.compare("publish") == 0)
	{
		pthread_mutex_lock(&publish_mutex);
		if (index_num >= max_index_num)
		{
			global_index_type* new_global_index = new global_index_type[max_index_num * 2];
			int* new_bloom_len = new int[max_index_num * 2];
			int** new_bloom_filter = new int*[max_index_num * 2];
			for (i = 0; i < max_index_num; i++)
			{
				new_global_index[i] = global_index[i];
				new_bloom_len[i] = bloom_len[i];
				new_bloom_filter[i] = bloom_filter[i];
			}
			delete[]bloom_len;
			delete[]global_index;
			delete[]bloom_filter;
			global_index = new_global_index;
			bloom_len = new_bloom_len;
			bloom_filter = new_bloom_filter;
			max_index_num *= 2;
		}
		for (i = 0; i < DIM; i++)
			global_index[index_num].mins[i] = atof(get_next_item(command));
		for (i = 0; i < DIM; i++)
			global_index[index_num].maxs[i] = atof(get_next_item(command));
		global_index[index_num].ip = atoi(get_next_item(command));
		global_index[index_num].key_num = atoi(get_next_item(command));
		index_num++;
		pthread_mutex_unlock(&publish_mutex);
		data_ret= itoa(index_num - 1);
	}
	else if (data1.compare("find_local") == 0)
	{
		float query_pt[DIM];
		for (i = 0; i < DIM; i++)
		{
			query_pt[i] = atof(get_next_item(command));
		}
		int query_num = rtree.Search(query_pt, query_pt);
		if (PRT_INFO) std::cout << query_num;
		data_ret=itoa(query_num);
	}
	else if (data1.compare("query") == 0)
	{
		int area_no, server_ip;
		std::string back_ips, result,data;
		if (TOTAL_SERVER >= 4)
			area_no = SERVER_NO / (TOTAL_SERVER / 4);
		else
			area_no = 0;
		int repre[4];
		int to_server;
		bool success;
		std::string value;
		repre[0] = 0;
		repre[1] = ((TOTAL_SERVER - 1) / 3);
		repre[2] = (repre[1] * 2);
		repre[3] = (repre[1] * 3);
		to_server = repre[area_no];
		data = "send_feedback " + itoa(to_server) + " ";
		data = data + "get_global";
		for (i = 0; i < DIM; i++)
			data = data + " " + get_next_item(data);
		for (i = 0; i < DIM; i++)
			data = data + " " + get_next_item(data);
		data = data + " " + itoa(area_no) + ' ' + itoa(TOTAL_SERVER*H) + ' ' + itoa(H);

		back_ips = send_msg(socketpool, data, route[to_server]);

		if (PRT_INFO) std::cout << "back_ips=" << back_ips;
		bool* ips_search = new bool[TOTAL_SERVER];
		for (i = 0; i < TOTAL_SERVER; i++)
			ips_search[i] = false;
		while (back_ips.length()>0) ips_search[atoi(get_next_item(back_ips))] = true;
		success = false;
		value = "";
		for (i = 0; i < TOTAL_SERVER; i++)
		{
			server_ip = i;
			if (ips_search[i] == false)
				continue;
			data = "send_feedback " + itoa(server_ip) + " ";
			data = data + "find_local";
			for (j = 0; j < DIM; j++)
				data = data + " " + get_next_item_str(command);
			for (j = 0; j < DIM; j++)
				data = data + " " + get_next_item_str(command);
			result = send_msg(socketpool, data, route[server_ip]);
			if (PRT_INFO) std::cout << data << std::endl;

			if (PRT_INFO) std::cout << "Server " << server_ip << " return: " << result << std::endl;

			if (result.compare(" ") != 0)
			{
				value = value + result;
				success = true;
			}
		}
		if (!success)
			value = "Fail to find the value of input key";
		data_ret=value;
	}
	else if (data1.compare("get_global") == 0)
	{
		float mins[DIM], maxs[DIM];
		int* lower_layer;
		int rep, metaserver, layer, cnt, rep_num, to_server, capacity, p, number_of_hash, d;
		std::string back_ips, back_ip_by_next, key_str;
		bool* back_ip_bool;
		back_ip_bool = new bool[TOTAL_SERVER];
		for (i = 0; i < TOTAL_SERVER; i++)
			back_ip_bool[i] = false;
		bool ispt, pass_bloom_filter;
		for (i = 0; i < DIM; i++)
			mins[i] = atof(get_next_item(command));
		for (i = 0; i < DIM; i++)
			maxs[i] = atof(get_next_item(command));
		rep = atoi(get_next_item(command));
		metaserver = atoi(get_next_item(command));
		layer = atoi(get_next_item(command));
		back_ips = " ";
		if (layer != 0)
		{
			if (PRT_INFO) std::cout << metaserver << mins << maxs << rep << layer;
			get_rep_lower_layer(metaserver, mins, maxs, rep, layer, rep_num, lower_layer);
			if (PRT_INFO) std::cout << "Lower layer metaserver= " << lower_layer[0] << " representative= " << lower_layer[1] << " relative location= " << lower_layer[2] << std::endl;
			for (cnt = 0; cnt < rep_num; cnt++)
			{
				to_server = lower_layer[3 * cnt + 1];
				rep = lower_layer[3 * cnt + 2];
				data = "send_feedback " + itoa(to_server) + " ";
				data = data + "get_global";
				for (i = 0; i < DIM; i++)
					data = data + " " + ftoa(mins[i]);
				for (i = 0; i < DIM; i++)
					data = data + " " + ftoa(maxs[i]);
				data = data + " " + itoa(rep) + " " + itoa(lower_layer[3 * cnt]) + " " + itoa(layer - 1);
				back_ip_by_next = send_msg(socketpool, data, route[to_server]);
				if (PRT_INFO) std::cout << "send to next layer representative " << lower_layer[3 * cnt + 1] << ": " << data << std::endl;
				if (PRT_INFO) std::cout << "ips return from " << lower_layer[2 * cnt + 1] << " = " << back_ip_by_next << std::endl;
				if (back_ip_by_next.compare("none") == 0) continue;
				while (back_ip_by_next.length() != 0)
				{
					back_ip_bool[atoi(get_next_item(back_ip_by_next))] = true;
				}
			}
		}

		if (PRT_INFO) std::cout << "This representative has " << index_num << " global index, Following cross(GI,query_rg):" << std::endl;
		ispt = ((mins == maxs) && (BLOOM_FILTER_STATE));
		if (ispt)
		{
			key_str = ftoa(mins[0]);
			for (d = 1; d < DIM; d++)
				key_str = key_str + " " + ftoa(mins[d]);
		}
		int prime[12] = { 2,5,7,11,13,17,23,29,31,37,41,43 };
		for (i = 0; i<index_num; i++)
			if (cross(global_index[i].mins, global_index[i].maxs, mins, maxs))
			{
				pass_bloom_filter = true;
				if (ispt)
				{
					capacity = bloom_len[i];
					number_of_hash = int(log(2) / log(exp(1))*capacity / global_index[i].key_num);
					for (j = 0; j < number_of_hash; j++)
						if (bloom_filter[i][hash(key_str.c_str(), prime[j], capacity)] == 0)
						{
							pass_bloom_filter = false;
							break;
						}
					if (!pass_bloom_filter)
						if (PRT_INFO)
							std::cout << "GI=" << display(global_index[i].mins, DIM) << display(global_index[i].maxs, DIM) << " not pass" << std::endl;
						else
						{
							if (PRT_INFO) std::cout << "GI=" << display(global_index[i].mins, DIM) << display(global_index[i].maxs, DIM) << " pass" << std::endl;
							back_ip_bool[global_index[i].ip] = true;
						}
				}
			}
		for (i = 0; i < TOTAL_SERVER; i++)
			if (back_ip_bool[i])
				if (back_ips.compare("") == 0)
					back_ips = itoa(i);
				else
					back_ips = back_ips + " " + itoa(i);
		if (back_ips.compare("") == 0) back_ips = "none";
		if (PRT_INFO) std::cout << "GI end" << std::endl;
		if (PRT_INFO) std::cout << "back_ips return= " << back_ips << std::endl;
		data_ret=back_ips;
	}
	else if (data1.compare("hash") == 0)
	{
		pthread_mutex_lock(&publish_mutex);
		int global_no;
		char hash_num;
		global_no = atoi(get_next_item(command));
		for (i = 0; i < command.length(); i++)
		{
			hash_num = command[i];
			if (hash_num == '1')
				bloom_filter[global_no][i] = 1;
			else
				bloom_filter[global_no][i] = 0;
		}
		pthread_mutex_unlock(&publish_mutex);
		data_ret="global_index["+itoa(global_no)+"]"+"hash_store_complete";
	}
	else if (data1.compare("store_pir") == 0)
	{
		int spaceloc, now_pir;
		spaceloc = command.find(" ");
		now_pir = atoi(get_next_item(command));
		for (i = 0; i < 2; i++)
			pir[now_pir].mins[i] = atof(get_next_item(command));
		for (i = 0; i < 2; i++)
			pir[now_pir].maxs[i] = atof(get_next_item(command));
		if (PRT_INFO)
		{
			std::cout << "pir[" << now_pir << "] = [" << pir[now_pir].mins[0] << "," << pir[now_pir].mins[1] << "] [" << pir[now_pir].maxs[0] << "," << pir[now_pir].maxs[1] << "]" << std::endl;
		}
		data_ret = "store_pir[" + itoa(now_pir) + "]at_server_" + itoa(SERVER_NO);
	}
	else if (data1.compare("new_socket") == 0)
		data_ret = "new_socket";
	else if (data1.compare("test") == 0)
	{
		data_ret = "test";
		test();
	}
	else 
		data_ret="test";
	return data_ret;
}
void* server_thread(void* arg0)
{
	ServerSocket new_sock = *((ServerSocket*)arg0);
	int i,j;
	std::string data;
	std::string data_ret;
	try
	{
		while (true)
		{
			new_sock >> data;
			data_ret=perform(data);
			if (PRT_INFO)
				std::cout << global_clock.get_time() <<" return : " << data_ret << std::endl;
			new_sock << data_ret;
		}
	}
	catch (SocketException&) {}
}

void get_local_ip(char* ip)
{
	struct ifaddrs * ifAddrStruct = NULL;
	struct ifaddrs * ifa = NULL;
	void * tmpAddrPtr = NULL;

	getifaddrs(&ifAddrStruct);

	for (ifa = ifAddrStruct; ifa != NULL; ifa = ifa->ifa_next) {
		if (!ifa->ifa_addr) {
			continue;
		}
		if (ifa->ifa_addr->sa_family == AF_INET) { // check it is IP4
												   // is a valid IP4 Address
			tmpAddrPtr = &((struct sockaddr_in *)ifa->ifa_addr)->sin_addr;
			char addressBuffer[INET_ADDRSTRLEN];
			inet_ntop(AF_INET, tmpAddrPtr, addressBuffer, INET_ADDRSTRLEN);
			printf("%s IP Address %s\n", ifa->ifa_name, addressBuffer);
			if (strcmp(addressBuffer, "127.0.0.1") != 0)
			{
				strcpy(ip, addressBuffer);
				return;
			}
		}
		else if (ifa->ifa_addr->sa_family == AF_INET6) { // check it is IP6
														 // is a valid IP6 Address
			tmpAddrPtr = &((struct sockaddr_in6 *)ifa->ifa_addr)->sin6_addr;
			char addressBuffer[INET6_ADDRSTRLEN];
			inet_ntop(AF_INET6, tmpAddrPtr, addressBuffer, INET6_ADDRSTRLEN);
			printf("%s IP Address %s\n", ifa->ifa_name, addressBuffer);
		}
	}
	if (ifAddrStruct != NULL) freeifaddrs(ifAddrStruct);
}

void get_server_ip()
{
	std::ifstream fin;
	int i;
	fin.open("../server_ip.txt", std::ios::in);
	char* local_ip=new char[20];
	get_local_ip(local_ip);
	
	std::cout << "Local ip=" << local_ip << std::endl;
	server_ip = new char*[TOTAL_SERVER];
	for (i = 0; i < TOTAL_SERVER; i++)
	{
		server_ip[i] = new char[20];
		fin >> server_ip[i];
		if (strcmp(server_ip[i], local_ip) == 0) SERVER_NO = i;
		std::cout << "server " << i << " ip= " << server_ip[i] << std::endl;
	}
	fin >> BASE_PORT;
	fin.close();
	std::cout << "This is server " << SERVER_NO << " PORT=" << BASE_PORT<<std::endl;
	std::ofstream fout;
	fout.open("../server_ip.txt", std::ios::out);
	for (i = 0; i < TOTAL_SERVER; i++)
	{
		fout << server_ip[i];
		if (i != TOTAL_SERVER - 1) fout << " ";
		else
		{
			fout << std::endl << BASE_PORT + 1 << std::endl;
		}
	}
	fout.close();
}
	
void test()
{
	std::cout << "start test...........\n";
	pthread_mutex_init(&publish_mutex, NULL);
	std::cout << "routing......\n";
	routing(SERVER_NO);
	std::cout << "testing function ms_to_rep_high.....\n";
	for (int i = 0; i <= TOTAL_SERVER*H; i++)
	{
		int repnum;
		int* rep=new int[4];
		ms_to_rep_high(i, repnum,rep);
		std::cout << global_clock.get_time() << " rep of metaserver " << i << " = ";
		for (int j = 0; j < repnum; j++)
			std::cout << rep[j] << " ";
		std::cout << std::endl;
	}
	std::cout << "finish test ms_to_rep_high*******************\n\n";
	std::cout << "testing function ms_to_rep...\n";
	for (int i = 0; i <= TWODIM_SERVER*TWODIM_H; i++)
	{
		int repnum;
		int* rep=new int[4];
		ms_to_rep(i, repnum,rep);
		std::cout << global_clock.get_time() << " rep of metaserver " << i << " = ";
		for (int j = 0; j < repnum; j++)
			std::cout << rep[j] << " ";
		std::cout << std::endl;
	} 
	std::cout << "***************\n";
	std::cout << "testing function just_cover\n";
	for (int i = 0; i <= TWODIM_SERVER*TWODIM_H; i++)
	{
		std::cout << "just cover [" << pir[i].mins[0] << "," << pir[i].mins[1] << "] [" << pir[i].maxs[0] << "," << pir[i].maxs[1] << "]";
		std::cout << " = metaserver ";
		int meta = just_cover(pir[i].mins, pir[i].maxs);
		std::cout<<meta<<" which has pir [" << pir[meta].mins[0] << "," << pir[meta].mins[1] << "] [" << pir[meta].maxs[0] << "," << pir[meta].maxs[1] << "]"<<std::endl;
	}
	std::cout << "*************\n";
	std::cout << "testing function get_rep_lower_layer\n";
	float mins[2] = {0,0 }; float maxs[2] = { 5000,5000 };
	int* result=new int[16];
	int rep_num;
	std::cout << "lower layer representative of metaserver 32, target area=[0,0] [5000,5000] from location part 1\n";
	get_rep_lower_layer(32, mins, maxs, 1, 2, rep_num, result);
	for (int i = 0; i < rep_num; i++)
	{
		std::cout << "metaserver= " << result[3 * i] << " representative= " << result[3 * i + 1] << " relative location= " << result[3 * i + 2] << std::endl;
	}
	// result[]={meta,representative,relative rep location}
	std::cout << "****************\n";
}
int main(int argc, int argv[])
{
	std::cout << "running....\n";
	pthread_mutex_init(&publish_mutex, NULL);
	get_server_ip();
	routing(SERVER_NO);
	int i,j;

	Socketpool* socketpool = init_socket(TOTAL_SERVER, server_ip, BASE_PORT);
	std::cout << "Socketpool initialized." << std::endl;

	timespec ts;
	ts.tv_sec = 0;
	ts.tv_nsec = 1000;

	int thread_num = 0;
	pthread_t pid[100];
	ServerSocket* new_sock=new ServerSocket[100];
	try
	{
		// Create the socket
		ServerSocket server(BASE_PORT);

		while (true)
		{
			thread_num++;
			server.accept(new_sock[thread_num - 1]);
			std::cout << "accept" << std::endl;
			pthread_create(&pid[thread_num], NULL, server_thread, &(new_sock[thread_num-1]));
		}
	}
	catch (SocketException& e)
	{
		std::cout << "Exception was caught:" << e.description() << "\nExiting.\n";
	}
	
	std::cin.get();
	return 0;
}
