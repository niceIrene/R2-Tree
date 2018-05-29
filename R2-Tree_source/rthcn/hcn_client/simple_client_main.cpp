#include "ClientSocket.h"
#include "SocketException.h"
#include <pthread.h>
#include <cstdlib>
#include <cstring>
#include <sys/time.h>
#include <fstream>
#include <sstream>
#include <iostream>
#include <string>
#include <cmath>

const int QUERY_NUM = 100;
const int MAX_SOCKET_NUM = 50;
const int DIM = 2;
const int H = 0;
const int TWODIM_H = H - DIM + 2;
const int PASS_BUF = 200;
int TOTAL_SERVER = pow(4, H);
int TWODIM_SERVER = pow(4, TWODIM_H);
int BASE_PORT;
int THREAD_NUM = 20;
int query_done = 0;
int thread_exist = 0;
int query_start = 0;
bool PRT_INFO = true;
bool TEST_FLAG = true;
bool TEST_MODE = true;
char** server_ip = new char*[TOTAL_SERVER];
const int INPUT_NUM = 2000;
float lst[INPUT_NUM][DIM]; 
int range_min[DIM] = { 0,0 };
int range_max[DIM] = { 5000,5000 };

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
			return   false;

		result.tv_sec = (end.tv_sec - begin.tv_sec);
		result.tv_usec = (end.tv_usec - begin.tv_usec);

		if (result.tv_usec<0) {
			result.tv_sec--;
			result.tv_usec += 1000000;
		}

		return true;
	}
	void refresh()
	{
		gettimeofday(&start_time, 0);
	}
	float get_time()
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


std::string itoa(const int &int_temp)
{
	std::stringstream stream;
	stream << int_temp;
	std::string st;
	stream >> st;
	return st;
}

std::string ftoa(const double &int_temp)
{
	std::stringstream stream;
	stream << int_temp;
	std::string st;
	stream >> st;
	return st;
}
/*
void* client_thread(void* arg0)
{
	my_clock clock_this_thread;
	int cnt;
	int thread_no = int(arg0);
	try
	{

		ClientSocket client_socket("192.168.20.129", 30000 + thread_no);

		std::string reply;

		while (true)
		{

			try
			{
				client_socket << "Test message.";
				client_socket >> reply;
			}
			catch (SocketException&) {}

			//std::cout << clock_this_thread.get_time() << ": We received " << cnt << " response from the server:\n\"" << reply << "\"\n";

			cnt++;
			if (clock_this_thread.get_time() >= 10)
				break;
		}

		std::cout << "get " << cnt << " replies in 10 seconds.\n";s
	}
	catch (SocketException& e)
	{
		std::cout << "Exception was caught:" << e.description() << "\n";
	}
}
*/

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

int apply_socket(Socketpool* socketpool,int server_no)
{
	int i;
	int unused_socket_id = -1;
	pthread_mutex_lock(&socketpool[server_no].mutex);
	for (i = 0; i < socketpool[server_no].num;i++)
	{
		if (not socketpool[server_no].used[i])
		{
			unused_socket_id = i;
			break;
		}
	}
	if (unused_socket_id != -1)
	{
		if (PRT_INFO) std::cout << global_clock.get_time() <<"socket[" << server_no << "][" << unused_socket_id << "] in use" << std::endl;
		socketpool[server_no].used[unused_socket_id] = true;
		pthread_mutex_unlock(&socketpool[server_no].mutex);
		return unused_socket_id;
	}
	else
	{
		std::string reply;
		std::cout << "connect to " << server_ip[server_no] << " port= " << BASE_PORT << std::endl;
		ClientSocket* new_sock=new ClientSocket(server_ip[server_no], BASE_PORT);
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
	socket_id = apply_socket(socketpool, server_no);
	if (PRT_INFO) std::cout << global_clock.get_time() <<" socket[" << server_no << "][" << socket_id << "]" << " send \"" << data << "\"" << std::endl;
	socketpool[server_no].socket[socket_id] << data;
	socketpool[server_no].socket[socket_id] >> reply;

	if (PRT_INFO) std::cout << global_clock.get_time() <<" socket[" << server_no << "][" << socket_id << "]" << " receive \"" << reply << "\"" << std::endl;
	free_socket(socketpool, server_no, socket_id);
	return reply;
}

int getmin(int a, int b)
{
	if (a < b) return a; else return b;
}

class pir_type
{
public:
	float mins[2];
	float maxs[2];
	pir_type()
	{
		mins[0] = 0; mins[1] = 0;
		maxs[0] = 0; maxs[1] = 0;
	}
	pir_type(int* min1, int* max1)
	{
		mins[0] = min1[0]; mins[1] = min1[1];
		maxs[0] = max1[0]; maxs[1] = max1[1];
	}
};

pir_type* pir = new pir_type[TWODIM_SERVER*(TWODIM_H + 1)];

double** average_pir;
void gen_average()
{
	int divisions, records, part_num, sample_num, d, i, repeat_num, cnt, square, smaller_than_me, part_belong,square1;
	int* number_in_part;
	double step, average_num,mirrored_part_max,mirrored_part_min;
	double* parts, *num_this_dim;
	double** mirror;
	divisions = pow(2, TWODIM_H);
	records = INPUT_NUM;
	part_num = getmin(records, int(5 * (log(records) / log(2))));
	parts = new double[part_num + 1];
	for (i = 0; i < part_num + 1; i++)
		parts[i] = 1.0 / part_num*i;
	step = 1.0 / part_num;
	sample_num = getmin(records, int(25 * log(records) / log(2)));
	if (TEST_FLAG)
		std::cout << "Part num=" << part_num << " Sample_num=" << sample_num << std::endl;
	if (sample_num < 1) sample_num = 1;
	num_this_dim = new double[sample_num];
	average_pir = new double*[DIM];
	for (d = 0; d < DIM; d++)
	{
		for (i = 0; i < sample_num; i++)
		{
			num_this_dim[i] = lst[rand()%records][d];
			num_this_dim[i] = (num_this_dim[i] - range_min[d]) / (range_max[d] - range_min[d]);
		}
		repeat_num = 10;
		mirror = new double*[repeat_num];
		average_num = double(sample_num) / part_num;
		cnt = -1;
		while (true)
		{
			cnt += 1;
			mirror[cnt] = new double[part_num + 1];
			number_in_part = new int[part_num];
			for (i = 0; i < part_num; i++)
				number_in_part[i] = 0;
			for (i = 0; i < sample_num; i++)
			{
				part_belong = int(num_this_dim[i] / step);
				if (part_belong == part_num) part_belong -= 1;
				number_in_part[part_belong]++;
			}
			square = 0;
			for (i = 0; i < part_num; i++)
				square += (number_in_part[i] - average_num)*(number_in_part[i] - average_num);
			smaller_than_me = 0;
			mirror[cnt][0] = 0;
			for (i = 0; i < part_num; i++)
			{
				smaller_than_me += number_in_part[i];
				mirror[cnt][i + 1] = float(smaller_than_me) / sample_num;
			}
			for (i = 0; i < sample_num; i++)
			{
				part_belong = int(num_this_dim[i] / step);
				if (part_belong == part_num) part_belong -= 1;
				mirrored_part_min = mirror[cnt][part_belong];
				mirrored_part_max = mirror[cnt][part_belong + 1];
				if (mirrored_part_min == mirrored_part_max)
					mirrored_part_max = mirrored_part_min + 0.0001;
				num_this_dim[i] = (num_this_dim[i] - parts[part_belong]) / step*(mirrored_part_max - mirrored_part_min) + mirrored_part_min;
			}
			for (i = 0; i < part_num; i++) number_in_part[i] = 0;
			for (i = 0; i < sample_num; i++)
			{
				part_belong = int(num_this_dim[i] / step);
				if (part_belong == part_num) part_belong -= 1;
				number_in_part[part_belong]++;
			}
			square1 = 0;
			for (i = 0; i < part_num; i++)
				square1 += (number_in_part[i] - average_num)*(number_in_part[i] - average_num);

			std::cout << "d=" << d << " round " << cnt + 1 << " : " << square << " " << square1 << std::endl;
			if (cnt > 2) break;
		}
		repeat_num = cnt + 1;
		average_pir[d] = new double[divisions + 1];
		average_pir[d][0] = 0;
		for (i = 0; i < divisions; i++)
			average_pir[d][i + 1] = (i + 1.0) / divisions;
		for (cnt = 0; cnt < repeat_num; cnt++)
		{
			part_belong = 0;
			for (i = 0; i < divisions; i++)
			{
				while (mirror[repeat_num - 1 - cnt][part_belong + 1] < average_pir[d][i]) part_belong++;
				mirrored_part_min = mirror[repeat_num - 1 - cnt][part_belong];
				mirrored_part_max = mirror[repeat_num - 1 - cnt][part_belong + 1];
				if (mirrored_part_max == mirrored_part_min) mirrored_part_max = mirrored_part_min + 0.0001;
				average_pir[d][i] = (average_pir[d][i] - mirrored_part_min) / (mirrored_part_max - mirrored_part_min)*step + parts[part_belong];
			}
		}
		for (i = 0; i < divisions + 1; i++)
			average_pir[d][i] = average_pir[d][i] * (range_max[d] - range_min[d]) + range_min[d];
		if (PRT_INFO)
		{
			std::cout << "dim " << d << " ";
			for (i = 0; i < divisions + 1; i++)
				std::cout << average_pir[d][i] << " ";
			std::cout << std::endl;
		}
	}
}

void gen_pir_layer(int metaserver, int min0,int min1,int max0,int max1, int layer)
{
	int step,starter;
	pir[metaserver].mins[0] = average_pir[0][min0];
	pir[metaserver].mins[1] = average_pir[1][min1];
	pir[metaserver].maxs[0] = average_pir[0][max0];
	pir[metaserver].maxs[1] = average_pir[1][max1];
	if (!(layer == 0))
	{
		step = pow(4, layer - 1);
		starter = TWODIM_SERVER*(layer - 1) + (metaserver - TWODIM_SERVER*layer);
		gen_pir_layer(starter, min0, min1, (min0 + max0) / 2, (min1 + max1) / 2, layer - 1);
		gen_pir_layer(starter + step, (min0 + max0) / 2, min1, max0, (min1 + max1) / 2, layer - 1);
		gen_pir_layer(starter + step + step, min0, (min1 + max1) / 2, (min0 + max0) / 2, max1, layer - 1);
		gen_pir_layer(starter + step + step + step, (min0 + max0) / 2, (min1 + max1) / 2, max0, max1, layer - 1);
	}
}
void gen_pir()
{
	gen_average();
	gen_pir_layer(TWODIM_SERVER*TWODIM_H, 0, 0, pow(2, TWODIM_H), pow(2, TWODIM_H), TWODIM_H);
}


struct query_pass
{
	int query_num;
	int to_server;
	float* query_data;
	Socketpool* socketpool;
};

my_clock query_clock;
float last_feedback_time;

void* query_thread(void* arg0)
{
	pthread_detach(pthread_self());
	query_pass pass_item = *((query_pass*)arg0);
	int socket_id;
	std::string reply;
	socket_id = apply_socket(pass_item.socketpool, pass_item.to_server);
	
	std::string query_str = "point_query";
	int i;
	for (i = 0; i < DIM; i++)
	{
		query_str += " ";
		query_str += pass_item.query_data[i];
	}
	if (PRT_INFO) std::cout << global_clock.get_time() << " socket[" << pass_item.to_server << "][" << socket_id << "]" << " send \"" << query_str << "\"" << std::endl;
	pass_item.socketpool[pass_item.to_server].socket[socket_id] << query_str;
	pass_item.socketpool[pass_item.to_server].socket[socket_id] >> reply;
	if (PRT_INFO) std::cout << global_clock.get_time() << " socket[" << pass_item.to_server << "][" << socket_id << "]" << " receive \"" << reply << "\"" << std::endl;
	free_socket(pass_item.socketpool, pass_item.to_server, socket_id);
	query_done++;
	if (PRT_INFO)
		std::cout << query_done << " done.\n";

	last_feedback_time = query_clock.get_time();
	thread_exist--;
}

struct build_para
{
	int i;
};

void* build_thread(void* arg0)
{
	pthread_detach(pthread_self());
	int to_server = ((build_para*)arg0)->i;
	send_msg(socketpool, "build", to_server);
}

int main(int argc, int argv[])
{
	int i,j;
	/* get ip from client_ip.txt start*/
	std::ifstream fin;
	std::string port_str;
	fin.open("../client_ip.txt", std::ios::in);
	for (i = 0; i < TOTAL_SERVER; i++)
	{
		server_ip[i] = new char[20];
		fin >> server_ip[i];
	}
	fin >> port_str;
	fin.close();
	
	BASE_PORT = atoi(port_str.c_str());
	
	if (TEST_FLAG)
	{
		for (i = 0; i < TOTAL_SERVER; i++)
		{
			std::cout << "server "<<i<<" : "<< server_ip[i]<<std::endl;
		}
		std::cout << "PORT : " << BASE_PORT << std::endl;
	}
	/* socketpool initialize*/
	socketpool = init_socket(TOTAL_SERVER, server_ip, BASE_PORT);
	std::cout << "Socketpool initialized." << std::endl;

	
	std::string command;
	
	while (true)
	{
		std::cin >> command;
		if (command.compare("store") == 0)
		{
			/* read data from input.txt*/
			std::ifstream fin;
			std::string temp;
			fin.open("../input.txt", std::ios::in);
			for (i = 0; i < INPUT_NUM; i++)
				for (j = 0; j < DIM; j++)
				{
					fin >> temp;
					lst[i][j] = atof(temp.c_str());
				}

			if (TEST_FLAG)
			{
				for (i = 0; i < INPUT_NUM; i++)
				{
					std::cout << "No. " << i << " : ";
					for (j = 0; j < DIM; j++)
						std::cout << lst[i][j] << ' ';
					std::cout << std::endl;
				}
			}
			/* send store command to servers*/
			std::cout << "enter send msg "<<std::endl;
			
			for (i = 0; i < TOTAL_SERVER; i++)
			{
				send_msg(socketpool, "store", i);
				if (TEST_MODE) break;
			}
			
			/* generate pir */
			gen_pir();
			/* prepare pir data*/
			std::string* data = new std::string[TWODIM_SERVER*H + 1];
			for (i = 0; i < H*TWODIM_SERVER + 1; i++)
			{
				data[i] = "store_pir " + itoa(i);
				for (j = 0; j < 2; j++)
				{
					data[i] += " ";
					data[i] += ftoa(pir[i].mins[j]);
				}
				for (j = 0; j < 2; j++)
				{
					data[i] += " ";
					data[i] += ftoa(pir[i].maxs[j]);
				}
				if (PRT_INFO)
				{
					std::cout << "pir[" << i << "] = [" << pir[i].mins[0] << "," << pir[i].mins[1] << "]" << " [" << pir[i].maxs[0] << "," << pir[i].maxs[1] << "]" << std::endl;
				}
			}
			/* send pir data to servers*/
			for (i = 0; i < TOTAL_SERVER; i++)
			{
				if (PRT_INFO) 
					std::cout << "Start store pir to server " << i;

				for (j = 0; j < H*TWODIM_SERVER + 1; j++)
					send_msg(socketpool, data[j], i);
				if (PRT_INFO)
					std::cout << "store pir to server " << i << " finish" << std::endl;
				if (TEST_MODE) break;
			}
		}
		else if (command.compare("build") == 0)
		{
			pthread_t* tid=new pthread_t[TOTAL_SERVER];
			int ret;
			for (i = 0; i < TOTAL_SERVER; i++)
			{
				build_para para;
				para.i = i;
				ret = pthread_create(&(tid[i]), NULL, build_thread, &para);
			}
			void* ret1;
			for (i = 0; i < TOTAL_SERVER; i++)
				pthread_join(tid[i], &ret1);
		}
		else if ((command.compare("point_query") == 0) || (command.compare("range_query")==0) || (command.compare("knn_query")==0))
		{
			int query_type;
			if (command.compare("point_query") == 0)
				query_type = 0;
			else if (command.compare("range_query") == 0)
				query_type = 1;
			else query_type = 2;
			int file_num,query_num;
			std::cin >> file_num >> query_num;
			std::ifstream fin;
			std::string filename = command;
			filename="../"+filename+"_" + itoa(file_num) + ".txt";
			fin.open(filename.c_str(), std::ios::in);
		    float query_file[QUERY_NUM][DIM*2];
			for (i = 0; i < QUERY_NUM; i++)
			{
				for (j = 0; j < DIM; j++)
					fin >> query_file[i][j];
				for (j = 0; j < DIM; j++)
					if (query_type != 1)
						query_file[i][j + DIM] = query_file[i][j];
					else
						fin >> query_file[i][j];
			}
			std::cout << QUERY_NUM << " point query items loaded\n";
			fin.close();

			query_clock.refresh();
			query_done = 0;
			query_start = 0;
			thread_exist = 0;
			query_pass pass_item[PASS_BUF];
			//pthread_attr_t attr[PASS_BUF];
			while ((query_done < query_num) && (query_clock.get_time()-last_feedback_time<1))
			{
				if ((query_start < query_num) && (thread_exist < THREAD_NUM))
				{
					
					int pass_num=query_start%PASS_BUF;
					pass_item[pass_num].query_num = query_start;
					pass_item[pass_num].to_server = rand() % TOTAL_SERVER;
					pass_item[pass_num].query_data = query_file[query_num%QUERY_NUM];
					pass_item[pass_num].socketpool = socketpool;

					query_start++;
					thread_exist++;

					pthread_t id;
					//pthread_attr_init(&(attr[query_start%PASS_BUF]));
					//pthread_attr_setdetachstate(&(attr[query_start%PASS_BUF]), PTHREAD_CREATE_DETACHED);
					int ret;
					ret = pthread_create(&id, NULL, query_thread, &(pass_item[pass_num]));
					//pthread_attr_destroy(&(attr[query_start%PASS_BUF]));
					if (ret != 0){
						std::cout << "Create pthread error!\n";
					}
				}
			}
			std::cout << query_done << " point queries finish in " << last_feedback_time << " seconds\n";
		}
		else if (command.compare("set_thread") == 0)
		{
			std::cin >> THREAD_NUM;
			std::cout << "query thread= " << THREAD_NUM;
		}
		else if (command.compare("set_port") == 0)
		{
			std::cin >> BASE_PORT;
			std::cout << "Connection port set to " << BASE_PORT << std::endl;
			std::ofstream fout;
			fout.open("../client_ip.txt", std::ios::out);
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
		else if (command.compare("test") == 0)
		{
			for (i = 0; i < 1; i++)
				std::cout << send_msg(socketpool, "test", i) << std::endl;

		}
		else
		{
			std::cout << "\"" << command << "\"" << " is not a valid command.\n";
		}
	}
	return 0;
}

