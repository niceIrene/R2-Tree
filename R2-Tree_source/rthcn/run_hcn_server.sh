#cd hcn_server
#python hcn_server.py

cd hcn_server
g++ -o simple_server_main Socket.cpp RTree.h ServerSocket.cpp ClientSocket.cpp simple_server_main.cpp -lpthread
./simple_server_main
