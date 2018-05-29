#cd hcn_server
#python hcn_server.py

cd hcn_client
g++ -o simple_client_main Socket.cpp RTree.h ServerSocket.cpp ClientSocket.cpp simple_client_main.cpp -lpthread
./simple_client_main
