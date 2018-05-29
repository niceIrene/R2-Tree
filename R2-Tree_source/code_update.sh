# "ec2-user" can be replaced by "ubuntu"

echo ***code-update.sh***

read clientip < clientip.txt

#1 client source code
echo client
scp -i key.pem rthcn/hcn_client/simple_client_main.cpp ec2-user@$clientip:~/hcn_client/simple_client_main.cpp
#scp -i key.pem rtcan/can_client/simple_client_main.cpp ec2-user@$clientip:~/can_client/simple_client_main.cpp

#############################################
declare -i a=0
ip=()
while read line
do
#echo ip[$a]=$line
ip[$a]=$line
a=a+1
done < ip/iptable.txt
#############################################

echo server
for  ((i = 0 ;i < $a ;i ++ )); do 
{
#1 server source code
scp -i key.pem rthcn/hcn_server/simple_server_main.cpp ec2-user@${ip[$i]}:~/hcn_server/simple_server_main.cpp
#scp -i key.pem rtcan/can_server/simple_server_main.cpp ec2-user@${ip[$i]}:~/can_server/simple_server_main.cpp
}
done


echo "******* New Code Update *******"
