# "ec2-user" can be replaced by "ubuntu"

echo ***s1-resource.sh***

read clientip < clientip.txt

#1 client source code
echo client

sudo scp -i key.pem prthcn/hcn_client.tar.gz ubuntu@$clientip:/tmp
sudo ssh -i key.pem ubuntu@$clientip -t 'mv /tmp/hcn_client.tar.gz ./'
#scp -i key.pem rtcan/can_client.tar.gz ubuntu@$clientip
#sudo ssh -i key.pem ubuntu@$clientip -t 'rm /tmp/hcn_client.tar.gz'
#sudo ssh -i key.pem ubuntu@$clientip -t 'rm ./hcn_client.tar.gz'
#2 server source code & run.sh
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
# code
#sudo scp -i key.pem prthcn/mmh3.tar.gz ubuntu@${ip[$i]}:/tmp
sudo scp -i key.pem prthcn/hcn_server_"${i}".tar.gz ubuntu@${ip[$i]}:/tmp
sudo ssh -i key.pem ubuntu@${ip[$i]} -t "mv /tmp/hcn_server_${i}.tar.gz ./"
#sudo ssh -i key.pem ubuntu@${ip[$i]} -t 'mv /tmp/hcn_server.tar.gz ./'
#sudo ssh -i key.pem ubuntu@${ip[$i]} -t 'rm /tmp/hcn_server.tar.gz'
#sudo ssh -i key.pem ubuntu@${ip[$i]} -t 'rm ./hcn_server.tar.gz'

}
done

#3 ip informations
#bash ip_update.sh

echo "******* Resources Finish *******"

