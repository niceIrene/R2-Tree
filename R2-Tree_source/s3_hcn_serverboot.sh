# "ec2-user" can be replaced by "ubuntu"

#############################################


echo server

declare -i a=0
ip=()
while read line
do
#echo ip[$a]=$line
ip[$a]=$line
a=a+1
done < ip/iptable.txt
#############################################

for  ((i = 0 ;i < $a ;i ++ )); do 
{
#sudo ssh -i key.pem ubuntu@${ip[$i]} -t 'sudo apt-get install python-pip'
#sudo ssh -i key.pem ubuntu@${ip[$i]} -t 'pip install --upgrade pip'
#sudo ssh -i key.pem ubuntu@${ip[$i]} -t 'pip install mmh3'
sudo ssh -i key.pem ubuntu@${ip[$i]} -t "python ./hcn_server_${i}/hcn_server.py"

}& 
done
wait

read clientip < clientip.txt

#client source code
echo client

sudo ssh -i key.pem ubuntu@$clientip -t 'python ./hcn_client/hcn_client.py'


echo "******* Servers_boot Finish *******"

