# "ec2-user" can be replaced by "ubuntu"

echo ***ip-update.sh***

read clientip < clientip.txt

echo client
sudo scp -i key.pem ip/client_ip.txt ubuntu@$clientip:~/client_ip.txt

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
curl ifconfig.me >> ip/public_ip.txt
for  ((i = 0 ;i < $a ;i ++ )); do 
{
echo ${ip[$i]}
#1 server_ip
sudo scp -i key.pem ip/iptable.txt ubuntu@${ip[$i]}:~/iptable.txt
#2 own public ip
sudo scp -i key.pem ip/ip_$i.txt ubuntu@${ip[$i]}:~/public_ip.txt
}
done


echo "******* Reboot IP Update *******"

