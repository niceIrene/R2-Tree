# "ec2-user" can be replaced by "ubuntu"

echo ***s2-configuration.sh***

read clientip < clientip.txt

#client source code
echo client
sudo ssh -i key.pem ubuntu@$clientip -t 'tar zxvf hcn_client.tar.gz'
#ssh -i key.pem ec2-user@$clientip 'tar zxvf can_client.tar.gz'

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

# run one by one!
echo server
for  ((i = 0 ;i < $a ;i ++ )); do 
{
#1 update (different for ubuntu)
#install python
sudo ssh -i key.pem ubuntu@${ip[$i]} -t 'sudo apt-get install python'
sudo ssh -i key.pem ubuntu@${ip[$i]} -t 'update-alternatives --install /usr/bin/python python /usr/bin/python2.7.1'

#2 g++ (different for ubuntu)
#ssh -i key.pem ubuntu@${ip[$i]} -t 'sudo yum -y install gcc-c++'

#3 python_dev (this works in ubuntu, not linux)
#ssh -i key.pem ec2-user@${ip[$i]} -t 'sudo yum -y install python_dev'

#4 tar hcn/can
sudo ssh -i key.pem ubuntu@${ip[$i]} -t "tar zxvf hcn_server_${i}.tar.gz"
#ssh -i key.pem ec2-user@${ip[$i]} 'tar zxvf can_server.tar.gz'
sudo ssh -i key.pem ubuntu@${ip[$i]} -t 'sudo apt-get install python-pip'
sudo ssh -i key.pem ubuntu@${ip[$i]} -t 'pip install --upgrade pip'
sudo ssh -i key.pem ubuntu@${ip[$i]} -t 'pip install mmh3'

#5 setup.py
#ssh -i key.pem ec2-user@${ip[$i]} -t 'cd hcn_server ; sudo python setup.py install'

}
done
wait

echo "******* Configurations Finish *******"




