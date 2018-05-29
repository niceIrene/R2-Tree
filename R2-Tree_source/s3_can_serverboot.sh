# "ec2-user" can be replaced by "ubuntu"

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

for  ((i = 0 ;i < $a ;i ++ )); do 
{
ssh -i key.pem ec2-user@${ip[$i]} 'bash run_can.sh'
}& 
done
wait

echo "******* Servers_boot Finish *******"

