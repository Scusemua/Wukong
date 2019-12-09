import boto3   
import time 
import paramiko 

# This will automatically launch Redis instances on EC2 VM's. 
# You can specify how many of the instances are "small" and how many are "big". 
# This is relevant because this also spits out the Python code to start the Wukong Scheduler 
# and the command line args for the Wukong Proxy (and those have separate arguments for big
# and small Redis instances).
# 
# The main() method at the bottom of the file is where you'd specify your parameters.
#
# This assumes that, for each IP address, you want to start an instance on each port you specify.
# So if your IPs are [A, B, C] and your ports are [1, 2], then the following servers would be started:
# A:1 B:1 C:1
# A:2 B:2 C:2
#
# Specifying the number of small shards means that some VMs will be reserved for the small cluster. The only 
# difference here is that there is a different argument for ports. So in reality, you'd have the following IPs (for example):
# IPs: [A, B, C, D, E]
# Let's say you want to reserve 2 VMs for "small" Redis instances.
# Then you'd have:
# Big IPs: [A, B, C]
# Small IPs: [D, E]
#
# You then specify the ports separately. 
# BigPorts: [1, 2, 3]
# Small Ports: [1, 2]
# 
# This would cause the following Redis instances to be created/started: 
#
# A:1 B:1 C:1 
# A:2 B:2 C:2
# A:3 B:3 C:3
#
# D:1 E:1
# D:2 E:2
#
# Note that the EC2 VM's should already be started. This does not automatically launch EC2 VM's for you.
# Note that you can also copy and paste this output to use for configuring the AWS Lambda functions too!
#
# This also assumes that, for each port, there is a config file in the /home/ec2-user/ directory of the form 
# redis_XXXX.conf where 'XXXX' is the port number, e.g. redis_6379.conf  

def get_public_ips(num_small, region_name="us-east-1"):
   ec2client = boto3.client('ec2', region_name = region_name)
   response = ec2client.describe_instances()
   public_ips = list()
   for reservation in response["Reservations"]:
      for instance in reservation["Instances"]:
         if instance["State"]["Name"] == "running":
            public_ips.append(instance["PublicDnsName"])
   print("Retrieved the following public IP addresses:")
   for ip in public_ips:
      print(ip)
   scheduler_address = public_ips[0]
   proxy_address = public_ips[1]            
   big_redis_ips = public_ips[1:-num_small] # Not the last 'num_small' IPs
   small_redis_ips = public_ips[-num_small:] # Last 'num_small' IPs 
   return (public_ips, scheduler_address, proxy_address, big_redis_ips, small_redis_ips)

# Print stuff formated for starting the proxy and scheduler 
def print_python_code(scheduler_address, proxy_address, big_redis_ips, small_redis_ips, big_redis_ports = [6279], small_redis_ports = [6379]):
   str_1 = "lc = LocalCluster(host='{}:8786',"
   str2 = "                  proxy_address = '{}',"
   str3 = "                  big_redis_endpoints = ["
   str4 = "                  small_redis_endpoints = ["
   _str = str_1.format(scheduler_address) + "\n" + str2.format(proxy_address) + "\n" + str3  
   print("Scheduler: ", scheduler_address)
   print("Proxy: ", proxy_address)
   counter = 1
   for IP in big_redis_ips:
      print("Big Redis #{}: {}".format(counter, IP))
      counter = counter + 1
   
   counter = 1
   for IP in small_redis_ips:
      print("Small Redis #{}: {}".format(counter, IP))
      counter = counter + 1
   
   for j in range(len(big_redis_ports)):
      port = big_redis_ports[j]
      for i in range(0, len(big_redis_ips)):
         ip = big_redis_ips[i]
         if i == 0:
            if j == 0:
               new_str = "('{}', {}),\n".format(ip, port)
            else:
               new_str = "                                     ('{}', {}),\n".format(ip, port)
            _str = _str + new_str    
         elif i != (len(big_redis_ips) - 1):
            new_str = "                                     ('{}', {}),\n".format(ip, port)
            _str = _str + new_str 
         elif i == (len(big_redis_ips) - 1) and j == (len(big_redis_ports) - 1):
            new_str = "                                     ('{}', {})]".format(ip, port)
            _str = _str + new_str + ","   
         else:
            new_str = "                                     ('{}', {}),\n".format(ip, port)
            _str = _str + new_str             
   _str += "\n" + str4 
   
   for j in range(len(small_redis_ports)):
      port = small_redis_ports[j]
      for i in range(0, len(small_redis_ips)):
         ip = small_redis_ips[i]
         if i == 0:
            if j == 0:
               new_str = "('{}', {}),\n".format(ip, port)
            else:
               new_str = "                                     ('{}', {}),\n".format(ip, port)
            _str = _str + new_str    
         elif i != (len(small_redis_ips) - 1):
            new_str = "                                     ('{}', {}),\n".format(ip, port)
            _str = _str + new_str 
         elif i == (len(small_redis_ips) - 1) and j == (len(small_redis_ports) - 1):
            new_str = "                                     ('{}', {})]".format(ip, port)
            _str = _str + new_str + ","   
         else:
            new_str = "                                     ('{}', {}),\n".format(ip, port)
            _str = _str + new_str   
   
   print(_str)

   proxy_str = "python3 proxy.py -bres"
   for port in big_redis_ports:
      for ip in big_redis_ips:
         proxy_str = proxy_str + " '{}'".format(ip)   

   proxy_str = proxy_str + " -brps "
   for port in big_redis_ports:
      proxy_str = proxy_str + ((str(port) + " ") * len(big_redis_ips))

   proxy_str = proxy_str + " -sres "
   for port in small_redis_ports:
      for ip in small_redis_ips:
         proxy_str = proxy_str + " '{}'".format(ip)  
         
   proxy_str = proxy_str + " -srps "
   for port in small_redis_ports:
      proxy_str = proxy_str + ((str(port) + " ") * len(small_redis_ips))
   print("\n\n", proxy_str)   
   
# Connect to VMs intended to host Redis
# Launch Redis servers.
def start_redis_servers(big_redis_ips, small_redis_ips, big_redis_ports = [6279], small_redis_ports = [6379], skip_big = False, skip_small = False, key_path = "C:\"):
   keyfile = paramiko.RSAKey.from_private_key_file(key_path)
   ssh_clients = list()
   timeout = 5

   big_redis_clients = list()
   small_redis_clients = list()
   
   if skip_big == False:
      for IP in big_redis_ips:
         ssh_redis = paramiko.SSHClient()
         ssh_clients.append((IP, ssh_redis))
         big_redis_clients.append((IP, ssh_redis))
         ssh_redis.set_missing_host_key_policy(paramiko.AutoAddPolicy())
         ssh_redis.connect(hostname = IP, username="ec2-user", pkey = keyfile) 
   
   if skip_small == False:
      for IP in small_redis_ips:
         ssh_redis = paramiko.SSHClient()
         ssh_clients.append((IP, ssh_redis))
         small_redis_clients.append((IP, ssh_redis))
         ssh_redis.set_missing_host_key_policy(paramiko.AutoAddPolicy())
         ssh_redis.connect(hostname = IP, username="ec2-user", pkey = keyfile) 
   
   cd_command = "cd /home/ec2-user/"
   redis_command = "sudo redis-server redis_{}.conf"   
   
   if skip_big == False:
      num_big = len(big_redis_clients) * len(big_redis_ports)
      current_count = 1
      for port in big_redis_ports:
         for IP,ssh_redis_client in big_redis_clients:
            success = False 
            num_tries = 0
            while success == False and num_tries < 1:
               num_tries += 1
               print("\n\nExecuting Commands for big " + str(IP) + " : " + str(port) + " ({}/{})".format(current_count, num_big))
               ssh_stdin, ssh_stdout, ssh_stderr = ssh_redis_client.exec_command(cd_command) 
               #print("Reading lines from stdout...")
               #lines = ssh_stdout.readlines()               
               print("Executed cd_command...")
               time.sleep(0.25)
               ssh_stdin, ssh_stdout, ssh_stderr = ssh_redis_client.exec_command(redis_command.format(port))
               print("Executed redis_command...")
               endtime = time.time() + timeout 
               while not ssh_stdout.channel.eof_received:
                  time.sleep(1)
                  if time.time() > endtime:
                     ssh_stdout.channel.close()
                     break
               print("Calling ssh_stdout.channel.recv_exit_status()...")
               ssh_stdout.channel.recv_exit_status()
               print("Reading lines from stdout...")
               lines = ssh_stdout.readlines()
               if len(lines) > 0:
                  success = True 
               for line in lines:
                   print(line)
            current_count = current_count + 1
            #print("ssh_stdout: {}, ssh_stderr: {}".format(ssh_stdout, ssh_stderr))
   
   num_small = len(small_redis_ports) * len(small_redis_clients)
   current_count = 1
   
   if skip_small == False:
      for port in small_redis_ports:
         for IP,ssh_redis_client in small_redis_clients:
            success = False 
            num_tries = 0
            while success == False and num_tries < 1:
               num_tries += 1
               print("\n\nExecuting Commands for small " + str(IP) + " : " + str(port) + " ({}/{})".format(current_count, num_small))
               ssh_stdin, ssh_stdout, ssh_stderr = ssh_redis_client.exec_command(cd_command) 
               print("Executed cd_command...")
               time.sleep(0.5)
               ssh_stdin, ssh_stdout, ssh_stderr = ssh_redis_client.exec_command(redis_command.format(port))
               print("Executed redis_command...")
               endtime = time.time() + timeout 
               while not ssh_stdout.channel.eof_received:
                  time.sleep(1)
                  if time.time() > endtime:
                     ssh_stdout.channel.close()
                     break
               print("Calling ssh_stdout.channel.recv_exit_status()...")
               ssh_stdout.channel.recv_exit_status()
               print("Reading lines from stdout...")
               lines = ssh_stdout.readlines()
               if len(lines) > 0:
                  success = True 
               for line in lines:
                   print(line)
            current_count = current_count + 1
            #print("ssh_stdout: {}, ssh_stderr: {}".format(ssh_stdout, ssh_stderr))   
   
   if skip_big == False:
      for ip, client in big_redis_clients:
         client.close()
   
   if skip_small == False:
      for ip, client in small_redis_clients:
         client.close()
  
# [6379, 6380, 6381, 6382, 6383, 6384, 6385, 6386, 6387, 6388]
def main():
   public_ips, scheduler_address, proxy_address, big_redis_ips, small_redis_ips = get_public_ips(num_small = 2, region_name="us-east-2")
   start_redis_servers(big_redis_ips, small_redis_ips, big_redis_ports = [6379, 6380, 6381, 6382, 6383, 6384, 6385], small_redis_ports = [6379, 6380, 6381], skip_big = False, skip_small = False, key_path = "C:\")
   print_python_code(scheduler_address, proxy_address, big_redis_ips, small_redis_ips, big_redis_ports = [6379, 6380, 6381, 6382, 6383, 6384, 6385], small_redis_ports = [6379, 6380, 6381])

if __name__ == "__main__":
   main()

