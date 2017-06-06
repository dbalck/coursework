#! /home/dbalck/anaconda2/bin/python 

import os
import sys
from contextlib import contextmanager
import paramiko

class Master(object):

    def __init__(self, hosts, pk_path):
        self.hosts = hosts
        self.pk_path = pk_path

    def deploy_agent(self, host):
        # copy downloader to nodes
        os.system("scp -i {} downloader.py mumbler.py root@{}:~ && ssh -i {} root@{}  'chmod +x downloader.py &&  chmod +x mumbler.py && rm -f /gpfs/gpfsfpo/google*'".format(self.pk_path, host, self.pk_path, host))

    def deploy_agents(self):
        for host in self.hosts:
            self.deploy_agent(host)

    # splits up indices between hosts, returns a list of lists of indices that should be queried
    def divy_up(self, start_idx, end_idx):
        host_num = len(self.hosts)
        chunk_total = end_idx - start_idx 
        quotient = chunk_total / host_num 
        remainder = chunk_total % host_num 
        all_chunklists = []
        chunklist = []
        count  = 0
        host_idx = 0
        for i in range(start_idx, end_idx):
            if (quotient > 0):
                quotient = quotient - 1
                chunklist.append(i)
            else:
                if (remainder > 0):
                    remainder -= 1
                    chunklist.append(i)
                else:
                    chunklist.append(i)
                all_chunklists.append(chunklist[:])
                chunklist = []
                quotient = chunk_total / host_num 

        all_chunklists.append(chunklist[:])
        return all_chunklists


    def start_downloader(self, host, chunklist):
        cmd = "./downloader.py {} &".format(",".join(map(str,chunklist)))
        return self.ssh_no_pass(host, cmd)

    # returns stdout of given command to specified host via ssh
    def ssh_no_pass(self, host, cmd):
        ssh = paramiko.SSHClient()
        k = paramiko.RSAKey.from_private_key_file(self.pk_path)
        ssh.load_system_host_keys()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try: 
            ssh.connect(host, username="root", pkey=k)
            stdin, stdout, stderr =  ssh.exec_command(cmd)
            print stderr.read()
            return stdout
        finally:
            ssh.close()
            
def main():
    hosts = sys.argv[1].split(",")
    hostnames = []
    ips = []
    for host in hosts:
        one, two = host.split(":")
        ips.append(one)
        hostnames.append(two)
    primary_host = ips[0]
    pk_path = sys.argv[2]
    master = Master(ips, pk_path)
    master.deploy_agents()
    chunklists = master.divy_up(0,1)
    os.system("ssh -i {} root@{} 'rm -f chunk_locations'".format(pk_path, primary_host))
    for i in range(len(hosts)):
        ch = ",".join(map(str,chunklists[i]))
        os.system("ssh -i {} root@{} echo '{} {} >> chunk_locations'".format(pk_path, primary_host, hostnames[i], ch ))
    #for i in range(len(hosts)):
    #    stdout = master.start_downloader(hosts[i], chunklists[i])
    #    print stdout.read()

if __name__ == "__main__":
    main()
