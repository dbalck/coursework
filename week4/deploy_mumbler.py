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
        os.system("scp -i {} downloader.py mumbler.py root@{}:~ &&  chmod +x downloader.py &&  chmod +x mumbler.py".format(self.pk_path, host))
        #os.system("ssh -i {} root@{} ".format(self.pk_path, host))

        # copy mumbler to nodes

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
            if stderr != None: print stderr.read()
            yield stdout
        finally:
            ssh.close()
            
def main():
    hosts = sys.argv[1].split(",")
    pk_path = sys.argv[2]
    master = Master(hosts, pk_path)
    master.deploy_agents()
    chunklists = master.divy_up(0, 6)
    for i in range(len(hosts)):
        stdout = master.start_downloader(hosts[i], chunklists[i])
        for i in stdout:
            print i.read()

if __name__ == "__main__":
    main()
