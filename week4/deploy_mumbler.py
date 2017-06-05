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
        os.system("scp -i {} downloader.py root@{}:~".format(self.pk_path, host))
        os.system("ssh -i {} root@{} chmod +x downloader.py".format(self.pk_path, host))

        # copy mumbler to nodes
        os.system("scp -i {} mumbler.py root@{}:~".format(self.pk_path, host))
        os.system("ssh -i {} root@{} chmod +x mumbler.py".format(self.pk_path, host))

    def deploy_agents(self):
        for host in self.hosts:
            self.deploy_agent(host)

    # splits up indices between hosts, returns a list of lists of indices that should be queried
    def divy_up(self, start_idx, end_idx):
        size = len(self.hosts)
        all_chunklists = []
        for i in range(size):
            chunklist = []
            for j in range(start_idx, end_idx):
                if j % size == i: 
                    chunklist.append(j)
            all_chunklists.append(chunklist)
        return all_chunklists

    def start_downloader(self, host, chunklist):
        cmd = "./dowloader.py {}".format(",".join(map(str,chunklist)))
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
            return stdout.read()
            
        finally:
            ssh.close()
            
def main():
    hosts = sys.argv[1].split(",")
    pk_path = sys.argv[2]
    master = Master(hosts, pk_path)
    master.deploy_agents()
    chunklists = master.divy_up(0, 100)
    for i in range(len(hosts)):
        print master.start_downloader(hosts[i], chunklists[i])

if __name__ == "__main__":
    main()
