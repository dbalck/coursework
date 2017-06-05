#! /usr/bin/python

import os
import sys
from contextlib import contextmanager
import paramiko

class Master(object):

    def __init__(self, hosts):
        self.hosts = hosts

    def copy_agents(self, host):
        # copy downloader to nodes
        os.system("scp downloader root@{}:~".format(host))

        # copy mumbler to nodes
        os.system("scp mumbler root@{}:~".format(host))

    def dispatch_dl_order(self, start_idx, end_idx):
        size = len(self.hosts)
        all_chunklists = []
        for i in range(size):
            chunklist = []
            for j in range(start_idx, end_idx):
                if j % size == i: 
                    chunklist.append(j)
            all_chunklists.append(chunklist)

        

    def ssh_no_pass(self, host, cmd):
        ssh = paramiko.SSHClient()
        ssh.load_system_host_keys()
        try: 
            ssh.connect(host, "root")
            ssh.exec_command(cmd)
            yield ssh
            
        finally:
            ssh.close()

def main():
    hosts = sys.argv[1]
    hosts = hosts.split(",")
    master = Master(hosts)
    master.dispatch_dl_order(0, 100)
    for i in range(len(hosts)):
        host = hosts[i]
        stdin, stdout, stderr = master.ssh_no_pass(host, 'ls -a')
        print (stdout.read())

if __name__ == "__main__": main()
