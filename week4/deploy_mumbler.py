#! /home/dbalck/anaconda2/bin/python 

import os
import sys
from contextlib import contextmanager
import paramiko
import Queue
import threading

class Master(object):

    def __init__(self, hosts, pk_path):
        self.hosts = hosts
        self.pk_path = pk_path
        self.outlock = threading.Lock()

    def deploy_agent(self, host):
        # copy downloader to nodes
        os.system("scp -i {} downloader.py mumbler.py root@{}:~ && ssh -i {} root@{}  'chmod +x downloader.py &&  chmod +x mumbler.py && rm -f /gpfs/gpfsfpo/google*'".format(self.pk_path, host, self.pk_path, host))

    def deploy_agents(self):
        for host in self.hosts:
            self.deploy_agent(host)

    # splits up indices between hosts, returns a list of lists of indices that should be queried
    def divy_up(self, hostnames, start_idx, end_idx):
        host_num = len(hostnames)
        chunklists = {}

        chunk_total = end_idx - start_idx
        quotient = chunk_total / host_num
        remainder = chunk_total % host_num
        multiple = 0
        counter = 0

        remainder_incre = 0
        for host in hostnames:
            if remainder > 0: 
                remainder_incre += 1
                remainder -= 1

            multiple += 1
            chunklists[host] = range(counter, (multiple * quotient) + remainder_incre)
            counter = (multiple * quotient) + remainder_incre


        print "chunklists to be deployed: " 
        print chunklists
        return chunklists


    def start_downloader(self, host, chunklist):
        cmd = "./downloader.py %s" % chunklist
        self.ssh_no_pass(host, cmd)

    # returns stdout of given command to specified host via ssh
    def ssh_no_pass(self, host, cmd):
        ssh = paramiko.SSHClient()
        k = paramiko.RSAKey.from_private_key_file(self.pk_path)
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try: 
            ssh.connect(host, username="root", pkey=k)
            stdin, stdout, stderr =  ssh.exec_command(cmd)
            with self.outlock:
                print stdout.readlines()
        finally:
            ssh.close()
            
def main():
    # Put True here if you want the downloader to execute
    execute_downloader = False 

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
    all_chunklists = master.divy_up(hostnames, 0,11)
    os.system("ssh -i {} root@{} 'rm -f chunk_locations'".format(pk_path, primary_host))

    if not execute_downloader: return

    threads = []
    for host in hosts:
        ip, hostname = host.split(":")
        cl = ",".join(map(str, all_chunklists[hostname]))
        t = threading.Thread(target=master.start_downloader, args=(ip, cl))
        t.daemon = True
        t.start()
        threads.append(t)

    for t in threads:
        t.join()
    

if __name__ == "__main__":
    main()
