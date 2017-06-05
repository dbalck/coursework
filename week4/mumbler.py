import paramiko
import urllib
from contextlib import contextmanager

host = '50.97.251.93'
username = 'root'
password = 'EGVa4fcm'


url_template = "http://storage.googleapis.com/books/ngrams/books/googlebooks-eng-all-2gram-20090715-{}.csv.zip"
start_file_idx = 0
end_file_idx = 1

class agent():

    def _init_():



    def ssh_no_pass(self, host=host, username=username, password=password):
        try: 
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy()) 
            ssh.connect(host, username=username, password=password)
            yield ssh
            
        finally:
            ssh.close()




def main():
    get_chunk()

# def main():
#     print "starting..."
#     client = create_ssh()
#     for i in client:
#         stdin, stdout, stderr = i.exec_command('ls -l')
#         print stdout.read()

if __name__ == "__main__": main()
