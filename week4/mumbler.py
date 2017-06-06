#! /usr/bin/python

import os
import sys
import urllib
from contextlib import contextmanager
import socket
import string
import csv

url_template = "http://storage.googleapis.com/books/ngrams/books/googlebooks-eng-all-2gram-20090715-{}.csv.zip"
start_file_idx = 0
end_file_idx = 1

class Mumbler(object):

    def __init__(self):
        f = open("/root/chunk_locations", 'r')
        self.location_dict = {}
        for line in f:
            temp = line.split(" ")
            host = temp[0]
            chunks = temp[1].split(",")
            self.location_dict[host] = chunks

    def get_local_chunks(self):
        hostname = socket.gethostname().split(".")[0]
        local_chunks = self.location_dict[hostname]
        return local_chunks

    def get_prior(self, word, filename):
        for i in open(filename):
            if (i.startswith(word)): yield i

    def mumble(self, word):
        local_indices = []
        for ch in self.get_local_chunks():
            local_indices.append("/gpfs/gpfsfpo/index-%s" % ch.strip())
        print local_indices
        data = [] 
        for f in local_indices:
            with open(f) as csvfile:
                data = csv.reader(csvfile, delimiter="\t", quoting=csv.QUOTE_NONE)
                file_as_list = [i for i in data]
                filtered = filter(lambda x: x[0].split(" ")[0] == word, file_as_list) 
                print filtered
                prior = reduce(lambda x, y: x[1] + y[1], filtered)
                print prior



def main():
    mumbler = Mumbler()
    mumbler.mumble("five")


if __name__ == "__main__": main()

