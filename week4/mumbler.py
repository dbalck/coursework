#! /usr/bin/python

import os
import sys
import urllib
from contextlib import contextmanager
import socket
import string
import csv
import random
import operator

url_template = "http://storage.googleapis.com/books/ngrams/books/googlebooks-eng-all-2gram-20090715-{}.csv.zip"
start_file_idx = 0
end_file_idx = 1

class Mumbler(object):

    def __init__(self):
        self.local_hostname = socket.gethostname().split(".")[0]
        self.local_indices = self.get_local_indices()

    def get_local_indices(self):
        local_gpfs = "/gpfs/gpfsfpo/%s/" % self.local_hostname
        files = []
        for (dirpath, dirnames, filenames) in os.walk(local_gpfs):
            files.extend(filenames)
        return [f for f in files if "index" in f ]

    def get_prior(self, word, filename):
        for i in open(filename):
            if (i.startswith(word)): yield i

    def pick_next(self, probs):
        rand = random.random()
        cumprob = 0
        for key, value in probs.iteritems():
            cumprob += value
            if cumprob > rand: return key.split(" ")[1]

    def print_top_probs(self, probs, num):
        sorted_items = sorted(probs.items(), key=operator.itemgetter(1), reverse=True)
        print sorted_items[:num]


    def local_mumble(self, chain):
        print chain
        local_indices = []
        for ch in self.local_indices:
            local_indices.append("/gpfs/gpfsfpo/%s/%s" % (self.local_hostname, ch))
        incidence = 0
        print local_indices
        for f in local_indices:
            with open(f) as csvfile:
                data = csv.reader(csvfile, delimiter="\t", quoting=csv.QUOTE_NONE)
                file_as_list = [i for i in data]
                filtered = filter(lambda x: x[0].split(" ")[0].lower() == chain[-1].lower(), file_as_list) 
                if not filtered: continue 
                col = [row[1] for row in filtered]
                incidence = reduce(lambda x, y: int(x) + int(y), col)
                mapped = map(lambda x: {x[0]: int(x[1]) / float(incidence)}, filtered)
                probs = {}
                for d in mapped:
                    probs.update(d)
                return probs
                # self.print_top_probs(probs, 5)

    def remote_mumble(self, host, chain):
        print "noop"
        return {}

    def mumble(self, chain):
        gpfs = "/gpfs/gpfsfpo/"
        directories = []
        global_probs = {}
        for (dirpath, dirnames, filenames) in os.walk(gpfs):
            directories.extend(dirnames)
        for d in directories:
            if not d.startswith('gpfs'): 
                continue
            elif d == self.local_hostname:
                global_probs.update(self.local_mumble(chain))
            else:
                global_probs.update(self.remote_mumble(d, chain))

        if not global_probs:
            print "no probs left!"
            return
        next_word = self.pick_next(global_probs)
        print next_word
        chain.append(next_word)
        self.mumble(chain)
        

        


def main():
    start_word = sys.argv[1]
    mumbler = Mumbler()
    mumbler.mumble([start_word])


if __name__ == "__main__": main()

