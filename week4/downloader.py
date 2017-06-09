#! /usr/bin/python

import csv
import sys
import urllib
import os
import zipfile
import codecs
import socket
import shutil
import string
import re

class Downloader(object):

    def __init__(self, chunklist, template_url):
        self.chunks = chunklist.split(",")
        self.template_url = template_url
        self.hostname = self.get_hostname()
        # clear out data if it exists
        # shutil.rmtree("/gpfs/gpfsfpo/%s/" % self.hostname)
        self.local_gpfs = "/gpfs/gpfsfpo/%s/" % self.hostname
        if not os.path.exists(self.local_gpfs):
            os.mkdir(self.local_gpfs)

    def get_data(self):
        for chunk in self.chunks:
            self.get_chunk(chunk)

    def get_hostname(self):
        return socket.gethostname().split(".")[0]

    def get_chunk(self, chunk_num):
        conn = urllib.FancyURLopener()
        if os.path.exists("%schunk-%s" % (self.local_gpfs, chunk_num)):
            return
        conn.retrieve(self.template_url % chunk_num, '%schunk-%s' % (self.local_gpfs, chunk_num))

    def unzip_data(self):
        print "unzipping!"
        files = []
        for (dirpath, dirnames, filenames) in os.walk(self.local_gpfs):
            files.extend(filenames)
        for f in files:
            if not f.startswith("chunk"):
                continue
            p = re.compile("\d+$")
            num = p.search(f).group(0)
            if not zipfile.is_zipfile(self.local_gpfs + f):
                print "zipfile already extracted, skipping."
                continue
            z = zipfile.ZipFile(self.local_gpfs + f, 'r')
            z.extractall(self.local_gpfs)
            z.close()
            source = "%sgooglebooks-eng-all-2gram-20090715-%s.csv" % (self.local_gpfs, num)
            target = "%schunk-%s" % (self.local_gpfs, num)
            os.rename(source, target)
            
    def iterate_chunk(self, filename):
        csv.field_size_limit(sys.maxsize)
        with open(filename, 'r') as csvfile:
            f = csv.reader(csvfile, delimiter='\t', lineterminator='\n', quoting=csv.QUOTE_NONE)
            ngram = ""
            count = 0
            for line in f:
                # drop line if it contains non-ascii
                if not line[0].translate(None, " ").isalpha():
                    continue
                # if the first word as been seen before
                if ngram == line[0]:
                    count = count + int(line[2])
                    continue
                # new primary word, write the line and clear count 
                else:
                    if ngram == "":
                        ngram = line[0]
                        count = int(line[2])
                        continue
                    yield ngram, count
                    ngram = line[0]
                    count = int(line[2])

    def index_all_chunks(self, chunklist):
        for ch in chunklist.split(","):
            local_chunk = "%schunk-%s" % (self.local_gpfs, ch)
            self.index_chunk(local_chunk)

    def index_chunk(self, filename):
        p = re.compile("\d+$")
        num = p.search(filename).group(0)
        if (os.path.exists("%sindex-%s" % (self.local_gpfs, num))):
            print "%s already indexed, skipping." % filename
            return

        print "indexing %s" % filename
        index = open("%sindex-%s" % (self.local_gpfs, num), 'a')
        it = self.iterate_chunk(filename)
        for ngram, count in it:
            index.write("%s\t%d\n" % (ngram, count))
        index.close()



    def grep_for_word(word):
        os.system("grep '^%s'" % word)


def main():
    chunklist = sys.argv[1]
    #print "downloading %s!" % chunklist 
    url_template = "http://storage.googleapis.com/books/ngrams/books/googlebooks-eng-all-2gram-20090715-%s.csv.zip"
    downloader = Downloader(chunklist, url_template)
    downloader.get_data()
    downloader.unzip_data()
    downloader.index_all_chunks(chunklist)



if __name__ == "__main__": main()


