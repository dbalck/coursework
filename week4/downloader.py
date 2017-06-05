#! /usr/bin/python

import csv
import sys
import urllib
from os import walk
import zipfile

class Downloader(object):

    def __init__(self, chunklist, template_url):
        self.chunks = chunklist.split(",")
        self.template_url = template_url

    def get_data(self):
        for chunk in self.chunks:
            self.get_chunk(chunk)

    def get_chunk(self, chunk):
        conn = urllib.FancyURLopener()
        print self.template_url % chunk
        conn.retrieve(self.template_url % chunk, '/gpfs/gpfsfpo/chunk-%s' % chunk)

    def open_data(self):
        chunks = []
        for (dirpath, dirnames, filenames) in walk('/gpfs/gpfsfpo'):
            chunks.extend(dirpath + filenames)
        for ch in chunks:
            z = zipfile.ZipFile(ch, 'r')
        

            
    def open_chunk(self, filename):
        with open(filename, 'r') as csv:
            f = csv.reader(csv, delimiter='\t')
            word = ""
            one_count = 0
            for line in f:
                if word == line[0]:
                    # this is not a new word, increment word count
                    count += line[2]

                else:
                     # new word, re-establish new count
                     print "nop"

def main():
    chunklist = sys.argv[1]
    #print "downloading %s!" % chunklist 
    url_template = "http://storage.googleapis.com/books/ngrams/books/googlebooks-eng-all-2gram-20090715-%s.csv.zip"
    downloader = Downloader(chunklist, url_template)
    downloader.get_data()




if __name__ == "__main__": main()


