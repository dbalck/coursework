#! /usr/bin/python

import csv
import sys
import urllib
import os
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

    def unzip_data(self):
        print "unzipping!"

        files = []
        for (dirpath, dirnames, filenames) in os.walk('/gpfs/gpfsfpo/'):
            files.extend(filenames)
            print files
        for f in files:
            z = zipfile.ZipFile('/gpfs/gpfsfpo/' + f, 'r')
            z.extractall('/gpfs/gpfsfpo/')
            z.close()
            os.remove('/gpfs/gpfsfpo/' + f)

            
    def index_chunk(self, filename):
        index = open(filename, 'a')
        with open(filename, 'r') as csvfile:
            f = csv.reader(csvfile, delimiter='\t')
            word = ""
            one_count = 0
            for line in f:
                if word == line[0]:
                    # this is not a new word, increment word count
                    one_count += line[3]

                else:
                     # new word, re-establish new count and write to index
                     index.write("%s %s %s" % (line[0], str(one_count), '\n'))
                     one_count = line[3] 
        index.close()

def main():
    chunklist = sys.argv[1]
    #print "downloading %s!" % chunklist 
    url_template = "http://storage.googleapis.com/books/ngrams/books/googlebooks-eng-all-2gram-20090715-%s.csv.zip"
    downloader = Downloader(chunklist, url_template)
    downloader.get_data()
    downloader.unzip_data()
    downloader.index_chunk("googlebooks-eng-all-2gram-20090715-0.csv")



if __name__ == "__main__": main()


