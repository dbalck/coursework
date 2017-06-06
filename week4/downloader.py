#! /usr/bin/python

import csv
import sys
import urllib
import os
import zipfile
import codecs

class Downloader(object):

    def __init__(self, chunklist, template_url):
        self.chunks = chunklist.split(",")
        self.template_url = template_url

    def get_data(self):
        for chunk in self.chunks:
            self.get_chunk(chunk)

    def get_chunk(self, chunk):
        conn = urllib.FancyURLopener()
        conn.retrieve(self.template_url % chunk, '/gpfs/gpfsfpo/chunk-%s' % chunk)

    def unzip_data(self):
        print "unzipping!"

        files = []
        for (dirpath, dirnames, filenames) in os.walk('/gpfs/gpfsfpo/'):
            files.extend(filenames)
        for f in files:
            num = f[-1]
            z = zipfile.ZipFile('/gpfs/gpfsfpo/' + f, 'r')
            z.extractall('/gpfs/gpfsfpo/')
            z.close()
            os.rename('/gpfs/gpfsfpo/googlebooks-eng-all-2gram-20090715-%s.csv' % num, "chunk-%s" % num)

            
    def iterate_chunk(self, filename):
        csv.field_size_limit(sys.maxsize)
        with open(filename, 'r') as csvfile:
            f = csv.reader(csvfile, delimiter='\t', lineterminator='\n', quoting=csv.QUOTE_NONE)
            ngram = ""
            count = 0
            for line in f:
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

    def index_chunk(self, filename):
        print "indexing"
        index = open("index-%s" % filename[-1], 'a')
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
    downloader.index_chunk("chunk-0")



if __name__ == "__main__": main()


