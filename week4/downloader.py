import csv
import sys

class downloader():

    def __init__(self, chunklist, template_url):
        self.chunks = chunklist.split(",")
        self.template_url = template_url

    def get_data(self, template, start, end):
        for chunk in self.chunks:
            get_chunk(chunk)

    def get_chunk(self):
        for i in self.chunks 
            conn = urllib.FancyURLopener()
            conn.retrieve(self.template_url.format(i), 'chunk-{}'.format(i))
            
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

def main():
    chunklist = sys.argv[1]
    url_template = "http://storage.googleapis.com/books/ngrams/books/googlebooks-eng-all-2gram-20090715-{}.csv.zip"
    start_file_idx = 0
    end_file_idx = 1

    downloader = downloader(chunklist, url_template)
    downloader.get_data()




if __name__ == "__main__": main()


