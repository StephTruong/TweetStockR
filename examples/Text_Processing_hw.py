__author__ = 'HowardW'

from nltk import word_tokenize, FreqDist
import nltk


class TweetFilter:
    def __init__(self):
        self.count = 0

    # This function filters out the list of stopwords and returns the cleaned up data
    def filter(self, file):
        stopWords = nltk.corpus.stopwords.words('english') + ['.',',','\'','?',':','#','@','rt','http','!',"'s",'-',';']
        total = []
        for items in file:
            if items.lower() not in stopWords:
                total.extend([items.lower()])
        return total



def main():
    obj = TweetFilter()
    with open("NBA_Warriors.txt", "r") as myFile:
        data = myFile.read().replace('\n',' ')
        data = unicode(data, 'utf-8')

    # This tokenizes each of the word in data
    tokenz = word_tokenize(data)

    # This passes the tokenz to the filter function
    newTokenz = obj.filter(tokenz)

    # Run a frequency distribution on the entire word list
    fdist1 = FreqDist(newTokenz)

    # Plots the top 30 words
    fdist1.plot(30, cumulative=False)

if __name__ == '__main__':
    main()