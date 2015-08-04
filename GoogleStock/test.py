import csv

mFile = open('tickerlist.csv','rb') 

reader =csv.reader(mFile)

tickerString=""
first=True

for i in reader:
	if not first:
		tickerString=tickerString+','

	tickerString=tickerString+'NYSE:'+i[0]
	first=False

print tickerString

