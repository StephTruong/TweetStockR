# Tweet-Stock Model Application

This application ingests tweets from the Twitter Streaming API, tracking stocks in the S&P 500. These tweets are analyzed for sentiment, which is then combined with stock ticker data to produce buy, sell, or hold suggestions for each individual stock.

App configuration directions for Amazon EC2 servers are in `App/AWS \STARTUP \ROUTINE.txt`. We recommend at least m3.xlarge type instance or better.

To run the app:

`python main.py`

This main execution file call three subprocesses that run tweet acquisition/parsing, stock price acquisition, and buy/sell projection.

### Requirements

Python Requirements:
```
Python 2.7+
requests # for stock acquisition
numpy # sentiment analysis
scipy # sentiment analysis
sklearn # sentiment analysis
tweepy # tweet acquisition
boto # aws interface
pymongo # mongodb interface
```

System Requirements:
```
multi-core processor for best results / scaling up
MongoDB 2.0+
aws cli # configured for s3 backups
```

Also ensure that a local MongoDB cluster is set up before running.

### Authorization

In order to properly run, inside the `App` directory, users must create a directory called 'auth' that contains a file names 'aws.json' containing JSON formatted access token and secret for AWS.

This folder must also contain twitter credentials for any twitter accounts that will be used in the stream.

### Active Scripts

`main.py` calls three programs:

* `tweet_stream.py` takes in three streams simultaneously from Twitter via the Tweepy Python library. These tweets are stored to a locally-running instance of MongoDB.
* `stock.py` acquires stock prices from the deprecated, but still active, Google Finance API endpoint. Google uses this API to produce stock price charts on their homepage, so it seems unlikely this API endpoint will stop responding until they roll out a new endpoint for their own usage.
* `buy_or_sell.py` is the stock momentum model calculator. It analyzes recent stock prices and Twitter history to determine whether or not both sentiment and prices are moving the same direction. It emits suggestions to a collection in MongoDB. Prediction is parallelized using the `multiprocessing` library in Python.

