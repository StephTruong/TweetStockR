from auth.credentials import AWS_CREDENTIALS
import json

def get_s3_connection(aws_credentials = AWS_CREDENTIALS):
	from boto.s3.connection import S3Connection
	return S3Connection(aws_credentials['access_key'], aws_credentials['access_key_secret'])


def main():
	conn = get_s3_connection()
	target = conn.lookup('warriorstags101001')

	with open('bulktweets.txt','wb') as f:
		t = 0
		for key in target.list():
			if t < 40:
				print key.key, t
				tweets = json.loads(key.get_contents_as_string())
				for tweet in tweets:
					try:
						text = tweet['text']
						text.replace('\n',' ')
						f.write(text+'\n')
					except UnicodeEncodeError:
						# print 'error', e
						pass
				t += 1


if __name__=="__main__":
	main()
