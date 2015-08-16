# Python Libraries
import os
import json
import logging

# Third Party libraries
from boto.s3.connection import S3Connection
from boto.s3.key import Key

DOCSTRING="""
	Helper functions for working with Amazon S3 through boto
"""

def get_s3_connection(authfile='auth/aws.json'):
	print os.getcwd()
	with open(authfile, 'rb') as auth:
		auth = json.load(auth)
		return S3Connection(auth['access_key'], auth['access_key_secret'])

def get_or_create_bucket(name, conn = get_s3_connection()):
	logging.debug('Getting bucket from S3: %s', name)
	bucket = conn.lookup(name) # method returns none if bucket does not exist
	if bucket is None:
		bucket = conn.create_bucket(name)
	return bucket

def write_string_to_key(bucket, keyname, string):
	logging.info('Writing to S3: %s', keyname)
	key = Key(bucket)
	key.key = keyname
	key.set_contents_from_string(string)
	logging.info('Finished Writing to S3: %s', keyname)

if __name__=="__main__":
	pass
