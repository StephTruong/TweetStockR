import os
import math
import subprocess
import shutil, tarfile
from aws import get_s3_connection
from filechunkio import FileChunkIO
import pymongo as pmg
from databases import get_mongo_conn
from datetime import datetime as dt, timedelta

###### BACKUP METHODS #######

def dump_mongo(outname = 'mongodumpdir', dumpargs = []):
	"""call mongodb `mongodump` to create db backup"""
	subprocess.call(['mongodump','--out', outname] + dumpargs) # add any additional arguments to cmd line call e.g. -v for verbose

def compress_mongodump(dumpdir='mongodumpdir', archive='mongodump_cmp'):
	"""compress directory to tar.gz file"""
	print 'Compressing Backup'
	dumpname = archive + dt.now().strftime('%Y%m%d%H%M%S')
	shutil.make_archive(dumpname, 'gztar', dumpdir)
	return dumpname + '.tar.gz'


# DEPRECATED
def send_backup_to_s3(bucketname='tweetstock-mongo-dump', keyname='mongodump_cmp.tar.gz', archive='mongodump_cmp.tar.gz'):

	"""chunked upload to S3
	adapted from http://boto.readthedocs.org/en/latest/s3_tut.html
	"""

	con = get_s3_connection()
	bucket = con.get_bucket(bucketname)

	mp = bucket.initiate_multipart_upload(keyname)

	filesize = os.stat(archive).st_size
	chunksize = 52428800
	chunkcount = int(math.ceil(filesize / float(chunksize)))

	for i in range(chunkcount):
		print "Uploading part", i + 1
		offset = chunksize * i
		b = min(chunksize, filesize - offset)
		with FileChunkIO(archive, 'r', offset=offset, bytes=b) as fc:
			mp.upload_part_from_file(fc, part_num = i + 1)
	mp.complete_upload()
	print 'Upload Complete'

###### RESTORE METHODS #######

def S3_download_progress(a,b):
	# callback function to report progress of download
	print "%d bytes out of %d. %f%% complete" % (a,b, 100*float(a)/b)

def download_archive_from_s3(bucketname='tweetstock-mongo-dump', keyname='mongodump_cmp.tar.gz', archive='mongodump_cmp.tar.gz'):
	con = get_s3_connection()
	bucket = con.get_bucket(bucketname)
	key = bucket.get_key(keyname)
	print "Downloading archive", keyname
	key.get_contents_to_filename(archive, cb=S3_download_progress)
	print 'Archive Download Complete', keyname

def restore_mongo_from_archive(archive='mongodump_cmp.tar.gz', dumpdir = 'mongodumpdir', dumpargs = []):
	print 'Unpacking archive', archive
	tar = tarfile.open(archive)
	tar.extractall(dumpdir)
	print 'Archive Unpacked'
	subprocess.call(['mongorestore', dumpdir] + dumpargs)


# def clean_up_collections(conn, daysback=2):
# 	prices = conn['tweetstock'].prices
# 	sentiment = conn['tweetstock'].sentiment

# 	limit = dt.now() - timedelta(days=daysback)

# 	print dt.now(), limit



if __name__=="__main__":

	# ARCHIVE
	dump_mongo(dumpargs=['--db', 'tweetstock'])
	dumpname = compress_mongodump()
	# send_backup_to_s3()
	subprocess.call( ['aws', 's3', 'cp', dumpname, 's3://tweetstock-mongo-dump'] )

	# RESTORE FROM ARCHIVE
	# download_archive_from_s3()
	# restore_mongo_from_archive()
