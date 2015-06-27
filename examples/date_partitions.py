from datetime import datetime, timedelta

api_date_format = '%Y-%m-%d'

def generate_dates(start, end):
	"""
	Expects input objects of type <type 'datetime.datetime'>
	End date is inclusive
	"""
	if end < start:
		raise Exception('End Date must be after Start Date')

	current = start
	while current <= end:
		yield current
		current += timedelta(days=1)

if __name__=="__main__":
	a = generate_dates('2015-01-01', '2015-01-02')
	for i in a:
		print i
