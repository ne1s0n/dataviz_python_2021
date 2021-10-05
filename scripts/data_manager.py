import os
import pandas as pd

def load_meteorites(local_file = '../data/NASA_meteorite_cleaned.pickle.zip', force_download = False, remote_url = "https://data.nasa.gov/api/views/gh4g-9sfh/rows.csv?accessType=DOWNLOAD"):
	"""Load and returns NASA meteorite data

	If the data are available locally they
	are just read from disk and returned. 
	Otherwise the data is downloaded, cleaned,
	saved locally and then returned.

	Parameters
	----------
	local_file : str, optional
	File name for the file to be read and saved locally, 
	containing the cleaned data
	
	force_download : boolean, optional
	If True the data is downloaded from the remote address anyway, 
	even if a local version is present
	
	remote_url : str, optional
	the URL for the remote file, to be downloaded in memory.
	"""
	
	#is the local file already available?
	local_available = os.path.isfile(local_file)
	
	#should we just return the data?
	if (local_available and not force_download):
		return(pd.read_pickle(local_file))
	
	#if we get here, data must be downloaded from the remote source
	df = pd.read_csv(remote_url)
	
	#--- data cleanup---
	#removing duplicated lines, and lines with missing data
	df = df.drop_duplicates() #removes zero rows
	df = df.dropna()          #removes ~7000 rows
	
	#example of value substitution
	#the column "fall" has two values: ['Fell' 'Found']
	#I want to update the first label to "Fallen"
	df.loc[df.fall == 'Fell', 'fall'] = 'Fallen'
	#print(df.fall.unique()) #this instruction to check the actual data
	
	#converting the 'year' column to proper date type
	#example of raw date: '01/01/1880 12:00:00 AM'
	#for details on codes: https://docs.python.org/3/library/datetime.html#strftime-and-strptime-behavior
	df['year_as_date'] = pd.to_datetime(df.year, format='%m/%d/%Y %I:%M:%S %p', errors = 'coerce')
	
	#Some dates have not been converted correctly, because pandas represents
	#time stamps as nanoseconds and it runs out of space for dates very
	#far in the past or in the futures. As a consequence some lines
	#have now NaT (Not a Time) as "year_as_date" value
	#print(df.year_as_date.isna().sum()) #this would print: 14
	
	#saving the data locally, serializing it to preserve types
	#doc: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_pickle.html
	df.to_pickle(local_file, compression='infer')
	
	#and we are done
	return (df)
