import pandas as pd
import requests as req
import logging
import geocoder
import config
import GeoTamu
import source_data

######  Set up logging ===> REFERENCE: https://python.readthedocs.io/en/latest/library/logging.html

# create an instance of the logging class
logger = logging.getLogger(__name__)

# set log level -- Debug is the most verbose https://python.readthedocs.io/en/latest/library/logging.html#levels
logger.setLevel(logging.DEBUG)

# set formatting for output https://python.readthedocs.io/en/latest/library/logging.html#logging.Formatter
formatter = logging.Formatter('%(name)s:%(asctime)s:%(message)s')

# setup for log file
file_handler = logging.FileHandler('geopy.log')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)

# now pass the file handler to the logger
logger.addHandler(file_handler)

try:
    # Invoke the Delta method, return df here for Geo Processing
    delta_df = source_data.delta()
    if len(delta_df) > 0:
        #print(delta_df.head())
        new_df = GeoTamu.GeoCoder(delta_df)
        # TODO: insert into GEO_MemberAddress table
except Exception as e:
    logger.exception(e)
    print(str("no dice loading df"))

# we're all done now
logger.info("Finished Geocoding this batch")

# write the results to csv TODO: Wrap in io safe try/catch with error logging
#pd.DataFrame(df).to_csv(output_filename, encoding='uft8')