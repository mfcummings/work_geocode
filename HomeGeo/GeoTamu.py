import pandas as pd
import geocoder
import json
import config


class GeoTamu:
    """
        Home of the Geocoder method(s)
        The GeoCoder method is a wrapper for the geocoder.tamu method that returns a dataframe with lat/long data 
    """

    @staticmethod
    def GeoCoder (df):

        with req.Session() as session:
            # iterate through row, extract parts of address to variables, then construct the json, execute the geocoder.json request, handle the return file, NEXT #

            for index, row in df.iterrows():
                geocoded = False
                while geocoded is not True:
                    try:
                        # this is a good place to use the session.get() method
                        # the geocode_results should be the result of the get method
                        output = ['location={loc}, city={c}, state={s}, zipcode={z}, key={k}'.format( loc=df["Address"], c=df["City"], s=df["State"], z=df["ZipCode"], dict_keys=df["key"] ) ]
                        geocode_results = geocoder.tamu(output) #This is the failure point in Jupyter, I've tried dicts, formatted strings, lists but so far haven't made it work
                        geocode_results.json
                        #TODO: insert only lat/lon back into df
                    except Exception as e:
                        logger.exception(e)
                        geocoded=True

                    # check results
                    if geocode_results['status'] == 'OVER_QUERY_LIMIT': # TODO: OVER LIMIT, EXCEEDED BANDWIDTH, 404, etc., may want to add more error conditions depending on real life lessons
                        logger.info("Hit our limit!")
                        geocoded=False
                    else:
                        # If all goes well, we want to save the results
                        if geocode_results['status'] != 'OK':
                            logger.warning("Error geocoding {}: {}".format(address, geocode_results['status']))
                        logger.debug("Geocoded {}: {}".format(address, geocode_results['status']))
                        results.append(geocode_results)
                        geocoded = True



        return df