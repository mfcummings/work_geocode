import config

class source_data():
    """
        Once invoked, returns a pandas dataframe populated with Name Prime member account information where NamePrime.Checksum != GeoMember.Checksum.
        One Class, two methods, each gathering different slices of data.  
            
            delta:  Check for address changes.  By using the "BINARY_CHECKSUM" function, we compare the Geo_MemberAddress table against Name Prime, returning those rows found in both but different checksum
            new_member: Check for accounts found in Name Prime not present in the Geo_MemberAddress table, returning those new member account addresses
    
    """
    import pyodbc
    import pandas as pd

    @staticmethod
    def delta():

        conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};'
                              'SERVER=aldrin;'
                              'DATABASE=ARCUSYM000;'
                              'Trusted_Connection=yes;')

        delta_query = """
                DECLARE @ProcessDate varchar(10) = (SELECT MAX(ProcessDate) FROM dbo.LOAN)
                ;WITH NamePrime AS (
                SELECT --TOP 10 
		                PARENTACCOUNT
	                ,	SSN
	                ,	STREET
	                ,	EXTRAADDRESS
	                ,	CITY
	                ,	STATE
	                ,	ZIPCODE
	                ,	BINARY_CHECKSUM( RTRIM(LTRIM(PARENTACCOUNT)) + LTRIM(RTRIM(STREET)) + LTRIM(RTRIM(ISNULL(EXTRAADDRESS, ''))) + LTRIM(RTRIM(CITY)) + LTRIM(RTRIM(STATE)) + LTRIM(RTRIM(ZIPCODE)) )  AS CHECK_SUM
                FROM	dbo.NAME
                WHERE	ProcessDate = @ProcessDate
                AND		STREET IS NOT NULL -- No point in geocoding where the address is missing
                AND		TYPE = 0 -- Primary mailing address only, geocoding a PO box would be of little value
                ),
                GeoName AS (
                SELECT 
		                Account AS PARENTACCOUNT
	                ,	SSN
	                ,	Street AS STREET
	                ,	ExtraAddress AS EXTRAADDRESS
	                ,	City AS CITY
	                ,	State AS STATE
	                ,	ZipCode AS ZIPCODE
	                ,	BINARY_CHECKSUM( RTRIM(LTRIM(Account)) + LTRIM(RTRIM(Street)) + LTRIM(RTRIM(ISNULL(ExtraAddress, ''))) + LTRIM(RTRIM(City)) + LTRIM(RTRIM(State)) + LTRIM(RTRIM(ZipCode)) )  AS CHECK_SUM
                FROM	[cu].[GEO_MemberAddress]
                )
                SELECT 
		                np.PARENTACCOUNT as Account
	                ,	np.SSN
	                ,	np.STREET
	                ,	np.EXTRAADDRESS
	                ,	np.CITY
	                ,	np.STATE
	                ,	np.ZIPCODE
	                ,	np.CHECK_SUM
                FROM	NamePrime np
                JOIN	GeoName gn ON np.PARENTACCOUNT =  gn.PARENTACCOUNT
                where np.CHECK_SUM != gn.CHECK_SUM
                """
        
        delta_df = pd.read_sql(delta_query, conn)
        # concatenate Street + ExtraAddress into new field 'ADDRESS'
        delta_df['ADDRESS'] = delta_df.STREET + ' ' + delta_df.EXTRAADDRESS.fillna('')
        delta_df['ADDRESS'] = delta_df['ADDRESS'].str.strip()
        delta_df['lat'] = None
        delta_df['lon'] = None
        delta_df['key'] = config.tamu_api_key

        return delta_df

    @staticmethod
    def new_member():

        conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};'
                        'SERVER=aldrin;'
                        'DATABASE=ARCUSYM000;'
                        'Trusted_Connection=yes;')

        new_member_query = """
        DECLARE @ProcessDate varchar(10) = (SELECT MAX(ProcessDate) FROM dbo.LOAN)
        ;WITH NamePrime AS (
        SELECT --TOP 10 
		        PARENTACCOUNT
	        ,	SSN
	        ,	STREET
	        ,	EXTRAADDRESS
	        ,	CITY
	        ,	STATE
	        ,	ZIPCODE
	        ,	BINARY_CHECKSUM( RTRIM(LTRIM(PARENTACCOUNT)) + LTRIM(RTRIM(STREET)) + LTRIM(RTRIM(ISNULL(EXTRAADDRESS, ''))) + LTRIM(RTRIM(CITY)) + LTRIM(RTRIM(STATE)) + LTRIM(RTRIM(ZIPCODE)) )  AS CHECK_SUM
        FROM	dbo.NAME
        WHERE	ProcessDate = @ProcessDate
        AND		STREET IS NOT NULL -- No point in geocoding where the address is missing
        AND		TYPE = 0 -- Primary mailing address only, geocoding a PO box would be of little value
        ),GeoName AS (
        SELECT 
		        Account AS PARENTACCOUNT
        FROM	[cu].[GEO_MemberAddress]
        )SELECT 
		        np.PARENTACCOUNT as Account
	        ,	np.SSN
	        ,	np.STREET
	        ,	np.EXTRAADDRESS
	        ,	np.CITY
	        ,	np.STATE
	        ,	np.ZIPCODE
	        ,	np.CHECK_SUM
        FROM	NamePrime np
        WHERE np.PARENTACCOUNT NOT IN  (select PARENTACCOUNT from GeoName)
        """
        
        new_member_df = pd.read_sql(new_member_query, conn)
        # I need to test this out, I cut&pasted from above, I think it works the same but I need to verify
        new_member_df['ADDRESS'] = new_member_df.STREET + ' ' + new_member_df.EXTRAADDRESS.fillna('')
        new_member_df['ADDRESS'] = new_member_df['ADDRESS'].str.strip()
        new_member_df['lat'] = None
        new_member_df['lon'] = None
        new_member_df['key'] = config.tamu_api_key

        """"
            g = geocoder.tamu(
            location=ADDRESS,
            city=CITY,
            state=STATE,
            zipcode=ZIPCODE,
            key=API_KEY)
            g.json
        """
        return new_member_df