#-------------------------------------------------------------------------------
# Name:        Capture AGOL Metadata & Metrics
# Purpose:  This script will capture details about assets stored within ArcGIS
#           Online along with usage data. Depending on configuration and the
#           number of assets stored in AGOL, you may experience a long run time.
#           In testing, ~1900 assets resulted in a first run of 24 hours, with
#           follow-up updates of about 1 hour to refresh the data. These time
#           frames were a result of pulling the full 2 years of data from AGOL
#           as opposed to smaller chunks. 2 years of data for 1900 items
#           resulted in ~1M rows in the metrics table.
#
# Author:      John Spence
#
#
#
# Created:  3/4/2022
# Modified: 1/15/2023
# Modification Purpose: Rewrote metrics capture processes. Speed up exponentially.
#                       (5/17/2022) Added tracking to determine where it came from.
#
#
#-------------------------------------------------------------------------------


# 888888888888888888888888888888888888888888888888888888888888888888888888888888
# ------------------------------- Configuration --------------------------------
#   Adjust the settings below to match your org. eMail functionality is not
#   present currently, though obviously can be built in later.
#
# ------------------------------- Dependencies ---------------------------------
# 1) ArcGIS Online account (preferably an account that can see the entire org.)
# 2) ArcGIS Pro v2.9 (Requires ArcGIS Python libraries.)
# 3) Internet connection
# 4) Patience...lots of patience.
#
# 888888888888888888888888888888888888888888888888888888888888888888888888888888

# Portal Config
portal_URL = 'https://www.arcgis.com/'
portal_uName = '' #Put your own User Name here (AGOL account only)
portal_pWord = '' #Security through obscurity...b64 your password and place here.
portal_Type = 'ArcGIS Online'

# Configure hard coded db connection here.
db_conn = ('Driver={ODBC Driver 17 for SQL Server};'  # This will require adjustment if you are using a different database.
                      r'Server=GISSQL2019;' #Set your database server or database/instance.
                      'Database=GISDBA;'  #Set your database
                      'Trusted_Connection=yes;'  #Only if you are using a AD account.
                      #r'UID=;'  # Comment out if you are using AD authentication.
                      #r'PWD='     # Comment out if you are using AD authentication.
                      )

# Initial Data Loaad
initLoad = 0 #Set to 1 if you are wanting a full initial pull.

# Debug on/off
debugBIN = 0 #Set to 1 if you want to see the output for each line of code.

# Superspeed
workFastest = 1 #Increases speed of capturing metrics data.

# Data Source
dataSource = 'AGOL'

# ------------------------------------------------------------------------------
# DO NOT UPDATE BELOW THIS LINE OR RISK DOOM AND DISPAIR!  Have a nice day!
# ------------------------------------------------------------------------------

import arcgis
from arcgis.gis import GIS
import datetime
import time
import smtplib
import base64
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import pyodbc
import concurrent.futures
from bs4 import BeautifulSoup
import urllib
import requests
import json
from tqdm import tqdm
import warnings
warnings.filterwarnings("ignore", category=UserWarning, module='bs4')

#-------------------------------------------------------------------------------
#
#
#                                 Functions
#
#
#-------------------------------------------------------------------------------

def main():
#-------------------------------------------------------------------------------
# Name:        Function - main
# Purpose:  Starts the whole thing.
#-------------------------------------------------------------------------------

    checkWorkspace()
    queryPortal (portal_URL, portal_uName, portal_pWord)
    dataCleaning()
    buildQueryForFast()

    return

def checkWorkspace():
#-------------------------------------------------------------------------------
# Name:        Function - checkWorkspace
# Purpose:  Creates the tables, views and indexes needed for the capture & use.
#-------------------------------------------------------------------------------
    print ('Checking Database & Configuration...')
    conn = pyodbc.connect(db_conn)
    cursor = conn.cursor()

    sqlCommand = '''
    IF OBJECT_ID ('[DBO].[GIS_Content]' , N'U') IS NULL
		    Begin
                CREATE TABLE [DBO].[GIS_Content](
                    [itemID] [VARCHAR] (64) NULL
                    , [title] [VARCHAR] (255) NULL
                    , [source] [VARCHAR] (255) NULL
                    , [type] [VARCHAR] (80) NULL
                    , [metadataScore] [NUMERIC] (3,0) NULL
                    , [owner] [VARCHAR] (100) NULL
                    , [dateCreated] [DATETIME2] (7) NULL
                    , [dateModified] [DATETIME2] (7) NULL
                    , [itemSummary] [VARCHAR] (max) NULL
                    , [itemDescription] [VARCHAR] (max) NULL
                    , [itemTermsofUse] [VARCHAR] (max) NULL
                    , [itemTags] [VARCHAR] (max) NULL
                    , [itemKeywords] [VARCHAR] (max) NULL
                    , [sharingConfig] [VARCHAR] (80) NULL
                    , [contentConfig] [VARCHAR] (80) NULL
                    , [contentCredits] [VARCHAR] (max) NULL
                    , [contentProtected] [VARCHAR] (5) NULL
                    , [storageUsed] [NUMERIC] (12,0) NULL
                    , [totalViews] [NUMERIC] (12,0) NULL
                    , [totalRatings] [NUMERIC] (12,0) NULL
                    , [avgRating] [DECIMAL] (3,2) NULL
                    , [collectorDisabled] [VARCHAR] (5) NULL
                    , [fieldMapsDisabled] [VARCHAR] (5) NULL
                    , [archived] [VARCHAR] (5) NULL
                    , [SysCaptureDate] [DATETIME2] (7) NULL
                    , [GlobalID] [UNIQUEIDENTIFIER] NOT NULL
                )
            End
    '''
    cursor.execute(sqlCommand)
    conn.commit()

    sqlCommand = '''
    IF OBJECT_ID ('[DBO].[GIS_ContentMetrics]' , N'U') IS NULL
		    Begin
                CREATE TABLE [DBO].[GIS_ContentMetrics](
                    [itemID] [VARCHAR] (64) NULL
                    , [periodDate] [DATE] NULL
                    , [requests] [NUMERIC] (12,0) NULL
                    , [archived] [VARCHAR] (5) NULL
                    , [SysCaptureDate] [DATETIME2] (7) NULL
                    , [FkID] [UNIQUEIDENTIFIER] NOT NULL
                    , [GlobalID] [UNIQUEIDENTIFIER] NOT NULL
                )
            End
    '''
    cursor.execute(sqlCommand)
    conn.commit()

    sqlCommand = '''
    IF OBJECT_ID ('[DBO].[View_SVC_GISContent]') IS NULL

        Begin
        EXECUTE ('

                CREATE view [dbo].[View_SVC_GISContent] as

                SELECT
	                CAST(ROW_NUMBER() over(order by [dateCreated] asc) as int) as [ObjectID]
	                , *
                FROM [dbo].[GIS_Content]')
        END

    '''

    cursor.execute(sqlCommand)
    conn.commit()

    sqlCommand = '''
    IF OBJECT_ID ('[DBO].[View_SVC_GISMetrics]') IS NULL

        Begin
        EXECUTE ('

                CREATE View [dbo].[View_SVC_GISMetrics] as

                SELECT
	                CAST(ROW_NUMBER() over(order by content.[dateCreated] asc) as int) as [ObjectID]
	                , content.[itemID]
	                , content.[title]
                    , content.[source]
	                , content.[type]
	                , content.[owner]
	                , content.[dateCreated]
	                , content.[dateModified]
	                , content.[itemTags]
	                , content.[sharingConfig]
	                , content.[fieldMapsDisabled]
	                , content.[archived]
	                , content.[SysCaptureDate]
	                , case
		                when (select SUM(metrics.[requests]) from [dbo].[GIS_ContentMetrics] metrics where metrics.[FkID] = content.[GlobalID]
			                and metrics.[periodDate] = cast(getdate()-1 as date)) is NULL then 0
		                else (select SUM(metrics.[requests]) from [dbo].[GIS_ContentMetrics] metrics where metrics.[FkID] = content.[GlobalID]
			                and metrics.[periodDate] = cast(getdate()-1 as date))
	                  end as [TotalUsage_Yesterday]
	                , case
		                when (select SUM(metrics.[requests]) from [dbo].[GIS_ContentMetrics] metrics where metrics.[FkID] = content.[GlobalID]
			                and datepart(ww, metrics.[periodDate]) = datepart(ww, cast(getdate() as date))) is null then 0
		                else (select SUM(metrics.[requests]) from [dbo].[GIS_ContentMetrics] metrics where metrics.[FkID] = content.[GlobalID]
			                and datepart(ww, metrics.[periodDate]) = datepart(ww, cast(getdate() as date)))
	                  end as [TotalUsage_ThisWeek]
	                , case
		                when (select SUM(metrics.[requests]) from [dbo].[GIS_ContentMetrics] metrics where metrics.[FkID] = content.[GlobalID]
			                and datepart(ww, metrics.[periodDate]) = datepart(ww, cast(getdate() as date))-1) is null then 0
		                else (select SUM(metrics.[requests]) from [dbo].[GIS_ContentMetrics] metrics where metrics.[FkID] = content.[GlobalID]
			                and datepart(ww, metrics.[periodDate]) = datepart(ww, cast(getdate() as date))-1)
	                  end as [TotalUsage_LastWeek]
	                , case
		                when (select SUM(metrics.[requests]) from [dbo].[GIS_ContentMetrics] metrics where metrics.[FkID] = content.[GlobalID]
			                and datepart(mm, metrics.[periodDate]) = datepart(mm, cast(getdate() as date))) is null then 0
		                else (select SUM(metrics.[requests]) from [dbo].[GIS_ContentMetrics] metrics where metrics.[FkID] = content.[GlobalID]
			                and datepart(mm, metrics.[periodDate]) = datepart(mm, cast(getdate() as date)))
	                  end as [TotalUsage_ThisMonth]
	                , case
		                when (select SUM(metrics.[requests]) from [dbo].[GIS_ContentMetrics] metrics where metrics.[FkID] = content.[GlobalID]
			                and datepart(mm, metrics.[periodDate]) = datepart(mm, cast(getdate() as date))-1) is null then 0
		                else (select SUM(metrics.[requests]) from [dbo].[GIS_ContentMetrics] metrics where metrics.[FkID] = content.[GlobalID]
			                and datepart(mm, metrics.[periodDate]) = datepart(mm, cast(getdate() as date))-1)
	                  end as [TotalUsage_LastMonth]
	                , case
		                when (select SUM(metrics.[requests]) from [dbo].[GIS_ContentMetrics] metrics where metrics.[FkID] = content.[GlobalID]
			                and datepart(yy, metrics.[periodDate]) = datepart(yy, cast(getdate() as date))) is null then 0
		                else (select SUM(metrics.[requests]) from [dbo].[GIS_ContentMetrics] metrics where metrics.[FkID] = content.[GlobalID]
			                and datepart(yy, metrics.[periodDate]) = datepart(yy, cast(getdate() as date)))
	                  end as [TotalUsage_ThisYear]
	                , case
		                when (select SUM(metrics.[requests]) from [dbo].[GIS_ContentMetrics] metrics where metrics.[FkID] = content.[GlobalID]
			                and datepart(yy, metrics.[periodDate]) = datepart(yy, cast(getdate() as date))-1) is null then 0
		                else (select SUM(metrics.[requests]) from [dbo].[GIS_ContentMetrics] metrics where metrics.[FkID] = content.[GlobalID]
			                and datepart(yy, metrics.[periodDate]) = datepart(yy, cast(getdate() as date))-1)
	                  end as [TotalUsage_LastYear]
	                , (select SUM(metrics.[requests]) from [dbo].[GIS_ContentMetrics] metrics where metrics.[FkID] = content.[GlobalID]) as [TotalUsage_AllTime]

                  FROM [dbo].[GIS_Content] as content')
        END

    '''

    cursor.execute(sqlCommand)
    conn.commit()

    # Close it out when not needed.
    conn.close()

    return ()

def queryPortal (portal_URL, portal_uName, portal_pWord):
#-------------------------------------------------------------------------------
# Name:        Function - queryPortal
# Purpose:  1st step in querying portal...logging in.
#-------------------------------------------------------------------------------

    errorLoc = 'queryPortal'

    try:
        portal_pWordDec = base64.b64decode(portal_pWord).decode("utf-8")
        gis = GIS('{}'.format(portal_URL), '{}'.format(portal_uName), '{}'.format(portal_pWordDec))
        errorCond = 0
        errorResponse = ''
        getInfo(gis)

    except Exception as errorResponse:
        print ('Error establishing connection to URL:  {}'.format(errorResponse))
        errorCond = 5
        # Gotta do something with this eventually.

    return

def getInfo(gis):
#-------------------------------------------------------------------------------
# Name:        Function - getInfo
# Purpose:  Snags up to 10,000 items from the portal and processes for capture.
#-------------------------------------------------------------------------------

    print ('Querying data from specified environment.....')
    search_results = gis.content.search (query='', sort_field='created', sort_order='desc', max_items=10000)

    dataStore = search_results
    for result in search_results:

        if debugBIN == 1:
            print ('Title:  {}'.format(result.title))
            print ('Type:  {}'.format(result.type))
            print ('Item ID:  {}'.format(result.itemid))
            print ('Item Metadata Completeness:  {}'.format(result.scoreCompleteness))
            print ('Item Owner:  {}'.format(owner))
            print ('Date Created:  {}'.format(dateCreated))
            print ('Date Updated:  {}\n'.format(dateModified))
            print ('    Item Snippet:  {}\n'.format(result.snippet))
            print ('    Item Description:  {}\n'.format(result.description))
            print ('    Item Terms of Use:  {}\n'.format(result.licenseInfo))
            print ('    Item Tags:  {}'.format(tag_content))
            print ('    Item Keywords:  {}'.format(keyword_content))
            print ('Share Setting:  {}'.format(result.access))
            print ('Content Status:  {}'.format(result.content_status))
            print ('Access Information:  {}'.format(result.accessInformation))
            print ('Protected:  {}'.format(result.protected))
            print ('Storage Used:  {}'.format(result.size))
            print ('Number of Views:  {}'.format(result.numViews))
            print ('Number of Ratings:  {}'.format(result.numRatings))
            print ('Average Rating:  {}\n'.format(result.avgRating))

    sendContent2Storage(dataStore)

    return

def dataCleaning():
#-------------------------------------------------------------------------------
# Name:        Function - dataCleaning
# Purpose:  Cleans up afterwards adding in metadata for fieldmaps, archived, etc.
#-------------------------------------------------------------------------------

    conn = pyodbc.connect(db_conn)
    cursor = conn.cursor()

    sqlCommand = '''

    update [dbo].[GIS_Content]
    set [fieldMapsDisabled] = 'TRUE'
        , [SysCaptureDate] = getdate()
        where [type] = 'Web Map'
        and [archived] is NULL
		and [itemKeywords] like '%FieldMapsDisabled%'
        and cast ([SysCaptureDate] as date) = cast (getdate() as date)
        and [source] = '{}'

    '''.format(dataSource)
    cursor.execute(sqlCommand)
    conn.commit()

    sqlCommand = '''

    update [dbo].[GIS_Content]
    set [fieldMapsDisabled] = 'FALSE'
        , [SysCaptureDate] = getdate()
        where [type] = 'Web Map'
        and [archived] is NULL
		and [fieldMapsDisabled] is NULL
        and cast ([SysCaptureDate] as date) = cast (getdate() as date)
        and [source] = '{}'

    '''.format(dataSource)

    cursor.execute(sqlCommand)
    conn.commit()

    sqlCommand = '''

    update [dbo].[GIS_Content]
    set [fieldMapsDisabled] = 'N/A'
        , [SysCaptureDate] = getdate()
        where [type] <> 'Web Map'
        and [archived] is NULL
        and [fieldMapsDisabled] is NULL
        and cast ([SysCaptureDate] as date) = cast (getdate() as date)
        and [source] = '{}'

    '''.format(dataSource)

    cursor.execute(sqlCommand)
    conn.commit()

    sqlCommand = '''

    update [dbo].[GIS_Content]
    set [collectorDisabled] = 'TRUE'
        , [SysCaptureDate] = getdate()
        where [type] = 'Web Map'
        and [archived] is NULL
		and [itemKeywords] like '%CollectorDisabled%'
        and [collectorDisabled] is NULL
        and cast ([SysCaptureDate] as date) = cast (getdate() as date)
        and [source] = '{}'

    '''.format(dataSource)

    cursor.execute(sqlCommand)
    conn.commit()

    sqlCommand = '''

    update [dbo].[GIS_Content]
    set [archived] = 'TRUE'
        , [SysCaptureDate] = getdate()
	   where cast ([SysCaptureDate] as date) <> cast (getdate() as date)
        and [archived] is NULL
        and [source] = '{}'

    '''.format(dataSource)

    cursor.execute(sqlCommand)
    conn.commit()
    conn.close()

    return


def sendContent2Storage(dataStore):
#-------------------------------------------------------------------------------
# Name:        Function - sendContent2Storage
# Purpose:  Fires off the input to the database.
#-------------------------------------------------------------------------------

    query_conn = pyodbc.connect(db_conn, autocommit = False)
    query_cursor = query_conn.cursor()

    print ('\nInserting & Updating Content Data...')
    for result in tqdm(dataStore):

        contentTitle = '{}'.format(result.title)
        contentTitle = contentTitle.replace("'", '\'\'')
        contentTitle = '\'{}\''.format(contentTitle)
        contentType = '\'{}\''.format(result.type)
        contentID = '\'{}\''.format(result.itemid)
        contentmetadataScore = '\'{}\''.format(result.scoreCompleteness)
        owner = result.owner.rstrip('_instanceName') #Removes additional tag on your GIS data. Change to match yours.
        owner = '\'{}\''.format(owner)
        dateCreated = datetime.datetime.fromtimestamp(result.created/1000).strftime('%Y-%m-%d %H:%M:%S')
        dateModified = datetime.datetime.fromtimestamp(result.modified/1000).strftime('%Y-%m-%d %H:%M:%S')
        dateCreated = '\'{}\''.format(dateCreated)
        dateModified = '\'{}\''.format(dateModified)
        if result.snippet != None:
            soup = BeautifulSoup (result.snippet, 'lxml')
            for data in soup (['style', 'script']):
                data.decompose()
            resultSnippet = (' '.join(soup.stripped_strings))
            resultSnippet = resultSnippet.replace("'", '\'\'')
            itemSummary = '\'{}\''.format(resultSnippet)
        else:
            itemSummary = 'NULL'
        if result.description != None:
            soup = BeautifulSoup (result.description, 'lxml')
            for data in soup (['style', 'script']):
                data.decompose()
            resultDescription = (' '.join(soup.stripped_strings))
            resultDescription = resultDescription.replace("'", '\'\'')
            itemDescription = '\'{}\''.format(resultDescription)
        else:
            itemDescription = 'NULL'
        if result.licenseInfo != None:
            soup = BeautifulSoup (result.licenseInfo, 'lxml')
            for data in soup (['style', 'script']):
                data.decompose()
            resultTOU = (' '.join(soup.stripped_strings))
            resultTOU = resultTOU.replace("'", '\'\'')
            itemTermsofUse = '\'{}\''.format(resultTOU)
        else:
            itemTermsofUse = 'NULL'
        tag_content = ''
        if len(result.tags) > 1:
            tag_content = result.tags[0]
            for tag in result.tags:
                tag_content = '{}, {}'.format(tag_content, tag)
            tag_content = tag_content.split(', ', 1)
            tag_content = tag_content[1]
        elif len(result.tags) == 1:
            tag_content = '{}'.format(result.tags[0])
        else:
            tag_content = 'None'

        if tag_content != 'None':
            itemTags = tag_content.replace("'", '\'\'')
            itemTags = '\'{}\''.format(itemTags)
        else:
            itemTags = 'NULL'
        keyword_content = ''

        if len(result.typeKeywords) > 1:
            keyword_content = result.typeKeywords[0]
            for keyword in result.typeKeywords:
                keyword_content = '{}, {}'.format(keyword_content, keyword)
            keyword_content = keyword_content.split(', ', 1)
            keyword_content = keyword_content[1]
        elif len(result.typeKeywords) == 1:
            keyword_content = '{}'.format(result.typeKeywords[0])
        else:
            keyword_content = 'None'

        if keyword_content != 'None':
            itemKeywords = '\'{}, {}\''.format(providerSource, keyword_content)
        else:
            itemKeywords = 'NULL'

        sharingConfig = '\'{}\''.format(result.access)

        if result.content_status != '':
            contentConfig = '\'{}\''.format(result.content_status)
        else:
            contentConfig = 'NULL'

        if result.accessInformation != None:
            contentCredits = '\'{}\''.format(result.accessInformation)
        else:
            contentCredits = 'NULL'
        contentProtected = '\'{}\''.format(result.protected)
        storageUsed = '{}'.format(result.size)
        totalViews = '{}'.format(result.numViews)
        totalRatings = '{}'.format(result.numRatings)
        avgRating = '{}'.format(result.avgRating)

        #Check if it exists...
        query_string = '''

        select [GlobalID] from [dbo].[GIS_Content] where
            [itemID] = {}

        '''.format(contentID)

        query_cursor.execute(query_string)
        db_return = query_cursor.fetchone()

        try:
            tableFK = db_return[0]
            if debugBIN == 1:
                print ('...Updating')
                print ('\n\n')

            sqlCommand = '''

            update [dbo].[GIS_Content]
            set [title] = {}
                , [source] = '{}'
                , [type] = {}
                , [metadataScore] = {}
                , [owner] = {}
                , [dateCreated] = {}
                , [dateModified] = {}
                , [itemSummary] = {}
                , [itemDescription] = {}
                , [itemTermsofUse] = {}
                , [itemTags] = {}
                , [itemKeywords] = {}
                , [sharingConfig] = {}
                , [contentConfig] = {}
                , [contentCredits] = {}
                , [contentProtected] = {}
                , [storageUsed] = {}
                , [totalViews] = {}
                , [totalRatings] = {}
                , [avgRating] = {}
                , [SysCaptureDate] = getdate()
                where [GlobalID] = '{}'

            '''.format(contentTitle, dataSource, contentType, contentmetadataScore,
                       owner, dateCreated, dateModified, itemSummary, itemDescription,
                       itemTermsofUse, itemTags, itemKeywords, sharingConfig, contentConfig,
                       contentCredits, contentProtected, storageUsed, totalViews, totalRatings,
                       avgRating, tableFK)

            query_cursor.execute(sqlCommand)

        except:
            if debugBIN == 1:
                print ('...Inserting')
                print ('\n\n')

            sqlCommand = '''

            insert into [dbo].[GIS_Content] (
                [itemID]
                ,[title]
                ,[source]
                ,[type]
                ,[metadataScore]
                ,[owner]
                ,[dateCreated]
                ,[dateModified]
                ,[itemSummary]
                ,[itemDescription]
                ,[itemTermsofUse]
                ,[itemTags]
                ,[itemKeywords]
                ,[sharingConfig]
                ,[contentConfig]
                ,[contentCredits]
                ,[contentProtected]
                ,[storageUsed]
                ,[totalViews]
                ,[totalRatings]
                ,[avgRating]
                ,[archived]
                ,[SysCaptureDate]
                ,[GlobalID]
            )
                Values ({}, {}, '{}',{}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {},
                {}, {}, {}, {}, {}, NULL, getdate(), newid())

            '''.format(contentID, contentTitle, dataSource, contentType, contentmetadataScore,
                       owner, dateCreated, dateModified, itemSummary, itemDescription,
                       itemTermsofUse, itemTags, itemKeywords, sharingConfig, contentConfig,
                       contentCredits, contentProtected, storageUsed, totalViews, totalRatings,
                       avgRating)

            query_cursor.execute(sqlCommand)

    query_conn.commit()
    query_cursor.close()
    query_conn.close()

    return

def getToken():
#-------------------------------------------------------------------------------
# Name:        Function - getToken
# Purpose:  Get's a authentication token from Portal.
#-------------------------------------------------------------------------------

    if portal_URL[-1] == '/':
        url = portal_URL + 'sharing/rest/generateToken'
        referrer = portal_URL[0: -1]
    else:
        url = portal_URL + '/sharing/rest/generateToken'
        referrer = portal_URL

    values = {'f': 'json',
              'username': portal_uName,
              'password': base64.b64decode(portal_pWord),
              'referer' : referrer,
              'expiration' : '120'}

    data = urllib.parse.urlencode(values).encode("utf-8")
    req = urllib.request.Request(url)

    response = None
    attempt = 0
    while response is None:
        attempt += 1
        if attempt > 3:
            time.sleep (10)
            attempt = 0
        try:
            response = urllib.request.urlopen(req,data=data)
        except:
            pass

    the_page = response.read()

    #Garbage Collection with some house building
    payload_json = the_page.decode('utf8')
    payload_json = json.loads(payload_json)

    data_token = payload_json['token']

    return (data_token)


def getPortalID():
#-------------------------------------------------------------------------------
# Name:        Function - getPortalID
# Purpose:  Get the portalID to build the URL strings.
#-------------------------------------------------------------------------------

    if portal_URL[-1] == '/':
        url = portal_URL + 'sharing/rest/portals/self'
        referrer = portal_URL[0: -1]
    else:
        url = portal_URL + '/sharing/rest/portals/self'
        referrer = portal_URL

    data_token = getToken()

    values = {'f': 'json',
              'token': data_token}

    data = urllib.parse.urlencode(values).encode("utf-8")
    req = urllib.request.Request(url)

    response = None
    while response is None:
        try:
            response = urllib.request.urlopen(req,data=data)
        except:
            pass
    the_page = response.read()

    #Garbage Collection with some house building
    payload_json = the_page.decode('utf8')
    payload_json = json.loads(payload_json)

    portalID = payload_json['id']

    return (portalID)

def getMetricTargets():
#-------------------------------------------------------------------------------
# Name:        Function - getMetricTargets
# Purpose:  Pull targets from the Database.
#-------------------------------------------------------------------------------

    query_string = '''

    select [itemID], [GlobalID], cast ([dateCreated] as date) [dateCreated]
    from [dbo].[GIS_Content]
    where [archived] is NULL
    and [source] = '{}'
    order by [dateCreated] asc

    '''.format(dataSource)

    query_conn = pyodbc.connect(db_conn)
    query_cursor = query_conn.cursor()
    query_cursor.execute(query_string)
    db_return = query_cursor.fetchall()
    query_cursor.close()
    query_conn.close()

    return(db_return)

def checkMetricTarget(searchStopDate, fkID):
#-------------------------------------------------------------------------------
# Name:        Function - checkMetricTarget
# Purpose:  Checks for metrics on the stop date.
#-------------------------------------------------------------------------------

    query_string = '''

    select * from [dbo].[GIS_ContentMetrics]
    where [periodDate] = '{}' and [FkID] = '{}'

    '''.format(searchStopDate, fkID)

    query_conn = pyodbc.connect(db_conn)
    query_cursor = query_conn.cursor()
    query_cursor.execute(query_string)
    db_return = query_cursor.fetchone()
    query_cursor.close()
    query_conn.close()

    return(db_return)

def buildDateWindow(startDate, zeroTime, timeLookbackWindow, searchStopDateTS):
#-------------------------------------------------------------------------------
# Name:        Function - buildDateWindow
# Purpose:  Build payload for date windows.
#-------------------------------------------------------------------------------

    windowTest = 0
    timeStopWindows = []
    while windowTest != 1:
        payload = []
        dayWindow = datetime.timedelta(timeLookbackWindow)
        searchDate = startDate - dayWindow
        timeWindow = datetime.datetime.combine(searchDate, zeroTime).timestamp()
        timeWindow = int(float(timeWindow)*1000)
        payload.append (searchDate)
        payload.append (timeWindow)
        if timeWindow <= searchStopDateTS:
            timeStopWindows.append(payload)
            timeLookbackWindow -= 1
        else:
            windowTest = 1

    return (timeStopWindows)

def buildSearchStop(timeLookbackStop):
#-------------------------------------------------------------------------------
# Name:        Function - buildSearchStop
# Purpose:  ID when to stop.
#-------------------------------------------------------------------------------

    startDate = datetime.date.today()
    dayStopWindow = datetime.timedelta(timeLookbackStop)
    searchStopDate = startDate - dayStopWindow
    zeroTime = datetime.datetime.min.time()
    searchStopDateTS = datetime.datetime.combine(searchStopDate, zeroTime).timestamp()
    searchStopDateTS = int(float(searchStopDateTS)*1000)  # Do not exceed this date.

    return (searchStopDateTS, startDate, zeroTime, searchStopDate)

def getMetric(portalID, data_token, itemID, timehackTS):
#-------------------------------------------------------------------------------
# Name:        Function - getMetric
# Purpose:  Query API to get metrics.
#-------------------------------------------------------------------------------

    if portal_URL[-1] == '/':
        url = portal_URL + 'sharing/rest/portals/{}/usage'.format(portalID)
        referrer = portal_URL[0: -1]
    else:
        url = portal_URL + '/sharing/rest/portals/{}/usage'.format(portalID)
        referrer = portal_URL

    tempTime = datetime.datetime.fromtimestamp(timehackTS/1000.0)
    deltaTime = datetime.timedelta(days=1)
    timehackTSE = tempTime + deltaTime
    timehackTSE = timehackTSE.timestamp()
    timehackTSE = int(float(timehackTSE)*1000)

    values = {'f': 'json',
              'startTime': timehackTS,
              'endTime': timehackTSE,
              'period': '1d',
              'vars': 'num',
              'groupby': 'name',
              'etype': 'svcusg',
              'name': itemID,
              'token': data_token}

    headers={'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36'}

    data = urllib.parse.urlencode(values).encode("utf-8")
    req = urllib.request.Request(url, data, headers)

    response = None
    attempt = 0
    while response is None:
        attempt += 1
        if attempt > 3:
            time.sleep (10)
            attempt = 0
        try:
            response = urllib.request.urlopen(req)
        except:
            pass

    the_page = response.read().decode(response.headers.get_content_charset())
    payload_json = json.loads(the_page)

    if len(payload_json['data']) != 0:
        metricsSTG = payload_json['data']
        useageMeter = int(metricsSTG[0]['num'][0][1])
    else:
        useageMeter = 0

    return (useageMeter)

def queryPortalUsage(workerPayload):
#-------------------------------------------------------------------------------
# Name:        Function - queryPortalUsage
# Purpose:  Get the useage data.
#-------------------------------------------------------------------------------

    query_conn = pyodbc.connect(db_conn)
    query_cursor = query_conn.cursor()
    itemID = workerPayload[0]
    fkID = workerPayload[1]
    startRecord = workerPayload[2]
    timeStopWindows = workerPayload[3]
    portalID = workerPayload[4]
    data_token = workerPayload[5]

    #Check if it exists...
    query_string = '''

    select [periodDate] from dbo.GIS_ContentMetrics
    where [itemID] = '{}' and
    [FkID] = '{}'
    order by [periodDate] desc

    '''.format(itemID, fkID)

    query_cursor.execute(query_string)
    listedInventory = query_cursor.fetchall()

    date2BeChecked = []
    for dateLook in listedInventory:
        add2List = dateLook[0]
        date2BeChecked.append(add2List)

    for timehacks in timeStopWindows:
        timehackDT = timehacks[0]
        timehackTS = timehacks[1]
        insertTrigger = 0

        if timehackDT >= startRecord:
            if debugBIN == 1:
                print ('\nData is within specifications for review.')
                print ('Check for insert-- ItemID: {} | Date: {} | timehackTS: {}'.format(itemID, timehackDT, timehackTS))

            if len(date2BeChecked) == 0:
                insertTrigger = 1
            else:
                if timehackDT.strftime('%Y-%m-%d') not in [uID.strftime('%Y-%m-%d') for uID in date2BeChecked]:
                    insertTrigger = 1

            if  insertTrigger == 1:
                if debugBIN == 1:
                    print ('*** No data found. Sending to storage...')
                useageMeter = getMetric(portalID, data_token, itemID, timehackTS)
                if debugBIN ==1:
                    print ('Inserting Metrics-- ItemID: {} | Date: {} | Usage: {}'.format(itemID, timehackDT, useageMeter))

                conn = pyodbc.connect(db_conn)
                cursor = conn.cursor()
                sqlCommand = '''

                insert into [dbo].[GIS_ContentMetrics] (
                    [itemID]
                    ,[periodDate]
                    ,[requests]
                    ,[archived]
                    ,[SysCaptureDate]
                    ,[FkID]
                    ,[GlobalID]
                )
                    Values ('{}', '{}', {}, NULL, getdate(), '{}', newid())

                '''.format(itemID, timehackDT, useageMeter, fkID)

                query_cursor.execute(sqlCommand)
                if debugBIN ==1:
                    print ('    Committed....\n')
            else:
                if debugBIN == 1:
                    print ('*** Data Already stored.')

    query_conn.commit()
    query_cursor.close()
    query_conn.close()

    return ()

def buildQueryForFast():
#-------------------------------------------------------------------------------
# Name:        Function - buildQueryForFast
# Purpose:  Do more faster.
#-------------------------------------------------------------------------------

    if initLoad == 1:
        timeLookbackWindow = 720 #Expressed in days - Max 2 years data available  || Change after first capture to 2
    else:
        timeLookbackWindow = 5

    timeLookbackStop = 1

    searchStopDateTS, startDate, zeroTime, searchStopDate = buildSearchStop(timeLookbackStop)
    timeStopWindows = buildDateWindow(startDate, zeroTime, timeLookbackWindow, searchStopDateTS)
    portalID = getPortalID()
    db_return = getMetricTargets()
    data_token = getToken()

    workerPayload = []
    print ('\nBuilding Payload For Metrics Scan & Capture....')
    if initLoad == 1 or workFastest == 1:
        for asset in db_return:
            itemID = asset[0]
            fkID = asset[1]
            startRecord = asset[2]
            prepData = (itemID, fkID, startRecord, timeStopWindows, portalID, data_token)
            workerPayload.append(prepData)
        print ('    -- Sending Payload....')
        with concurrent.futures.ThreadPoolExecutor(max_workers=None, thread_name_prefix='AGOL_') as executor:
            results = list(tqdm(executor.map(queryPortalUsage, workerPayload), total = len(workerPayload)))
    else:
        print ('\nSending Payloads For Metrics Scan & Capture via slow-mo mode....')
        for asset in tqdm(db_return):
            itemID = asset[0]
            fkID = asset[1]
            startRecord = asset[2]
            prepData = (itemID, fkID, startRecord, timeStopWindows, portalID, data_token)
            queryPortalUsage(prepData)

    return

#-------------------------------------------------------------------------------
#
#
#                                 MAIN SCRIPT
#
#
#-------------------------------------------------------------------------------

if __name__ == "__main__":
    main()
