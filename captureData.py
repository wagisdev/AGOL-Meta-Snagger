#-------------------------------------------------------------------------------
# Name:        Capture AGOL Metadata & Metrics
# Purpose:  This script will capture details about assets stored within ArcGIS 
#           Online along with usage data. Depending on configuration and the
#           number of assets stored in AGOL, you may experience a long run time.
#           In testing, ~1800 assets resulted in a first run of 24 hours, with
#           follow-up updates of about 1 hour to refresh the data. These time
#           frames were a result of pulling the full 2 years of data from AGOL
#           as opposed to smaller chunks. 2 years of data for 1800 items
#           resulted in ~1M rows in the metrics table.
#
# Author:      John Spence
#
#
#
# Created:  3/4/2022
# Modified:
# Modification Purpose:
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

# Send confirmation of rebuild to
adminNotify = 'john@gis.dev'

# Configure the e-mail server and other info here.
mail_server = 'smtprelay.google.com'
mail_from = 'Metadata Capture<noreply@gis.dev>'

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
    print ('Connecting...')
    conn = pyodbc.connect(db_conn)
    cursor = conn.cursor()

    sqlCommand = '''
    IF OBJECT_ID ('[DBO].[GIS_Content]' , N'U') IS NULL
		    Begin
                CREATE TABLE [DBO].[GIS_Content](
                    [itemID] [VARCHAR] (64) NULL
                    , [title] [VARCHAR] (255) NULL
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

    search_results = gis.content.search (query='', sort_field='created', sort_order='desc', max_items=10000)

    for result in search_results:

        print ('Title:  {}'.format(result.title))
        contentTitle = '{}'.format(result.title)
        contentTitle = contentTitle.replace("'", '\'\'')
        contentTitle = '\'{}\''.format(contentTitle)

        print ('Type:  {}'.format(result.type))
        contentType = '\'{}\''.format(result.type)

        print ('Item ID:  {}'.format(result.itemid))
        contentID = '\'{}\''.format(result.itemid)

        print ('Item Metadata Completeness:  {}'.format(result.scoreCompleteness))
        contentmetadataScore = '\'{}\''.format(result.scoreCompleteness)
        
        # Make owner data just e-mail or Portal Account
        owner = result.owner.rstrip('_cobgis')
        print ('Item Owner:  {}'.format(owner))
        owner = '\'{}\''.format(owner)


        dateCreated = datetime.datetime.fromtimestamp(result.created/1000).strftime('%Y-%m-%d %H:%M:%S')
        dateModified = datetime.datetime.fromtimestamp(result.modified/1000).strftime('%Y-%m-%d %H:%M:%S')
        print ('Date Created:  {}'.format(dateCreated))
        print ('Date Updated:  {}\n'.format(dateModified))
        dateCreated = '\'{}\''.format(dateCreated)
        dateModified = '\'{}\''.format(dateModified)
                     
        # Item Summary
        print ('    Item Snippet:  {}\n'.format(result.snippet))
        if result.snippet != None:
            soup = BeautifulSoup (result.snippet, 'html.parser')
            for data in soup (['style', 'script']):
                data.decompose()
            resultSnippet = (' '.join(soup.stripped_strings))
            resultSnippet = resultSnippet.replace("'", '\'\'')
            itemSummary = '\'{}\''.format(resultSnippet)
        else:
            itemSummary = 'NULL'

        # Item Description        
        print ('    Item Description:  {}\n'.format(result.description))
        if result.description != None:
            soup = BeautifulSoup (result.description, 'html.parser')
            for data in soup (['style', 'script']):
                data.decompose()
            resultDescription = (' '.join(soup.stripped_strings))
            resultDescription = resultDescription.replace("'", '\'\'')
            itemDescription = '\'{}\''.format(resultDescription)
        else:
            itemDescription = 'NULL'

        # Terms of Use
        print ('    Item Terms of Use:  {}\n'.format(result.licenseInfo))
        if result.licenseInfo != None:
            soup = BeautifulSoup (result.licenseInfo, 'html.parser')
            for data in soup (['style', 'script']):
                data.decompose()
            resultTOU = (' '.join(soup.stripped_strings))
            resultTOU = resultTOU.replace("'", '\'\'')
            itemTermsofUse = '\'{}\''.format(resultTOU)
        else:
            itemTermsofUse = 'NULL'
        
        # Setup for pretty Tags
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
        
        print ('    Item Tags:  {}'.format(tag_content))
        if tag_content != 'None':
            itemTags = tag_content.replace("'", '\'\'')
            itemTags = '\'{}\''.format(itemTags)
        else:
            itemTags = 'NULL'

        # Setup for pretty Keywords
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

        print ('    Item Keywords:  {}'.format(keyword_content))
        if keyword_content != 'None':
            itemKeywords = '\'{}\''.format(keyword_content)
        else:
            itemKeywords = 'NULL'

        print ('Share Setting:  {}'.format(result.access))
        sharingConfig = '\'{}\''.format(result.access)

        print ('Content Status:  {}'.format(result.content_status))
        if result.content_status != '':
            contentConfig = '\'{}\''.format(result.content_status)
        else:
            contentConfig = 'NULL'

        print ('Access Information:  {}'.format(result.accessInformation))
        if result.accessInformation != None:
            contentCredits = '\'{}\''.format(result.accessInformation)
        else:
            contentCredits = 'NULL'

        print ('Protected:  {}'.format(result.protected))
        contentProtected = '\'{}\''.format(result.protected)

        print ('Storage Used:  {}'.format(result.size))
        storageUsed = '{}'.format(result.size)

        print ('Number of Views:  {}'.format(result.numViews))
        totalViews = '{}'.format(result.numViews)

        print ('Number of Ratings:  {}'.format(result.numRatings))
        totalRatings = '{}'.format(result.numRatings)

        print ('Average Rating:  {}'.format(result.avgRating))
        avgRating = '{}'.format(result.avgRating)

        sendContent2Storage(contentID, contentTitle, contentType, contentmetadataScore, 
                            owner, dateCreated, dateModified, itemSummary, itemDescription, 
                            itemTermsofUse, itemTags, itemKeywords, sharingConfig, 
                            contentConfig, contentCredits, contentProtected, storageUsed, 
                            totalViews, totalRatings, avgRating)
                              
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

    '''
    cursor.execute(sqlCommand)
    conn.commit()

    sqlCommand = '''

    update [dbo].[GIS_Content]
    set [collectorDisabled] = 'TRUE'
        , [SysCaptureDate] = getdate()
        where [type] = 'Web Map'
        and [archived] is NULL
		and [itemKeywords] like '%CollectorDisabled%'

    '''
    cursor.execute(sqlCommand)
    conn.commit()

    sqlCommand = '''

    update [dbo].[GIS_Content]
    set [archived] = 'TRUE'
        , [SysCaptureDate] = getdate()
	where cast ([SysCaptureDate] as date) <> cast (getdate() as date)
    and [archived] is NULL

    '''
    cursor.execute(sqlCommand)
    conn.commit()
    conn.close()

    return


def sendContent2Storage(contentID, contentTitle, contentType, contentmetadataScore, 
                        owner, dateCreated, dateModified, itemSummary, itemDescription,
                        itemTermsofUse, itemTags, itemKeywords, sharingConfig, 
                        contentConfig, contentCredits, contentProtected, storageUsed,
                        totalViews, totalRatings, avgRating):
#-------------------------------------------------------------------------------
# Name:        Function - sendContent2Storage
# Purpose:  Fires off the input to the database.
#-------------------------------------------------------------------------------

    #Check if it exists...
    query_string = '''

    select [GlobalID] from [dbo].[GIS_Content] where
        [itemID] = {}

    '''.format(contentID)

    query_conn = pyodbc.connect(db_conn)
    query_cursor = query_conn.cursor()
    query_cursor.execute(query_string)
    db_return = query_cursor.fetchone()
    query_cursor.close()
    query_conn.close()

    try:
        tableFK = db_return[0]
        print ('...Updating')
        print ('\n\n')
        conn = pyodbc.connect(db_conn)
        cursor = conn.cursor()

        sqlCommand = '''

        update [dbo].[GIS_Content]
        set [title] = {}
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

        '''.format(contentTitle, contentType, contentmetadataScore, 
                   owner, dateCreated, dateModified, itemSummary, itemDescription, 
                   itemTermsofUse, itemTags, itemKeywords, sharingConfig, contentConfig, 
                   contentCredits, contentProtected, storageUsed, totalViews, totalRatings, 
                   avgRating, tableFK)

        cursor.execute(sqlCommand)
        conn.commit()
        conn.close()

    except:
        print ('...Inserting')
        print ('\n\n')
        conn = pyodbc.connect(db_conn)
        cursor = conn.cursor()
        sqlCommand = '''

        insert into [dbo].[GIS_Content] (
            [itemID]
            ,[title]
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
            Values ({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, 
            {}, {}, {}, {}, {}, NULL, getdate(), newid())

        '''.format(contentID, contentTitle, contentType, contentmetadataScore, 
                   owner, dateCreated, dateModified, itemSummary, itemDescription, 
                   itemTermsofUse, itemTags, itemKeywords, sharingConfig, contentConfig, 
                   contentCredits, contentProtected, storageUsed, totalViews, totalRatings, 
                   avgRating)

        cursor.execute(sqlCommand)
        conn.commit()
        conn.close()


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
              'expiration' : '60'}

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
    order by [dateCreated] desc

    '''

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

    data = urllib.parse.urlencode(values).encode("utf-8")
    req = urllib.request.Request(url)

    response = None
    while response is None:
        try:
            response = urllib.request.urlopen(req,data=data)
        except:
            pass
            time.sleep (60)

    the_page = response.read()

    payload_json = the_page.decode('utf8')
    payload_json = json.loads(payload_json)
    if len(payload_json['data']) != 0: 
        metricsSTG = payload_json['data']
        useageMeter = int(metricsSTG[0]['num'][0][1])
    else:
        useageMeter = 0

    return (useageMeter)

def commitStorage(itemID, timehackDT, fkID, portalID, data_token, timehackTS):
#-------------------------------------------------------------------------------
# Name:        Function - commitStorage
# Purpose:  Commit metrics to storage.
#-------------------------------------------------------------------------------

    #Check if it exists...
    query_string = '''

    select [GlobalID] from dbo.GIS_ContentMetrics 
    where [itemID] = '{}' and
    [FkID] = '{}' and
    [periodDate] = '{}'

    '''.format(itemID, fkID, timehackDT)

    query_conn = pyodbc.connect(db_conn)
    query_cursor = query_conn.cursor()
    query_cursor.execute(query_string)
    db_return = query_cursor.fetchone()
    query_cursor.close()
    query_conn.close()

    if db_return == None:
        useageMeter = getMetric(portalID, data_token, itemID, timehackTS)
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

        cursor.execute(sqlCommand)
        conn.commit()
        conn.close()
        print ('    Committed....\n')

        return

def queryPortalUsage(workerPayload):
#-------------------------------------------------------------------------------
# Name:        Function - queryPortalUsage
# Purpose:  Get the useage data.
#-------------------------------------------------------------------------------

    itemID = workerPayload[0]
    time.sleep (10)
    fkID = workerPayload[1]
    startRecord = workerPayload[2]
    timeStopWindows = workerPayload[3]
    portalID = workerPayload[4]
    data_token = getToken()
    for timehacks in timeStopWindows:
        timehackDT = timehacks[0]
        timehackTS = timehacks[1]
        if timehackDT >= startRecord:
            commitStorage(itemID, timehackDT, fkID, portalID, data_token, timehackTS)

    return

def buildQueryForFast():
#-------------------------------------------------------------------------------
# Name:        Function - buildQueryForFast
# Purpose:  Do more faster.
#-------------------------------------------------------------------------------

    timeLookbackWindow = 5 #Expressed in days - Max 2 years data available  || Change after first capture to 2
    timeLookbackStop = 1

    searchStopDateTS, startDate, zeroTime, searchStopDate = buildSearchStop(timeLookbackStop)
    timeStopWindows = buildDateWindow(startDate, zeroTime, timeLookbackWindow, searchStopDateTS)
    portalID = getPortalID()
    db_return = getMetricTargets()

    workerPayload = []
    for asset in db_return:
        itemID = asset[0]
        fkID = asset[1]
        startRecord = asset[2]
        prepData = (itemID, fkID, startRecord, timeStopWindows, portalID)
        workerPayload.append(prepData)

    print ('Sending Payload....')

    with concurrent.futures.ThreadPoolExecutor(max_workers=None, thread_name_prefix='AGOL_') as executor:
        executor.map(queryPortalUsage, workerPayload)

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
