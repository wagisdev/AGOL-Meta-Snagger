#-------------------------------------------------------------------------------
# Name:        Capture Geocortex Metadata & Metrics
# Purpose:  Using Selenium, extracts data from Geocortex Analytics.
#
# Author:      John Spence
#
#
#
# Created:  4/7/2022
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
#
#
#
# 888888888888888888888888888888888888888888888888888888888888888888888888888888

# GTX Config
gtx_RESTURLs = [(r'https://yourserverurl.com/Geocortex/Essentials/EssentialsInternal/REST', 'Essentials Internal'),
                (r'https://yourserverurl.com/Geocortex/Essentials/EssentialsExternal/REST', 'Essentials External')]

gtx_AnalyticsBaseURL = r'https://analytics.yourserverurl.com/AnalyticsReports/'

# Configure hard coded db connection here.
db_conn = ('Driver={ODBC Driver 17 for SQL Server};'  # This will require adjustment if you are using a different database.
                      r'Server=GISPRODDB\GIS;'
                      'Database=GISDBA;'
                      'Trusted_Connection=yes;'  #Only if you are using a AD account.
                      #r'UID=;'  # Comment out if you are using AD authentication.
                      #r'PWD='     # Comment out if you are using AD authentication.
                      )

# Content Ownership // Set ownership of all GTX apps to...
appOwner = 'General Ownership'

# Send confirmation of rebuild to
adminNotify = 'john@gis.dev'

# Configure the e-mail server and other info here.
mail_server = 'smtprelay.youserver.com'
mail_from = 'Metadata Capture<noreply@gis.dev>'

# ------------------------------------------------------------------------------
# DO NOT UPDATE BELOW THIS LINE OR RISK DOOM AND DISPAIR!  Have a nice day!
# ------------------------------------------------------------------------------

import datetime
import time
import pyodbc
import requests
import json
from requests_negotiate_sspi import HttpNegotiateAuth
from bs4 import BeautifulSoup
import os
import csv
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

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
    getInfo()
    dataCleaning()
    queryAnalytics()
    importCSVFiles()


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
    IF OBJECT_ID ('[DBO].[GTX_Content]' , N'U') IS NULL
		    Begin
                CREATE TABLE [DBO].[GTX_Content](
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

    # Close it out when not needed.
    conn.close()

    return ()

def getInfo():
#-------------------------------------------------------------------------------
# Name:        Function - checkWorkspace
# Purpose:  Creates the tables, views and indexes needed for the capture & use.
#-------------------------------------------------------------------------------

    for target in gtx_RESTURLs:
        urlTarget = '{}'.format(target[0])
        mainTarget = urlTarget + '/sites?f=pjson'
        response = requests.get(mainTarget)
        payload_json = response.json()

        for site in payload_json['sites']:
            gtxID = site['id']
            gtxName = site['displayName']
            gtxInstance = target[1]

            with requests.session() as authSession:
                authTarget = urlTarget + r'/security/signIn?idp_name=AD AUTHORITY'
                response = authSession.get(authTarget, auth = HttpNegotiateAuth())
                subTarget = urlTarget + '/sites/{}?f=pjson'.format(gtxID)
                response = authSession.get(subTarget)
                payload_json = response.json()
                #print (payload_json)
                gtxDescription = payload_json['description']
                gtxSecurityEnabled = payload_json['signInEnabled']

            # Item Title
            print ('Title:  {}'.format(gtxName))
            contentTitle = '{}'.format(gtxName)
            contentTitle = contentTitle.replace("'", '\'\'')
            contentTitle = '\'{}\''.format(contentTitle)

            # Item Type
            print ('Type:  Geocortex Essentials Site')
            contentType = '\'Geocortex Essentials Site\''

            # Item ID
            print ('Item ID:  {}'.format(gtxID))
            contentID = '\'{}\''.format(gtxID)

            # Item Metedata Score
            print ('Item Metadata Completeness:  0')
            contentmetadataScore = '\'0\''

            # Item Owner
            print ('Item Owner:  Unknown')
            owner = '\'GTX Essentials\''

            # Date Created/Updated
            print ('Date Created:  Unknown')
            print ('Date Updated:  Unknown\n')
            dateCreated = 'NULL'
            dateModified = 'NULL'

            # Item Summary
            print ('    Item Snippet:  {}\n'.format(gtxDescription))
            if gtxDescription != None:
                soup = BeautifulSoup (gtxDescription, 'html.parser')
                for data in soup (['style', 'script']):
                    data.decompose()
                resultSnippet = (' '.join(soup.stripped_strings))
                resultSnippet = resultSnippet.replace("'", '\'\'')
                itemSummary = '\'{}\''.format(resultSnippet)
            else:
                itemSummary = 'NULL'

            # Item Description        
            print ('    Item Description:  {}\n'.format(gtxDescription))
            if gtxDescription != None:
                soup = BeautifulSoup (gtxDescription, 'html.parser')
                for data in soup (['style', 'script']):
                    data.decompose()
                resultDescription = (' '.join(soup.stripped_strings))
                resultDescription = resultDescription.replace("'", '\'\'')
                itemDescription = '\'{}\''.format(resultDescription)
            else:
                itemDescription = 'NULL'

            # Terms of Use
            print ('    Item Terms of Use:  None\n')
            itemTermsofUse = 'NULL'

            # Tags
            itemTags = '\'{}, Geocortex, Geocortex Essentials\''.format(gtxName)

            # Keywords
            itemKeywords = '\'{}, Geocortex, Geocortex Essentials\''.format(gtxInstance)

            # Sharing Configuration & Content Configuration
            if gtxSecurityEnabled == True:
                print ('Share Setting:  org')
                sharingConfig = '\'org\''
                print('Content Status:  org_authoritative')
                contentConfig = '\'org_authoritative\''
            else:
                print('Share Setting:  public')
                sharingConfig = '\'public\''
                print('Content Status:  public_authoritative')
                contentConfig = '\'public_authoritative\''

            # Content Credits
            print ('Access Information:  {}'.format(appOwner))
            if appOwner != '':
                contentCredits = '\'{}\''.format(appOwner)
            else:
                contentCredits = 'NULL'

            # Content Protected
            print ('Protected:  True')
            contentProtected = '\'True\''

            print ('Storage Used:  0')
            storageUsed = '0'

            print ('Number of Views:  0')
            totalViews = '0'

            print ('Number of Ratings:  0')
            totalRatings = '0'

            print ('Average Rating:  0')
            avgRating = '0'

            sendContent2Storage(contentID, contentTitle, contentType, contentmetadataScore, 
                                owner, dateCreated, dateModified, itemSummary, itemDescription, 
                                itemTermsofUse, itemTags, itemKeywords, sharingConfig, 
                                contentConfig, contentCredits, contentProtected, storageUsed, 
                                totalViews, totalRatings, avgRating)


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

    select [GlobalID] from [dbo].[GTX_Content] where
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

        update [dbo].[GTX_Content]
        set [title] = {}
            , [type] = {}
            , [metadataScore] = {}
            , [dateModified] = getdate()
            , [itemSummary] = {}
            , [itemDescription] = {}
            , [itemTermsofUse] = {}
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
                   itemSummary, itemDescription, itemTermsofUse, sharingConfig, contentConfig, 
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

        insert into [dbo].[GTX_Content] (
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
            Values ({}, {}, {}, {}, {}, getdate(), getdate(), {}, {}, {}, {}, {}, {}, {}, {}, 
            {}, {}, {}, {}, {}, NULL, getdate(), newid())

        '''.format(contentID, contentTitle, contentType, contentmetadataScore, 
                   owner, itemSummary, itemDescription, 
                   itemTermsofUse, itemTags, itemKeywords, sharingConfig, contentConfig, 
                   contentCredits, contentProtected, storageUsed, totalViews, totalRatings, 
                   avgRating)

        cursor.execute(sqlCommand)
        conn.commit()
        conn.close()


    return

def dataCleaning():
#-------------------------------------------------------------------------------
# Name:        Function - dataCleaning
# Purpose:  Cleans up afterwards adding in metadata for fieldmaps, archived, etc.
#-------------------------------------------------------------------------------

    conn = pyodbc.connect(db_conn)
    cursor = conn.cursor()

    sqlCommand = '''

    update [dbo].[GTX_Content]
    set [archived] = 'TRUE'
        , [SysCaptureDate] = getdate()
	where cast ([SysCaptureDate] as date) <> cast (getdate() as date)
    and [archived] is NULL

    '''
    cursor.execute(sqlCommand)
    conn.commit()
    conn.close()

    return

def queryAnalytics():
#-------------------------------------------------------------------------------
# Name:        Function - queryAnalytics
# Purpose:  Cleans up afterwards adding in metadata for fieldmaps, archived, etc.
#-------------------------------------------------------------------------------

    print ('Trying login...')
    driver = webdriver.Chrome(r'D:\Selenium\chromedriver_win32\chromedriver.exe')
    driver.get (gtx_AnalyticsBaseURL)
    wait = WebDriverWait(driver, 15)
    wait.until(EC.element_to_be_clickable((By.XPATH, "//*[@id=\"application-region\"]/div/div/div/div[3]/div[3]/div/div/div/button"))).click()
    WebDriverWait(driver=driver, timeout=10).until(lambda x: x.execute_script('return document.readyState === \'complete\''))

    print ('Trying Data Capture...')   
    metricsData = gtx_AnalyticsBaseURL + r'/?feature=Dashboard&dashId=test&relativeDate=Yesterday'

    driver.get (metricsData)
    wait = WebDriverWait(driver, 15)
    time.sleep (30)
    wait.until(EC.element_to_be_clickable((By.XPATH, "//*[@id=\"application-region\"]/div/div/div/div[3]/div[3]/div/div/div[2]/div[2]/div[1]/div[1]/div/div[1]/div/a[2]"))).click()
    WebDriverWait(driver=driver, timeout=10).until(lambda x: x.execute_script('return document.readyState === \'complete\''))
    time.sleep (10)
    wait.until(EC.element_to_be_clickable((By.XPATH, "//*[@id=\"application-region\"]/div/div/div/div[3]/div[3]/div/div/div[2]/div[2]/div[2]/div[1]/div/div[1]/div/a[2]"))).click()
    WebDriverWait(driver=driver, timeout=10).until(lambda x: x.execute_script('return document.readyState === \'complete\''))
    time.sleep (10)
    wait.until(EC.element_to_be_clickable((By.XPATH, "//*[@id=\"application-region\"]/div/div/div/div[3]/div[3]/div/div/div[2]/div[2]/div[3]/div[1]/div/div[1]/div/a[2]"))).click()
    WebDriverWait(driver=driver, timeout=10).until(lambda x: x.execute_script('return document.readyState === \'complete\''))
    time.sleep (10)
    wait.until(EC.element_to_be_clickable((By.XPATH, "//*[@id=\"application-region\"]/div/div/div/div[3]/div[3]/div/div/div[2]/div[2]/div[4]/div[1]/div/div[1]/div/a[2]"))).click()
    WebDriverWait(driver=driver, timeout=10).until(lambda x: x.execute_script('return document.readyState === \'complete\''))
    time.sleep (10)
    wait.until(EC.element_to_be_clickable((By.XPATH, "//*[@id=\"application-region\"]/div/div/div/div[3]/div[3]/div/div/div[2]/div[2]/div[5]/div[1]/div/div[1]/div/a[2]"))).click()
    WebDriverWait(driver=driver, timeout=10).until(lambda x: x.execute_script('return document.readyState === \'complete\''))
    time.sleep (10)
    wait.until(EC.element_to_be_clickable((By.XPATH, "//*[@id=\"application-region\"]/div/div/div/div[3]/div[3]/div/div/div[2]/div[2]/div[6]/div[1]/div/div[1]/div/a[2]"))).click()
    WebDriverWait(driver=driver, timeout=10).until(lambda x: x.execute_script('return document.readyState === \'complete\''))
    time.sleep (10)
    wait.until(EC.element_to_be_clickable((By.XPATH, "//*[@id=\"application-region\"]/div/div/div/div[3]/div[3]/div/div/div[2]/div[2]/div[7]/div[1]/div/div[1]/div/a[2]"))).click()
    WebDriverWait(driver=driver, timeout=10).until(lambda x: x.execute_script('return document.readyState === \'complete\''))
    time.sleep (30)
    driver.close()

    return

def importCSVFiles():
#-------------------------------------------------------------------------------
# Name:        Function - queryAnalytics
# Purpose:  Cleans up afterwards adding in metadata for fieldmaps, archived, etc.
#-------------------------------------------------------------------------------

    startDate = datetime.date.today()
    dayStopWindow = datetime.timedelta(1)
    searchStopDate = startDate - dayStopWindow
    timehackDT = searchStopDate.strftime('%Y-%m-%d')
    searchStopDate = searchStopDate.strftime('%m_%d_%Y')

    getCSVPrecursors = queryForCSV()

    for targetPrep in getCSVPrecursors:
        appID = targetPrep[0]
        itemID = targetPrep[2]
        fkID = targetPrep[3]
        if targetPrep[1] == 'Essentials External':
            appLoc = 'EssentialsExternal'
        elif targetPrep[1] == 'Essentials Internal':
            appLoc = 'EssentialsInternal'
        else:
            print ('New keyword found! Adjust your settings.')
            quit()

        genfileName = appLoc + ' - ' + appID + ' - Viewers (' + searchStopDate + ').csv'
        makePath = r'C:\Users\JSpence\Downloads\{}'.format(genfileName)
        print (makePath)

        with open (makePath) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            useCount = 0
            for row in csv_reader:
                if len(row) > 1 and 'Viewer' in row[1]:
                    useCount += int(row[2])
            print (useCount)
            useageMeter = useCount
            insertResults(itemID, fkID, timehackDT, useageMeter)
        
        # Cleaning Up.

        if os.path.exists(makePath):
            os.remove(makePath)
            print ('File Purged.')


    return

def queryForCSV():
#-------------------------------------------------------------------------------
# Name:        Function - queryAnalytics
# Purpose:  Cleans up afterwards adding in metadata for fieldmaps, archived, etc.
#-------------------------------------------------------------------------------

    query_string = '''

    select 
	    [title]
	    , LEFT([itemKeywords],CHARINDEX(',', [itemKeywords]) - 1) as [Keyword] 
	    , [itemID]
	    , [GlobalID]
    from dbo.GTX_Content
    order by [Keyword] ASC

    '''

    query_conn = pyodbc.connect(db_conn)
    query_cursor = query_conn.cursor()
    query_cursor.execute(query_string)
    db_return = query_cursor.fetchall()
    query_cursor.close()
    query_conn.close()


    return (db_return)


def insertResults(itemID, fkID, timehackDT, useageMeter):
#-------------------------------------------------------------------------------
# Name:        Function - queryAnalytics
# Purpose:  Cleans up afterwards adding in metadata for fieldmaps, archived, etc.
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

    return ()



#-------------------------------------------------------------------------------
#
#
#                                 MAIN SCRIPT
#
#
#-------------------------------------------------------------------------------

if __name__ == "__main__":
    main()
