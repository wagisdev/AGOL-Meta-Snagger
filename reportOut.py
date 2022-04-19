#-------------------------------------------------------------------------------
# Name:        Metrics Report Out
# Purpose:    Reports out to HTML email to user about app performance.
#           
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
# 1) ArcGIS Online account (preferably an account that can see the entire org.)
# 2) ArcGIS Pro v2.9 (Requires ArcGIS Python libraries.)
# 3) Internet connection
# 4) Patience...lots of patience.
#
# 888888888888888888888888888888888888888888888888888888888888888888888888888888

# Configure hard coded db connection here.
db_conn = ('Driver={ODBC Driver 17 for SQL Server};'  # This will require adjustment if you are using a different database.
                      r'Server=GISPRODDB\GIS;'
                      'Database=GISDBA;'
                      'Trusted_Connection=yes;'  #Only if you are using a AD account.
                      #r'UID=;'  # Comment out if you are using AD authentication.
                      #r'PWD='     # Comment out if you are using AD authentication.
                      )

# Send confirmation of rebuild to
adminNotify = 'john@gis.dev'

# Configure the e-mail server and other info here.
mail_server = 'smtprelay.yourserver.com'
mail_from = 'GIS Applications Report<noreply@gis.dev>'

# Test User Override
testUser = ['']

# ------------------------------------------------------------------------------
# DO NOT UPDATE BELOW THIS LINE OR RISK DOOM AND DISPAIR!  Have a nice day!
# ------------------------------------------------------------------------------

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import pyodbc
import datetime
import time
import smtplib
import concurrent.futures
from bs4 import BeautifulSoup

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

    resultUserList = getContentOwners()
    processOwnerData(resultUserList)

    return

def runQuery(query_string):
#-------------------------------------------------------------------------------
# Name:        Function - getMetricTargets
# Purpose:  Pull targets from the Database.
#-------------------------------------------------------------------------------

    query_conn = pyodbc.connect(db_conn)
    query_cursor = query_conn.cursor()
    query_cursor.execute(query_string)
    db_return = query_cursor.fetchall()
    query_cursor.close()
    query_conn.close()

    return(db_return)


def getContentOwners():
#-------------------------------------------------------------------------------
# Name:        Function - main
# Purpose:  Starts the whole thing.
#-------------------------------------------------------------------------------

    query_string = '''

    SELECT 
	    distinct(metRes.[owner]) as [Ownership]
	    , case	
		    when metRes.[owner] not like '%@gis.dev%' and metRes.[owner] in ('gisdba') then LOWER(metRes.[owner])+'@gis.dev'
		    when metRes.[owner] not like '%@gis.dev%' then NULL
		    else LOWER(metRes.[owner]) 
	    end as [emailContact]
	    , (select count(*) from [dbo].[GIS_Content] where [owner] = metRes.[owner]) as [Items]
	    , (select avg([metaDataScore]) from [dbo].[GIS_Content] where [owner] = metRes.[owner]) as [AvgMetadataScore]
	    , (select (sum([storageUsed]) * 0.000001) from [dbo].[GIS_Content] where [owner] = metRes.[owner]) as [TotalStorageUse MB]
	    , (select top 1 ([itemID]) from [dbo].[MV_SVC_GISContent] where [owner] = metRes.[owner] and [type] != 'Geocortex Essentials Site' order by dateCreated asc) as [ItemIDOldest]
	    , (select top 1 ([title]) from [dbo].[MV_SVC_GISContent] where [owner] = metRes.[owner] order by dateCreated asc) as [ItemTitleOldest]
	    , (select top 1 ([itemID]) from [dbo].[MV_SVC_GISContent] where [owner] = metRes.[owner] and [type] != 'Geocortex Essentials Site' order by dateCreated desc) as [ItemIDNewest]
	    , (select top 1 ([title]) from [dbo].[MV_SVC_GISContent] where [owner] = metRes.[owner] order by dateCreated desc) as [ItemTitleNewest]
	    , (select top 1 ([itemID]) from [dbo].[MV_SVC_GISContent] where [owner] = metRes.[owner] and [type] != 'Geocortex Essentials Site' order by dateModified desc) as [ItemIDLastModified]
	    , (select top 1 ([title]) from [dbo].[MV_SVC_GISContent] where [owner] = metRes.[owner] and [type] != 'Geocortex Essentials Site' order by dateModified desc) as [ItemTitleLastModified]
	    , (select top 1 ([itemID]) from [dbo].[MV_SVC_GISContent] where [owner] = metRes.[owner] and [type] != 'Geocortex Essentials Site' and [archived] = 'TRUE' order by [SysCaptureDate] desc) as [ItemIDLastDeleted]
	    , (select top 1 ([title]) from [dbo].[MV_SVC_GISContent] where [owner] = metRes.[owner] and [type] != 'Geocortex Essentials Site' and [archived] = 'TRUE' order by [SysCaptureDate] desc) as [ItemTitleLastDeleted]
	    , (select top 1 (cast(DATEADD(day, -1, [SysCaptureDate]) as date)) from [dbo].[MV_SVC_GISContent] where [owner] = metRes.[owner] and [type] != 'Geocortex Essentials Site' and [archived] = 'TRUE' order by [SysCaptureDate] desc) as [ItemLastSeen]
    FROM [dbo].[MV_SVC_GISMetrics] metRes
    order by [Ownership] ASC

    '''

    ownerReturn = runQuery(query_string)

    return (ownerReturn)

def getOwnerData(userID):
#-------------------------------------------------------------------------------
# Name:        Function - main
# Purpose:  Starts the whole thing.
#-------------------------------------------------------------------------------

    query_string = '''

    select * from (
        select * from [dbo].[MV_SVC_GISMetrics] 
        where [type] in ('Web Mapping Application', 'Geocortex Essentials Site', 
        'Hub Initiative', 'Hub Site Application', 'Application', 'Dashboard', 'Web Experience') 
        and itemKeywords not in ('Geocortex Workflow')

        union all

        select * from [dbo].[MV_SVC_GISMetrics] 
        where [type] in ('Web Map') 
        and [fieldMapsDisabled] = 'FALSE') as dataPull

        where 
	        itemTags not like 'gcx-user-preferences'
	        and 
	        itemTags not like 'Geocortex Workflow'
            and
            dataPull.[owner] = '{}'

        order by dataPull.[TotalUsage_ThisWeek] DESC

    '''.format(userID)

    ownerDataReturn = runQuery(query_string)

    return (ownerDataReturn)


def processOwnerData(resultUserList):
#-------------------------------------------------------------------------------
# Name:        Function - main
# Purpose:  Starts the whole thing.
#-------------------------------------------------------------------------------

    for userDATA in resultUserList:
        userID = userDATA[0]
        emailContact = userDATA[1]
        items = userDATA[2]
        avgMetadataScore = userDATA[3]
        totalStorage = userDATA[4]
        itemIDOldest = userDATA[5]
        itemTitleOldest = userDATA[6]
        itemIDNewest = userDATA[7]
        itemTitleNewest = userDATA[8]
        itemIDLastModified = userDATA[9]
        itemTitleLastModified = userDATA[10]
        itemIDLastDeleted = userDATA[11]
        itemTitleLastDeleted = userDATA[12]
        itemLastDeletedLastSeen = userDATA[13]

        ownerDataReturn = getOwnerData(userID)
        shippingFormat(userDATA, ownerDataReturn)


    return

def shippingFormat(userDATA, ownerDataReturn):
#-------------------------------------------------------------------------------
# Name:        Function - main
# Purpose:  Starts the whole thing.
#-------------------------------------------------------------------------------

    userID = userDATA[0]
    emailContact = userDATA[1]
    items = userDATA[2]
    avgMetadataScore = userDATA[3]
    if avgMetadataScore == None:
        avgMetadataScore = 0
    totalStorage = userDATA[4]
    if totalStorage == None:
        totalStorage = 0
    itemIDOldest = userDATA[5]
    itemTitleOldest = userDATA[6]
    itemIDNewest = userDATA[7]
    itemTitleNewest = userDATA[8]
    itemIDLastModified = userDATA[9]
    itemTitleLastModified = userDATA[10]
    if itemTitleLastModified == None:
        itemTitleLastModified = 'N/A'
    itemIDLastDeleted = userDATA[11]
    itemTitleLastDeleted = userDATA[12]
    if itemTitleLastDeleted == None:
        itemTitleLastDeleted = 'N/A'
    itemLastDeletedLastSeen = userDATA[13]

    rowOutput = ''

    for od in ownerDataReturn:
        itemID = od[1]
        title = od[2]
        type = od[3]
        dateCreated = od[5]
        sharingConfig = od[9]
        totalUse_Lastweek = od[15]
        fieldMapsDisabled = od[10]

        if type == 'Web Map':
            if fieldMapsDisabled == 'FALSE':
                type = 'Field Map'

        rowLine = '''

          <tr>
            <td>{}</td>
            <td>{}</td>
            <td>{}</td>
            <td>{}</td>
            <td>{}</td>
          </tr>

        '''.format(title, type, sharingConfig, dateCreated, totalUse_Lastweek)
        rowOutput = rowOutput + rowLine

    payLoadHTMLFront = '''

    <html>
    <head>
    <style>

    table {
      font-family: arial, sans-serif;
      border-collapse: collapse;
      width: 100%;
    }

    td, th {
      border: 1px solid #dddddd;
      text-align: left;
      padding: 8px;
    }

    tr:nth-child(even) {
      background-color: #dddddd;
    }

    </style>
    <!--<h1 style="font-family:verdana;"><b>Your Map Applications</b></h1>-->
    </head>

    <body>
   
    <div>
    <h3 style="font-family:verdana;">Your Application Inventory (Sorted by Use)</h3>
    <table>
      <tr>
        <th>Application</th>
        <th>Type</th>
        <th>Sharing</th>
        <th>Date Created</th>
        <th>Total Usage (Last Week)</th>
      </tr>'''

    payLoadHTMLmid = '''
    </table>
    </div>

    '''

    payLoadHTMLend = '''
    <div>
    <h3 style="font-family:verdana;">Additional Details</h3>
    <table style="width: 50%;">
      <tr>
        <td><b># of Items (AGOL/Portal)</b></td>
        <td>{}</td>
      </tr>
      <tr>
        <td><b>Average Metadata Score</b></td>
        <td>{}%</td>
      </tr>
      <tr>
        <td><b>Total Storage</b></td>
        <td>{} MB</td>
      </tr>
      <tr>
        <td><b>Oldest Item</b></td>
        <td>{}</td>
      </tr>
      <tr>
        <td><b>Newest Item</b></td>
        <td>{}</td>
      </tr>
      <tr>
        <td><b>Last Updated</b></td>
        <td>{}</td>
      </tr>
      <tr>
        <td><b>Last Deleted</b></td>
        <td>{}</td>
      </tr>    
    </table>
    </div>
    <br>
    <div>
    [This is an automated system message. Please contact john@gis.dev for all questions.]
    </div>

    </body>
    </html>

    '''.format(items, avgMetadataScore, totalStorage, itemTitleOldest , itemTitleNewest, itemTitleLastModified, itemTitleLastDeleted)

    payLoadHTMLALTfront = '''

    <html>
    <head>
    <style>

    table {
      font-family: arial, sans-serif;
      border-collapse: collapse;
      width: 100%;
    }

    td, th {
      border: 1px solid #dddddd;
      text-align: left;
      padding: 8px;
    }

    tr:nth-child(even) {
      background-color: #dddddd;
    }

    </style>
    <!--<h1 style="font-family:verdana;"><b>Your Map Applications</b></h1>-->
    </head>

    <body>
   
    <h3 style="font-family:verdana;">Your Application Inventory</h3>
    <table>
      <tr>
        <th>Application</th>
        <th>Type</th>
        <th>Sharing</th>
        <th>Date Created</th>
        <th>Total Usage (Last Week)</th>
      </tr>
	</table>
    <h5 style="font-family:verdana;"><center>You have created no applications</center></h5>
    
      '''

    if len (ownerDataReturn) > 0:
        payLoadHTML = payLoadHTMLFront + '{}'.format(rowOutput) + payLoadHTMLmid + payLoadHTMLend
    else:
        payLoadHTML = payLoadHTMLALTfront + payLoadHTMLend

    payLoadTXT = 'Test Text'

    if emailContact != None:
        sendNotification (payLoadHTML, payLoadTXT, emailContact)

    return ()

def sendNotification (payLoadHTML, payLoadTXT, emailContact):
#-------------------------------------------------------------------------------
# Name:        Function - main
# Purpose:  Starts the whole thing.
#-------------------------------------------------------------------------------

    partTXT = MIMEText(payLoadTXT, 'plain')
    partHTML = MIMEText(payLoadHTML, 'html')
    msg = MIMEMultipart('alternative')
    msg['Subject'] = 'Weekly Applications Report'
    msg['From'] = mail_from

    if len(testUser) > 0:

        if emailContact in testUser:

            print ('Sending data to {}'.format(emailContact))
        
            msg['To'] = emailContact

            msg.attach(partTXT)
            msg.attach(partHTML)

            server = smtplib.SMTP(mail_server)

            server.sendmail(mail_from, emailContact, msg.as_string())
            server.quit()

    else:

        print ('Sending data to {}'.format(emailContact))

        msg['To'] = emailContact

        msg.attach(partTXT)
        msg.attach(partHTML)

        server = smtplib.SMTP(mail_server)

        server.sendmail(mail_from, emailContact, msg.as_string())
        server.quit()

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
