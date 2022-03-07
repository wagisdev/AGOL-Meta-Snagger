# AGOL-Meta-Snagger
This script will capture details about assets stored within ArcGIS Online along with usage data. Depending on configuration and the number of assets stored in AGOL, you may experience a long run time. In testing, ~1800 assets resulted in a first run of 24 hours, with follow-up updates of about 1 hour to refresh the data. These time frames were a result of pulling the full 2 years of data from AGOL as opposed to smaller chunks. 2 years of data for 1800 items resulted in ~1M rows in the metrics table.

Take care to update the following variables -

    portal_URL
    portal_uName
    portal_pWord !!remember base64 that password or remove the whole decode chunk.
    db_conn
    timeLookbackWindow !!You will see this around line 883. It is set to 5 days right now, but if you set it to 730 for a full 2 year lookback first, you can get plenty of sample data together.
    
