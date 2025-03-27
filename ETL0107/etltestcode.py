"""
Author : Ratnesh
Dept   : Data Analytics & Innovation, Covermore Group
Date   : 2018-02-01
Usage  : "python  parse_impulse_cba_archive_sessions_delta.py <run_mode> <start_date> <end_date>"
          "python parse_impulse_cba_archive_sessions_delta.py run_mode=AUTO"
          "python parse_impulse_cba_archive_sessions_delta.py run_mode=MANUAL start_date=2018-12-01 end_date=2019-01-01" #thsi will extract data for the month of December-2018
Params : Parameter 1 - RunMode - Accepted values are "AUTO"/"MANUAL" (Pass dates as argument in Manual mode. No dates need to be passed in AUTO mode.)
         Parameter 2 - Extraction Start Date in YYYY-MM-DD format. This date is inclusive.
         Parameter 3 - Extraction End Date in YYYY-MM-DD format. This date is exclusive.
         
Desc   : 

Configuration requirements : 1) Deployment of this script.
                             2) Scheduling using agent.
                             3) 


DB Changes    

    
Change History:
20190201        RATNESH      
20190225        RATNESH      Parsing traveller data and loading to stage.
20230331        ST           Optimising quote travallers and destinations data processing

Pending Bits: 



"""

import pandas as pd 
import numpy as np
import json
import datetime

# -------------------------------------------------------------------------------------
# Main processing block

print("***************************************************************************")
print('Main processing block started at:'+str(datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S')))

url=r"E:\ETL\Data\BigQuery\Out\debug-folder\testfile.csv"

#df = pd.read_csv(url,delimiter='|',error_bad_lines=True,header=None,names=['sessiontoken','sessiondata','lastupdatetime'],encoding='utf-16')
df = pd.read_csv(url,delimiter='|',header=0,names=['sessiontoken','sessiondata','lastupdatetime'],encoding='utf-8')
#print(df.sessiondata.head(2).apply(json.loads).apply(pd.Series).keys())
#df.info()
#df.head()
#pd.options.display.max_columns = None

#df_impulse_archive_sessions=df["sessiondata"].head(5000).apply(json.loads).apply(pd.Series)
df_impulse_archive_sessions=df["sessiondata"].apply(json.loads).apply(pd.Series)
#df_impulse=df["sessiondata"].apply(json.dumps).apply(pd.Series)
#df_impulse_archive_sessions=df_impulse.apply(json.loads).apply(pd.Series)
#
#df_impulse=json.dumps(df["sessiondata"].to_json())
#df_impulse_archive_sessions=json.loads(df_impulse)
print(df_impulse_archive_sessions)
'''df_impulse_archive_sessions.Trip=df_impulse_archive_sessions.Trip.apply(json.dumps)
df_impulse_archive_sessions.Quote=df_impulse_archive_sessions.Quote.apply(json.dumps)
df_impulse_archive_sessions.Addons=df_impulse_archive_sessions.Addons.apply(json.dumps)
df_impulse_archive_sessions.Issuer=df_impulse_archive_sessions.Issuer.apply(json.dumps)
df_impulse_archive_sessions.Contact=df_impulse_archive_sessions.Contact.apply(json.dumps)
df_impulse_archive_sessions.Payment=df_impulse_archive_sessions.Payment.apply(json.dumps)
df_impulse_archive_sessions.Travellers=df_impulse_archive_sessions.Travellers.apply(json.dumps)
df_impulse_archive_sessions.CreatedDateTime=df_impulse_archive_sessions.CreatedDateTime.astype('datetime64')
df_impulse_archive_sessions.PartnerMetadata=df_impulse_archive_sessions.PartnerMetadata.apply(json.dumps)
df_impulse_archive_sessions.LastTransactionTime=df_impulse_archive_sessions.LastTransactionTime.astype('datetime64')
df_impulse_archive_sessions.AppliedPromoCodes=df_impulse_archive_sessions.AppliedPromoCodes.apply(json.dumps)
df_impulse_archive_sessions.MemberPointsDataList=df_impulse_archive_sessions.MemberPointsDataList.apply(json.dumps)'''
#df_impulse_archive_sessions.Id=df_impulse_archive_sessions.Id.apply(json.dumps)
#df_impulse_archive_sessions.Trip=df_impulse_archive_sessions.Trip.apply(json.loads)
#df_impulse_archive_sessions.Agent=df_impulse_archive_sessions.Agent.apply(json.dumps)
#df_impulse_archive_sessions.Quote=df_impulse_archive_sessions.Quote.apply(json.dumps)