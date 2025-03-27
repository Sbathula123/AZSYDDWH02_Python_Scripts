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


Pending Bits: 



"""

import pandas
import numpy
import json
import datetime

#-------------------------------------------------------------------------------------
#Main processing block
try:
    print('***************************************************************************')
    print('Main processing block started at:'+str(datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S')))
    url=r"E:\ETL\Data\BigQuery\Out\debug-folder\Test\impulse_cba_archive_policies_delta.csv"
    df = pandas.read_csv(url,delimiter='|',error_bad_lines=True,header=None,names=['sessiontoken','policynumber','policydata','lastupdatetime'],encoding='latin-1') 

    df.replace('null',numpy.nan,inplace=True)#replace all fields containing null to None
    from sqlalchemy import create_engine
    engine = create_engine('mssql+pymssql://@AZSYDDWH02:1433/db-au-stage', pool_recycle=3600, pool_size=5)
    df[['sessiontoken','policynumber','policydata','lastupdatetime']].to_sql('impulse_archive_policies_test', engine, if_exists='replace',index=False,chunksize=100)

except Exception as excp:
    #generic_module.cnxn.close()#closing cursor, doesnt clear lock. Closing connection is important to release the lock.
    raise Exception("Error while executing parse_impulse_cba_archive_policies_delta. Full error description is:"+str(excp))   