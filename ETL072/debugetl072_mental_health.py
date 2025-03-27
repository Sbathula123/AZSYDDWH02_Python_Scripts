#! /usr/bin/python
# -*- coding: utf-8 -*-
"""
Author : Ratnesh
Dept   : Data Analytics & Innovation, Covermore Group
Date   : 11-jul-2019
Usage  : "python filename.py <PARAM1> <PARAM2>"
Params : Parameter 1 
         Parameter 2 
         #######################################################Parameter 3 - Load duration (Number of days). for full load use a very large number i.e. 1825 for 5 years. Max value for this parameter is 24855.
         Parameter 3 - Load Start Date in YYYY-MM-DD format. Both dates are inclusive.
         Parameter 3 - Load End Date in YYYY-MM-DD format. Both dates are inclusive.
Desc   : 
             1) 

Configuration requirements : 1) 

Pending Bits: 

Change History: 20190708 Base Version
               
"""

#import required libraries
#import requests
#import json
#import pyodbc 
import sys
import os
import datetime
#import requests
# 20220708      CHG0036132 decommissioned the proxy servers, and due to being unable to affect the system environment variables that CmdExec job steps use, 
# this workaround removes proxy info temporarily to avoid a connection error
os.environ["HTTPS_PROXY"] = ""
os.environ["HTTP_PROXY"] = ""

sys.path.append(r'E:\ETL\ClaimLog\\')
import generic_module
generic_module.set_module(os.path.basename(__file__))

#Variable declaration
claims_tag_site_url='https://api.meaningcloud.com/class-1.1?key=176e9b618ade0fd86651b0043054b7d1&of=xml&verbose=n&model=MENTAL&txt=' 
# CHG0036132 decommissioned proxy servers in exchange for dedicated firewall proxy, removing proxy usage in scripts
#https_proxy = "https://10.2.0.67:8080"
# "https://10.2.20.10:8080"
#       ftp_proxy   = "ftp://10.2.20.10:8080"
#proxy_dict = { 
#              #"http"  : http_proxy, 
#              "https" : https_proxy #, this option is applicable .
#              #"ftp"   : ftp_proxy
#            }
success_list=[]
error_list=[]




print('\n\n\n\n\n************************************EXECUTION LOG*********************************************************')
generic_module.validate_parameters(1)#There should be atleast one parameter value passed.
generic_module.parse_parameters(**dict(arg.split('=') for arg in sys.argv[1:]))#Parse rest of the parameters.
#cursor=generic_module.create_sql_cursor(server_name='uldwh02',database_name='db-au-stage')

#*****************************main processing block for claims_tag Data***************************
print('--------Mental health ETL started@:'+str(datetime.datetime.now()))
vsql="""
select top 10000
    f.ClaimKey,
    left(f.EventDescription, 3000) EventDescription
from
    [db-au-cmdwh]..vClaimTagsFeeder f WITH (NOLOCK)
    inner join [db-au-cmdwh]..clmClaim cl with(nolock) on
        cl.ClaimKey = f.ClaimKey
where
    --f.ClaimKey in ('AU-952512','AU-952511') and 
    --len(replace(f.EventDescription, f.Peril, '')) >= 25 and
   -- cl.ClaimNo % 2 = 0 and
    not exists
    (
        select
            null
        from
            [db-au-cmdwh]..clmClaimTags t  with(index(idx_clmClaimTags_ClaimKey) NOLOCK)
        where
            t.ClaimKey = cl.ClaimKey and
            Classification = 'Mental Health' and
            not
            (
                ClassificationText not like '%OK%' or
                ClassificationText like '%request rate%' or
                ClassificationText like '%Model not supported%' or
                ClassificationText like '%error%' or
                ClassificationText like '%Credits per subscription exceeded%'
            ) and
            t.UpdateTime >= convert(date, getdate()-10)
    )
order by
    cl.CreateDate desc"""
#cursor.execute(vsql)
claim_df=generic_module.execute_sql_and_return_result(server_name='uldwh02',database_name='db-au-stage',sql=vsql)
#print(claim_df)

for index,row in claim_df.iterrows():
    try:
        claims_tag_claim_key=row.ClaimKey
        claims_tag_event_description=row.EventDescription
        url=claims_tag_site_url+claims_tag_event_description
        #print('Fetching claims_tag issues data using url :'+url)
        print('Processing Claim Key :'+claims_tag_claim_key)
        classification_text=generic_module.call_rest_api_and_return_response(url,auth=None,error_message='MeaningCloud connectivity issue.',response_type='TEXT')
        #print(classification_text)
        update_sql="""
                    merge into [db-au-cmdwh].dbo.clmClaimTags t
                    using 
                        (
                            select 
                                '"""+classification_text+"""' ClassificationText,
                                '"""+claims_tag_claim_key+"""' ClaimKey,
                                'Mental Health' Classification
                        ) s on
                            s.ClaimKey = t.ClaimKey and
                            s.Classification = t.Classification
                    
                    when matched then
                    
                        update
                        set
                            ClassificationText = s.ClassificationText,
                            UpdateTime = getdate()
                    
                    when not matched by target then
                        insert
                        (
                            ClaimKey,
                            Classification,
                            ClassificationText,
                            UpdateTime
                        )
                        values
                        (
                            s.ClaimKey,
                            s.Classification,
                            s.ClassificationText,
                            getdate()
                        )
                    ;
                    """
        #print(update_sql)
        #sys.exit()
        generic_module.execute_sql_dml(server_name='uldwh02',database_name='db-au-stage',sql=update_sql)
        success_list.append({'MERGE',claims_tag_claim_key})
    except Exception as excp:
        print('Error description: '+str(excp)+' claims_tag_claim_key: '+claims_tag_claim_key)
        error_list.append({'MERGE',claims_tag_claim_key})

print("""Load Summary:
        {} records successfully processed.
        {} records encountered error while processing, please check, resolve and reload the data.
        """.format(len(success_list),len(error_list)))
if len(error_list)>0:
        print("Processing failed for (Operation,EmployeeID):"+str(error_list))
        raise;            

print('--------Mental health ETL finished@:'+str(datetime.datetime.now())+'\n')
#sys.exit()
#*********************************************************************************************
