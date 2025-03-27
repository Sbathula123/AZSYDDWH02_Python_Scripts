"""
Author : Ratnesh
Dept   : Data Analytics & Innovation, Covermore Group
Date   : 2019-05-20
Usage  : "<filename>.py <run_mode> <start_date> <end_date>"
          "python <filename>.py interval=last3days"
          "python <filename>.py start_date=2018-12-01 end_date=2019-01-01" #this will extract data for the month of December-2018
Params : Parameter 1 - interval - Accepted values are "last3days"/"lastmonth" (do not pass start_date and end_date if passing interval.)
         Parameter 2 - Extraction Start Date in YYYY-MM-DD format. This date is inclusive.
         Parameter 3 - Extraction End Date in YYYY-MM-DD format. This date is exclusive.
         
Desc   : This process is used to push predata (HR) data from uldwh02 to Qualtrics in order to create contacts for surveys.

Configuration requirements : 1) Deployment of this script.
                             2) Scheduling using agent.
                             3) 


DB Changes    

    
Change History:
20190520        RATNESH      


Pending Bits: 

"""
#import pyodbc 
import datetime
import sys
import time
#import voice_analytics_generic_module
import os
#import uuid
from sqlalchemy import create_engine
import requests

#enable following line if running on local workstation.
#sys.path.append(r'\\azsyddwh02\ETL\Python Scripts')
sys.path.append(r'\\aust.covermore.com.au\user_data\NorthSydney_Users\ratneshs\Home\Projects\generic\\')
#import environment_settings
import generic_module
generic_module.set_module(os.path.basename(__file__))

#local Constants
#API_TOKEN = "x0yzLWBPeR88oGqEIT4mUvFKsUVXj7LXOFb5EPRa" old nicole token (expired)
API_TOKEN = "V4lbyWcGcFsBqNklnW1BG87pI1YBP3sOm8JjDsZT"#new nicole token 20-may-2019
QUALTRICS_DATA_CENTRE = "covermorepc.au1"
COVERMOREPC_LIBRARY_ID="GR_6x7wayJMUZjrZVr"
HEADERS = {
    "x-api-token": API_TOKEN,
    "Content-Type": "application/json"
    }


#Local Variables
success_list=[]
error_list=[]

def get_mailing_list_id(mailing_list_name,headers):
    try:
        response = requests.get(url='https://covermorepc.au1.qualtrics.com/API/v3/mailinglists', headers=headers)
        generic_module.checkApiCallSuccess(response,'Error while executing get_mailing_list_id().')
        for i in response.json().get("result").get("elements"):
            if (i["name"]==mailing_list_name):
                return(i["id"])
        #print(response.text)
        #return(response.json().get("result").get("id"))
    except Exception as excp:
        raise Exception("Error while executing get_mailing_list_id(). Full error description is:"+str(excp)) 

def create_mailing_list(url,payload,headers):
    try:
        response = requests.post(url, json=payload, headers=headers)
        generic_module.checkApiCallSuccess(response,'Error while executing create_mailing_list().')
        #print(response.text)
        return(response.json().get("result").get("id"))
    except Exception as excp:
        raise Exception("Error while executing create_mailing_list(). Full error description is:"+str(excp)) 

def create_contact(url,payload,headers):
    try:
        response = requests.post(url, json=payload, headers=headers)
        generic_module.checkApiCallSuccess(response,'Error while executing create_contact().')
        #print(response.text)
        return(response.json().get("result").get("id"))
    except Exception as excp:
        raise Exception("Error while executing create_contact(). Full error description is:"+str(excp)) 
    
def update_contact(url,payload,headers):
    try:
        response = requests.post(url, json=payload, headers=headers)
        generic_module.checkApiCallSuccess(response,'Error while executing update_contact().')
        #print(response.text)
        #return(response.json().get("result").get("id"))
    except Exception as excp:
        raise Exception("Error while executing update_contact(). Full error description is:"+str(excp)) 

def delete_contact(url,headers):
    try:
        response = requests.delete(url, headers=headers)
        print(response.text)
        #return(response.json().get("result").get("id"))
    except Exception as excp:
        raise Exception("Error while executing delete_contact(). Full error description is:"+str(excp))

def delete_all_contacts():
    try:
        print('ji')
    except Exception as excp:
        raise Exception("Error while executing delete_contact(). Full error description is:"+str(excp))

#-------------------------------------------------------------------------------------
#Main processing block
try:
    print('***************************************************************************')
    print('Main processing block started at:'+str(datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S')))
    generic_module.validate_parameters(1)#There should be atleast one parameter value passed.
    generic_module.parse_parameters(**dict(arg.split('=') for arg in sys.argv[1:]))#Parse rest of the parameters.
    #generic_module.connect_db()
    ####################generic_module.extract_historical_oneoff_bq_data()#ONLY ENABLE after a written APPROVAL.
    #generic_module.bot_processing_cba()


    """if not generic_module.is_step_complete(generic_module.module_name,1):
        cursor_ref=generic_module.create_sql_cursor('uldwh02','db-au-stage')
        cursor_ref.execute("exec [db-au-stage]..syssp_createbatch @SubjectArea = 'Qualtrics Interfacing',@StartDate = '"+generic_module.start_date+"',@EndDate = '"+generic_module.end_date+"'");
        cursor_ref.commit()
        generic_module.mark_step_complete(generic_module.module_name,1)
    """
    
    #delete_contact(BASE_URL+'/MLRP_7PSXv8EK48gGSpL',HEADERS)
    #delete_contact(BASE_URL+'/MLRP_6zG5jBdrk2HzLBX',HEADERS)
    #delete_contact(BASE_URL+'/MLRP_5uQbo0gE67FzQfX',HEADERS)
    #delete_contact(BASE_URL+'/MLRP_9YLpOb02qYn5NUF',HEADERS)
    #delete_contact(BASE_URL+'/MLRP_5yWHMhlq3hPsvT7',HEADERS)
    #delete_contact(BASE_URL+'/MLRP_7ajqdKsBnYEHS8l',HEADERS)
    #delete_contact(BASE_URL+'/MLRP_exMd44AF5PC2P9r',HEADERS)
    
    #find mailing list id to write contacts to
    mailing_list_id=get_mailing_list_id(mailing_list_name='PrecedaMailingList', headers=HEADERS)
    if mailing_list_id is None:
            try:
                payload={"name":"PrecedaMailingList","libraryId":COVERMOREPC_LIBRARY_ID}
                mailing_list_id=create_mailing_list('https://covermorepc.au1.qualtrics.com/API/v3/mailinglists',payload,HEADERS)
                print('Created mailing list with mailing_list_ID: '+ mailing_list_id)
            except Exception as excp:
                    print('Mailing list creation failed with error description: '+str(excp)+' payload: '+str(payload))
    else:
        print('Using mailing list with id: '+mailing_list_id)
                    
    BASE_URL = "https://{0}.qualtrics.com/API/v3/mailinglists/{1}/contactimports".format(QUALTRICS_DATA_CENTRE,mailing_list_id)
        
    #create table ETL115_etl_penQuoteSummaryPenguin
    vsql="""SELECT distinct 
       FirstName
      ,LastName
      ,Email
      ,EmployeeID
     ,Company
      ,Division
      ,Department
      --,ReportsToPositionTitle
      ,Gender
      ,PersonnelType
      ,EmploymentType
      ,Location
      ,Country
      ,cast(HireDate as date) HireDate
      --,cast(SelectionDate as date) SelectionDate  --Causes duplicates so ignored.
      --,cast(DOB as date) DOB
      --,SeniorityLevel
      --,cast(isnull(TerminationDate,'9999-12-31') as date) TerminationDate
      --,Hashkey
      --,LoadDate
      --,UpdateDate
      --,isSynced
      --,SyncedDateTime
      ,QualtricsContactID
  FROM [db-au-cmdwh].[dbo].[usrPrecedaEmployee]
  where email   not in ('','-','N/A')
  --and TerminationDate is null
  --and employeeid in (11563)--4399,3577,2592,2143,2700,2968,3613
  and isSynced is null"""
  #and employeeid=4031
  #--and isnull(TerminationDate,'9999-01-01') <> '1900-01-01'#include this line in query if expired contacts are also required.
    preceda_df=generic_module.execute_sql_and_return_result('uldwh02','db-au-cmdwh',vsql)
    #print(preceda_df)
    #print(vsql)
    #print(type(preceda_df))
    for index,row in preceda_df.iterrows():
        #print(row)
        if row.QualtricsContactID is None:
            payload={
            "contacts": 
                                             [ 
                                             {
            "firstName": row.FirstName,
            "lastName": row.LastName,
            "email": row.Email,
            "externalReference": str(row.EmployeeID),
            "embeddedData": {
            "Company": row.Company,
            "Division": row.Division,
            #"Entity": row.Entity,
            "Department": row.Department,
            #"DOB": row.DOB,
            "Gender": row.Gender,
            "PersonnelType": row.PersonnelType,
            "EmploymentType": row.EmploymentType,
           "Location": row.Location,
            "Country": row.Country,
            "HireDate": row.HireDate#,
            #"SelectionDate": row.SelectionDate
            #"SeniorityLevel": row.SeniorityLevel,
            #,"TerminationDate": row.TerminationDate
            },
            "language": "EN",
            "unsubscribed": 0
            } 
                                             ]
            }
            
            #print('Inserting:'+str(payload))
        
            try:
                qualtrics_contact_id=create_contact(BASE_URL,payload,HEADERS)
                print('Created contact with Contact_ID: '+ qualtrics_contact_id)
                vsql="""
                update [db-au-cmdwh].[dbo].[usrPrecedaEmployee]
                set SyncedDateTime=getdate(),isSynced='Y',QualtricsContactID='{}'
                where EmployeeID={}
                --and TerminationDate is null 
                and isSynced is null
                """.format(qualtrics_contact_id,row.EmployeeID)
                #print(vsql)
                generic_module.execute_sql_dml('uldwh02','db-au-cmdwh',vsql)
                success_list.append({'CREATE',row.EmployeeID})
            except Exception as excp:
                    print('Insertion failed with error description: '+str(excp)+' payload: '+str(payload))
                    error_list.append({'CREATE',row.EmployeeID})
            
        else:
            payload={
            "contacts": 
                                             [ 
                                             {
            "id": row.QualtricsContactID, 
            "firstName": row.FirstName,
            "lastName": row.LastName,
            "email": row.Email,
            "externalReference": str(row.EmployeeID),
            "embeddedData": {
            "Company": row.Company,
            "Division": row.Division,
            #"Entity": row.Entity,
            "Department": row.Department,
            #"DOB": row.DOB,
            "Gender": row.Gender,
            "PersonnelType": row.PersonnelType,
            "EmploymentType": row.EmploymentType,
            "Location": row.Location,
            "Country": row.Country,
            "HireDate": row.HireDate#,
            #"SelectionDate": row.SelectionDate
            #"SeniorityLevel": row.SeniorityLevel,
            #,"TerminationDate": row.TerminationDate
            },
            "language": "EN",
            "unsubscribed": 0
            } 
                                             ]
            }
            
            #print('Updating:'+str(payload))
        
            try:
                update_contact(BASE_URL,payload,HEADERS)
                print('Updated contact with Contact_ID: '+ row.QualtricsContactID)
                vsql="""
                update [db-au-cmdwh].[dbo].[usrPrecedaEmployee]
                set SyncedDateTime=getdate(),isSynced='Y'
                where EmployeeID={}
                --and TerminationDate is null
                and isSynced is null
                """.format(row.EmployeeID)
                #print(vsql)
                generic_module.execute_sql_dml('uldwh02','db-au-cmdwh',vsql)
                success_list.append({'UPDATE',row.EmployeeID})
            except Exception as excp:
                print('Updation failed with error description: '+str(excp) + ' payload: '+str(payload))
                error_list.append({'UPDATE',row.EmployeeID})
        
    #generic_module.cnxn.close()#closing cursor, doesnt clear lock. Closing connection is important to release the lock.
    #generic_module.update_cnxn.close()
    #print(success_list)
    #print('debug1')
    #print(error_list)
    print("""Load Summary:
        {} records successfully processed.
        {} records encountered error while processing, please check, resolve and reload the data.
        """.format(len(success_list),len(error_list)))
    if len(error_list)>0:
        print("Processing failed for (Operation,EmployeeID):"+str(error_list))
        raise;
    print('Main processing block finished at:'+str(datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S')))
except Exception as excp:
    #generic_module.cnxn.close()#closing cursor, doesnt clear lock. Closing connection is important to release the lock.
    #raise Exception("Error while executing bot_processing_cba(). Full error description is:"+str(excp))
    raise
#finally:
    #cursor.close()
   #generic_module.cnxn.close()#closing cursor, doesnt clear lock. Closing connection is important to release the lock.