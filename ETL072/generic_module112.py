"""
Author : Ratnesh
Dept   : Data Analytics & Innovation, Covermore Group
Date   : 21-May-2018
Desc   : This is generic module contains common code.
         This code is not called directly but via other scripts.

Parameters parsing Notes:
    
    Parameters can be passed in any order but need to be separated by space.
    
    Parameter Name: exec_mode
    Example : exec_mode=FULL
    Parameter Description: Execution Mode (FULL or RESUME)
    Default value: RESUME
    Behaviour: Will start execution from point of failure. For full execution i.e. to execute each and every step set this as exec_mode=FULL
      
    Parameter Name: debug_mode
    Example : debug_mode=TRUE
    Parameter Description: Debug Mode (TRUE or FALSE)
    Default value: FALSE
    Behaviour: Set this mode to look at the application behaviour. Actual changes will not be done in debug mode.

    Parameter Name: interval (start_date and end_date parameters if passed, have precedence over this parameter and this parameter will be ignored in that case).
    Example : interval=last3days
    Parameter Description: time interval for which ETL processing will be done.
    Default value: last3days
    Behaviour: if you passthis value then start_date and end_date are automatically derived based on the value of interval.
               else pass a specific value for start_date and end_date parameters in YYYY-MM-DD format.

    Parameter Name: start_date and end_date (dates for data processing). Filters are inclusive of both the dates.
    Example : start_date=2019-01-03 end_date=2019-01-05
    Parameter Description: dates for which ETL processing will be done.
    Default value: N/A
    Behaviour: if you passthis value then start_date and end_date will be used for data filtering.
               else pass a specific value for interval.
               

    Parameter Name: run_mode -DEPRECATED
    Example : run_mode=AUTO
    Parameter Description: Run Mode (AUTO or MANUAL)
    Default value: AUTO
    Behaviour: in Auto mode dates/parameters are automatically derived based on the value of <TBD>. in Manual mode parameter values need to be passed manually.    

Important Naming Convention: 
                             variable name are like my_variable
                             global variable are like my_global_variable
                             function name are like my_function
                             module name are in all lower with underscore used as separator like generic_module.
                             package names are in all lower with NO underscore used.
                             Class names are like MyClass.
                             Type variable names are like MyTypeVariable.
                             Exception names are like MyException.
                             Constants are like MAX_CONSTANT.
                             comments are like "# This is comment."
                             
Coding Practice :           use of global variable is discouraged.
                            Dont use Tab for indenting when using notepad etc.
                            Use 4 spaces for each level of indentation.
                            Use "WITH (NOLOCK)" after tablenames in sql queries to avoid locking.
                             

Configuration requirements:

Signature: List of reusable procedures under this module.
    
    #common generic utility functions
    --set_module
    --validate_parameters
    --parse_parameters
    --is_step_complete
    --mark_step_complete
    --delete_complete_marker
    --create_sql_cursor
    --send_email
    --download_email_attachments
    --does_directory_exist
    --create_directory
    --archive_files
    
    #GCP
    --initialise_gcs_client
    --initialise_bq_client
    --get_gcp_bucket_details
    --does_dataset_exist
    --check_bq_table_size_in_gb
    --download_gcs_file
    --get_default_load_job_config
    --get_default_query_job_config
    --execute_sql_bigquery_and_return_result
    --create_bigquery_table_from_queryresult
    --create_bigquery_table_from_dataframe
    --create_bigquery_table_from_localfile
    --create_bigquery_table_from_gcsfile
    --create_bigquery_table_from_schema
    --delete_bigquery_table    
    --get_bq_extract_metadata
    --extract_bq_data
    --download_bq_data
    --create_bq_table_backup_for_time_window
    --call_save_dataframe_to_bigquery(Under development)
    --call_execute_sql_bigquery_and_return_result(Under development)
    --Under development
    --Under development
    
    #Rest API
    --check_rest_api_call_success
    --call_rest_api_and_return_response
    
    
Pending Bits: 
    1. use of bq function to check table size.
    2. Download logic may be using a separate Gsutil statement.
        enable download in parallel.
    3. implement log_msg function. 
    4. 
    5. 
    6. 
    7. implement check_dependency()

Change History:
20190212        RATNESH          Base version
20190801        RATNESH          Changes made in download_email_attachments func post office 365 migration.

    

"""

#import system libraries
import requests
#import json
import pyodbc 
import sys
import datetime
#from tqdm import tqdm
#import io
import os
import glob #for dropping files/cleanup
import pandas
import smtplib
from email.message import EmailMessage
import exchangelib #in download email attachments function
#import pyarrow
#import uuid#to generate unique identifier to be given to job names
import dateutil.relativedelta#to calculate first day of previous month.
# Imports the Google Cloud client library
#from google.cloud import speech_v1p1beta1 as speech
#from google.cloud import bigquery
#from google.cloud import storage
import google.cloud.bigquery as bigquery
import google.cloud.storage as storage

#import sustome libraries
import environment_settings112

#Variable declaration
#rest_url="http://10.2.30.16/api/rest/"
#credentials file directory
generic_module_instance = sys.modules[__name__]
generic_module_version='1.0.0'
generic_module_instance_lmd='2019-02-18'
#module_name=os.path.basename(__file__)
#print('module_name is:'+module_name )
#if os.getenv('COMPUTERNAME','defaultValue') in ('ULDWH02','AZSYDDWH02'):
    #vAudioFilesDir = 'E:\\ETL\\Python Scripts\\VoiceAnalytics\\AudioFiles\\'

#following comes from environment_settings112 now
#vGCPCredentialsFileDir = r'\\Uldwh02\ETL\Python Scripts\generic\\'
vLogDir = r'\\Uldwh02\ETL\Log\\'
    
#elif os.getenv('COMPUTERNAME','defaultValue')=='DESKTOP-PC0002':
    #vAudioFilesDir = 'C:\\temp\\'
#    vGCPCredentialsFileDir = 'C:\\temp\\'
#else:
    #vAudioFilesDir = 'C:\\temp\\'
#    vGCPCredentialsFileDir = 'C:\\temp\\'

#pass GCP credentials for Authentication.
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = environment_settings112.vGCPCredentialsFileDir+"Cover-More Data and Analytics-fd15ac400401.json"


# define user-defined exceptions
class Error(Exception):
   """Base class for other exceptions"""
   pass

class DbConnectionError(Error):
   """Raised when the Database connection fails"""
   pass

class ApiCallError(Error):
   """Raised when api call fails due to various reasons including connectivity/credentials"""
   pass

class InvalidParameterError(Error):
   """Raised when api call fails due to various reasons including connectivity/credentials"""
   pass

#-------------------------------------------------------------------------------------
#set module_name for debugging and reexecution/resumption purposes
def set_module(module_name):
    generic_module_instance.module_name=module_name.replace('.py','')
    #print('module name is set as:'+generic_module_instance.module_name)

#-------------------------------------------------------------------------------------
#Check if valid parameters are passed
def validate_parameters(minimum_parameter_count):
  #sys.argv will be 7 if 6 parameters are passed
 if len(sys.argv)-1<minimum_parameter_count:
   raise Exception('Error while executing validate_parameters(). Please execute the script with minimum {} parameter(s).'.format(minimum_parameter_count))

#-------------------------------------------------------------------------------------
#parse parameters and initialise instance variables
def parse_parameters(**kwargs):
    try:
        print("Parsing arguments and setting instance parameters.")
        for parameter_name, parameter_value in kwargs.items():
         #print( '{0} = {1}'.format(parameter_name, parameter_value))
         #generic_module_instance.parameter_name=parameter_value
         #print(generic_module_instance.parameter_name)
             print("Setting     generic_module_instance."+parameter_name+"='"+parameter_value+"'")
             exec("generic_module_instance."+parameter_name+"='"+parameter_value+"'")
        

        #check if both date parameters are passed this take priority.    
        #if id(generic_module_instance.start_date) != None and id(generic_module_instance.end_date) != None:
        if 'start_date' in str(locals()) and 'end_date' in str(locals()):
            #try:
                #fail if interval is also passed.
                if 'interval' in str(locals()):
                    #print("Cannot pass interval and date parameters together. Please correct and reexecute.")
                    raise InvalidParameterError("Cannot pass interval and date parameters together. Please correct and reexecute.")
                else:
                    #Ignore interval
                    exec("generic_module_instance.interval='Ignore'")
        elif ('start_date' in str(locals()) and 'end_date' not in str(locals())) or ('start_date' not in str(locals()) and 'end_date' in str(locals())):
                #print("only one of the date parameters is passed.")
                raise InvalidParameterError("only one of the date parameters is passed.")
        #if not date parameters passed then check interval. set default interval if not passed.
        elif 'interval' not in str(locals()):
            #if start_date and end_date are not passed and inteval is also not passed, then set default interval.
            exec("generic_module_instance.interval='last3days'")
        

        #derive start and end dates based on interval vallue
        if 'last' in generic_module_instance.interval and 'month' in generic_module_instance.interval:
            number_of_months=generic_module_instance.interval.replace('last','').replace('months','').replace('month','')
            generic_module_instance.start_date = datetime.date.today().replace(day=1) - dateutil.relativedelta.relativedelta(months=int(number_of_months))
            generic_module_instance.lastMonth=generic_module_instance.start_date.strftime("%Y%m")
            generic_module_instance.start_date=generic_module_instance.start_date.strftime("%Y-%m-%d")#lastMonthsFirstDay
            print('Interval lastmonth. Setting generic_module_instance.start_date='+generic_module_instance.start_date)
            
            generic_module_instance.end_date = datetime.date.today().replace(day=1)
            generic_module_instance.end_date=generic_module_instance.end_date.strftime("%Y-%m-%d")#CurrentMonthsFirstDay
            print('Interval lastmonth. Setting generic_module_instance.end_date='+generic_module_instance.end_date)
        elif 'last' in generic_module_instance.interval and 'day' in generic_module_instance.interval:
            number_of_days=generic_module_instance.interval.replace('last','').replace('days','').replace('day','')
            generic_module_instance.start_date = datetime.date.today() - dateutil.relativedelta.relativedelta(days=int(number_of_days))
            generic_module_instance.start_date=generic_module_instance.start_date.strftime("%Y-%m-%d")#lastMonthsFirstDay
            print('Interval last3days. Setting generic_module_instance.start_date='+generic_module_instance.start_date)
            
            generic_module_instance.end_date = datetime.date.today()
            generic_module_instance.end_date=generic_module_instance.end_date.strftime("%Y-%m-%d")#todays date
            print('Interval last3days. Setting generic_module_instance.end_date='+generic_module_instance.end_date) 
        elif generic_module_instance.interval=='Ignore':
            pass
        else:
                raise InvalidParameterError("Invalid interval value passed.")
        
        #Validating all parameter values.
        '''if generic_module_instance.run_mode=='AUTO':
            generic_module_instance.start_date = datetime.date.today().replace(day=1) - dateutil.relativedelta.relativedelta(months=1)
            generic_module_instance.lastMonth=generic_module_instance.start_date.strftime("%Y%m")
            generic_module_instance.start_date=generic_module_instance.start_date.strftime("%Y-%m-%d")#lastMonthsFirstDay
            print('AUTO Mode Used. Setting generic_module_instance.start_date='+generic_module_instance.start_date)
            
            generic_module_instance.end_date = datetime.date.today().replace(day=1)
            generic_module_instance.end_date=generic_module_instance.end_date.strftime("%Y-%m-%d")#CurrentMonthsFirstDay
            print('AUTO Mode Used. Setting generic_module_instance.end_date='+generic_module_instance.end_date)
        elif generic_module_instance.run_mode=='MANUAL':
            generic_module_instance.lastMonth=datetime.datetime.strptime(generic_module_instance.start_date,'%Y-%m-%d').strftime("%Y%m")#derive extraction monthname for creating the directory in GCS bucket and filesystem.
        else:
            raise Exception("Invalid run_mode parameter value. run_mode can only be AUTO or MANUAL. Error while executing parse_parameters()") '''
            
        try:
            id(generic_module_instance.debug_mode)#checking if debug mode is set, if not it will be set as off by default
        except:
            print('Setting Default DEBUG_MODE as (OFF).')
            exec("generic_module_instance.debug_mode='FALSE'")
        
        if generic_module_instance.debug_mode== 'TRUE':
                print('DEBUG_MODE : ON')
                generic_module_instance.debug_mode=True
        elif generic_module_instance.debug_mode== 'FALSE':
                print('DEBUG_MODE : OFF')
                generic_module_instance.debug_mode=False
        else:
                raise InvalidParameterError("Invalid debug_mode value. debug_mode can only be TRUE or FALSE.") 

        try:
            id(generic_module_instance.exec_mode)#checking if execution mode mode is set, if not it will be set as ON by default
        except:
            print('Setting Default execution mode as (RESUME).')
            exec("generic_module_instance.exec_mode='RESUME'")
        
        if generic_module_instance.exec_mode== 'FULL':
                print('EXECUTION MODE : FULL Execution of all steps. Deleting all previous markers.')
                delete_complete_marker(generic_module_instance.module_name)
        elif generic_module_instance.exec_mode== 'RESUME':
                print('EXECUTION MODE : Run only failed/incomplete steps.')
        else:
                raise InvalidParameterError("Invalid exec_mode parameter value. exec_mode mode can only be TRUE or FALSE.") 
                      
    except InvalidParameterError as excp:
        raise
    except Exception as excp:
        raise Exception("Error while executing parse_parameters(). Full error description is:"+str(excp)) 

'''def initialise_dates():
    try:
        generic_module_instance.start_date#check if the variable was initialised 
    except NameError:
        print('undefined')
        #set variable'''

#----------------------------------------------------------------------------        
def is_step_complete(module_name,step_no):
    try:
        directorypath=environment_settings112.vLogDir+module_name+'.'+str(step_no)+'.done'
        #if generic_module_instance.exec_mode=='FULL':
        #    print('Ignoring all previous execution in FULL execution mode.')
        #    return(False)#ignore done flag in case of full execution.
        #elif generic_module_instance.exec_mode=='RESUME' and os.path.exists(directorypath):
        if os.path.exists(directorypath):
            print('step {} is already executed for module {} and marked by {}'.format(step_no,module_name,directorypath))
            return(True)
        else:
            return(False)
    except Exception as excp:
        raise Exception("Error while executing is_step_complete(). Full error description is:"+str(excp))


#----------------------------------------------------------------------------        
def mark_step_complete(module_name,step_no):
    try:
        directorypath=environment_settings112.vLogDir+module_name+'.'+str(step_no)+'.done'
        print('Marking step {} as complete for module {} with marker {} at {}'.format(step_no,module_name,directorypath,str(datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S'))))
        with open(directorypath, 'w'):
            None
    except Exception as excp:
        raise Exception("Error while executing is_step_complete(). Full error description is:"+str(excp))

#----------------------------------------------------------------------------        
def delete_complete_marker(module_name):
    try:
        directorypath=environment_settings112.vLogDir+module_name+'.*.done'
        for i in glob.glob(directorypath):
            #print(i)
            os.remove(i)
        print('Deleted marker .done files {} for module {}.'.format(directorypath,module_name))
    except Exception as excp:
        raise Exception("Error while executing delete_complete_marker(). Full error description is:"+str(excp))        
        
#-------------------------------------------------------------------------------------
#MS SQL Server Connectivity and session/cursor creation     
def create_sql_cursor(server_name,database_name):
 try:
     #global cnxn;
     cnxn = pyodbc.connect("Driver={SQL Server};"
                      "Server="+server_name+";"
                      "Database="+database_name+";"
                      "Trusted_Connection=yes;")
     #global cursor;
     cursor = cnxn.cursor()
     cursor.rollback();
     
     '''global update_cnxn;
     update_cnxn = pyodbc.connect("Driver={SQL Server};"
                      "Server="+server_name+";"
                      "Database="+database_name+";"
                      "Trusted_Connection=yes;")
     global update_cursor;
     update_cursor = update_cnxn.cursor()
     update_cursor.rollback();'''
     #print('Database connection successful.')
     return cursor;
 except Exception as excp:
     raise Exception("Error while executing create_sql_cursor(). Full error description is:"+str(excp))

#-------------------------------------------------------------------------------------
#Send email function     
def send_mail(to_email, subject, message, server='outlook.covermore.com',
              from_email='covermorereport@covermore.com.au'):
    # import smtplib
    msg = EmailMessage()
    msg['Subject'] = subject
    msg['From'] = from_email
    msg['To'] = ', '.join(to_email)
    msg.set_content(message)
    print(msg)
    server = smtplib.SMTP(server)
    #server.set_debuglevel(1)
    #server.login(from_email, 'password')  # user & password
    server.send_message(msg)
    server.quit()
    print('successfully sent the mail.')

#-------------------------------------------------------------------------------------
#download email attachments 
def download_email_attachments(subject_pattern, #picks only the latest email containing pattern in subject
                               #received_after,
                               attachment_name_startswith='', # case insensitive check. Skipping will save all attachments.
                               attachment_type='',#.csv  case insensitive check. Skipping will save all attachments.
                               #username="COVERMORE\\bi_guest", #commenting post office365 migration and adding new office365 username.
                               username="analytics@covermore.com",
                               #password="KnockKnockKnock3x", #commenting post office365 migration and adding new office365 password.
                               password="Cover2018!",
                               #server='outlook.covermore.com', #commenting post office365 migration and adding new office365 server.
                               server='smtp.office365.com',
                               #primary_smtp_address="bi_inbox@covermore.com.au", #commenting post office365 migration and adding new office365 email address.
                               primary_smtp_address="analytics@covermore.com",
                               download_location=r"\\uldwh02\ETL\Data\FX Data"
                               ):
   

    creds = exchangelib.Credentials(username=username, password=password)

    config = exchangelib.Configuration(server=server, credentials=creds)

    account = exchangelib.Account(
    primary_smtp_address=primary_smtp_address,
    autodiscover=False, 
    config = config,
    access_type=exchangelib.DELEGATE)
    
    #for msg in mailbox.all():
    for msg in account.inbox.filter(subject__icontains=subject_pattern
                                    #,start__gte=tz.localize(exchangelib.EWSDateTime(2019,7,1))
                                    ).order_by('-datetime_received')[:1]: #this picks the lastest email containing the pattern in subject line
        print(msg.subject,'|', msg.sender,'|', msg.datetime_received)
        for attachment in msg.attachments:
            #fpath = os.path.join(r'C:\temp', attachment.name)
            if attachment.name.lower().startswith(attachment_name_startswith.lower()) \
                and attachment.name.lower().endswith(attachment_type.lower()):
                fpath = os.path.join(download_location, attachment.name)
                with open(fpath, 'wb') as f:
                    f.write(attachment.content)
                    print('Attachment '+fpath+' saved.')
    
    
#-------------------------------------------------------------------------------------
#get extract metadata from metadata excel
def does_directory_exist(directorypath):
    try:
        return os.path.exists(directorypath)
    except Exception as excp:
        raise Exception("Error while executing does_directory_exist(). Full error description is:"+str(excp))
        
#-------------------------------------------------------------------------------------
#get extract metadata from metadata excel
def create_directory(directorypath):
    try:
        if not os.path.exists(directorypath) and not generic_module_instance.debug_mode:
            os.makedirs(directorypath)
    except Exception as excp:
        raise Exception("Error while executing create_directory(). Full error description is:"+str(excp))

#-------------------------------------------------------------------------------------
#archive
def archive_files(source_directorypath, #r"\\uldwh02\ETL\Data\FX Data"
                  archive_directorypath, #r"\\uldwh02\ETL\Data\FX Data\Archived"
                  file_name_startswith, #'USD_End' #pass empty string to select all files in directory 
                  file_type):  #.csv case insensitive search #pass empty string to select all files in directory
    try:
        if not does_directory_exist(source_directorypath):
            raise Exception('Directory '+source_directorypath+' does not exist.')
        elif not does_directory_exist(archive_directorypath):
            raise Exception('Directory '+archive_directorypath+' does not exist.')
        for i in glob.glob(source_directorypath+"\\"+file_name_startswith+"*"+file_type):
            #print(i)
            #if file_name_startswith in i.lower() and \
            #   i.lower().endswith(file_type.lower()):
            print('Archiving file '+i+' as '+archive_directorypath+i.replace(source_directorypath,''))
            os.rename(i,archive_directorypath+i.replace(source_directorypath,''))
    except Exception as excp:
        raise Exception("Error while executing archive_files(). Full error description is:"+str(excp))

#-------------------------------------------------------------------------------------
#Check if API call was successful, raise error otherwise
def checkApiCallSuccess(response,errorMessage):
  if 'error' in str(response).lower() or '200' not in str(response):
    print('checkApiCallSuccess failed. API Response is as below:')
    print(response.text)
    #print('Reciprocating above exception to main() and stopping execution.')
    raise Exception(errorMessage)
        
#-------------------------------------------------------------------------------------
#initialise GCS client
def initialise_gcs_client():
    try: 
        gcsclient =storage.Client()
        return gcsclient
    except Exception as excp:
        raise Exception("Error in initialise_gcs_client(). Full error description is:"+str(excp))    
        
        
#initialise BQ client
def initialise_bq_client():
    try: 
        bqclient = bigquery.Client()
        return bqclient
    except Exception as excp:
        raise Exception("Error in initialise_bq_client(). Full error description is:"+str(excp))    

#-------------------------------------------------------------------------------------
#Authentication routine
def get_gcp_bucket_details(bucket_name):
    try: 
        #Passing google cloud credentials
        #os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = vGCPCredentialsFileDir+"Cover-More Data and Analytics-fd15ac400401.json"
        #instantiate a storage client for file upload to GCP bucket
        #print('GCP Authentication started at:'+str(datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S')))
        #storageclient =storage.Client()
        #gcsclient=initialise_gcs_client()
        bucket = generic_module_instance.gcsclient.get_bucket(bucket_name)
        print(bucket)
        #blob = bucket.blob(vWavFileNameOrigBucket)
        #blob.upload_from_filename(vWavFileNameOrigLocal)
        #blob = bucket.blob(vWavFileNameMonoBucket)
        #blob.upload_from_filename(vWavFileNameMonoLocal)
         
        #removing the local files after upload
        #os.remove(vWavFileNameOrigLocal)
        #os.remove(vWavFileNameMonoLocal)
    except ApiCallError as excp:
     print(str(excp))
     raise;
    except Exception as excp:
     raise Exception("Error while executing get_gcp_bucket_details(). Full error description is:"+str(excp))

#-------------------------------------------------------------------------------------     
# [START bigquery_dataset_exists]
def does_dataset_exist(client, dataset_reference):
    """Return if a dataset exists.
    Args:
        client (google.cloud.bigquery.client.Client):
            A client to connect to the BigQuery API.
        dataset_reference (google.cloud.bigquery.dataset.DatasetReference):
            A reference to the dataset to look for.
    Returns:
        bool: ``True`` if the dataset exists, ``False`` otherwise.
    """
    from google.cloud.exceptions import NotFound

    try:
        client.get_dataset(dataset_reference)
        return True
    except NotFound:
        return False

#-------------------------------------------------------------------------------------
#check tablesize and return true if table is larger than 1 GB
def check_bq_table_size_in_gb(project,dataset_id,table_id):
    try:
        #bqclient = initialise_bq_client()
        sql = "SELECT cast(sum(size_bytes)/(1024*1024*1024) as numeric) as size from " \
                +dataset_id+".__TABLES__ where table_id='"        \
                +table_id+"'"
        
        if not generic_module_instance.debug_mode:
            query_job = generic_module_instance.bqclient.query(sql)
            for row in query_job:
                #print('size of table {} is {}'.format(table_id,row[0]))
                #print(row)
                '''if row[0] > 1:
                    print('{}.{} is larger than 1GB. This will be exported in multiple parts.'.format(dataset_id,table_id))
                    return(True)
                else:
                    #print('Small table')
                    return(False)'''
                return(row[0])
        else:
            print(sql)
            print('Can not check table size in debug mode.')
            return(False)
    except Exception as excp:
        raise Exception("Error while executing is_table_larger_than_1_GB(). Full error description is:"+str(excp))



#-------------------------------------------------------------------------------------
#Extract all historical tables in BQ metadata sheet to GCS storage bucket. This is done for entire data before the first date of previous month (including full non partitioned data).
'''def extract_historical_oneoff_bq_data():
    try:
        print('Starting historical data extraction at:'+str(datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S')))
        generic_module_instance.bqclient = initialise_bq_client()
        generic_module_instance.gcsclient = initialise_gcs_client()
        bq_extract_metadata=get_bq_extract_metadata()
        

        extract_job_config = bigquery.ExtractJobConfig() #Other configuration settings like delimiter etc will be passed using this parameter.
        #extract_job_config.destination_format = 'NEWLINE_DELIMITED_JSON'
        extract_job_config.field_delimiter = '|'
        

        for idx, row in bq_extract_metadata.iterrows():
            project = row.SourceBQProjectName
            dataset_id = row.SourceBQDatasetName
            table_id = row.SourceTableName
            OutputFileName=row.OutputFileName
            location=row.location
            
            #create a new folder with last month name in YYYYMM format under OutputFilePath on GCS
            #bucket = generic_module_instance.gcsclient.get_bucket(row.ExtractionBucketName)
            #blob = bucket.blob(row.OutputFilePath+generic_module_instance.lastMonth)
            #blob.upload_from_string('')
            
            if row.TableType=='Partitioned':
                dataset_id,table_id=create_bq_table_Backup_for_time_window(project,dataset_id,table_id,row.PartitionColumn,'1900-01-01',generic_module_instance.start_date)#,location
            if is_table_larger_than_1_GB(project,dataset_id,table_id):
                OutputFileName=OutputFileName+'-*'#adding a wildcard since multiple files will be generated for tables larger than 1GB of size
            #destination_uri = 'gs://'+row.ExtractionBucketName+'/'+row.OutputFilePath+generic_module_instance.lastMonth+'/'+ OutputFileName +row.OutputFileExt #after including wildcard in case of larger tables. 
            destination_uri = 'gs://'+row.ExtractionBucketName+'/'+row.OutputFilePath+'FULL_HISTORICAL_BACKUP_TILL_LASTMONTH'+'/'+ OutputFileName +row.OutputFileExt #after including wildcard in case of larger tables. 
            dataset_ref = generic_module_instance.bqclient.dataset(dataset_id, project=project)
            table_ref = dataset_ref.table(table_id)
            job_id=row.SourceTableName+str(uuid.uuid4())#unique job identifier need to be passed otherwise a non unique jobid is generated and only first extract operation works.
            extract_job = generic_module_instance.bqclient.extract_table(table_ref,destination_uri,
                                                 job_id=job_id,
                                                 job_config=extract_job_config,
                                                 location=location)  # API request. Keep location as last parameter and dont put it before job_id else job_id will be generated by GCP and raise error.
            if not generic_module_instance.debug_mode:
                extract_job.result()  # Waits for job to complete.
                print('Exported {}:{}.{} to {}'.format(project, dataset_id, table_id, destination_uri))
        print('Historical data extraction completed at:'+str(datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S')))
    except Exception as excp:
        raise Exception("Error while executing extract_historical_oneoff_bq_data(). Full error description is:"+str(excp))'''
    

#-------------------------------------------------------------------------------------
#downloads one file at a time
def download_gcs_file(bucket_name, source_blob_name, destination_file_name):
    try:
        #def download_blob(bucket_name, source_blob_name, destination_file_name):
        """Downloads a blob from the bucket."""
        #gcsclient = initialise_gcs_client()
        bucket = generic_module_instance.gcsclient.get_bucket(bucket_name)
        blob = bucket.blob(source_blob_name)
        if not generic_module_instance.debug_mode:
            blob.download_to_filename(destination_file_name)
        '''print('Blob {} downloaded to {}.'.format(
            source_blob_name,
            destination_file_name))'''
    except Exception as excp:
        raise Exception("Error while executing download_gcs_file(). Full error description is:"+str(excp))        
        
#-------------------------------------------------------------------------------------
#Check if API call was successful
'''def checkApiCallSuccess(response,errorMessage,raiseErrorAndStop):
  if str(response) != '<Response [200]>':
    print('checkApiCallSuccess failed. API Response is as below:')
    print(response.text)
    if raiseErrorAndStop=='Y':
     print('Reciprocating above exception to main() and stopping execution.')
     raise Exception(errorMessage)
    else:
     return 'F'
  else:
      return 'S'
'''      
#-------------------------------------------------------------------------------------
'''
http_proxy  = "http://10.2.20.10:8080"
https_proxy = "https://10.2.20.10:8080"
ftp_proxy   = "ftp://10.2.20.10:8080"
proxyDict = { 
              "http"  : http_proxy, 
              "https" : https_proxy,
              "ftp"   : ftp_proxy
            }
'''        

#----------------------------------------------------------------------------
    
def get_default_load_job_config(write_disposition='WRITE_TRUNCATE'):
    try:
        #load job config is mostly used to load staging tables and so 
        #write_disposition is set to WRITE_TRUNCATE by default. 
        #Be careful not to use this procedure to load to 
        load_job_config = bigquery.LoadJobConfig()
        load_job_config.source_format = bigquery.SourceFormat.CSV#enabled on 8-mar by ratnesh.
        #load_job_config.schema = schema
        load_job_config.skip_leading_rows = 0
        load_job_config.field_delimiter = '|'
        load_job_config.use_legacy_sql = False#added on 8-mar by ratnesh additionally
        #load_job_config.autodetect = True
        #load_job_config.create_disposition # default value create if neeeded will be used.
        load_job_config.write_disposition = write_disposition
        return load_job_config
    except Exception as excp:
        raise Exception("Error while executing get_default_load_job_config(). Full error description is:"+str(excp))
#----------------------------------------------------------------------------
    
def get_default_query_job_config(write_disposition='WRITE_TRUNCATE',query_parameters=None):
    try:
        #load job config is mostly used to load staging tables and so 
        #write_disposition is set to WRITE_TRUNCATE by default. 
        #Be careful not to use this procedure to load to main dw tables.
        query_job_config = bigquery.QueryJobConfig()
        query_job_config.use_legacy_sql = False
        if query_parameters is not None:
            query_job_config.query_parameters=query_parameters
        #query_job_config.allow_large_results=True #commented on 20190228 for execute query job only.
        #query_job_config.create_disposition # default value create if neeeded will be used.
        if write_disposition != None:
            query_job_config.write_disposition=write_disposition
        return query_job_config
    except Exception as excp:
        raise Exception("Error while executing get_default_query_job_config(). Full error description is:"+str(excp))

#----------------------------------------------------------------------------
def execute_sql_bigquery_and_return_result(project,dataset_id,sql,write_disposition='WRITE_TRUNCATE'):
    try:
        #execute query and return results as dataframe
        generic_module_instance.bqclient = initialise_bq_client()
        #dataset_ref = generic_module_instance.bqclient.dataset(dataset_id, project)
        
        query_params = [
                        bigquery.ScalarQueryParameter('start_date', 'STRING', generic_module_instance.start_date),
                        bigquery.ScalarQueryParameter('end_date', 'STRING', generic_module_instance.end_date)
                        #bigquery.ScalarQueryParameter('min_word_count', 'INT64', 250)
                        ]
        query_job_config=get_default_query_job_config(write_disposition,query_parameters=query_params)
        query_job_config.encoding='utf-8'
        
        if not generic_module_instance.debug_mode:
            query_job = generic_module_instance.bqclient.query(
                                            sql,
                                            #table_ref,
                                            job_config=query_job_config)
            query_job.result()#this is important otherwise it will return control before executing the query.
        #print('Table {}.{}.{} created.'.format(project,dataset_id,table_id))
            print('SQL is executed. Returning result.')
            return(query_job)
    except Exception as excp:
        raise Exception("Error while executing execute_sql_bigquery_and_return_result(). Full error description is:"+str(excp)+str(query_job.errors))

#----------------------------------------------------------------------------
def execute_sql_and_return_result(server_name,database_name,sql):
    #this function returns the result of query exeuction as a dataframe.
    try:
        if not generic_module_instance.debug_mode:
            from sqlalchemy import create_engine
            engine = create_engine('mssql+pymssql://@'+server_name+':1433/'+database_name, pool_recycle=3600, pool_size=5)
            if "(NOLOCK)" not in sql.upper():
                print("WARNING: USE 'WITH (NOLOCK)' HINT IN SQL TO AVOID LOCKING ISSUES.")
            df_result=pandas.read_sql(sql,engine)
        #query_job.result()
        #print('Table {}.{}.{} created.'.format(project,dataset_id,table_id))
            #print('SQL is executed. Returning result.')
            return(df_result)
            
                
    except Exception as excp:
        raise Exception("Error while executing execute_sql_and_return_result(). Full error description is:"+str(excp))

#----------------------------------------------------------------------------
def execute_sql_dml(server_name,database_name,sql):
    #this function returns the result of query exeuction as a dataframe.
    try:
        if not generic_module_instance.debug_mode:
            create_sql_cursor(server_name,database_name).execute(sql).commit()
            #print('SQL DML is executed.')
    except Exception as excp:
        raise Exception("Error while executing execute_sql_dml(). Full error description is:"+str(excp))        
#----------------------------------------------------------------------------
def create_bigquery_table_from_queryresult(project,dataset_id,table_id,PartitionColumn,sql):
    try:
        #Limitation : in case of a table already exists this method will not drop and recreate the table but would drop the rows and insert.
        #Do not use this method for partitioning an existing non partitioned tabel but manually drop the table instead.
        generic_module_instance.bqclient = initialise_bq_client()
        dataset_ref = generic_module_instance.bqclient.dataset(dataset_id, project)
        table_ref = dataset_ref.table(table_id)
        #table = bigquery.Table(table_ref, schema=schema)
        #change need to be made to pass these parameter values from outside of generic module during each call to this method. 
        query_params = [
                        bigquery.ScalarQueryParameter('start_date', 'STRING', generic_module_instance.start_date),
                        bigquery.ScalarQueryParameter('end_date', 'STRING', generic_module_instance.end_date)
                        #bigquery.ScalarQueryParameter('min_word_count', 'INT64', 250)
                        ]
        query_job_config=get_default_query_job_config(query_parameters=query_params)
        query_job_config.destination = table_ref
        query_job_config.allow_large_results=True#this requires a destination table.
        
        if PartitionColumn != None:
            #table.time_partitioning
            query_job_config.time_partitioning = bigquery.TimePartitioning(
                                                type_=bigquery.TimePartitioningType.DAY,#for partition by date or timestamp columns. Ingestion time partitioning is not used.
                                                field=PartitionColumn#,  # name of column to use for partitioning
                                                #expiration_ms=7776000000  # 90 days  # Commented to never expire partition
                                                )
        #table = generic_module_instance.bqclient.create_table(table)
        if not generic_module_instance.debug_mode:
            load_job = generic_module_instance.bqclient.query(
                                            sql,
                                            #table_ref,
                                            job_config=query_job_config)
        
            load_job.result()
        print('Table {}.{}.{} created.'.format(project,dataset_id,table_id))
    except Exception as excp:
        raise Exception("Error while executing create_bigquery_table_from_queryresult(). Full error description is:"+str(excp)+str(load_job.errors))

#----------------------------------------------------------------------------
def create_bigquery_table_from_dataframe(project,dataset_id,table_id,schema,PartitionColumn,dataframe,write_disposition):
    #notes
    #This requires pyarrow to be installed first.
    
    #while defining schema for date columns alwasy define it as timestamp as below.
    #bigquery.SchemaField('dateofbirth', 'timestamp')
    #and convert the datatype from object to other datetime format which bigquery can understand.
    #df.dateofbirth=df.dateofbirth.astype('datetime64')
    #as of 8-mar-2019 date and datetime column type are not working.
    try:
        #print('Starting Monthly data extraction at:'+str(datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S')))
        generic_module_instance.bqclient = initialise_bq_client()
        
        #extract_job_config = bigquery.ExtractJobConfig() #Other configuration settings like delimiter etc will be passed using this parameter.
        #extract_job_config.destination_format = 'NEWLINE_DELIMITED_JSON'
        #extract_job_config.field_delimiter = '|'
        
        load_job_config = bigquery.LoadJobConfig()
        #load_job_config.source_format = bigquery.SourceFormat.CSV
        load_job_config.schema = schema
        #load_job_config.skip_leading_rows = 0
        #load_job_config.field_delimiter = '|'
        #load_job_config.autodetect = True
        if write_disposition != None:
            load_job_config.write_disposition = write_disposition#'WRITE_TRUNCATE' #WRITE_APPEND
        '''job_config.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field='date',  # name of column to use for partitioning
        expiration_ms=7776000000)  # 90 days'''
        
        #dataframe=pandas.DataFrame(data,columns=['col1','col2','col3'])
        #print(df)
        '''records = [
                {'title': 'The Meaning of Life', 'release_year': 1983},
                {'title': 'Monty Python and the Holy Grail', 'release_year': 1975},
                {'title': 'Life of Brian', 'release_year': 1979},
                {
                    'title': 'And Now for Something Completely Different',
                    'release_year': 1971
                },
            ]
        index = ['Q24980', 'Q25043', 'Q24953', 'Q16403']
        dataframe = pandas.DataFrame(
        records, index=pandas.Index(index, name='wikidata_id'))'''
        dataset_ref = generic_module_instance.bqclient.dataset(dataset_id, project)
        table_ref = dataset_ref.table(table_id)
        #job_id=row.SourceTableName+str(uuid.uuid4())#unique job identifier need to be passed otherwise a non unique jobid is generated and only first extract operation works.
        if not generic_module_instance.debug_mode:
            load_job = generic_module_instance.bqclient.load_table_from_dataframe(dataframe,table_ref,
                                             #job_id=job_id#,
                                             job_config=load_job_config#,
                                             #location=location
                                             )  # API request. Keep location as last parameter and dont put it before job_id else job_id will be generated by GCP and raise error.
            load_job.result()  # Waits for job to complete.
        print('Table saved')
        #print('Exported {}:{}.{} to {}'.format(project, dataset_id, table_id, destination_uri))    
        #print('Monthly data extraction completed at:'+str(datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S')))
    except Exception as excp:
        raise Exception("Error while executing create_bigquery_table_from_dataframe(). Full error description is:"+str(excp))
        
#----------------------------------------------------------------------------        
def create_bigquery_table_from_localfile(project,dataset_id,table_id,schema,PartitionColumn,localfileuri,write_disposition):
    try:
        #Limitation : in case of a table already exists this method will not drop and recreate the table but would drop the rows and insert.
        #Do not use this method for partitioning an existing non partitioned tabel but manually drop the table instead.
        #similar behavirour observed if any column datatype is changed.
        generic_module_instance.bqclient = initialise_bq_client()
        dataset_ref = generic_module_instance.bqclient.dataset(dataset_id, project)
        table_ref = dataset_ref.table(table_id)
        #table = bigquery.Table(table_ref, schema=schema)
        
        if write_disposition != None:
            load_job_config=get_default_load_job_config(write_disposition)
        
        load_job_config.schema=schema
        
        if PartitionColumn != None:
            #table.time_partitioning
            load_job_config.time_partitioning = bigquery.TimePartitioning(
                                                type_=bigquery.TimePartitioningType.DAY,#for partition by date or timestamp columns. Ingestion time partitioning is not used.
                                                field=PartitionColumn#,  # name of column to use for partitioning
                                                #expiration_ms=7776000000  # 90 days  # Commented to never expire partition
                                                )

        #table = generic_module_instance.bqclient.create_table(table)
        with open(localfileuri, 'rb') as source_file:
            if not generic_module_instance.debug_mode:
                load_job = generic_module_instance.bqclient.load_table_from_file(
                    source_file,
                    table_ref,
                    job_config=load_job_config)  # API request
                load_job.result()
        print('Table {}.{}.{} created.'.format(project,dataset_id,table_id))
    except Exception as excp:
        raise Exception("Error while executing create_bigquery_table_from_localfile(). Full error description is:"+str(excp)+str(load_job.errors))

#----------------------------------------------------------------------------
def create_bigquery_table_from_gcsfile(project,dataset_id,table_id,schema,PartitionColumn,gcsfileuri):
    try:
        #Limitation : in case of a table already exists this method will not drop and recreate the table but would drop the rows and insert.
        #Do not use this method for partitioning an existing non partitioned tabel but manually drop the table instead.
        generic_module_instance.bqclient = initialise_bq_client()
        dataset_ref = generic_module_instance.bqclient.dataset(dataset_id, project)
        table_ref = dataset_ref.table(table_id)
        #table = bigquery.Table(table_ref, schema=schema)
        
        load_job_config=get_default_load_job_config()
        load_job_config.schema=schema
        
        if PartitionColumn != None:
            #table.time_partitioning
            load_job_config.time_partitioning = bigquery.TimePartitioning(
                                                type_=bigquery.TimePartitioningType.DAY,#for partition by date or timestamp columns. Ingestion time partitioning is not used.
                                                field=PartitionColumn#,  # name of column to use for partitioning
                                                #expiration_ms=7776000000  # 90 days  # Commented to never expire partition
                                                )
        #table = generic_module_instance.bqclient.create_table(table)
        if not generic_module_instance.debug_mode:
            load_job = generic_module_instance.bqclient.load_table_from_uri(
                                            gcsfileuri,
                                            #dataset_ref.table(table_id),
                                            table_ref,
                                            job_config=load_job_config)
            load_job.result()
        print('Table {}.{}.{} created.'.format(project,dataset_id,table_id))
    except Exception as excp:
        raise Exception("Error while executing create_bigquery_table_from_gcsfile(). Full error description is:"+str(excp)+str(load_job.errors))
        
#----------------------------------------------------------------------------
def create_bigquery_table_from_schema(project,dataset_id,table_id,schema,PartitionColumn):
    try:
        generic_module_instance.bqclient = initialise_bq_client()
        dataset_ref = generic_module_instance.bqclient.dataset(dataset_id, project)
        table_ref = dataset_ref.table(table_id)
        table = bigquery.Table(table_ref, schema=schema)
        if PartitionColumn != None:
            table.time_partitioning = bigquery.TimePartitioning(
                                                type_=bigquery.TimePartitioningType.DAY,#for partition by date or timestamp columns. Ingestion time partitioning is not used.
                                                field=PartitionColumn#,  # name of column to use for partitioning
                                                #expiration_ms=7776000000  # 90 days  # Commented to never expire partition
                                                )
        if not generic_module_instance.debug_mode:
            table = generic_module_instance.bqclient.create_table(table)
        print('Table {}.{}.{} created.'.format(project,dataset_id,table_id))
    except Exception as excp:
        raise Exception("Error while executing create_bigquery_table_from_schema(). Full error description is:"+str(excp))

#----------------------------------------------------------------------------
def delete_bigquery_table(project,dataset_id,table_id):
    try:
        generic_module_instance.bqclient = initialise_bq_client()
        dataset_ref = generic_module_instance.bqclient.dataset(dataset_id, project)
        table_ref = dataset_ref.table(table_id)
        table = bigquery.Table(table_ref)
        if not generic_module_instance.debug_mode:
            table = generic_module_instance.bqclient.delete_table(table)
        print('Table {}.{}.{} deleted.'.format(project,dataset_id,table_id))
    except Exception as excp:
        raise Exception("Error while executing delete_bigquery_table(). Full error description is:"+str(excp))

#-------------------------------------------------------------------------------------
#read excel file and return dataframe
def read_file_in_dataframe(fileurl,filetype,columns=None):
    try:
        if 'xls' in filetype:
            df=pandas.read_excel(fileurl)
        #print('returning following metadata')
        #print(bq_extract_metadata)
        return df
    except Exception as excp:
        raise Exception("Error while executing read_file_in_dataframe(). Full error description is:"+str(excp))
    
        
#-------------------------------------------------------------------------------------
#store last months data to a temporary table and return reference for extraction.
def create_bq_table_backup_for_time_window(project,dataset_id,table_id,PartitionColumn,start_date,end_date):
    try:
        #bqclient = initialise_bq_client()
        query_job_config = bigquery.QueryJobConfig()
        #dataset_ref = bqclient.dataset(dataset_id, project=project)
        sql = "SELECT * \
            FROM `"+project+"."+dataset_id+"."+table_id+"`" \
            +" where "+PartitionColumn +">='" + start_date \
            +"' and " +PartitionColumn +"<'" + end_date +"'"
        print(sql)
        
        bkp_dataset_id='cmbq_workspace' #temporary table will alwasy be writtne to workspace and exported from there.
        bkp_dataset_ref = generic_module_instance.bqclient.dataset(bkp_dataset_id, project=project)
        bkp_table_id=table_id+'_BACKUP'#temporary table name will be different.
        bkp_table_ref = bkp_dataset_ref.table(bkp_table_id)
        query_job_config.destination = bkp_table_ref
        query_job_config.write_disposition = "WRITE_TRUNCATE"
        
        # Start the query, passing in the extra configuration.
        query_job = generic_module_instance.bqclient.query(
        sql,
        # Location must match that of the dataset(s) referenced in the query
        # and of the destination table.
        #location=location,
        job_config=query_job_config)  # API request - starts the query
    
        if not generic_module_instance.debug_mode:#Commit only if debug mode is off
            query_job.result()  # Waits for the query to finish
            print('Query results loaded to table {}'.format(bkp_table_ref.path))
            return(bkp_dataset_id,bkp_table_id)
        else:
            print('Can not create backup tables in debug mode. Returning original table names.')
            return(bkp_dataset_id,bkp_table_id)
    except Exception as excp:
        raise Exception("Error while executing create_bq_table_Backup_for_time_window(). Full error description is:"+str(excp))


#----------------------------------------------------------------------------        
def call_save_dataframe_to_bigquery():
    #data=[['Ratan',3],['John',4]]
    data=[]
    dataframe=pandas.DataFrame(data,columns=['full_name','age'])
    dataframe.set_index('full_name',inplace=True)
    print(dataframe)
    '''schema = [
    bigquery.SchemaField('full_name', 'STRING', mode='REQUIRED'),
    bigquery.SchemaField('age', 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('DOB', 'DATE', mode='NULLABLE'),
    ]'''
    #create_bigquery_table_from_schema('cover-more-data-and-analytics','cmbq_workspace','test_tab',schema,None)
    #create_and_load_bigquery_table_from_gcsfile('cover-more-data-and-analytics','cmbq_workspace','test_tab',schema,'DOB','gs://temp_export_bucket/test_gcs_file.txt')
    #create_and_load_bigquery_table_from_localfile('cover-more-data-and-analytics','cmbq_workspace','test_tab',schema,'DOB','c://temp//test_gcs_file.txt')
    create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cmbq_workspace','test_tab','DOB','select current_timestamp as dob')
    #save_dataframe_to_bigquery('cover-more-data-and-analytics','cmbq_workspace','test_tab',schema,dataframe,None,'WRITE_TRUNCATE');    
    
#----------------------------------------------------------------------------        
def call_execute_sql_bigquery_and_return_result():
    #data=[['Ratan',3],['John',4]]
    #create_bigquery_table_from_schema('cover-more-data-and-analytics','cmbq_workspace','test_tab',schema,None)
    #create_and_load_bigquery_table_from_gcsfile('cover-more-data-and-analytics','cmbq_workspace','test_tab',schema,'DOB','gs://temp_export_bucket/test_gcs_file.txt')
    #create_and_load_bigquery_table_from_localfile('cover-more-data-and-analytics','cmbq_workspace','test_tab',schema,'DOB','c://temp//test_gcs_file.txt')
    #x=execute_sql_bigquery_and_return_result('cover-more-data-and-analytics','cmbq_workspace','select current_timestamp as dob')
    #import sys
    #sys.stdin.encoding='utf-32'
    print(sys.stdin.encoding)
     
    #create_bigquery_table_from_queryresult(project,dataset_id,table_id,PartitionColumn,sql)
    #create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cmbq_workspace','temp',None,str('select case when ''Cura‡ao''=''Cura‡ao'' then ''Curacao'' end'.encode('utf-8').decode('utf-8')))
        
    x=execute_sql_bigquery_and_return_result('cover-more-data-and-analytics','cmbq_workspace',str('select case when ''Cura‡ao''=''Cura‡ao'' then ''Curacao'' end'.encode('utf-8').decode('utf-8')))
    print('debug2')
    print(x)
    print('debug3')
    for row in x:
        print('debug4')
        print(row)
        print('debug5')
    #save_dataframe_to_bigquery('cover-more-data-and-analytics','cmbq_workspace','test_tab',schema,dataframe,None,'WRITE_TRUNCATE');        
    
    
#----------------------------------------------------------------------------        
def call_create_bigquery_table_from_localfile():
    schema = [
    bigquery.SchemaField('SessionId', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('TravellerIdentifier', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('Title', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('FirstName', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('LastName', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('MemberId', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('PrimaryTraveller', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('Age', 'INT64', mode='NULLABLE'),
    bigquery.SchemaField('isPlaceholderAge', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('isPlaceholderAge', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('isPlaceholderAge', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('isPlaceholderAge', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('isPlaceholderAge', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('isPlaceholderAge', 'STRING', mode='NULLABLE')
    ]
    create_bigquery_table_from_localfile('cover-more-data-and-analytics','cmbq_workspace','test_tab',schema,None,'C:\\temp\\travellers_tmp_small.csv')
    
#----------------------------------------------------------------------------        
#Check if rest API call was successful
def check_rest_api_call_success(response,error_message):
  if str(response) != '<Response [200]>':
    print('checkApiCallSuccess failed. API Response is as below:')
    print(response.text)
    print('Reciprocating above exception to main() and stopping execution.')
    raise Exception('Error:'+error_message)    

#----------------------------------------------------------------------------            
#Jira rest API call to fetch the API response.
def call_rest_api_and_return_response(url,auth=None,error_message=None,proxies=None,response_type='JSON'):
    try:
       response = requests.get(url,auth=auth,proxies=proxies)
       check_rest_api_call_success(response,error_message)
       if response_type=='JSON':
           return(response.json())
       elif response_type=='TEXT':
           return(response.text)
       else:
           return(response.json())
    except Exception as excp:
       raise Exception("Error occurred during Rest API data fetch. Full error description is:"+str(excp))
