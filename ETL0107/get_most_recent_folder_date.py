import boto3   
from botocore.exceptions import ClientError

import sys
import os
from pathlib import Path
from configparser import ConfigParser

import re
import datetime
import traceback
from typing import List, Tuple

import shutil

import pandas as pd

def get_most_recent_parent_s3_folder(s3_client, bucket_name, bucket_object_keys) -> str:
    """ finds all the top-level s3 folders/directories that are in the form 'YYYY-MM-DD/' and returns the most recent one.
    This is used to ensure that the most recent files are retrieved regardless of if new folders are uploaded daily, weekly, monthly, etc.
    """
    bucket_object_keys = sorted(bucket_object_keys, key=lambda tup: tup[1], reverse=True)
    directory_date_pairs: list = []
    # note that directory keys have a '/' appended to the end of the string
    
    for ele in bucket_object_keys:
        # find keys that are top level directories with the right name
        # (a string that starts with YYYY-MM-DD pattern, and is followed by only a '/' to indicate it's a directory)
        """
        if re.match(r"^(\d{4}-\d{2}-\d{2}\/)$", ele): 
            try:
                ele_dt = datetime.datetime.strptime(ele, "%Y-%m-%d/") # parses YYYY-MM-DD format into datetime object
                directory_date_pairs.append((ele, ele_dt))
            except ValueError:
                pass
        """
        key = ele[0]
        last_modified = ele[1]
        ele_dir = re.search(r"^(\d{4}-\d{2}-\d{2}\/)", key).group()
        ele_dt = last_modified.date()
        if (ele_dir, ele_dt) not in directory_date_pairs:
            directory_date_pairs.append((ele_dir, ele_dt))           
    # finds the folder key for the most recent date. 
    # the date_dictionary_pairs list is sorted by the ele_dt with the most recent date first
    sorted_dir_date_pairs = sorted(directory_date_pairs, key=lambda tup: tup[1], reverse=True)
    print(sorted_dir_date_pairs)
    parent_s3_folder: str = sorted_dir_date_pairs[0][0] # specifically gets the s3 object key from within the first tuple
    
    return parent_s3_folder
    
# loads the config file into memory
script_dirpath: Path = Path(sys.path[0])
config_path: Path = script_dirpath.joinpath("auth.ini")
    
config = ConfigParser()
config.read(config_path)
auth_params = config["default"]    

bucket_name: str = "cmanalytics-bifiles"
s3_client = boto3.client(
        service_name="s3",
        aws_access_key_id=auth_params["aws_access_key_id"],
        aws_secret_access_key=auth_params["aws_secret_access_key"]
    ) 
 
try:
    # retrieves all the object keys (object keys are the paths to folders and files on s3)
    bucket_object_keys = []
    df_bucket_contents = pd.DataFrame(s3_client.list_objects(Bucket=bucket_name)["Contents"])
    df_bucket_contents.to_csv(r"E:\ETL\Python Scripts\download-impulse-files-from-s3-bucket\bucket_contents.csv",sep="|", index=False)
    contents = s3_client.list_objects(Bucket=bucket_name)["Contents"]
    
    for ele in s3_client.list_objects(Bucket=bucket_name)["Contents"]:
      if "Key" and "LastModified" in ele:
        key = ele["Key"]
        modified = ele["LastModified"]
        bucket_object_keys.append((key, modified))
        
    #print(bucket_object_keys)  
    print(sorted(bucket_object_keys, key=lambda tup: tup[1], reverse=True))
    df_bucket_object_keys = pd.DataFrame(bucket_object_keys)
    df_bucket_object_keys.to_csv(r"E:\ETL\Python Scripts\download-impulse-files-from-s3-bucket\bucket_object_keys.csv",sep="|", index=False)
        # determines which folder on s3 bucket contains the most recent files
    parent_s3_folder: str = get_most_recent_parent_s3_folder(
            s3_client, 
            bucket_name=bucket_name, 
            bucket_object_keys=bucket_object_keys
        )
        
    print(f"Most recent parent s3 folder that is in the correct format:\n\t{parent_s3_folder}")
        
except ClientError as e:
        print("boto3 client encountered an error. This likely indicates that there is an issue with either:")
        print("\t- the authentication details as provided in the auth.ini file")
        print("\t- the ability to connect to AWS from this server (AZSYDDWH02)")
        print(f"Original error message:\n")
        traceback.print_exc() # prints the stack trace of the error
        sys.exit(1)
