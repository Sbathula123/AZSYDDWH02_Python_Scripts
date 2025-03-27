"""
    Author: 
        Oscar Gardner
    Description: 
        Retrieves files uploaded by Impulse team to AWS s3 bucket and downloads them to AZSYDDWH02 - files are used by ETL107.
        This replaces prior Job logic where mssql server had a direct linked server connection to Impulse, 
            but it was removed due to CBA security requirements.
"""


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


def get_most_recent_parent_s3_folder(s3_client, bucket_name, bucket_object_keys:List[str]) -> str:
    """ finds all the top-level s3 folders/directories that are in the form 'YYYY-MM-DD/' and returns the most recent one.
    This is used to ensure that the most recent files are retrieved regardless of if new folders are uploaded daily, weekly, monthly, etc.
    """
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
        ele_dir = re.search(r"^(\d{4}-\d{2}-\d{2}\/)", ele).group()
        ele_dt = datetime.datetime.strptime(ele_dir, "%Y-%m-%d/") # parses YYYY-MM-DD format into datetime object
        if (ele_dir, ele_dt) not in directory_date_pairs:
            directory_date_pairs.append((ele_dir, ele_dt))           
    # finds the folder key for the most recent date. 
    # the date_dictionary_pairs list is sorted by the ele_dt with the most recent date first
    sorted_dir_date_pairs = sorted(directory_date_pairs, key=lambda tup: tup[1], reverse=True)
    #print(sorted_dir_date_pairs)
    parent_s3_folder: str = sorted_dir_date_pairs[0][0] # specifically gets the s3 object key from within the first tuple
    
    return parent_s3_folder
    
    
def download_files_from_bucket(bucket_name: str, download_output_dirpath: Path, filenames: List[str]): 
    # specifies the output directory to save the downloaded files into
    
    if not download_output_dirpath.exists():
        download_output_dirpath.mkdir()
    
    # loads the config file into memory
    script_dirpath: Path = Path(sys.path[0])
    config_path: Path = script_dirpath.joinpath("auth.ini")
    
    config = ConfigParser()
    config.read(config_path)
    auth_params = config["default"]
  
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html#boto3.session.Session.client
    # loads authentication id and secret from the config file, and create the client
    s3_client = boto3.client(
        service_name="s3",
        aws_access_key_id=auth_params["aws_access_key_id"],
        aws_secret_access_key=auth_params["aws_secret_access_key"]
    )
    
    try:
        # retrieves all the object keys (object keys are the paths to folders and files on s3) 
        bucket_object_keys = []
        for ele in s3_client.list_objects(Bucket=bucket_name)["Contents"]:
            if "Key" in ele:
               bucket_object_keys.append(ele["Key"])
        
        # determines which folder on s3 bucket contains the most recent files
        parent_s3_folder: str = get_most_recent_parent_s3_folder(
            s3_client, 
            bucket_name=bucket_name, 
            bucket_object_keys=bucket_object_keys
        )
        
        print(f"Most recent parent s3 folder that is in the correct format:\n\t{parent_s3_folder}")

        # creates the full paths to where the file objects should be located
        full_s3_object_filepaths = [
            parent_s3_folder + filename
            for filename in filenames
        ]
        
        # checks to see if all the files are in the expected parent s3 folder
        unmatched_object_filepaths = []
        for object_filepath in full_s3_object_filepaths:
            if not object_filepath in bucket_object_keys:
                unmatched_object_filepaths.append(object_filepath)
        
        # if not all the files can be found in the s3 bucket where they should be, exit early
        if len(unmatched_object_filepaths) > 0:
            print(f"Error: one or more expected files were not found within the s3 folder ({parent_s3_folder}):\n")
            print("\n".join(unmatched_object_filepaths))
            print("\nAll expected filepaths:")
            print("\n".join(full_s3_object_filepaths))
            print("\nPerhaps the missing files haven't been uploaded to the folder yet, or have a different name.")
            sys.exit(1)
            
        print(f"Downloading files to {download_output_dirpath}")
        for filename in filenames:
            object_s3_filepath: str = parent_s3_folder + filename
            
            # creates the file locally as boto3 downloads the contents to the file
            with download_output_dirpath.joinpath(filename).open("wb") as outfile:
                s3_client.download_fileobj(bucket_name, object_s3_filepath, outfile)
                print(f"\tDownloaded: {filename}")
    
    except ClientError as e:
        print("boto3 client encountered an error. This likely indicates that there is an issue with either:")
        print("\t- the authentication details as provided in the auth.ini file")
        print("\t- the ability to connect to AWS from this server (AZSYDDWH02)")
        print(f"Original error message:\n")
        traceback.print_exc() # prints the stack trace of the error
        sys.exit(1)


if __name__ == "__main__":
    #BUCKET_NAME: str = "cmanalytics-bifiles-test"#siddhesh
    BUCKET_NAME: str = "cmanalytics-bifiles-test"
    #debug comment out prod locations
    #download_output_dirpath: Path = Path(r"E:/ETL/Python Scripts/download-impulse-files-from-s3-bucket/downloaded-files")
    #final_dirpath: Path = Path(r"E:/ETL/Data/BigQuery/out/")
    
    #debug dirpaths
    download_output_dirpath: Path = Path(r"E:/ETL/Python Scripts/download-impulse-files-from-s3-bucket/download-from-latest-folder-debug")
    final_dirpath: Path = Path(r"E:/ETL/Data/BigQuery/Out/debug-folder/Test")
    
    filenames = [
        "impulse_cba_archive_policies_delta.csv",
        "impulse_cba_archive_sessions_delta.csv",
        "impulse_cba_sessions_delta.csv"
    ]
    
    
    # runs the code that downloads the files to the local download folder.
    download_files_from_bucket(
        bucket_name=BUCKET_NAME,
        download_output_dirpath=download_output_dirpath,
        filenames=filenames
    )
  
    # copies the files to their final destination    
    if not final_dirpath.exists():
        final_dirpath.mkdir()
        
    print(f"\nCopying files to final location: {final_dirpath}")
    for filename in filenames:
        source_filepath: Path = download_output_dirpath.joinpath(filename)
        shutil.copy2(source_filepath, final_dirpath)
        print(f"\tSuccessfully copied {filename}")
    

    
    
    
        
        