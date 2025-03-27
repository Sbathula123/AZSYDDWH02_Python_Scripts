""" Automates the retrieval of new data and updates the PolicyMasterData.xlsx files on the CBA server.

This script handles updating the PMD.xlsx file located on the CBA server (AZSYDDWH02). It utilises the virtual environment "pmd-automation-venv"
to avoid any package conflicts with other projects. Activated with the command: "pmd-automation-venv/Scripts/activate"

Example:
    $ python policymasterdata-automation-cba.py

The major steps of this script are as follows:
    1. Retrieves the new data from the SQL server that needs to be added to PMD.xlsx
    2. Checks if there is any new data to add, else there is no need to continue
    3. Archives the PMD.xlsx file by making a copy in a specific archive directory.
    4. Opens the PMD.xlsx file, appends the updated data to the Product, Outlet and 'Unmapped Alpha' sheets, and saves it.
    
Note on 'Unmapped Alpha Sheet':
    Originally the data in this sheet was being automatically updated by an Excel "external data source". 
    This is Excel's way of creating an OLE-DB connection to a database table, to effectively take tabular data from a database 
        and put it into an excel sheet.
    This script safely replaces that external data connection and removes the external data source connection.
    It had to be removed because openpyxl doesn't support external data sources.
    
This script uses google's python styleguide: https://google.github.io/styleguide/pyguide.html

Update History
    7 Mar 2022 Oscar Gardner
        - creation of working script as well as docstrings
    9 Mar 2022 Oscar Gardner
        - refactored script and moved it onto the AZSYDDWH02's fileserver. Decision was made to have individual jobs per script to keep servers logically separated.
        - improved debugging capability
    20 Apr 2022 Oscar Gardner
        - replaced the SQL query that retrieves CBA Product tab data with a workaround that runs on ULDWH02 instead
        
"""
import shutil
import logging
import sys
import os
from typing import Dict, Optional, Union
from datetime import datetime
from pathlib import Path

import pandas as pd
import openpyxl
from openpyxl.utils.dataframe import dataframe_to_rows
from sqlalchemy import create_engine
from sqlalchemy.sql import text

def load_text_from_file(filepath: Union[Path,str], **kwargs) -> str:
    with open(filepath, **kwargs) as infile:
        return infile.read()
 
 
def retrieve_dataframes_from_server(sql_statement_dir: Path) -> Dict[str,pd.DataFrame]:
    """ Retrieves the new data from the SQL server that needs to be added to PMD.xlsx
    
    Note: All data is cleaned and filled-in by the SQL queries themselves as opposed to performing the data cleaning in python.
    
    Args:
        sql_statement_dir: directory containing all the relevant .SQL files that will then be executed and used to create the dataframes.
    Returns:
        A dictionary containing DataFrames that correspond to new data for the Product, Outlet and 'Unmapped Alpha' sheets within the PMD.xlsx file.
        
    """
    
    dataframe_dict = dict()
    
    # open connection to the server
    conn = create_engine('mssql+pymssql://@AZSYDDWH02:1433', pool_recycle=300, pool_size=5)
    
    # Outlet sheet data
    sql_statement = text(load_text_from_file(sql_statement_dir / "CBA-outlet.sql"))
    result = conn.execute(sql_statement)
    dataframe_dict["cba-outlet"] = pd.DataFrame(result.fetchall(), columns = result.keys())
    
    # Product sheet data (commented out until the source view has been fixed on AZSYDDWH02
    # sql_statement = text(load_text_from_file(sql_statement_dir / "CBA-product.sql"))
    # result = conn.execute(sql_statement)
    # dataframe_dict["cba-product"] = pd.DataFrame(result.fetchall(), columns = result.keys())
    # 
    conn = create_engine('mssql+pymssql://@ULDWH02:1433', pool_recycle=300, pool_size=5)
    
    # Product sheet data, using workaround to a server name issue involving the source view
    sql_statement = load_text_from_file(sql_statement_dir / "fixtest-CBA-product-bad-servername-workaround.sql")
    with open("loadtextsqltest.txt",'w') as infile:
        infile.write(sql_statement)
    print("saved to test file")
    result = conn.execute(sql_statement)
    dataframe_dict["cba-product"] = pd.DataFrame(result.fetchall(), columns = result.keys())
    
    # 'Unmapped Alpha' sheet data
    sql_statement = text(load_text_from_file(sql_statement_dir / "unmapped-alpha.sql"))
    result = conn.execute(sql_statement)
    
    dataframe_dict["unmapped-alpha"] = pd.DataFrame(result.fetchall(), columns = result.keys())
    
    return dataframe_dict
    
def check_if_data_needs_updating(dataframe_dict: Dict[str, pd.DataFrame], pmd_filepath: Path) -> bool:
    """ Checks whether the PMD.xlsx file needs to be updated or not.
    
    The three sheets are handled differently:
    - The SQL query that retrieves data for the Product and Outlet sheets only retrieves new data.
    - The SQL query that retrieves data for 'Unmapped Alpha' sheet retrieves both new and old data. (as of Feb 2022 ~57 rows worth).
    
    
    Args:
        dataframe_dict: dictionary containing dataframes. 
            These dataframes contain the new data that is being appended to the Product, Outlet and 'Unmapped Alpha' sheets of PMD.xlsx.
            Note that it is intended that dataframe_dict is the result from the retrieve_dataframes_from_server function.
            
        pmd_filepath: location of the PMD.xlsx file
        
    Returns:
        bool:
            if True: indicates that an update is necessary
            if False: indicates that an update is NOT necessary
        
    """
    # checks the new data for the Product and Outlet sheets
    if len(dataframe_dict["cba-product"]) > 0 or len(dataframe_dict["cba-outlet"]) > 0:
        return True
    
    # checks the new data for the 'Unmapped Alpha' sheet
    previous_ua_data = pd.read_excel(str(pmd_filepath), sheet_name="Unmapped Alpha")
    new_ua_data = dataframe_dict["unmapped-alpha"]
    
    # quickly checks to see if the ua data is obviously different
    if len(previous_ua_data) != len(new_ua_data):
        return True

    # if the intersection (inner join) between two equal length sets is as long as any one of the sets, it indicates that the two sets are completely identical
    # put more simply: if all the column values in the previous data match the same column in the new data, then no update is necessary
    unique_colname = "OutletAlphaKey"
    intersection_for_unique_column = set(previous_ua_data[unique_colname]).intersection(set(new_ua_data[unique_colname]))
    if len(previous_ua_data) != len(intersection_for_unique_column):
        return True       
    
    # finally returns false as there is no data that needs updating.
    return False
    

# TODO: Is identical for both CM and CBA versions of script. Potentially move to a utils.py file and import from there.   
def archive_pmd_file(pmd_filepath: Path, archive_root_dir: Path):
    """ Creates a copy of the PMD.xlsx file, appends the filename with today's date and saves it in an archive directory. 
    
    The archive file is stored in a subfolder named based on current year.
        For example: if today's date was 24 Feb 2022, the PMD.xlsx file would be saved to "{archive_root_dir}/2022/PolicyMasterData_20220224.xlsx"
    
    Args:
        pmd_filepath: location of the PMD.xlsx file
        
        archive_root_directory: location of where archives of PMD.xlsx are kept.
    Returns
        archive_already_exist_flag
    """
    
    # determines the specific parent directory for the archive file
    current_year_str = str(datetime.now().year)
    archive_parent_dir = archive_root_dir / current_year_str
    
    # creates the parent directory if it doesn't exist yet
    if not archive_parent_dir.exists():
        archive_parent_dir.mkdir(parents=False, exist_ok=True)
        logger.info(f"Created archive directory:\t{archive_parent_dir}")
    
    # creates the suffix to append to the archived file's filename
    archive_date_suffix = datetime.strftime(datetime.now(), "_%Y%m%d")
    
    # creates the full filepath for the new archived copy
    archive_filename = f"{pmd_filepath.stem}{archive_date_suffix}{pmd_filepath.suffix}"
    archive_filepath = archive_parent_dir / archive_filename
    
    # copies the file if it doesn't exist in the destination already
    if not archive_filepath.exists():
        shutil.copy(str(pmd_filepath), str(archive_filepath))
        logger.debug(f"Saved a copy of:\t{pmd_filepath}\nto:\t{archive_filepath}")
    else:
        logger.info(f"Archive file already exists. This indicates that the script has already ran today successfully, and therefore "
                      "any rerun on the same day poses a high risk of data duplication. As such, this script will now close without error.")
        
        sys.exit(0)
        
    return archive_filepath

        
        
def update_pmd_file_with_new_data(pmd_filepath: Path, dataframe_dict: Dict[str,pd.DataFrame], destination_filepath: Optional[Path]=None):
    """ Adds new data to the PMD.xlsx Product, Outlet and 'Unmapped Alpha' sheets and saves it to the destination_filepath.
    
    Changes to each sheet:
        Outlet:
            New data is appended to the sheet
        Product:
            New data is appended to the sheet
        Unmapped Alpha:
            The sheet is deleted, recreated and then new data is appended to the empty sheet.
    
    Args:
        pmd_filepath: location of the PMD.xlsx file
        
        dataframe_dict: dictionary containing dataframes. 
            These dataframes contain the new data that is being appended to the Product, Outlet and 'Unmapped Alpha' sheets of PMD.xlsx.
            Note that it is intended that dataframe_dict is the result from the retrieve_dataframes_from_server function.
            
        destination_filepath: the location to save the updated PMD.xlsx file to.
            When destination_filepath = None (default), this function saves to pmd_filepath, meaning the file is overridden.
            When destination_filepath != None, this function saves to a different location. Useful for testing.
        
    """
    # if no destination specified, overwrite the pmd_filepath with the changes
    if not destination_filepath:
        destination_filepath = pmd_filepath
    
    workbook = openpyxl.load_workbook(str(pmd_filepath))
    
    # updates Outlet tab in-memory
    sheet = workbook["Outlet"]
    for row in dataframe_to_rows(dataframe_dict["cba-outlet"], index=False, header=False):
        sheet.append(row)
    logger.debug(f"Appended {len(dataframe_dict['cba-outlet'])} row(s) to the Outlet sheet.")
    
    # updates Product sheet in-memory
    sheet = workbook["Product"]
    for row in dataframe_to_rows(dataframe_dict["cba-product"], index=False, header=False):
        sheet.append(row)
    logger.debug(f"Appended {len(dataframe_dict['cba-product'])} row(s) to the Product sheet.")
    
    # updates 'Unmapped Alpha' tab in-memory
    sheet_name = "Unmapped Alpha"
    del workbook[sheet_name]
    sheet = workbook.create_sheet(index=-1, title=sheet_name)
    for row in dataframe_to_rows(dataframe_dict["unmapped-alpha"], index=False, header=True):
        sheet.append(row)
        
    logger.debug(f"Deleted then re-added {len(dataframe_dict['unmapped-alpha'])} row(s) to the '{sheet_name}' sheet.")
     
    # saves the in-memory changes to the destination_filepath
    workbook.save(str(destination_filepath))
    logger.info(f"Saved pmd updates to file. Product sheet: {len(dataframe_dict['cba-product'])} new row(s). "
                 f"Outlet sheet: {len(dataframe_dict['cba-outlet'])} new row(s). "
                 f"'Unmapped Alpha': {len(dataframe_dict['unmapped-alpha'])} row(s) re-added. "
                 f"Filename: {destination_filepath}")
    
    
if __name__ == "__main__":
    logging_filepath = Path(r"E:/ETL/Python Scripts/PolicyMasterData-automation-CBA/pmd-automation.log")
    sql_statement_dir = Path(r"E:/ETL/Python Scripts/PolicyMasterData-automation-CBA/SQL Queries")
    DEBUG_FLAG = True # change to True when debugging
    SERVER_NAME = "CBA"
    LOGGING_LEVEL = logging.INFO

    # sets formatting for the logs.
    
    # handlers
    log_file_handler = logging.FileHandler(logging_filepath)
    
    # add formatter to handlers
    log_formatter = logging.Formatter(f'%(asctime)s [{SERVER_NAME} data] %(message)s')
    log_file_handler.setFormatter(log_formatter)
    
    # add handlers to logger
    logger = logging.getLogger("mainlog") # instantiates the Logger object
    logger.addHandler(log_file_handler)
    logger.setLevel(LOGGING_LEVEL)
    
    if DEBUG_FLAG: # debug runs the script on a copy of PolicyMasterData.xlsx, and uses a fake archive as well.
        testing_parent_dir = Path(r"E:\ETL\Python Scripts\PolicyMasterData-Automation-CBA\manual-testing-files\test-etl-data-dir")
        pmd_filepath= testing_parent_dir / Path(r"TestPolicyMasterDataCBA.xlsx") 
        archive_root_dir = testing_parent_dir / Path(r"test-archive-dir")
        destination_filepath = testing_parent_dir / Path(r"pmdout.xlsx")
        
        # uncomment this for debugging to see the logging data printed to the console in addition to the logfile
        #log_stream_handler = logging.StreamHandler(sys.stdout)
        #log_stream_handler.setFormatter(log_formatter)
        #logger.addHandler(log_stream_handler)
        
    else: # when DEBUG_FLAG == False, the script runs on the production file. Note: for CBA there will very rarely be updates
        pmd_filepath = Path(r"E:\ETL\Data\PolicyMasterData.xlsx")
        archive_root_dir = Path(r"E:ETL\Data\Archive\PolicyMasterData")
        destination_filepath=pmd_filepath
    
        
    
    logger.info(f"=== Beginning Script Run ===")
    # Queries the CM server and retrieves the new data
    dataframe_dict: Dict[str,pd.DataFrame] = retrieve_dataframes_from_server(sql_statement_dir)
    
    logger.debug(f"pmd exists? {pmd_filepath.exists()}")
    
    # Checks if there is any new data to add, else there is no need to continue
    # DEBUG 
    if check_if_data_needs_updating(dataframe_dict, pmd_filepath):
        # Archives the PMD.xlsx file by making a copy in a specific archive directory.
        try:
            archive_filepath = archive_pmd_file(
                pmd_filepath=pmd_filepath,
                archive_root_dir = archive_root_dir
            )
        except SystemExit as exc:
            # note: sys.exit raises a SystemExit exception if it isn't running in __main__
            
            # if this exception is raised, it indicates that an archive file already exists for today, therefore the job step succeeded
            sys.exit(0)
        
        # Opens the PMD.xlsx file, appends the updated data to the Product, Outlet and 'Unmapped Alpha' sheets, and saves.
        try:
            update_pmd_file_with_new_data(
                pmd_filepath=pmd_filepath,
                dataframe_dict=dataframe_dict,
                destination_filepath=destination_filepath
            )
            
        # handles edge-case where the archive file was successfully created but the pmd file itself wasn't correctly updated
        except Exception as exc:
            logger.info(f"pmd file update failed. Rolling back changes by removing the archive file at: {archive_filepath}")
            if archive_filepath.exists():
                archive_filepath.unlink()
                
            logger.info(f"Rollback successful. Original error was as follows:", exc_info=True)
            
            # job step failed
            sys.exit(1)
            
        
    else:
        logger.info("No data to add to any sheet. Job step was successful.")