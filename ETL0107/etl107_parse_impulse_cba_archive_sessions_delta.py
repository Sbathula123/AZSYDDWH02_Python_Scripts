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
try:
    print("***************************************************************************")
    print('Main processing block started at:'+str(datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S')))
  
    url=r"E:\ETL\Data\BigQuery\Out\debug-folder\Test\impulse_cba_archive_sessions_delta_test.csv"

    #df = pd.read_csv(url,delimiter='|',error_bad_lines=True,header=None,names=['sessiontoken','sessiondata','lastupdatetime'],encoding='utf-16')
    df = pd.read_csv(url,delimiter='|',error_bad_lines=True,header=None,names=['sessiontoken','sessiondata','lastupdatetime'],encoding='utf-16')
    #print(df.sessiondata.head(2).apply(json.loads).apply(pd.Series).keys())
    #df.info()
    #df.head()
    #pd.options.display.max_columns = None
        
    #df_impulse_archive_sessions=df["sessiondata"].head(5000).apply(json.loads).apply(pd.Series)
    df_impulse_archive_sessions=df["sessiondata"].apply(json.loads).apply(pd.Series)
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
    df_impulse_archive_sessions.Trip=df_impulse_archive_sessions.Trip.apply(json.dumps)
    df_impulse_archive_sessions.Agent=df_impulse_archive_sessions.Agent.apply(json.dumps)
    df_impulse_archive_sessions.Quote=df_impulse_archive_sessions.Quote.apply(json.dumps)
    #df_impulse_archive_sessions.Token=df_impulse_archive_sessions.Token.apply(json.dumps)
    df_impulse_archive_sessions.Addons=df_impulse_archive_sessions.Addons.apply(json.dumps)
    df_impulse_archive_sessions.Issuer=df_impulse_archive_sessions.Issuer.apply(json.dumps)
    df_impulse_archive_sessions.Contact=df_impulse_archive_sessions.Contact.apply(json.dumps)
    #df_impulse_archive_sessions.Culture=df_impulse_archive_sessions.Culture.apply(json.dumps)
    df_impulse_archive_sessions.GigyaID=df_impulse_archive_sessions.GigyaID.apply(json.dumps)
    df_impulse_archive_sessions.Payment=df_impulse_archive_sessions.Payment.apply(json.dumps)
    df_impulse_archive_sessions.IsClosed=df_impulse_archive_sessions.IsClosed.apply(json.dumps)
    df_impulse_archive_sessions.Policies=df_impulse_archive_sessions.Policies.apply(json.dumps)
    df_impulse_archive_sessions.CapRegion=df_impulse_archive_sessions.CapRegion.apply(json.dumps)
    df_impulse_archive_sessions.ChannelID=df_impulse_archive_sessions.ChannelID.apply(json.dumps)
    #df_impulse_archive_sessions.QuoteDate=df_impulse_archive_sessions.QuoteDate.apply(json.dumps)
    df_impulse_archive_sessions.QuoteDate=df_impulse_archive_sessions.QuoteDate.astype('datetime64')
    df_impulse_archive_sessions.CampaignID=df_impulse_archive_sessions.CampaignID.apply(json.dumps)
    df_impulse_archive_sessions.PromoCodes=df_impulse_archive_sessions.PromoCodes.apply(json.dumps)
    df_impulse_archive_sessions.Travellers=df_impulse_archive_sessions.Travellers.apply(json.dumps)
    df_impulse_archive_sessions.IsPurchased=df_impulse_archive_sessions.IsPurchased.apply(json.dumps)
    df_impulse_archive_sessions.OfferQuotes=df_impulse_archive_sessions.OfferQuotes.apply(json.dumps)
    #df_impulse_archive_sessions.SavedQuoteID=df_impulse_archive_sessions.SavedQuoteID.apply(json.dumps)
    df_impulse_archive_sessions.BusinessUnitID=df_impulse_archive_sessions.BusinessUnitID.apply(json.dumps)
    df_impulse_archive_sessions.MatchedOfferID=df_impulse_archive_sessions.MatchedOfferID.apply(json.dumps)
    df_impulse_archive_sessions.ChargedRegionID=df_impulse_archive_sessions.ChargedRegionID.apply(json.dumps)
    #df_impulse_archive_sessions.CreatedDateTime=df_impulse_archive_sessions.CreatedDateTime.apply(json.dumps)
    df_impulse_archive_sessions.CreatedDateTime=df_impulse_archive_sessions.CreatedDateTime.astype('datetime64')
    df_impulse_archive_sessions.PartnerMetadata=df_impulse_archive_sessions.PartnerMetadata.apply(json.dumps)
    df_impulse_archive_sessions.SelectedOfferID=df_impulse_archive_sessions.SelectedOfferID.apply(json.dumps)
    df_impulse_archive_sessions.CoverMoreQuoteId=df_impulse_archive_sessions.CoverMoreQuoteId.apply(json.dumps)
    df_impulse_archive_sessions.AppliedPromoCodes=df_impulse_archive_sessions.AppliedPromoCodes.apply(json.dumps)
    df_impulse_archive_sessions.AdditionalPayments=df_impulse_archive_sessions.AdditionalPayments.apply(json.dumps)
    #df_impulse_archive_sessions.ChargedCountryCode=df_impulse_archive_sessions.ChargedCountryCode.apply(json.dumps)
    df_impulse_archive_sessions.CoverMoreDiscounts=df_impulse_archive_sessions.CoverMoreDiscounts.apply(json.dumps)
    df_impulse_archive_sessions.MatchedConstructID=df_impulse_archive_sessions.MatchedConstructID.apply(json.dumps)
    #df_impulse_archive_sessions.LastTransactionTime=df_impulse_archive_sessions.LastTransactionTime.apply(json.dumps)
    df_impulse_archive_sessions.LastTransactionTime=df_impulse_archive_sessions.LastTransactionTime.astype('datetime64')
    df_impulse_archive_sessions.RelatedSessionToken=df_impulse_archive_sessions.RelatedSessionToken.apply(json.dumps)
    df_impulse_archive_sessions.MemberPointsDataList=df_impulse_archive_sessions.MemberPointsDataList.apply(json.dumps)
    #df_impulse_archive_sessions.PartnerTransactionID=df_impulse_archive_sessions.PartnerTransactionID.apply(json.dumps)
    df_impulse_archive_sessions.replace('null',np.nan,inplace=True)#replace all fields containing null to None
    #df_impulse_archive_sessions.info()  
    from sqlalchemy import create_engine
    engine = create_engine('mssql+pymssql://@AZSYDDWH02:1433/db-au-stage', pool_recycle=3600, pool_size=5)
    df_impulse_archive_sessions[["Id","Trip","Agent","Quote","Token","Addons","Issuer","Contact","Culture","GigyaID","Payment","IsClosed","Policies","CapRegion","ChannelID","QuoteDate","CampaignID","PromoCodes","Travellers","IsPurchased","OfferQuotes","SavedQuoteID","BusinessUnitID","MatchedOfferID","ChargedRegionID","CreatedDateTime","PartnerMetadata","SelectedOfferID","CoverMoreQuoteId","AppliedPromoCodes","AdditionalPayments","ChargedCountryCode","CoverMoreDiscounts","MatchedConstructID","LastTransactionTime","RelatedSessionToken","MemberPointsDataList","PartnerTransactionID"]].to_sql('impulse_archive_sessions_test', engine, if_exists='replace',index=False,chunksize=100)
    #df_impulse_archive_sessions.to_sql('pandas_df_impulse_archive_sessions_new', engine, if_exists='replace',chunksize=100)
    
    #############################Process Travellers data###############################################################################
    print('Travellers processing started at:'+str(datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S')))
    # create an empty dataframe to hold travelllers data.
    # df_travellers=pd.DataFrame(columns=["Identifier","Title","FirstName","LastName","MemberId","IsPrimary","Age","IsPlaceholderAge","DateOfBirth","BinNumber","PersonalIdentifiers","QuoteId","partnerUniqueId"])
    # df_travellers=pd.DataFrame()
    # start of optimisation
    travellers_list = []  # list of dicts
    for index, row in df_impulse_archive_sessions[df_impulse_archive_sessions.Travellers.notnull()][["Id", "Travellers"]].iterrows():
        # each row when the Travellers column is deserialised becomes multiple rows - some columns get set with constant values for all the deserialised rows
        travellers_row_list = json.loads(row["Travellers"])
        for i, ele_dict in enumerate(travellers_row_list.copy()):
            # populates QuoteID additionally
            travellers_row_list[i]["SessionId"] = row["Id"]
            # create a new key / column to track the order of passengers within each grouping
            travellers_row_list[i]["TravellerOrder"] = i
            # if travellers_row_list[i]["SessionId"] == "63231fe3a5965c2304edea2d": print(travellers_row_list[i])
            # retrieves the partnerUniqueId from a nested dict if it exists, otherwise the default value is np.nan
            try:
                travellers_row_list[i]["partnerUniqueId"] = ele_dict[
                    "PersonalIdentifiers"
                ]["partnerUniqueId"]
            except KeyError:
                # either ele_dict["PersonalIdentifiers"] key doesn't exist or ele_dict["PersonalIdentifiers"]["partnerUniqueId"] doesn't exist
                travellers_row_list[i]["partnerUniqueId"] = np.nan
            except TypeError:
                # the ele_dict["PersonalIdentifiers"] key does exist but the value is None,
                # meaning there is no ele_dict["PersonalIdentifiers"]["partnerUniqueId"] key to be retrieved
                travellers_row_list[i]["partnerUniqueId"] = np.nan

        # adds the dictionaries containing traveller information to a larger list inplace
        travellers_list.extend(travellers_row_list)

    # specifying a list of columns to later ensure that the dataframe doesn't have any extra columns and that the columns are ordered correctly
    required_traveller_columns = ["SessionId","Identifier","Title","FirstName","LastName","MemberId","IsPrimary","Age","IsPlaceholderAge","DateOfBirth","BinNumber","PersonalIdentifiers","partnerUniqueId","TravellerOrder"]
    # use a classmethod to create a DataFrame, loading in the rows from the list of dictionaries
    df_travellers = pd.DataFrame.from_dict(travellers_list)[required_traveller_columns]
    # end of optimisation

    # old code relied on columns having specific data types so some columns are cast and cleaned to match up.
    df_travellers.PersonalIdentifiers.fillna(np.nan, inplace=True)
    df_travellers.PersonalIdentifiers = df_travellers.PersonalIdentifiers.astype(str)  # This is a dict object causes problems while saving to database if not converted to string explicitly
    df_travellers.IsPrimary = df_travellers.IsPrimary.astype(str)  # need to convert to str explicitly else this is save in numeric format
    df_travellers.IsPlaceholderAge = df_travellers.IsPlaceholderAge.astype(str)  # need to convert to str explicitly else this is save in numeric format

    # standardises the naming for future usage
    df_travellers.rename(
        columns={
            "Identifier": "TravellerIdentifier",
            "FirstName": "firstName",
            "LastName": "lastName",
            "MemberId": "memberId",
            "IsPrimary": "PrimaryTraveller",
            "Age": "age",
            "IsPlaceholderAge": "isPlaceholderAge",
            "DateOfBirth": "dateOfBirth",
            "partnerUniqueId": "PartnerUniqueId"},inplace=True)
    # df_travellers.info()
    
    #df_travellers.to_sql('impulse_archive_travellers_python', engine, if_exists='replace',index=False,chunksize=100)
    df_travellers.to_csv(r"E:\ETL\Data\BigQuery\Out\debug-folder\Test\impulse_cba_quote_travellers_delta_test.csv",sep='|',header=False,index=False)
    
    #############################Process Destination data###############################################################################
    print("Destination processing started at:"+ str(datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d %H:%M:%S")))
    # create an empty dataframe to hold destination data.
    # df_quote_destinations=pd.DataFrame()
    # start of optimisation
    quote_dest_list = []  # list of dicts
    for index, row in df_impulse_archive_sessions[df_impulse_archive_sessions.Trip.notnull()][["Id", "Trip"]].iterrows():
        # get Trip dict for each session, DestinationCountryCodes is multi-value key
        quote_dest_col = json.loads(row["Trip"])["DestinationCountryCodes"]
        for i, ele_dest in enumerate(quote_dest_col):
            # Populate QuoteID additionally
            quote_dest_row_list = {}
            quote_dest_row_list["DestinationCountryCodes"] = ele_dest
            quote_dest_row_list["DestinationOrder"] = i
            quote_dest_row_list["SessionId"] = row["Id"]
            # adds the dictionaries containing traveller information to a larger list inplace
            quote_dest_list.append(quote_dest_row_list)

    required_quote_dest_columns = ["SessionId","DestinationCountryCodes","DestinationOrder"]
    df_quote_destinations = pd.DataFrame.from_dict(quote_dest_list)[required_quote_dest_columns]
    # end of optimisation
    # just before saving converting datatype of some fields
    # df_quote_destinations["DestinationOrder"]=df_quote_destinations.index#destination travellr order by index. 

    df_quote_destinations.to_csv(r"E:\ETL\Data\BigQuery\Out\debug-folder\Test\impulse_cba_quote_destinations_delta_test.csv",sep='|',header=False,index=False)

    # df_travellers.info()

    print("Main processing block finished at:"+ str(datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d %H:%M:%S")))

    # df_impulse_archive_sessions[["Id","Trip","Agent","Quote","Token","Addons","Issuer","Contact","Culture","GigyaID","Payment","IsClosed","Policies","CapRegion","ChannelID","QuoteDate","CampaignID","PromoCodes","Travellers","IsPurchased","OfferQuotes","SavedQuoteID","BusinessUnitID","MatchedOfferID","ChargedRegionID","CreatedDateTime","PartnerMetadata","SelectedOfferID","CoverMoreQuoteId","AppliedPromoCodes","AdditionalPayments","ChargedCountryCode","CoverMoreDiscounts","MatchedConstructID","LastTransactionTime","RelatedSessionToken","MemberPointsDataList","PartnerTransactionID"]].to_sql('pandas_df_impulse_archive_sessions_new', engine, if_exists='replace',index=False,chunksize=100)
    # df_impulse_archive_sessions
except Exception as excp:
    # generic_module.cnxn.close()#closing cursor, doesnt clear lock. Closing connection is important to release the lock.
    raise Exception(
        "Error while executing parse_impulse_cba_archive_sessions_delta. Full error description is:"
        + str(excp)
    )
# finally:
# cursor.close()
# generic_module.cnxn.close()#closing cursor, doesnt clear lock. Closing connection is important to release the lock.
