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
    url=r"E:\ETL\Data\BigQuery\Out\impulse_cba_archive_sessions_delta.csv"
    df = pandas.read_csv(url,delimiter='|',error_bad_lines=True,header=None,names=['sessiontoken','sessiondata','lastupdatetime'],encoding='latin-1')
    #print(df.sessiondata.head(2).apply(json.loads).apply(pandas.Series).keys())
    #df.info()
    #df.head()
    #pandas.options.display.max_columns = None
    
    #df_impulse_archive_sessions=df["sessiondata"].head(5000).apply(json.loads).apply(pandas.Series)
    df_impulse_archive_sessions=df["sessiondata"].apply(json.loads).apply(pandas.Series)
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
    df_impulse_archive_sessions.replace('null',numpy.nan,inplace=True)#replace all fields containing null to None
    #df_impulse_archive_sessions.info()
    from sqlalchemy import create_engine
    engine = create_engine('mssql+pymssql://@AZSYDDWH02:1433/db-au-stage', pool_recycle=3600, pool_size=5)
    df_impulse_archive_sessions[["Id","Trip","Agent","Quote","Token","Addons","Issuer","Contact","Culture","GigyaID","Payment","IsClosed","Policies","CapRegion","ChannelID","QuoteDate","CampaignID","PromoCodes","Travellers","IsPurchased","OfferQuotes","SavedQuoteID","BusinessUnitID","MatchedOfferID","ChargedRegionID","CreatedDateTime","PartnerMetadata","SelectedOfferID","CoverMoreQuoteId","AppliedPromoCodes","AdditionalPayments","ChargedCountryCode","CoverMoreDiscounts","MatchedConstructID","LastTransactionTime","RelatedSessionToken","MemberPointsDataList","PartnerTransactionID"]].to_sql('impulse_archive_sessions', engine, if_exists='replace',index=False,chunksize=100)
    #df_impulse_archive_sessions.to_sql('pandas_df_impulse_archive_sessions_new', engine, if_exists='replace',chunksize=100)
    
    #############################Process Travellers data###############################################################################
    print('Travellers processing started at:'+str(datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S')))
    #create an empty dataframe to hold travelllers data.
    #df_travellers=pandas.DataFrame(columns=["Identifier","Title","FirstName","LastName","MemberId","IsPrimary","Age","IsPlaceholderAge","DateOfBirth","BinNumber","PersonalIdentifiers","QuoteId","partnerUniqueId"])
    df_travellers=pandas.DataFrame()
    #read travellers data column in a loop and populate travellers dataframe tmp_df_travellers with it one by one for each traveller.
    #filter empty traveller records else read_json will raise error
    for index,row in df_impulse_archive_sessions[df_impulse_archive_sessions.Travellers.notnull()][["Id","Travellers"]].head(10000000).iterrows():
        #if row["Id"]=='5c6b442aa5965c1618c24e6f':#str(row["Travellers"])!='nan' : #and 
        #if row["Id"]=='5c6b42c1a5965a1064ff7a75':#str(row["Travellers"])!='nan' : #and 
            #populate temporary traveller dataframe for all travellers under a quote
            tmp_df_travellers=pandas.read_json(row["Travellers"])#a temporary df is created for multiple rows per quote
            #Populate QuoteID additionally
            tmp_df_travellers["SessionId"]=row["Id"]#this would remain same for all records.
            #some rows of tmp_df_travellers have PersonalIdentifiers value as None converting the same to empty dict to handle consistently.
            #tmp_df_travellers["PersonalIdentifiers"].replace(numpy.nan,'{}',inplace=True)
            #tmp_df_travellers=tmp_df_travellers.applymap(lambda x: {} if pandas.isnull(x) else x)
            
            
            #if tmp_df_travellers["PersonalIdentifiers"].all != numpy.nan and not tmp_df_travellers["PersonalIdentifiers"].empty  :
            #for pi_index,pi_row in tmp_df_travellers[tmp_df_travellers.PersonalIdentifiers.notnull()].iterrows():
    
            #some rows of tmp_df_travellers have PersonalIdentifiers value as NaN and some as empty dict which need to be filtered out.
            #Need to extract partnerUniqueId column value from all other valid rows
            #print('debug 4***'+str(len(tmp_df_travellers[(tmp_df_travellers.PersonalIdentifiers.notnull()) & (tmp_df_travellers.PersonalIdentifiers != {})]["PersonalIdentifiers"])))
            if len(tmp_df_travellers[(tmp_df_travellers.PersonalIdentifiers.notnull()) & (tmp_df_travellers.PersonalIdentifiers != {})]["PersonalIdentifiers"]) > 0 :
                #PersonalIdentifiers is defined as a series but we need to pick the first item which is partnerUniqueId
                tmp_df_travellers.loc[(tmp_df_travellers.PersonalIdentifiers.notnull()) & (tmp_df_travellers.PersonalIdentifiers != {}),"partnerUniqueId"]=tmp_df_travellers[(tmp_df_travellers.PersonalIdentifiers.notnull()) & (tmp_df_travellers.PersonalIdentifiers != {})]["PersonalIdentifiers"][0].get('partnerUniqueId')
            else:
                #add empty column to keep the structure same for merging
                tmp_df_travellers['partnerUniqueId']=numpy.nan
            tmp_df_travellers=tmp_df_travellers[["SessionId","Identifier","Title","FirstName","LastName","MemberId","IsPrimary","Age","IsPlaceholderAge","DateOfBirth","BinNumber","PersonalIdentifiers","partnerUniqueId"]]
            #print('debug1')
            df_travellers=df_travellers.append(tmp_df_travellers)
            #print(df_travellers)        
    
    #just before saving converting datatype of some fields
    #df_travellers.DateOfBirth=df_travellers.DateOfBirth.astype('datetime64',errors='ignore')#there are invalide dates present in format like 55-09-05 00:00:00 which causes problem, so this column will be treated like timestamp.
    df_travellers.PersonalIdentifiers=df_travellers.PersonalIdentifiers.astype(str)#This is a dic object causes problems while saving to database if not converted to string explicitly
    df_travellers.IsPrimary=df_travellers.IsPrimary.astype(str)#need to convert to str explicitly else this is save in numeric format
    df_travellers.IsPlaceholderAge =df_travellers.IsPlaceholderAge.astype(str)#need to convert to str explicitly else this is save in numeric format
    df_travellers["TravellerOrder"]=df_travellers.index#Generate travellr order by index.
    df_travellers.rename(columns={'Identifier':'TravellerIdentifier','FirstName':'firstName','LastName':'lastName','MemberId':'memberId','IsPrimary':'PrimaryTraveller','Age':'age','IsPlaceholderAge':'isPlaceholderAge','DateOfBirth':'dateOfBirth','partnerUniqueId':'PartnerUniqueId'},inplace=True)
    #df_travellers.to_sql('impulse_archive_travellers_python', engine, if_exists='replace',index=False,chunksize=100)
    df_travellers.to_csv(r"E:\ETL\Data\BigQuery\Out\impulse_cba_quote_travellers_delta.csv",sep='|',header=False,index=False)
    
    #############################Process Destination data###############################################################################
    print('Destination processing started at:'+str(datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S')))
    #create an empty dataframe to hold destination data.
    df_quote_destinations=pandas.DataFrame()
    #read Trip data column in a loop and populate destination dataframe tmp_df_quote_destinations with it one by one for each trip.
    #filter empty Trip records else read_json will raise error
    for index,row in df_impulse_archive_sessions[df_impulse_archive_sessions.Trip.notnull()][["Id","Trip"]].head(10000000000).iterrows():
        #if row["Id"]=='5c6b442aa5965c1618c24e6f':#str(row["Travellers"])!='nan' : #and 
        #if row["Id"]=='5c6b42c1a5965a1064ff7a75':#str(row["Travellers"])!='nan' : #and 
            #populate temporary destination dataframe for all destinations under a quote
            tmp_df_quote_destinations=pandas.read_json(row["Trip"])#a temporary df is created for multiple rows per quote
            #Populate QuoteID additionally
            tmp_df_quote_destinations["SessionId"]=row["Id"]#this would remain same for all records.
            
            tmp_df_quote_destinations=tmp_df_quote_destinations[["SessionId","DestinationCountryCodes"]]
            df_quote_destinations=df_quote_destinations.append(tmp_df_quote_destinations)
    
    #just before saving converting datatype of some fields
    df_quote_destinations["DestinationOrder"]=df_quote_destinations.index#destination travellr order by index.
    df_quote_destinations.to_csv(r"E:\ETL\Data\BigQuery\Out\impulse_cba_quote_destinations_delta.csv",sep='|',header=False,index=False)
    #df_travellers.info()
    
    print('Main processing block finished at:'+str(datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S')))
    
    #df_impulse_archive_sessions[["Id","Trip","Agent","Quote","Token","Addons","Issuer","Contact","Culture","GigyaID","Payment","IsClosed","Policies","CapRegion","ChannelID","QuoteDate","CampaignID","PromoCodes","Travellers","IsPurchased","OfferQuotes","SavedQuoteID","BusinessUnitID","MatchedOfferID","ChargedRegionID","CreatedDateTime","PartnerMetadata","SelectedOfferID","CoverMoreQuoteId","AppliedPromoCodes","AdditionalPayments","ChargedCountryCode","CoverMoreDiscounts","MatchedConstructID","LastTransactionTime","RelatedSessionToken","MemberPointsDataList","PartnerTransactionID"]].to_sql('pandas_df_impulse_archive_sessions_new', engine, if_exists='replace',index=False,chunksize=100)
    #df_impulse_archive_sessions
except Exception as excp:
    #generic_module.cnxn.close()#closing cursor, doesnt clear lock. Closing connection is important to release the lock.
    raise Exception("Error while executing parse_impulse_cba_archive_sessions_delta. Full error description is:"+str(excp))
#finally:
    #cursor.close()
    #generic_module.cnxn.close()#closing cursor, doesnt clear lock. Closing connection is important to release the lock.
