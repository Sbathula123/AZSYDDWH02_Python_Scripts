"""
Author : Ratnesh
Dept   : Data Analytics & Innovation, Covermore Group
Date   : 2018-02-01
Usage  : "<filename>.py <run_mode> <start_date> <end_date>"
          "python <filename>.py interval=last3days"
          "python <filename>.py start_date=2018-12-01 end_date=2019-01-01" #this will extract data for the month of December-2018
Params : Parameter 1 - interval - Accepted values are "last3days"/"lastmonth" (do not pass start_date and end_date if passing interval.)
         Parameter 2 - Extraction Start Date in YYYY-MM-DD format. This date is inclusive.
         Parameter 3 - Extraction End Date in YYYY-MM-DD format. This date is exclusive.
         
Desc   : This process extracts the monthly data from google Bigquery tables and downloads the same to a local filesystem location.
         The processing is based on configuration defined in metadata excel BQExtractionMetaData.xlsx
         There are two flags to control extraction and download separately which are isActive (for extraction) and DownloadFlag (for downloading).
         Download flag need to be set carefully after considering cost implications and with prior approval. Download opetation involves significant cost 
         and is done in a controlled manner.

Configuration requirements : 1) Deployment of this script.
                             2) Scheduling using agent.
                             3) 


DB Changes    

    
Change History:
20190201        RATNESH      


Pending Bits: 
Create batch start process.
create running process.
repeat process if fails once.
do a test by putting all sort of special characters in a functions excel sheet and run using that. Put jobs,functions, dependencies tab in it.
parameterise dates.


test number of children adult age and gross premium.


"""
#import pyodbc 
import datetime
import sys
import time
#import voice_analytics_generic_module
import os
#import uuid
from sqlalchemy import create_engine

#enable following line if running on local workstation.
#sys.path.append(r'\\azsyddwh02\ETL\Python Scripts')
sys.path.append(r'\\aust.covermore.com.au\user_data\NorthSydney_Users\ratneshs\Home\Projects\generic\\')
#import environment_settings
import generic_module
generic_module.set_module(os.path.basename(__file__))

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


    if not generic_module.is_step_complete(generic_module.module_name,1):
        cursor_ref=generic_module.create_sql_cursor('azsyddwh02','db-au-stage')
        cursor_ref.execute("exec [db-au-stage]..syssp_createbatch @SubjectArea = 'CDG Quote Bigquery',@StartDate = '"+generic_module.start_date+"',@EndDate = '"+generic_module.end_date+"'");
        cursor_ref.commit()
        generic_module.mark_step_complete(generic_module.module_name,1)
        
    #create table ETL115_etl_penQuoteSummaryPenguin
    vsql="""select 
        cast(q.CreateDate as date) QuoteDate,
        1 QuoteSource,
        q.CountryKey,
        q.CompanyKey,
        q.OutletAlphaKey,		
        q.StoreCode,
        u.UserKey,
        cu.CRMUserKey,
        q.SaveStep,
        q.CurrencyCode,
        q.Area,
        q.Destination,
        q.PurchasePath,
        --convert(nvarchar(200),isnull(q.CountryKey,'') + '-' + isnull(q.CompanyKey,'') + '' + convert(varchar,isnull(q.DomainID,0)) + '-' + isnull(q.ProductCode,'') + '-' + isnull(q.ProductName,'') + '-' + isnull(q.ProductDisplayName,'') + '-' + isnull(q.PlanName,'')) ProductKey,
        concat(ifnull(q.CountryKey,''),'-',ifnull(q.CompanyKey,''),cast(ifnull(q.DomainID,0) as string),'-',ifnull(q.ProductCode,''),'-',ifnull(q.ProductName,''),'-',ifnull(q.ProductDisplayName,''),'-',ifnull(q.PlanName,'')) ProductKey,
        q.ProductCode,
        q.ProductName,
        q.PlanCode,
        q.PlanName,
        q.PlanType,
        q.MaxDuration,
        q.Duration,
        case
--            when date_diff(cast(q.CreateDate as date), cast(q.DepartureDate as date),day) < 0 then 0
            when datetime_diff(q.DepartureDate,cast(q.CreateDate as datetime),day) < 0 then 0
--            else date_diff(cast(q.CreateDate as date), cast(q.DepartureDate as date),day)
            else datetime_diff(q.DepartureDate,cast(q.CreateDate as datetime),day)
        end LeadTime,
        q.Excess,
        qcmp.CompetitorName,
        case
            when qcmp.CompetitorPrice is null or q.QuotedPrice is null then 0
            else round((q.QuotedPrice - qcmp.CompetitorPrice) / 50.0, 0) * 50
        end CompetitorGap,
        qpc.PrimaryCustomerAge,
        qpc.PrimaryCustomerSuburb,
        qpc.PrimaryCustomerState,
        qpc.YoungestAge,
        qpc.OldestAge,
        sum(ifnull(q.NumberOfChildren, 0)) NumberOfChildren,
        sum(ifnull(q.NumberOfAdults, 0)) NumberOfAdults,
        sum(ifnull(q.NumberOfPersons, 0)) NumberOfPersons,
        sum(ifnull(q.QuotedPrice, 0)) QuotedPrice,
        count(distinct q.SessionID) QuoteSessionCount,
        count(case when q.ParentQuoteID is null then q.QuoteKey else null end) QuoteCount,
        sum(
            case
                when q.ParentQuoteID is null and q.QuotedPrice is not null then 1
                else 0
            end 
        ) QuoteWithPriceCount,
        sum(case when q.ParentQuoteID is null and ifnull(q.IsSaved,0) = 1 then 1 else 0 end) SavedQuoteCount,
        sum(
            case
                when ifnull(q.PolicyKey, '') <> '' then 1
                else 0
            end 
        ) ConvertedCount,
        sum(case when q.ParentQuoteID is null and ifnull(q.IsExpo,0) = 1 then 1 else 0 end) ExpoQuoteCount,
        sum(case when q.ParentQuoteID is null and ifnull(q.IsAgentSpecial, 0) = 1 then 1 else 0 end) AgentSpecialQuoteCount,
        sum(
            case
                when q.ParentQuoteID is null and ifnull(q.PromoCode, '') <> '' then 1
                else 0
            end 
        ) PromoQuoteCount,
        sum(case when q.ParentQuoteID is null and ifnull(q.IsUpSell,0) = 1 then 1 else 0 end) UpsellQuoteCount,
        sum(case when q.ParentQuoteID is null and ifnull(q.IsPriceBeat, 0) = 1 then 1 else 0 end) PriceBeatQuoteCount,
        sum(
            case
                when q.ParentQuoteID is null and q.PreviousPolicyNumber is not null then 1
                else 0
            end 
        ) QuoteRenewalCount,
        sum(case when q.ParentQuoteID is null and ifnull(qa.HasCancellation, 0) = 1 then 1 else 0 end) CancellationQuoteCount,
        sum(case when q.ParentQuoteID is null and ifnull(qa.HasLuggage, 0) = 1 then 1 else 0 end) LuggageQuoteCount,
        sum(case when q.ParentQuoteID is null and ifnull(qa.HasMotorcycle, 0) = 1 then 1 else 0 end) MotorcycleQuoteCount,
        sum(case when q.ParentQuoteID is null and ifnull(qa.HasWinter, 0) = 1 then 1 else 0 end) WinterQuoteCount,
        sum(case when q.ParentQuoteID is null and ifnull(qpc.HasEMC, 0) = 1 then 1 else 0 end) EMCQuoteCount        
    from
        cbabq_prod.penQuote q
     left outer join cbabq_prod.penQuoteCompetitor qcmp
            on qcmp.QuoteKey = q.QuoteCountryKey
        left outer join 
           (
            select 
                qc.QuoteCountryKey,
                max(
                    case
                        when IsPrimary = 1 then qc.Age 
                        else 0
                    end 
                ) PrimaryCustomerAge,
                max(c.Town) PrimaryCustomerSuburb,
                max(c.State) PrimaryCustomerState,
                max(0 + qc.HasEMC) HasEMC,
                min(qc.Age) YoungestAge,
                max(qc.Age) OldestAge
            from
                cbabq_prod.penQuoteCustomer qc
                left join cbabq_prod.penCustomer c on
                    c.CustomerKey = qc.CustomerKey and
                    qc.IsPrimary = 1
                    group by qc.QuoteCountryKey
        ) qpc
        on      qpc.QuoteCountryKey = q.QuoteCountryKey
        left outer join 
        (select QuoteCountryKey,
                max(
                    case
                        when AddOnGroup = 'Cancellation' then 1
                        else 0
                    end
                ) HasCancellation,
                max(
                    case
                        when AddOnGroup = 'Luggage' then 1
                        else 0
                    end
                ) HasLuggage,
                max(
                    case
                        when AddOnGroup = 'Motorcycle' then 1
                        else 0
                    end
                ) HasMotorcycle,
                max(
                    case
                        when AddOnGroup = 'Winter Sport' then 1
                        else 0
                    end
                ) HasWinter
            from cbabq_prod.penQuoteAddOn
            group by QuoteCountryKey
            ) qa
            on qa.QuoteCountryKey = q.QuoteCountryKey
        left outer join
                (select distinct OutletAlphaKey,Login,UserKey from cbabq_prod.penUser) u
         on     (u.Login = q.UserName and upper(u.OutletAlphaKey) = upper(q.OutletAlphaKey))
        left outer join (select distinct UserName,CRMUserKey from (select UserName,CRMUserKey,row_number() over (partition by UserName order by CRMUSERKEY) row_number from cbabq_prod.penCRMUser) where row_number=1) cu
            on ifnull(cu.UserName,'xyz') = ifnull(q.CRMUserName,'xyz')
    where
        --q.CreateDate >= cast((select batch_date from cbabq_prod.Batch_Run_Status where  Subject_Area='CDG Quote Bigquery' and Batch_Status='Running') as timestamp) and
        q.CreateDate >= cast(@start_date as timestamp) and
        --q.CreateDate <  cast(date_add((select batch_to_date from cbabq_prod.Batch_Run_Status where  Subject_Area='CDG Quote Bigquery' and Batch_Status='Running'),interval 1 day) as timestamp) and
        q.CreateDate <=  cast(@end_date as timestamp) and
        q.QuoteKey not like 'AU-CM-%' and        --excludes duplicate quotekey records
        q.QuoteKey not like 'AU-TIP-%'
    group by
        cast(q.CreateDate as date),
        q.CountryKey,
        q.CompanyKey,
        q.OutletAlphaKey,
        q.StoreCode,
        u.UserKey,
        cu.CRMUserKey,
        q.SaveStep,
        q.CurrencyCode,
        q.Area,
        q.Destination,
        q.PurchasePath,
        --isnull(q.CountryKey,'') + '-' + isnull(q.CompanyKey,'') + '' + convert(varchar,isnull(q.DomainID,0)) + '-' + isnull(q.ProductCode,'') + '-' + isnull(q.ProductName,'') + '-' + isnull(q.ProductDisplayName,'') + '-' + isnull(q.PlanName,''),
        concat(ifnull(q.CountryKey,''),'-',ifnull(q.CompanyKey,''),cast(ifnull(q.DomainID,0) as string),'-',ifnull(q.ProductCode,''),'-',ifnull(q.ProductName,''),'-',ifnull(q.ProductDisplayName,''),'-',ifnull(q.PlanName,'')),
        q.ProductCode,
        q.ProductName,
        q.PlanCode,
        q.PlanName,
        q.PlanType,
        q.MaxDuration,
        q.Duration,
        case
--            when date_diff(cast(q.CreateDate as date), cast(q.DepartureDate as date),day) < 0 then 0
            when datetime_diff(q.DepartureDate,cast(q.CreateDate as datetime),day) < 0 then 0
--            else date_diff(cast(q.CreateDate as date), cast(q.DepartureDate as date),day)
            else datetime_diff(q.DepartureDate,cast(q.CreateDate as datetime),day)
        end,
        q.Excess,
        qcmp.CompetitorName,
        case
            when qcmp.CompetitorPrice is null or q.QuotedPrice is null then 0
            else round((q.QuotedPrice - qcmp.CompetitorPrice) / 50.0, 0) * 50
        end,
        qpc.PrimaryCustomerAge,
        qpc.PrimaryCustomerSuburb,
        qpc.PrimaryCustomerState,
        qpc.YoungestAge,
        qpc.OldestAge"""
    #print(vsql)
    if not generic_module.is_step_complete(generic_module.module_name,2):
        generic_module.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cbabq_stage','ETL115_etl_penQuoteSummaryPenguin',None,vsql)
        generic_module.mark_step_complete(generic_module.module_name,2)
    
    

    #create table ETL115_etl_penQuoteSummary
    vsql="""    select 
        QuoteDate,
        QuoteSource,
        CountryKey,
        CompanyKey,
        OutletAlphaKey,
        StoreCode,
        UserKey,
        CRMUserKey,
        SaveStep,
        CurrencyCode,
        Area,
        Destination,
        PurchasePath,
        ProductKey,
        ProductCode,
        ProductName,
        PlanCode,
        PlanName,
        PlanType,
        MaxDuration,
        Duration,
        LeadTime,
        Excess,
        CompetitorName,
        CompetitorGap,
        PrimaryCustomerAge,
        PrimaryCustomerSuburb,
        PrimaryCustomerState,
        YoungestAge,
        OldestAge,
        NumberOfChildren,
        NumberOfAdults,
        NumberOfPersons,
        QuotedPrice,
        QuoteSessionCount,
        QuoteCount,
        QuoteWithPriceCount,
        SavedQuoteCount,
        ConvertedCount,
        ExpoQuoteCount,
        AgentSpecialQuoteCount,
        PromoQuoteCount,
        UpsellQuoteCount,
        PriceBeatQuoteCount,
        QuoteRenewalCount,
        CancellationQuoteCount,
        LuggageQuoteCount,
        MotorcycleQuoteCount,
        WinterQuoteCount,
        EMCQuoteCount
    from
        cbabq_stage.ETL115_etl_penQuoteSummaryPenguin"""
    #print(vsql)
    if not generic_module.is_step_complete(generic_module.module_name,3):
        generic_module.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cbabq_stage','ETL115_etl_penQuoteSummary',None,vsql)
        generic_module.mark_step_complete(generic_module.module_name,3)
    
    
    #impulse 1 applicable
    #impulse 2 NOT applicable
    
    #create table ETL115_etl_impulse2Quote.
    vsql="""select
        '' GroupIndex,
        cast(q.quoteDateUTC as date) QuoteDate,
        q.outletAlphakey OutletAlphaKey,
        'AUD' CurrencyCode,--currency code will always be AUD for CBA quotes.
        ifnull(trim(r.Region), '') Area,
        ifnull(trim(cd.DestinationCountry), '') Destination,
        '' as PurchasePath,
        ifnull(trim(qp.ProductKey), '') ProductKey,
        ifnull(trim(cp.ProductCode), '') ProductCode,
        ifnull(trim(cp.Product), '') ProductName,
        ifnull(trim(cp.PlanCode), '') PlanCode,
         cast(q.quoteDuration as int64) Duration,
        case
            when ifnull(timestamp_diff(q.tripStartDate,q.createdDateTime,day), 0) < 0 then 0
            else ifnull(timestamp_diff(q.tripStartDate,q.createdDateTime,day), 0)
        end LeadTime,
        icqt.PrimaryCustomerAge,
        icqt.YoungestAge,
        icqt.OldestAge,
        ifnull(ics.NumChildren, 0) --* q.IsPrimaryTraveller
        NumberOfChildren,
        ifnull(ics.NumAdults, 0) --* q.IsPrimaryTraveller 
        NumberOfAdults,
        ifnull(q.quoteGross, 0) --* q.IsPrimaryTraveller 
        QuotedPrice,
        --count(distinct q.savedQuoteID) as  QuoteCount,--added on 2019-03-28 for fix. code need to chage as per impulse 2
        count(distinct q.sessionID) as  QuoteCount,--changed as advised by Linus on 2019-04-02
		count(distinct q.sessionID) as  QuoteSessionCount,--added on 2019-03-28 for fix. code need to chage as per impulse 2
		case when q.quoteGross is not null then 1
			 else 0
		end as QuoteWithPriceCount,
        cast(q.CampaignID as string) CampaignSessionID,
        Null ImpressionID,
        q.SavedQuoteID,--this will be used for quote count
        case         when q.quoteGross is not null then cast(q.savedQuoteID as string)            else null        end QuoteWithPriceID,
        case when q.Policies is Null  then null else replace(replace(policies,'["',''),'"]','') end ConvertedQuoteID,
        case when q.Policies is Null  then 0 else 1 end ConvertedQuoteCount,
        ifnull(cast(q.Domain as string),'AU') Domain,
        cbu.BusinessUnit,
        cc.Channel Channel,
        q.outletAlphakey AlphaCode
        from
        --cmbq_prod.cdgQuote q
        cbabq_prod.impulse_cba_quotes q
        left outer join 
        cmbq_prod.cdgRegion r
        on cast(q.RegionID as int64) = r.RegionID
        left outer join
        cbabq_prod.impulse_cba_quote_destinations icqd
        on q.sessionid=icqd.sessionid
         and icqd.destinationOrder=0--added on 2019-03-28 to keep the granularity at session level
        left outer join
        cmbq_prod.cdgDestination cd
        on icqd.DestinationcountryCode=cd.DestinationCountryCode
        left outer join
        cmbq_prod.usrCDGQuoteProduct qp
        on  qp.ProductID = q.QuoteProductID
        left outer join
          cmbq_prod.cdgProduct cp
          on q.quoteProductID = cp.ProductID
          left outer join
          cbabq_prod.impulse_cba_sessions ics
          on q.sessiontoken=ics.sessiontoken
          left outer join
          --cbabq_prod.impulse_cba_quote_travellers icqt
          (select sessionid,max(case when PrimaryTraveller='True' then Age end) PrimaryCustomerAge,
            min(age) YoungestAge,
            max(age) OldestAge
            from cbabq_prod.impulse_cba_quote_travellers
            group by sessionid) icqt
          on q.sessionid=icqt.sessionid
          left outer join
          cmbq_prod.cdgBusinessUnit cbu
          on cast(q.businessUnitID as int64)=cbu.BusinessUnitID
          left outer join
          cmbq_prod.cdgChannel cc
          on cast(q.channelid as int64)=cc.channelid
            where
                q.quoteDateUTC >= cast(@start_Date as timestamp) and
                q.quoteDateUTC <=  cast(@end_Date as timestamp) --and
                --q.isDeleted = 0
		group by cast(q.quoteDateUTC as date),
        q.outletAlphakey,
        ifnull(trim(r.Region), ''),
        ifnull(trim(cd.DestinationCountry), ''),
        ifnull(trim(qp.ProductKey), ''),
        ifnull(trim(cp.ProductCode), ''),
        ifnull(trim(cp.Product), ''),
        ifnull(trim(cp.PlanCode), ''),
         q.quoteDuration ,--Duration,
        case
            when ifnull(timestamp_diff(q.tripStartDate,q.createdDateTime,day), 0) < 0 then 0
            else ifnull(timestamp_diff(q.tripStartDate,q.createdDateTime,day), 0)
        end,
        icqt.PrimaryCustomerAge,
        icqt.YoungestAge,
        icqt.OldestAge,
        ifnull(ics.NumChildren, 0),--        NumberOfChildren,
        ifnull(ics.NumAdults, 0),--        NumberOfAdults,
        ifnull(q.quoteGross, 0) ,--        QuotedPrice,
        --count(distinct q.savedQuoteID),-- as  QuoteCount,--added on 2019-03-28 for fix. code need to chage as per impulse 2
		--count(distinct q.sessionID),-- as  QuoteSessionCount,--added on 2019-03-28 for fix. code need to chage as per impulse 2
		case when q.quoteGross is not null then 1
			 else 0
		end ,--as QuoteWithPriceCount,
        cast(q.CampaignID as string) ,--CampaignSessionID,
        --Null ImpressionID,
        q.SavedQuoteID,--this will be used for quote count
        case         when q.quoteGross is not null then cast(q.savedQuoteID as string)            else null        end ,--QuoteWithPriceID,
        case           when q.Policies is Null  then null            else replace(replace(policies,'["',''),'"]','')        end ,--ConvertedQuoteID,
        case when q.Policies is Null  then 0 else 1 end,-- ConvertedQuoteCount
        ifnull(cast(q.Domain as string),'AU'),
        cbu.BusinessUnit,
        cc.Channel,-- Channel,
        q.outletAlphakey --AlphaCode"""
    #print(vsql)
    if not generic_module.is_step_complete(generic_module.module_name,4):
        generic_module.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cbabq_stage','ETL115_etl_impulse2Quote',None,vsql)
        generic_module.mark_step_complete(generic_module.module_name,4)
    
    #print('sleeping for 5 seconds')
    #time.sleep(5)
    #update1 will update OutletAlphaKey
    vsql="""update cbabq_stage.ETL115_etl_impulse2Quote q
    set
        q.OutletAlphaKey = 
            (select min(po.OutletAlphaKey)
            from
                cmbq_prod.penOutlet po
            where
                po.CountryKey = q.Domain and
				po.AlphaCode = q.AlphaCode and
				po.OutletStatus = 'Current' 
        )
        where q.OutletAlphaKey is null
        """
    if not generic_module.is_step_complete(generic_module.module_name,5):
        query_output=generic_module.execute_sql_bigquery_and_return_result('cover-more-data-and-analytics','cbabq_workspace',vsql,write_disposition=None)
        generic_module.mark_step_complete(generic_module.module_name,5)  
        
    #print('sleeping for 5 seconds')
    #time.sleep(5)
    #update2
    vsql="""update cbabq_stage.ETL115_etl_impulse2Quote q
    set
        q.OutletAlphaKey = 						--if penOutlet mapping fails, try manual CDG alpha mapping. Note Impulse SQL version2 does not use channel.
        (
            select min(om.OutletAlphaKey)
            from
                cmbq_prod.usrCDGQuoteAlpha om
            where
                om.Domain = q.Domain and
                om.BusinessUnit = q.BusinessUnit and
				q.AlphaCode is null
        )
        where (q.OutletAlphaKey = '' or q.OutletAlphaKey is null)
        """
    if not generic_module.is_step_complete(generic_module.module_name,6):
        query_output=generic_module.execute_sql_bigquery_and_return_result('cover-more-data-and-analytics','cbabq_workspace',vsql,write_disposition=None)
        generic_module.mark_step_complete(generic_module.module_name,6)  
    
    #print('sleeping for 5 seconds')
    #time.sleep(5)
    #update 3 
    vsql="""update cbabq_stage.ETL115_etl_impulse2Quote q
    set
        q.OutletAlphaKey =   --LT: if manual CDG alpha mapping fails, get the first OutletAlphaKey where #cdgQuote2 alpha code is not null
        (
            select min(om.OutletAlphaKey)
            from
                cmbq_prod.usrCDGQuoteAlpha om
            where
                om.Domain = q.Domain and
				om.AlphaCode = q.AlphaCode and
				q.AlphaCode is not null
        )      
        where (q.OutletAlphaKey = '' or q.OutletAlphaKey is null)"""
    #print(vsql)
    if not generic_module.is_step_complete(generic_module.module_name,7):
        query_output=generic_module.execute_sql_bigquery_and_return_result('cover-more-data-and-analytics','cbabq_workspace',vsql,write_disposition=None)
        generic_module.mark_step_complete(generic_module.module_name,7)  

    #print('sleeping for 5 seconds')
    #time.sleep(5)
    #update 4 this is not required for impulse2
    vsql="""update cbabq_stage.ETL115_etl_impulse1Quote--which is not an aggregated table
    set
        GroupIndex =
                    Concat(Cast(QuoteDate as string) , '|' ,
                    ifnull(OutletAlphaKey,'') , '|' ,
                    CurrencyCode , '|' ,
                    ifnull(Area,'') , '|' ,
                    ifnull(Destination,'') , '|' ,
                    ifnull(cast(PurchasePath as string),'') , '|' ,
                    ifnull(ProductKey,'') , '|' ,
                    ifnull(ProductCode,'') , '|' ,
                    ifnull(ProductName,'') , '|' ,
                    ifnull(PlanCode,'') , '|' ,
                    ifnull(cast(Duration as string),'') , '|' ,
                    ifnull(cast(LeadTime as string),'') , '|' ,
                    ifnull(cast(PrimaryCustomerAge as string),'') , '|' ,
                    ifnull(cast(YoungestAge as string),'') , '|' ,
                    ifnull(cast(OldestAge as string),''))
                where 1=1"""
        #print(vsql)
    """if not generic_module.is_step_complete(generic_module.module_name,6):
        query_output=generic_module.execute_sql_bigquery_and_return_result('cover-more-data-and-analytics','cbabq_workspace',vsql,write_disposition=None)
        generic_module.mark_step_complete(generic_module.module_name,6) """
    
    #this is also not required for impulse2
    #create table ETL115_etl_compressed_impulse1
    vsql="""    select
        GroupIndex,
        --count(distinct ImpressionID) QuoteCount,
        count(distinct SavedQuoteID) QuoteCount,
        --count(distinct case when ImpressionID is null then null else CampaignSessionID end) QuoteSessionCount,
        count(distinct case when SavedQuoteID is null then null else SavedQuoteID end) QuoteSessionCount,
        count(distinct QuoteWithPriceID) QuoteWithPriceCount,
        count(distinct ConvertedQuoteID) ConvertedQuoteCount,
        sum(QuotedPrice) QuotedPrice,
        sum(NumberOfChildren) NumberOfChildren,
        sum(NumberOfAdults) NumberOfAdults
    from
        cbabq_stage.ETL115_etl_impulse1Quote--which is not an aggregated table
    group by
        GroupIndex"""
     #print(vsql)
    """if not generic_module.is_step_complete(generic_module.module_name,7):
        generic_module.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cbabq_stage','ETL115_etl_compressed_impulse1',None,vsql)
        generic_module.mark_step_complete(generic_module.module_name,7)"""
        
    
    #create table ETL115_etl_rolled_impulse1
    vsql="""select --logic change from imp1 to imp2 on 2019-03-28
        QuoteDate,
        --2 QuoteSource,
        3 QuoteSource,
        o.CountryKey,
        o.CompanyKey,
        t.OutletAlphaKey,
--        '' StoreCode,
        cast(null as string) StoreCode,
        u.UserKey,
--        '' CRMUserKey,
        cast(null as string) CRMUserKey,
        0 SaveStep,
        CurrencyCode,
        Area,
        Destination,
        PurchasePath,
        ProductKey,
        ProductCode,
        ProductName,
        PlanCode,
        null PlanName,
        case
            when ProductName like '%Annual Multi%' then 'Annual Multi Trip'
            else 'Single Trip'
        end PlanType,
        null MaxDuration,
        Duration,
        LeadTime,
        null Excess,
        null CompetitorName,
        0 CompetitorGap,
        PrimaryCustomerAge,
        null PrimaryCustomerSuburb,
        null PrimaryCustomerState,
        YoungestAge,
        OldestAge,
        t.NumberOfChildren,
        t.NumberOfAdults,
        t.NumberOfChildren + t.NumberOfAdults NumberOfPersons,
        t.QuoteCount,
        t.QuoteSessionCount,
        QuoteWithPriceCount,
        ConvertedQuoteCount,
        t.QuotedPrice,
        0 ExpoQuoteCount,
        0 AgentSpecialQuoteCount,
        0 PromoQuoteCount,
        0 UpsellQuoteCount,
        0 PriceBeatQuoteCount,
        0 QuoteRenewalCount,
        0 CancellationQuoteCount,
        0 LuggageQuoteCount,
        0 MotorcycleQuoteCount,
        0 WinterQuoteCount,
        0 EMCQuoteCount
    from
        cbabq_stage.ETL115_etl_impulse2Quote t
        join
                cbabq_prod.penOutlet o
            
     on                 o.OutletAlphaKey = t.OutletAlphaKey and
                o.OutletStatus = 'Current'
        left outer join
        (select distinct OutletKey,UserKey from cmbq_prod.penUser where Login = 'webuser') u
            on
                u.OutletKey = o.OutletKey 
                """
    #print(vsql)
    if not generic_module.is_step_complete(generic_module.module_name,8):
        generic_module.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cbabq_stage','ETL115_etl_rolled_impulse2',None,vsql)
        generic_module.mark_step_complete(generic_module.module_name,8)
    
    #insert impulse1 data to penQuoteSummary
    vsql="""insert into cbabq_stage.ETL115_etl_penQuoteSummary( QuoteDate,
        QuoteSource,
        CountryKey,
        CompanyKey,
        OutletAlphaKey,
        StoreCode,
        UserKey,
        CRMUserKey,
        SaveStep,
        CurrencyCode,
        Area,
        Destination,
        PurchasePath,
        ProductKey,
        ProductCode,
        ProductName,
        PlanCode,
        PlanName,
        PlanType,
        MaxDuration,
        Duration,
        LeadTime,
        Excess,
        CompetitorName,
        CompetitorGap,
        PrimaryCustomerAge,
        PrimaryCustomerSuburb,
        PrimaryCustomerState,
        YoungestAge,
        OldestAge,
        NumberOfChildren,
        NumberOfAdults,
        NumberOfPersons,
        QuotedPrice,
        QuoteSessionCount,
        QuoteCount,
        QuoteWithPriceCount,
        SavedQuoteCount,
        ConvertedCount,
        ExpoQuoteCount,
        AgentSpecialQuoteCount,
        PromoQuoteCount,
        UpsellQuoteCount,
        PriceBeatQuoteCount,
        QuoteRenewalCount,
        CancellationQuoteCount,
        LuggageQuoteCount,
        MotorcycleQuoteCount,
        WinterQuoteCount,
        EMCQuoteCount)
    select 
        QuoteDate,
        QuoteSource,
        CountryKey,
        CompanyKey,
        OutletAlphaKey,
        StoreCode,
        UserKey,
        CRMUserKey,
        SaveStep,
        CurrencyCode,
        Area,
        Destination,
        PurchasePath,
        ProductKey,
        ProductCode,
        ProductName,
        PlanCode,
        cast(PlanName as string),--temporarily done to fix error
        PlanType,
        MaxDuration,
        Duration,
        LeadTime,
        Excess,
        cast(CompetitorName as string),--temporarily done to fix error
        CompetitorGap,
        PrimaryCustomerAge,
        cast(PrimaryCustomerSuburb as string),--temporarily done to fix error
        cast(PrimaryCustomerState as string),--temporarily done to fix error
        YoungestAge,
        OldestAge,
        NumberOfChildren,
        NumberOfAdults,
        NumberOfPersons,
        QuotedPrice,
        QuoteSessionCount,
        QuoteCount,
        QuoteWithPriceCount,
        0 SavedQuoteCount,
        ConvertedQuoteCount ConvertedCount,
        ExpoQuoteCount,
        AgentSpecialQuoteCount,
        PromoQuoteCount,
        UpsellQuoteCount,
        PriceBeatQuoteCount,
        QuoteRenewalCount,
        CancellationQuoteCount,
        LuggageQuoteCount,
        MotorcycleQuoteCount,
        WinterQuoteCount,
        EMCQuoteCount
    from
        cbabq_stage.ETL115_etl_rolled_impulse2"""
    #print(vsql)
    if not generic_module.is_step_complete(generic_module.module_name,9):
        query_output=generic_module.execute_sql_bigquery_and_return_result('cover-more-data-and-analytics','cbabq_workspace',vsql,write_disposition=None)
        generic_module.mark_step_complete(generic_module.module_name,9)
    




    
    
    #---------------------------DW & CUBE PROCESSING---------------------------------------------------
    
    #delete existing records from penQuoteSummary
    vsql="""delete from cbabq_prod.penQuoteSummary
        where
            QuoteDate >= cast(@start_date as timestamp) and
            QuoteDate <= cast(@end_date as timestamp)"""
    #print(vsql)
    if not generic_module.is_step_complete(generic_module.module_name,50):
        query_output=generic_module.execute_sql_bigquery_and_return_result('cover-more-data-and-analytics','cbabq_workspace',vsql,write_disposition=None)
        generic_module.mark_step_complete(generic_module.module_name,50)        
            
    #insert new data to penQuoteSummary
    vsql="""insert into cbabq_prod.penQuoteSummary
        (
            QuoteDate,
            QuoteSource,
            CountryKey,
            CompanyKey,
            OutletAlphaKey,
            StoreCode,
            UserKey,
            CRMUserKey,
            SaveStep,
            CurrencyCode,
            Area,
            Destination,
            PurchasePath,
            ProductKey,
            ProductCode,
            ProductName,
            PlanCode,
            PlanName,
            PlanType,
            MaxDuration,
            Duration,
            LeadTime,
            Excess,
            CompetitorName,
            CompetitorGap,
            PrimaryCustomerAge,
            PrimaryCustomerSuburb,
            PrimaryCustomerState,
            YoungestAge,
            OldestAge,
            NumberOfChildren,
            NumberOfAdults,
            NumberOfPersons,
            QuotedPrice,
            QuoteSessionCount,
            QuoteCount,
            QuoteWithPriceCount,
            SavedQuoteCount,
            ConvertedCount,
            ExpoQuoteCount,
            AgentSpecialQuoteCount,
            PromoQuoteCount,
            UpsellQuoteCount,
            PriceBeatQuoteCount,
            QuoteRenewalCount,
            CancellationQuoteCount,
            LuggageQuoteCount,
            MotorcycleQuoteCount,
            WinterQuoteCount,
            EMCQuoteCount
        )        
        select 
            cast(QuoteDate as timestamp),
            QuoteSource,
            CountryKey,
            CompanyKey,
            OutletAlphaKey,
            StoreCode,
            UserKey,
            CRMUserKey,
            SaveStep,
            CurrencyCode,
            Area,
            Destination,
            PurchasePath,
            ProductKey,
            ProductCode,
            ProductName,
            PlanCode,
            PlanName,
            PlanType,
            MaxDuration,
            Duration,
            LeadTime,
            Excess,
            CompetitorName,
            cast(CompetitorGap as int64),
            PrimaryCustomerAge,
            PrimaryCustomerSuburb,
            PrimaryCustomerState,
            YoungestAge,
            OldestAge,
            NumberOfChildren,
            NumberOfAdults,
            NumberOfPersons,
            QuotedPrice,
            QuoteSessionCount,
            QuoteCount,
            QuoteWithPriceCount,
            SavedQuoteCount,
            ConvertedCount,
            ExpoQuoteCount,
            AgentSpecialQuoteCount,
            PromoQuoteCount,
            UpsellQuoteCount,
            PriceBeatQuoteCount,
            QuoteRenewalCount,
            CancellationQuoteCount,
            LuggageQuoteCount,
            MotorcycleQuoteCount,
            WinterQuoteCount,
            EMCQuoteCount
        from
            cbabq_stage.ETL115_etl_penQuoteSummary
            """
    #print(vsql)
    if not generic_module.is_step_complete(generic_module.module_name,51):
        query_output=generic_module.execute_sql_bigquery_and_return_result('cover-more-data-and-analytics','cbabq_workspace',vsql,write_disposition=None)
        generic_module.mark_step_complete(generic_module.module_name,51)        

    #downloading the QuoteSummaryBot Table. This will be a non partitioned small sized table containing aggregated data.
    if not generic_module.is_step_complete(generic_module.module_name,52):
        bq_query_result=generic_module.execute_sql_bigquery_and_return_result('cover-more-data-and-analytics','cbabq_workspace','select * from cbabq_stage.ETL115_etl_penQuoteSummary')
        df_bq_query_result=bq_query_result.to_dataframe()
        #print(df_bq_query_result.head())
        #for row in bq_query_result:
        #    print(row)
        engine = create_engine('mssql+pymssql://@azsyddwh02:1433/db-au-stage', pool_recycle=3600, pool_size=5)
        #df_bq_query_result.to_sql('etl_penQuoteSummaryCBA', engine, if_exists='replace',index=False,chunksize=100)
        df_bq_query_result.to_sql('etl_penQuoteSummary', engine, if_exists='replace',index=False,chunksize=100)
        generic_module.mark_step_complete(generic_module.module_name,52)
        
    
    #merging data to penguin quote summary table in sql server
    if not generic_module.is_step_complete(generic_module.module_name,53):
        cursor_ref=generic_module.create_sql_cursor('azsyddwh02','db-au-stage')
        cursor_ref.execute("exec [db-au-stage].dbo.etlsp_ETL115_RetrievePenQuoteSummaryDelta");
        cursor_ref.commit()
        generic_module.mark_step_complete(generic_module.module_name,53)
    
    
    #creating factQuoteSummary in star schema as part of cube
    vsql="""declare
                @batchid int,
                @start date,
                @end date
            
            exec [db-au-stage]..syssp_getrunningbatch
                @SubjectArea = 'CDG Quote Bigquery',
                @BatchID = @batchid out,
                @StartDate = @start out,
                @EndDate = @end out
                
            exec [db-au-stage]..[etlsp_ETL115_factQuoteSummary] 
                @DateRange = '_User Defined',
                @StartDate = @start,
                @EndDate = @end
            """
    if not generic_module.is_step_complete(generic_module.module_name,54):
        cursor_ref=generic_module.create_sql_cursor('azsyddwh02','db-au-stage')
        #cursor_ref.execute("exec [db-au-stage].dbo.etlsp_ETL115_factQuoteSummaryCBA  @DateRange = '_User Defined',@StartDate = '"+generic_module.start_date+"',@EndDate = '"+generic_module.end_date+"'");
        cursor_ref.execute(vsql);
        cursor_ref.commit()
        generic_module.mark_step_complete(generic_module.module_name,54)
   
    
    #closing batch
    vsql="""declare
                @batchid int,
                @start date,
                @end date
            
            exec [db-au-stage]..syssp_getrunningbatch
                @SubjectArea = 'CDG Quote Bigquery',
                @BatchID = @batchid out,
                @StartDate = @start out,
                @EndDate = @end out
            
            exec [db-au-stage]..syssp_closebatch
                @BatchID = @batchid
            """
    if not generic_module.is_step_complete(generic_module.module_name,55):
        cursor_ref=generic_module.create_sql_cursor('azsyddwh02','db-au-stage')
        cursor_ref.execute(vsql);
        cursor_ref.commit()
        generic_module.mark_step_complete(generic_module.module_name,55)
    
    #generic_module.cnxn.close()#closing cursor, doesnt clear lock. Closing connection is important to release the lock.
    #generic_module.update_cnxn.close()
    print('Main processing block finished at:'+str(datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S')))
except Exception as excp:
    #generic_module.cnxn.close()#closing cursor, doesnt clear lock. Closing connection is important to release the lock.
    #raise Exception("Error while executing bot_processing_cba(). Full error description is:"+str(excp))
    raise
#finally:
    #cursor.close()
    #generic_module.cnxn.close()#closing cursor, doesnt clear lock. Closing connection is important to release the lock.