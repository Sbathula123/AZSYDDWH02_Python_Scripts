"""
Author : Ratnesh
Dept   : Data Analytics & Innovation, Covermore Group
Date   : 2018-02-01
Usage  : "bot_processing_cba.py <run_mode> <start_date> <end_date>"
          "python bot_processing_cba.py interval=last3days"
          "python bot_processing_cba.py start_date=2018-12-01 end_date=2019-01-01" #this will extract data for the month of December-2018
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

consultant, area, destinatino, product , savedquotecount, convertedquotecount,expoquotecount, agentspecialquote, and all other addon counts columns has some problem. 

put a union all part in [db-au-star].[dbo].[vfactQuoteSummary]

"""
#import pyodbc 
import datetime
import sys
#import voice_analytics_generic_module
import os
#import uuid
from sqlalchemy import create_engine

#enable following line if running on local workstation.
#sys.path.append(r'\\azsyddwh02\ETL\Python Scripts')
#sys.path.append(r'\\aust.covermore.com.au\user_data\NorthSydney_Users\ratneshs\Home\Projects\generic\\')
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

    #create table ETL113_process_penguinquote
    vsql="""select 
        q.QuoteKey,
        substr(q.quotekey,0,2) BusinessUnitID,
        --q.OutletGroup,
        o.GroupName OutletGroup,
        date(q.CreateTime) QuoteDate,
        q.SessionID,
        --q.TransactionHour,
        extract(hour from q.CreateTime) as TransactionHour,
        q.Destination,
        q.Duration,
        --q.LeadTime,
        datetime_diff(DepartureDate,CreateTime,day) LeadTime,
        --q.TravellerAge Age,
        qc.TravellerAge Age,
        q.NumberOfAdults AdultCount,
        q.NumberOfChildren ChildrenCount,
        --q.ConvertedFlag,
        case
            when ifnull(q.PolicyKey, '') = '' then 0
            else 1
        end ConvertedFlag,
        max(coalesce(b.BotFlag, 0)) BotFlag
    from
        cbabq_prod.penQuote q
        left join cbabq_prod.penguin_quote_ma b on
            q.QuoteKey = b.QuoteKey
            left outer join cbabq_prod.penOutlet o
            on q.OutletAlphaKey=o.OutletAlphaKey
            left outer join
             (select * from (    select 
                QuoteCountryKey,Age TravellerAge,row_number() over(partition by QuoteCountryKey order by QuoteCustomerID) row_number
            from
                cbabq_prod.penQuoteCustomer --with(nolock)
            --where
--                qc.QuoteCountryKey = q.QuoteCountryKey
            --order by
              --  qc.QuoteCustomerID
              ) where row_number=1) qc
              on qc.QuoteCountryKey = q.QuoteCountryKey
    where
        --CreateTime >= cast(datetime_add(datetime_add(current_datetime("Australia/Sydney"), interval -15 day),interval -6 month) as timestamp)--modified by ratnesh on 12nov18 to keep all time as local
        --q.CreateTime >= cast(datetime_add(datetime_add(current_datetime("Australia/Sydney"), interval -15 day),interval -6 month) as datetime)--modified by ratnesh on 12nov18 to keep all time as local
        q.CreateTime >= cast(datetime_add(datetime_add(cast(@start_date as datetime), interval -15 day),interval -6 month) as datetime)--only greater condition used in original redshift code.
        and q.CreateTime <= cast(@end_date as datetime)--added by Ratnesh
    group by
        q.QuoteKey,
        substr(q.quotekey,0,2),
        --q.OutletGroup,
        o.GroupName,
        date(q.CreateTime),
        q.SessionID,
        --q.TransactionHour,
        extract(hour from q.CreateTime),
        q.Destination,
        q.Duration,
        --q.LeadTime,
        datetime_diff(DepartureDate,CreateTime,day),
        --q.TravellerAge,
        qc.TravellerAge,
        q.NumberOfAdults,
        q.NumberOfChildren,
        --q.ConvertedFlag
         case
            when ifnull(q.PolicyKey, '') = '' then 0
            else 1
        end"""
    #print(vsql)
    if not generic_module.is_step_complete(generic_module.module_name,1):
        generic_module.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cbabq_stage','ETL113_process_penguinquote',None,vsql)
        generic_module.mark_step_complete(generic_module.module_name,1)
    
    #create table ETL113_process_penguinquote_destination
    vsql="""select 
        BusinessUnitID,
        OutletGroup,
        Destination,
        QuoteDate,
        row_number() over (partition by BusinessUnitID,OutletGroup,Destination order by QuoteDate) RowNum,
        count(distinct QuoteKey)  QuoteCount,
        count(distinct case when BotFlag = 1 then QuoteKey else null end) BotCount
    from
        cbabq_stage.ETL113_process_penguinquote
    group by
        BusinessUnitID,
        OutletGroup,
        Destination,
        QuoteDate"""
    #print(vsql)
    if not generic_module.is_step_complete(generic_module.module_name,2):
        generic_module.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cbabq_stage','ETL113_process_penguinquote_destination',None,vsql)
        generic_module.mark_step_complete(generic_module.module_name,2)
    
    #create table ETL113_process_penguinquote_destination_rank
    vsql="""select 
        *,
        substr(cast(QuoteDate as string),1,7) QuoteMonth,--changed for bigquery
        dense_rank() over (partition by BusinessUnitID,OutletGroup,Destination,substr(cast(QuoteDate as string),1,7) order by QuoteCount desc) RankInMonth,--changed for bigquery (aggregate function changed)
        count(QuoteDate) over (partition by BusinessUnitID,OutletGroup,Destination,substr(cast(QuoteDate as string),1,7)) RecordNum--changed for bigquery
    from
        cbabq_stage.ETL113_process_penguinquote_destination"""
    #print(vsql)
    if not generic_module.is_step_complete(generic_module.module_name,3):
        generic_module.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cbabq_stage','ETL113_process_penguinquote_destination_rank',None,vsql)
        generic_module.mark_step_complete(generic_module.module_name,3)    
       
    
    #create table ETL113_process_penguinquote_destination_outlier
    vsql="""select 
        *,
        case
            when RecordNum < 3 then 0
            when RankInMonth <= 0.15 * RecordNum then 1
            when RankInMonth >= 0.75 * RecordNum then 1
            else 0
        end isOutlier
    from
        cbabq_stage.ETL113_process_penguinquote_destination_rank"""
    #print(vsql)
    if not generic_module.is_step_complete(generic_module.module_name,4):
        generic_module.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cbabq_stage','ETL113_process_penguinquote_destination_outlier',None,vsql)
        generic_module.mark_step_complete(generic_module.module_name,4)


    #create table ETL113_process_penguinquote_destination_ma
    vsql="""select 
        BusinessUnitID,
        OutletGroup,
        Destination,
        QuoteDate,
        RowNum,
        QuoteCount,
        RankInMonth,
        isOutlier,
        coalesce
        (
            (
                select 
                    avg(r.QuoteCount - r.BotCount)
                from
                    cbabq_stage.ETL113_process_penguinquote_destination_outlier r
                where
                    r.BusinessUnitID = t.BusinessUnitID and
                    r.OutletGroup = t.OutletGroup and
                    r.Destination = t.Destination and
                    --hard-coded MA window, no sub-sub-query in redshift
                    r.RowNum < t.RowNum and
                    r.RowNum >= t.RowNum - 60 and
                    r.isOutlier = 0
            ),
            t.QuoteCount
        ) DestinationMA
    from
        cbabq_stage.ETL113_process_penguinquote_destination_outlier t"""
    #print(vsql)
    if not generic_module.is_step_complete(generic_module.module_name,5):
        generic_module.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cbabq_stage','ETL113_process_penguinquote_destination_ma',None,vsql)
        generic_module.mark_step_complete(generic_module.module_name,5)        



    #create table ETL113_process_penguinquote_duration
    vsql=""" select 
        BusinessUnitID,
        OutletGroup,
        duration,
        QuoteDate,
        row_number() over (partition by BusinessUnitID,OutletGroup,duration order by QuoteDate) RowNum,
        cast(count(distinct QuoteKey) as float64) QuoteCount,
        cast(count(distinct case when BotFlag = 1 then QuoteKey else null end) as float64) BotCount
    from
        cbabq_stage.ETL113_process_penguinquote
    group by
        BusinessUnitID,
        OutletGroup,
        duration,
        QuoteDate"""
    #print(vsql)
    if not generic_module.is_step_complete(generic_module.module_name,6):
        generic_module.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cbabq_stage','ETL113_process_penguinquote_duration',None,vsql)
        generic_module.mark_step_complete(generic_module.module_name,6)

    #create table ETL113_process_penguinquote_duration_rank
    vsql="""select 
        *,
        substr(cast(QuoteDate as string),1,7) QuoteMonth,--changed for bigquery
        dense_rank() over (partition by BusinessUnitID,OutletGroup,duration,substr(cast(QuoteDate as string),1,7) order by QuoteCount desc) RankInMonth,--changed for bigquery aggregate function changed
        count(QuoteDate) over (partition by BusinessUnitID,OutletGroup,duration,substr(cast(QuoteDate as string),1,7)) RecordNum--cahnged for bigquery
    from
        cbabq_stage.ETL113_process_penguinquote_duration"""
    #print(vsql)
    if not generic_module.is_step_complete(generic_module.module_name,7):
        generic_module.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cbabq_stage','ETL113_process_penguinquote_duration_rank',None,vsql)
        generic_module.mark_step_complete(generic_module.module_name,7)


    #create table ETL113_process_penguinquote_duration_outlier
    vsql="""select 
        *,
        case
            when RecordNum < 3 then 0
            when RankInMonth <= 0.15 * RecordNum then 1
            when RankInMonth >= 0.75 * RecordNum then 1
            else 0
        end isOutlier
    from
        cbabq_stage.ETL113_process_penguinquote_duration_rank"""
    #print(vsql)
    if not generic_module.is_step_complete(generic_module.module_name,8):
        generic_module.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cbabq_stage','ETL113_process_penguinquote_duration_outlier',None,vsql)
        generic_module.mark_step_complete(generic_module.module_name,8)
        
    #create table ETL113_process_penguinquote_duration_ma
    vsql="""select 
        BusinessUnitID,
        OutletGroup,
        duration,
        QuoteDate,
        RowNum,
        QuoteCount,
        RankInMonth,
        isOutlier,
        coalesce
        (
            (
                select 
                    avg(r.QuoteCount - r.BotCount)
                from
                    cbabq_stage.ETL113_process_penguinquote_duration_outlier r
                where
                    r.BusinessUnitID = t.BusinessUnitID and
                    r.OutletGroup = t.OutletGroup and
                    r.duration = t.duration and
                    --hard-coded MA window, no sub-sub-query in redshift
                    r.RowNum < t.RowNum and
                    r.RowNum >= t.RowNum - 60 and
                    r.isOutlier = 0
            ),
            t.QuoteCount
        ) durationMA
    from
        cbabq_stage.ETL113_process_penguinquote_duration_outlier t"""
    #print(vsql)
    if not generic_module.is_step_complete(generic_module.module_name,9):
        generic_module.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cbabq_stage','ETL113_process_penguinquote_duration_ma',None,vsql)
        generic_module.mark_step_complete(generic_module.module_name,9)
        
    #create table ETL113_process_penguinquote_leadtime
    vsql="""   select 
        BusinessUnitID,
        OutletGroup,
        leadtime,
        QuoteDate,
        row_number() over (partition by BusinessUnitID,OutletGroup,leadtime order by QuoteDate) RowNum,
        cast(count(distinct QuoteKey) as float64) QuoteCount,
        cast(count(distinct case when BotFlag = 1 then QuoteKey else null end) as float64) BotCount
    from
        cbabq_stage.ETL113_process_penguinquote
    group by
        BusinessUnitID,
        OutletGroup,
        leadtime,
        QuoteDate"""
    #print(vsql)
    if not generic_module.is_step_complete(generic_module.module_name,10):
        generic_module.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cbabq_stage','ETL113_process_penguinquote_leadtime',None,vsql)
        generic_module.mark_step_complete(generic_module.module_name,10)
        
    #create table ETL113_process_penguinquote_leadtime_rank
    vsql="""   select 
        *,
        substr(cast(QuoteDate as string),1,7) QuoteMonth,--changed for bigquery
        dense_rank() over (partition by BusinessUnitID,OutletGroup,leadtime,substr(cast(QuoteDate as string),1,7) order by QuoteCount desc) RankInMonth,--changed for big query (aggregate function changed)
        count(QuoteDate) over (partition by BusinessUnitID,OutletGroup,leadtime,substr(cast(QuoteDate as string),1,7)) RecordNum--changed for bigquery
    from
        cbabq_stage.ETL113_process_penguinquote_leadtime"""
    #print(vsql)
    if not generic_module.is_step_complete(generic_module.module_name,11):
        generic_module.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cbabq_stage','ETL113_process_penguinquote_leadtime_rank',None,vsql)
        generic_module.mark_step_complete(generic_module.module_name,11)
        
    #create table ETL113_process_penguinquote_leadtime_outlier
    vsql="""select 
        *,
        case
            when RecordNum < 3 then 0
            when RankInMonth <= 0.15 * RecordNum then 1
            when RankInMonth >= 0.75 * RecordNum then 1
            else 0
        end isOutlier
    from
        cbabq_stage.ETL113_process_penguinquote_leadtime_rank"""
    #print(vsql)
    if not generic_module.is_step_complete(generic_module.module_name,12):
        generic_module.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cbabq_stage','ETL113_process_penguinquote_leadtime_outlier',None,vsql)
        generic_module.mark_step_complete(generic_module.module_name,12)
        
    #create table ETL113_process_penguinquote_leadtime_ma
    vsql="""    select 
        BusinessUnitID,
        OutletGroup,
        leadtime,
        QuoteDate,
        RowNum,
        QuoteCount,
        RankInMonth,
        isOutlier,
        coalesce
        (
            (
                select 
                    avg(r.QuoteCount - r.BotCount)
                from
                    cbabq_stage.ETL113_process_penguinquote_leadtime_outlier r
                where
                    r.BusinessUnitID = t.BusinessUnitID and
                    r.OutletGroup = t.OutletGroup and
                    r.leadtime = t.leadtime and
                    --hard-coded MA window, no sub-sub-query in redshift
                    r.RowNum < t.RowNum and
                    r.RowNum >= t.RowNum - 60 and
                    r.isOutlier = 0
            ),
            t.QuoteCount
        ) leadtimeMA
    from
        cbabq_stage.ETL113_process_penguinquote_leadtime_outlier t"""
    #print(vsql)
    if not generic_module.is_step_complete(generic_module.module_name,13):
        generic_module.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cbabq_stage','ETL113_process_penguinquote_leadtime_ma',None,vsql)
        generic_module.mark_step_complete(generic_module.module_name,13)
        
    #create table ETL113_process_penguinquote_age
    vsql="""    select 
        BusinessUnitID,
        OutletGroup,
        age,
        QuoteDate,
        row_number() over (partition by BusinessUnitID,OutletGroup,age order by QuoteDate) RowNum,
        cast(count(distinct QuoteKey) as float64) QuoteCount,
        cast(count(distinct case when BotFlag = 1 then QuoteKey else null end) as float64) BotCount
    from
        cbabq_stage.ETL113_process_penguinquote
    group by
        BusinessUnitID,
        OutletGroup,
        age,
        QuoteDate"""
    #print(vsql)
    if not generic_module.is_step_complete(generic_module.module_name,14):
        generic_module.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cbabq_stage','ETL113_process_penguinquote_age',None,vsql)
        generic_module.mark_step_complete(generic_module.module_name,14)
        
    #create table ETL113_process_penguinquote_age_rank
    vsql="""    select 
        *,
        substr(cast(QuoteDate as string),1,7) QuoteMonth,--changed for bigquery
        dense_rank() over (partition by BusinessUnitID,OutletGroup,age,substr(cast(QuoteDate as string),1,7) order by QuoteCount desc) RankInMonth,--changed for bigquery (aggregate function changed)
        count(QuoteDate) over (partition by BusinessUnitID,OutletGroup,age,substr(cast(QuoteDate as string),1,7)) RecordNum--changed for bigquery
    from
        cbabq_stage.ETL113_process_penguinquote_age"""
    #print(vsql)
    if not generic_module.is_step_complete(generic_module.module_name,15):
        generic_module.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cbabq_stage','ETL113_process_penguinquote_age_rank',None,vsql)
        generic_module.mark_step_complete(generic_module.module_name,15)
        
    #create table ETL113_process_penguinquote_age_outlier
    vsql="""select 
        *,
        case
            when RecordNum < 3 then 0
            when RankInMonth <= 0.15 * RecordNum then 1
            when RankInMonth >= 0.75 * RecordNum then 1
            else 0
        end isOutlier
    from
        cbabq_stage.ETL113_process_penguinquote_age_rank"""
    #print(vsql)
    if not generic_module.is_step_complete(generic_module.module_name,16):
        generic_module.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cbabq_stage','ETL113_process_penguinquote_age_outlier',None,vsql)
        generic_module.mark_step_complete(generic_module.module_name,16)


    #create table ETL113_process_penguinquote_age_ma
    vsql=""" select 
        BusinessUnitID,
        OutletGroup,
        age,
        QuoteDate,
        RowNum,
        QuoteCount,
        RankInMonth,
        isOutlier,
        coalesce
        (
            (
                select 
                    avg(r.QuoteCount - r.BotCount)
                from
                    cbabq_stage.ETL113_process_penguinquote_age_outlier r
                where
                    r.BusinessUnitID = t.BusinessUnitID and
                    r.OutletGroup = t.OutletGroup and
                    r.age = t.age and
                    --hard-coded MA window, no sub-sub-query in redshift
                    r.RowNum < t.RowNum and
                    r.RowNum >= t.RowNum - 60 and
                    r.isOutlier = 0
            ),
            t.QuoteCount
        ) ageMA
    from
        cbabq_stage.ETL113_process_penguinquote_age_outlier t"""
    #print(vsql)
    if not generic_module.is_step_complete(generic_module.module_name,17):
        generic_module.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cbabq_stage','ETL113_process_penguinquote_age_ma',None,vsql)
        generic_module.mark_step_complete(generic_module.module_name,17)
        
    #create table ETL113_process_penguinquote_transactionhour
    vsql=""" select 
        BusinessUnitID,
        OutletGroup,
        transactionhour,
        QuoteDate,
        row_number() over (partition by BusinessUnitID,OutletGroup,transactionhour order by QuoteDate) RowNum,
        cast(count(distinct QuoteKey) as float64) QuoteCount,
        cast(count(distinct case when BotFlag = 1 then QuoteKey else null end) as float64) BotCount
    from
        cbabq_stage.ETL113_process_penguinquote
    group by
        BusinessUnitID,
        OutletGroup,
        transactionhour,
        QuoteDate"""
    #print(vsql)
    if not generic_module.is_step_complete(generic_module.module_name,18):
        generic_module.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cbabq_stage','ETL113_process_penguinquote_transactionhour',None,vsql)
        generic_module.mark_step_complete(generic_module.module_name,18)

    #create table ETL113_process_penguinquote_transactionhour_rank
    vsql=""" select 
        *,
        substr(cast(QuoteDate as string),1,7) QuoteMonth,--changed for bigquery
        dense_rank() over (partition by BusinessUnitID,OutletGroup,transactionhour,substr(cast(QuoteDate as string),1,7) order by QuoteCount desc) RankInMonth,--changed for bigquery(aggregate function changed)
        count(QuoteDate) over (partition by BusinessUnitID,OutletGroup,transactionhour,substr(cast(QuoteDate as string),1,7)) RecordNum--changed for bigquery
    from
        cbabq_stage.ETL113_process_penguinquote_transactionhour"""
    #print(vsql)
    if not generic_module.is_step_complete(generic_module.module_name,19):
        generic_module.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cbabq_stage','ETL113_process_penguinquote_transactionhour_rank',None,vsql)
        generic_module.mark_step_complete(generic_module.module_name,19)

    #create table ETL113_process_penguinquote_transactionhour_outlier
    vsql="""    select 
        *,
        case
            when RecordNum < 3 then 0
            when RankInMonth <= 0.15 * RecordNum then 1
            when RankInMonth >= 0.75 * RecordNum then 1
            else 0
        end isOutlier
    from
        cbabq_stage.ETL113_process_penguinquote_transactionhour_rank"""
    #print(vsql)
    if not generic_module.is_step_complete(generic_module.module_name,20):
        generic_module.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cbabq_stage','ETL113_process_penguinquote_transactionhour_outlier',None,vsql)
        generic_module.mark_step_complete(generic_module.module_name,20)

    #create table ETL113_process_penguinquote_transactionhour_ma
    vsql="""    select 
        BusinessUnitID,
        OutletGroup,
        transactionhour,
        QuoteDate,
        RowNum,
        QuoteCount,
        RankInMonth,
        isOutlier,
        coalesce
        (
            (
                select 
                    avg(r.QuoteCount - r.BotCount)
                from
                    cbabq_stage.ETL113_process_penguinquote_transactionhour_outlier r
                where
                    r.BusinessUnitID = t.BusinessUnitID and
                    r.OutletGroup = t.OutletGroup and
                    r.transactionhour = t.transactionhour and
                    --hard-coded MA window, no sub-sub-query in redshift
                    r.RowNum < t.RowNum and
                    r.RowNum >= t.RowNum - 60 and
                    r.isOutlier = 0
            ),
            t.QuoteCount
        ) transactionhourMA
    from
        cbabq_stage.ETL113_process_penguinquote_transactionhour_outlier t"""
    #print(vsql)
    if not generic_module.is_step_complete(generic_module.module_name,21):
        generic_module.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cbabq_stage','ETL113_process_penguinquote_transactionhour_ma',None,vsql)
        generic_module.mark_step_complete(generic_module.module_name,21)        
    
    
    #create table ETL113_ma_penguinquote
    vsql=""" select
        a.QuoteKey,
        case 
            when DestinationMA = 0 then 0 
            else (b.QuoteCount - DestinationMA) / DestinationMA
        end as DestinationMA,
        case 
            when DurationMA = 0 then 0 
            when (a.Duration >= 28) and (mod(a.Duration,7) = 0) then (c.QuoteCount - (DurationMA / 8)) / (DurationMA / 8)--mod operator used for bigquery
            when (a.Duration > 30) and (mod(a.Duration,30) = 0) then (c.QuoteCount - (DurationMA / 8)) / (DurationMA / 8)--mod operator used for bigquery
            else (c.QuoteCount - DurationMA) / DurationMA
        end as DurationMA,
        case 
            when LeadTimeMA = 0 then 0 
            else (d.QuoteCount - LeadTimeMA) / LeadTimeMA
        end as LeadTimeMA,
        case 
            when AgeMA = 0 then 0 
            else (e.QuoteCount - AgeMA) / AgeMA
        end as AgeMA,
        case 
            when TransactionHourMA = 0 then 0 
            else (f.QuoteCount - TransactionHourMA) / TransactionHourMA
        end as TransactionHourMA,
        ssq.SameSessionQuoteCount,
        ConvertedFlag
    from
        cbabq_stage.ETL113_process_penguinquote a
        left join cbabq_stage.ETL113_process_penguinquote_destination_ma b on 
            a.BusinessUnitID = b.BusinessUnitID and
            a.QuoteDate = b.QuoteDate and
            a.OutletGroup = b.OutletGroup and
            a.Destination = b.Destination
        left join cbabq_stage.ETL113_process_penguinquote_duration_ma c on 
            a.BusinessUnitID = c.BusinessUnitID and
            a.QuoteDate = c.QuoteDate and
            a.OutletGroup = c.OutletGroup and
            a.duration = c.duration
        left join cbabq_stage.ETL113_process_penguinquote_leadtime_ma d on 
            a.BusinessUnitID = d.BusinessUnitID and
            a.QuoteDate = d.QuoteDate and
            a.OutletGroup = d.OutletGroup and
            a.leadtime = d.leadtime
        left join cbabq_stage.ETL113_process_penguinquote_age_ma e on 
            a.BusinessUnitID = e.BusinessUnitID and
            a.QuoteDate = e.QuoteDate and
            a.OutletGroup = e.OutletGroup and
            a.age = e.age
        left join cbabq_stage.ETL113_process_penguinquote_transactionhour_ma f on 
            a.BusinessUnitID = f.BusinessUnitID and
            a.QuoteDate = f.QuoteDate and
            a.OutletGroup = f.OutletGroup and
            a.transactionhour = f.transactionhour
        left join
        (
            select 
                SessionID, 
                count(1) SameSessionQuoteCount
            from
                cbabq_stage.ETL113_process_penguinquote
            group by
                SessionID
        ) ssq on 
            a.SessionID = ssq.SessionID
    where
        --datetime(a.QuoteDate) >= datetime_add(current_datetime("Australia/Sydney"), interval -15 day)--function changed for bigquery. modified by ratnesh on 12nov18 to keep all time as local.
        datetime(a.QuoteDate) >= datetime_add(cast(@start_date as datetime), interval -15 day)--only greater condition is present in original redshift code.
        and datetime(a.QuoteDate) <= cast(@end_date as datetime)
        """
    #print(vsql)
    if not generic_module.is_step_complete(generic_module.module_name,22):
        generic_module.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cbabq_stage','ETL113_ma_penguinquote',None,vsql)
        generic_module.mark_step_complete(generic_module.module_name,22)    
    
    #create table ETL113_flagged_penguinquote
    vsql=""" select
        QuoteKey,
        DestinationMA,
        DurationMA,
        LeadTimeMA,
        AgeMA,
        TransactionHourMA,
        SameSessionQuoteCount,
        ConvertedFlag,
        max(
            case 
                when ConvertedFlag = 1 then 0
                when
                    (
                        (
                            case 
                                when LeadTimeMA >= 3*6 then 2 
                                when LeadTimeMA >= 3 then 1 
                                when LeadTimeMA > 1 then 0.125
                                else 0 
                            end
                        ) * 2.0 + 
                        (
                            case 
                                when DurationMA >= 3*8 then 3
                                when DurationMA >= 3*6 then 2 
                                when DurationMA >= 3*3 then 1.5
                                when DurationMA >= 3 then 1 
                                when DurationMA > 1 then 0.25
                                else 0 
                            end
                        ) * 1.0 + 
                        (
                            case
                                when AgeMA >= 3*6 then 3
                                when AgeMA >= 3*3 then 2 
                                when AgeMA >= 3 then 1 
                                when AgeMA > 1 then 0.25
                                else 0 
                            end
                        ) * 1 + 
                        (
                            case 
                                when DestinationMA >= 3*6 then 2 
                                when DestinationMA >= 3 then 1 
                                when DestinationMA > 1 then 0.5
                                else 0 
                            end
                        ) * 0.5 +
                        (
                            case 
                                when TransactionHourMA >= 3*6 then 2 
                                when TransactionHourMA >= 3 then 1 
                                else 0 
                            end
                        ) * 0.25
                    ) > 2 then 1
                else 0
            end 
        ) BotFlag
    from
        cbabq_stage.ETL113_ma_penguinquote a
    group by
        QuoteKey,
        DestinationMA,
        DurationMA,
        LeadTimeMA,
        AgeMA,
        TransactionHourMA,
        SameSessionQuoteCount,
        ConvertedFlag"""
    #print(vsql)
    if not generic_module.is_step_complete(generic_module.module_name,23):
        generic_module.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cbabq_stage','ETL113_flagged_penguinquote',None,vsql)
        generic_module.mark_step_complete(generic_module.module_name,23)
  
    
    #create table cbabq_prod.penguin_quote_ma
    vsql="""SELECT
              *
            FROM
              cbabq_prod.penguin_quote_ma
            WHERE
              quotekey NOT IN (
              SELECT
                quotekey
              FROM
                cbabq_stage.ETL113_flagged_penguinquote)
            UNION ALL
            SELECT
              *,cast(current_datetime("Australia/Sydney") as timestamp)--reverted by ratnesh on 12nov18 to keep all time as local
            --current_timestamp()
            FROM
              cbabq_stage.ETL113_flagged_penguinquote"""
    #print(vsql)
    if not generic_module.is_step_complete(generic_module.module_name,24):
        generic_module.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cbabq_prod','penguin_quote_ma',None,vsql)
        generic_module.mark_step_complete(generic_module.module_name,24)
        
    #create table cbabq_prod.botQuotePenguin
    vsql="""with
            stg_bot_penguin as (
             select
            	            QuoteKey,
            	            DestinationMA as DestinationMAFactor,
            	            DurationMA as DurationMAFactor,
            	            LeadTimeMA as LeadTimeMAFactor,
            	            AgeMA as AgeMAFactor,
            	            TransactionHourMA as TransactionHourMAFactor,
            	            SameSessionQuoteCount,
            	            ConvertedFlag,
            	            BotFlag
                        from
                             cbabq_prod.penguin_quote_ma
                        where date(UpdateTime) >= date(current_datetime("Australia/Sydney"))
                        and BotFlag=1)
            select QuoteKey,DestinationMAFactor,DurationMAFactor,LeadTimeMAFactor,AgeMAFactor,TransactionHourMAFactor,SameSessionQuoteCount,ConvertedFlag,BotFlag from cbabq_prod.botQuotePenguin where QuoteKey not in (select QuoteKey from stg_bot_penguin)
            union all
            select * from stg_bot_penguin"""
    #print(vsql)
    if not generic_module.is_step_complete(generic_module.module_name,25):
        generic_module.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cbabq_prod','botQuotePenguin',None,vsql)
        generic_module.mark_step_complete(generic_module.module_name,25)    

    #create table ETL113_etl_penQuoteSummaryBot
    vsql="""select 
        --convert(date, q.CreateDate) QuoteDate,
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
        --isnull(q.CountryKey,'') + '-' + isnull(q.CompanyKey,'') + '' + convert(varchar,isnull(q.DomainID,0)) + '-' + isnull(q.ProductCode,'') + '-' + isnull(q.ProductName,'') + '-' + isnull(q.ProductDisplayName,'') + '-' + isnull(q.PlanName,'') ProductKey,
        concat(ifnull(q.CountryKey,'') , '-' , ifnull(q.CompanyKey,'') , '' , cast(ifnull(q.DomainID,0) as string) , '-' , ifnull(q.ProductCode,'') , '-' , ifnull(q.ProductName,'') , '-' , ifnull(q.ProductDisplayName,'') , '-' , ifnull(q.PlanName,'')) ProductKey,
        q.ProductCode,
        q.ProductName,
        q.PlanCode,
        q.PlanName,
        q.PlanType,
        q.MaxDuration,
        q.Duration,
        case
            --when datediff(day, q.CreateDate, q.DepartureDate) < 0 then 0
            when date_diff(cast(q.DepartureDate as date),cast(q.CreateDate as date),day) < 0 then 0
            --else datediff(day, q.CreateDate, q.DepartureDate)
            else date_diff(cast(q.DepartureDate as date),cast(q.CreateDate as date),day)
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
        --sum(isnull(q.NumberOfChildren, 0)) NumberOfChildren,
        sum(ifnull(q.NumberOfChildren, 0)) NumberOfChildren,
        --sum(isnull(q.NumberOfAdults, 0)) NumberOfAdults,
        sum(ifnull(q.NumberOfAdults, 0)) NumberOfAdults,
        --sum(isnull(q.NumberOfPersons, 0)) NumberOfPersons,
        sum(ifnull(q.NumberOfPersons, 0)) NumberOfPersons,
        --sum(isnull(q.QuotedPrice, 0)) QuotedPrice,
        sum(ifnull(q.QuotedPrice, 0)) QuotedPrice,
        count(distinct q.SessionID) QuoteSessionCount,
        count(case when q.ParentQuoteID is null then q.QuoteKey else null end) QuoteCount,
        sum(
            case
                when q.ParentQuoteID is null and q.QuotedPrice is not null then 1
                else 0
            end 
        ) QuoteWithPriceCount,
        --sum(case when q.ParentQuoteID is null and isnull(q.IsSaved,0) = 1 then 1 else 0 end) SavedQuoteCount,
        sum(case when q.ParentQuoteID is null and ifnull(q.IsSaved,0) = 1 then 1 else 0 end) SavedQuoteCount,
        sum(
            case
                --when isnull(q.PolicyKey, '') <> '' then 1
                when ifnull(q.PolicyKey, '') <> '' then 1
                else 0
            end 
        ) ConvertedCount,
        --sum(case when q.ParentQuoteID is null and isnull(q.IsExpo,0) = 1 then 1 else 0 end) ExpoQuoteCount,
        sum(case when q.ParentQuoteID is null and ifnull(q.IsExpo,0) = 1 then 1 else 0 end) ExpoQuoteCount,
        --sum(case when q.ParentQuoteID is null and isnull(q.IsAgentSpecial, 0) = 1 then 1 else 0 end) AgentSpecialQuoteCount,
        sum(case when q.ParentQuoteID is null and ifnull(q.IsAgentSpecial, 0) = 1 then 1 else 0 end) AgentSpecialQuoteCount,
        sum(
            case
                --when q.ParentQuoteID is null and isnull(q.PromoCode, '') <> '' then 1
                when q.ParentQuoteID is null and ifnull(q.PromoCode, '') <> '' then 1
                else 0
            end 
        ) PromoQuoteCount,
        --sum(case when q.ParentQuoteID is null and isnull(q.IsUpSell,0) = 1 then 1 else 0 end) UpsellQuoteCount,
        sum(case when q.ParentQuoteID is null and ifnull(q.IsUpSell,0) = 1 then 1 else 0 end) UpsellQuoteCount,
        --sum(case when q.ParentQuoteID is null and isnull(q.IsPriceBeat, 0) = 1 then 1 else 0 end) PriceBeatQuoteCount,
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
    --into etl_penQuoteSummaryBot
    from
        --[db-au-cmdwh]..penQuote q
        cbabq_prod.penQuote q
        --outer apply
        left outer join
        --(
        (select --top 1
                CompetitorName,
                CompetitorPrice,
                Quotekey from(
            select --top 1
                CompetitorName,
                CompetitorPrice,
                Quotekey,
                row_number() over (partition by QuoteKey order by CompetitorName,CompetitorPrice) as row_number
            from
                --[db-au-cmdwh]..penQuoteCompetitor qcmp
                cbabq_prod.penQuoteCompetitor )
                where row_number=1) qcmp
            --where
            on
                qcmp.QuoteKey = q.QuoteCountryKey
        --) qcmp
        --outer apply
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
                --[db-au-cmdwh]..penQuoteCustomer qc
                cbabq_prod.penQuoteCustomer qc
                --left join [db-au-cmdwh]..penCustomer c on
                left join cbabq_prod.penCustomer c on
                    c.CustomerKey = qc.CustomerKey and
                    qc.IsPrimary = 1
                    group by qc.QuoteCountryKey
            --where
              --  qc.QuoteCountryKey = q.QuoteCountryKey
        ) qpc
        on qpc.QuoteCountryKey = q.QuoteCountryKey
        --outer apply
        left outer join
        (
            select QuoteCountryKey,
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
            from
                --[db-au-cmdwh]..penQuoteAddOn qa
                cbabq_prod.penQuoteAddOn --qa
                group by QuoteCountryKey
            --where
        ) qa
            on 
                qa.QuoteCountryKey = q.QuoteCountryKey
        --outer apply
        left outer join
       /* (select UserKey,OutletAlphaKey,Login from (
            select --top 1
                pu.UserKey,o.OutletAlphaKey,pu.Login,
                row_number() over (partition by o.outletkey order by --o.outletkey,pu.UserKey
                pu.Login desc--this is done to keep webuser at first position which is used in most of the data.
                ) as row_number
            from
                cbabq_prod.penUser pu
                inner join cbabq_prod.penOutlet o on
                    o.OutletKey = pu.OutletKey and
                    o.OutletStatus = 'Current'
            where
                pu.UserStatus = 'Current' --and
        ) where row_number=1)  u*/
        (select distinct pu.UserKey,pu.OutletAlphaKey,pu.Login from 
                cbabq_prod.penUser pu
                inner join cbabq_prod.penOutlet o on
                    o.OutletKey = pu.OutletKey and
                    o.OutletStatus = 'Current'
            where
                pu.UserStatus = 'Current' --and
         )  u
        on u.Login = q.UserName and
                --u.OutletAlphaKey = q.OutletAlphaKey
                upper(u.OutletAlphaKey) = upper(q.OutletAlphaKey)
        --outer apply
        left outer join
        (select CRMUserKey,UserName from (
            select --top 1
                CRMUserKey,UserName,
                row_number() over (partition by UserName order by CRMUserKey) as row_number
            from
                cbabq_prod.penCRMUser)
                where row_number=1) cu
            --where
                on 
                --cu.UserName = q.CRMUserName
                ifnull(cu.UserName,'xyz') = ifnull(q.CRMUserName,'xyz')
        --) cu
    where
        --q.CreateDate >= @rptStartDate and
        date(q.CreateDate) >= cast(@start_date as date)
        and date(q.CreateDate) <= cast(@end_date as date)--casted as date to include current day.
        --q.CreateDate <  dateadd(day, 1, @rptEndDate) and
        --q.CreateDate <  convert(date, getdate()) 
        and
        q.QuoteKey not like 'AU-CM-%' and        --excludes duplicate quotekey records
        q.QuoteKey not like 'AU-TIP-%' and
        --exists
        q.QuoteKey in 
        (
            select 
                QuoteKey
            from
                cbabq_prod.botQuotePenguin qb
            where
                --qb.QuoteKey = q.QuoteKey and
                qb.BotFlag = 1
        )
    group by
        --convert(date, q.CreateDate),
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
        concat(ifnull(q.CountryKey,'') , '-' , ifnull(q.CompanyKey,'') , '' , cast(ifnull(q.DomainID,0) as string) , '-' , ifnull(q.ProductCode,'') , '-' , ifnull(q.ProductName,'') , '-' , ifnull(q.ProductDisplayName,'') , '-' , ifnull(q.PlanName,'')),
        q.ProductCode,
        q.ProductName,
        q.PlanCode,
        q.PlanName,
        q.PlanType,
        q.MaxDuration,
        q.Duration,
        case
            --when datediff(day, q.CreateDate, q.DepartureDate) < 0 then 0
            when date_diff(cast(q.DepartureDate as date), cast(q.CreateDate as date), day) < 0 then 0
            --else datediff(day, q.CreateDate, q.DepartureDate)
            else date_diff(cast(q.DepartureDate as date), cast(q.CreateDate as date),day )
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
    if not generic_module.is_step_complete(generic_module.module_name,26):
        generic_module.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cbabq_stage','ETL113_etl_penQuoteSummaryBot',None,vsql)
        generic_module.mark_step_complete(generic_module.module_name,26)    
    
    
    #create table ETL113_process_penguinquote
    """vsql=
    #print(vsql)
    if not generic_module.is_step_complete(generic_module.module_name,27):
        generic_module.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cbabq_stage','ETL113_process_penguinquote',None,vsql)
        generic_module.mark_step_complete(generic_module.module_name,27)"""
    
    #create table cbabq_stage.ETL113_process_cdgquote
    vsql="""select 																																																		
        3 PlatformVersion,                          
        q.sessionid AnalyticsSessionID,                                                                   
        cast(q.businessunitid as int64) BusinessUnitID,                                                                  
        cast(q.quoteDateUTC as date) QuoteDate,                                                           
        extract(hour from q.quoteDateUTC) as TransactionHour,                                             
		d.destinationcountrycode as Destination,                                                              
		cast(q.quoteDuration as int64) Duration,                                                                             
		timestamp_diff(q.tripstartdate,q.quotedateUTC,day) as LeadTime,                                       
		t.Age as Age,                                                                                         
		case when policies is not null then 1 else 0 end as ConvertedFlag,                                    
        --t.PrimaryTraveller as isPrimaryTraveller,                                                         
        case when t.PrimaryTraveller='True' then 1 else 0 end as isPrimaryTraveller,
        max(coalesce(b.BotFlag, 0)) BotFlag                                                               
    from                                                                                                  
		cbabq_prod.impulse_cba_quotes q                                                                       
		left join cbabq_prod.impulse_cba_quote_travellers t on                                                
		q.SessionId=t.SessionId                                                                               
		left join cbabq_prod.impulse_cba_quote_destinations d on                                              
		q.SessionId=d.SessionId                                                                               
		left join cbabq_prod.impulse_quote_ma b on                                                            
			3 = b.PlatformVersion and                                                                           
            q.SessionID = cast(b.AnalyticsSessionID as string)                                            
    where                                                                                                 
        --q.quoteDateUTC >= cast(datetime_add(datetime_add(current_datetime('Australia/Sydney'),interval -15 day),interval -6 month) as timestamp)
        q.quoteDateUTC >= cast(datetime_add(datetime_add(cast(@start_date as datetime),interval -15 day),interval -6 month) as timestamp)
        and q.quoteDateUTC <=  cast(@end_date as timestamp)--added by ratnesh
    group by                                                                                              
        q.sessionid,                                                                                      
        q.BusinessUnitID,                                                                                 
        cast(q.quoteDateUTC as date),                                                                     
		extract(hour from q.quoteDateUTC),                                                                    
		d.destinationcountrycode ,                                                                            
		q.quoteDuration,                                                                                      
		timestamp_diff(q.tripstartdate,q.quotedateUTC,day),                                                   
		t.Age,                                                                                                
		case when policies is not null then 1 else 0 end,                                                     
		t.PrimaryTraveller,                                                                                   
        coalesce(b.BotFlag, 0)"""
    #print(vsql)
    if not generic_module.is_step_complete(generic_module.module_name,28):
        generic_module.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cbabq_stage','ETL113_process_cdgquote',None,vsql)
        generic_module.mark_step_complete(generic_module.module_name,28)
    
    #create table cbabq_stage.ETL113_process_cdgquote_destination
    vsql="""select                                                                                                   
        BusinessUnitID,                                                                                            
        Destination,                                                                                               
        QuoteDate,                                                                                                 
        row_number() over (partition by BusinessUnitID,Destination order by QuoteDate) RowNum,                     
        cast(count(distinct AnalyticsSessionID) as float64) QuoteCount,                                            
        cast(count(distinct case when BotFlag = 1 then AnalyticsSessionID else null end) as float64) BotCount      
    from                                                                                                           
        cbabq_stage.ETL113_process_cdgquote                                                                
    group by                                                                                                       
        BusinessUnitID,                                                                                            
        Destination,                                                                                               
        QuoteDate"""                                                                                          
    #print(vsql)   
    if not generic_module.is_step_complete(generic_module.module_name,29):
        generic_module.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cbabq_stage','ETL113_process_cdgquote_destination',None,vsql)
        generic_module.mark_step_complete(generic_module.module_name,29)
    
    
    vsql="""select 
        *, 
        substr(cast(QuoteDate as string),1,7) QuoteMonth, 
        dense_rank() over (partition by BusinessUnitID,Destination,substr(cast(QuoteDate as string),1,7) order by QuoteCount desc) RankInMonth, 
        count(QuoteDate) over (partition by BusinessUnitID,Destination,substr(cast(QuoteDate as string),1,7)) RecordNum 
    from 
        cbabq_stage.ETL113_process_cdgquote_destination"""
    #print(vsql)   
    if not generic_module.is_step_complete(generic_module.module_name,30):
        generic_module.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cbabq_stage','ETL113_process_cdgquote_destination_rank',None,vsql)
        generic_module.mark_step_complete(generic_module.module_name,30)
    
    vsql="""select 
        *,  
        case  
            when RecordNum < 3 then 0  
            when RankInMonth <= 0.15 * RecordNum then 1  
            when RankInMonth >= 0.75 * RecordNum then 1  
            else 0  
        end isOutlier  
    from  
        cbabq_stage.ETL113_process_cdgquote_destination_rank"""
    #print(vsql)   
    if not generic_module.is_step_complete(generic_module.module_name,31):
        generic_module.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cbabq_stage','ETL113_process_cdgquote_destination_outlier',None,vsql)
        generic_module.mark_step_complete(generic_module.module_name,31)
        
        
    vsql="""select    
        BusinessUnitID,   
        Destination,   
        QuoteDate,   
        RowNum,   
        QuoteCount,   
        RankInMonth,   
        isOutlier,   
        coalesce   
        (   
            (   
                select    
                    avg(r.QuoteCount - r.BotCount)   
                from   
                    cbabq_stage.ETL113_process_cdgquote_destination_outlier r   
                where   
                    r.BusinessUnitID = t.BusinessUnitID and   
                    r.Destination = t.Destination and   
                    r.RowNum < t.RowNum and   
                    r.RowNum >= t.RowNum - 90 and   
                    r.isOutlier = 0   
                    and   
                    (   
                        BusinessUnitID not in (32) or    
                        (   
                            BusinessUnitID = 32 and   
                            (   
                                (   
                                    t.QuoteDate >= '2016-04-19' and   
                                    r.QuoteDate >= '2016-04-19'   
                                ) or   
                                t.QuoteDate < '2016-04-19'   
                            )   
                        )   
                    )   
            ),   
            t.QuoteCount   
        ) DestinationMA   
    from   
        cbabq_stage.ETL113_process_cdgquote_destination_outlier t"""
    #print(vsql)   
    if not generic_module.is_step_complete(generic_module.module_name,32):
        generic_module.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cbabq_stage','ETL113_process_cdgquote_destination_ma',None,vsql)
        generic_module.mark_step_complete(generic_module.module_name,32)    
        
        
    vsql="""select    
        BusinessUnitID,  
        duration,  
        QuoteDate,  
        row_number() over (partition by BusinessUnitID,duration order by QuoteDate) RowNum,  
        cast(count(distinct AnalyticsSessionID) as float64) QuoteCount,  
        cast(count(distinct case when BotFlag = 1 then AnalyticsSessionID else null end) as float64) BotCount  
    from  
        cbabq_stage.ETL113_process_cdgquote  
    group by  
        BusinessUnitID,  
        duration,  
        QuoteDate"""
    #print(vsql)   
    if not generic_module.is_step_complete(generic_module.module_name,33):
        generic_module.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cbabq_stage','ETL113_process_cdgquote_duration',None,vsql)
        generic_module.mark_step_complete(generic_module.module_name,33)    
        
        
        
    vsql="""select     
        *,   
        substr(cast(QuoteDate as string),1,7) QuoteMonth,   
        dense_rank() over (partition by BusinessUnitID,duration,substr(cast(QuoteDate as string),1,7) order by QuoteCount desc) RankInMonth,   
        count(QuoteDate) over (partition by BusinessUnitID,duration,substr(cast(QuoteDate as string),1,7)) RecordNum   
    from   
        cbabq_stage.ETL113_process_cdgquote_duration"""
    #print(vsql)   
    if not generic_module.is_step_complete(generic_module.module_name,34):
        generic_module.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cbabq_stage','ETL113_process_cdgquote_duration_rank',None,vsql)
        generic_module.mark_step_complete(generic_module.module_name,34)    
        
        
    vsql="""select   
        *,  
        case  
            when RecordNum < 3 then 0  
            when RankInMonth <= 0.15 * RecordNum then 1  
            when RankInMonth >= 0.75 * RecordNum then 1  
            else 0  
        end isOutlier  
    from  
        cbabq_stage.ETL113_process_cdgquote_duration_rank"""
    #print(vsql)   
    if not generic_module.is_step_complete(generic_module.module_name,35):
        generic_module.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cbabq_stage','ETL113_process_cdgquote_duration_outlier',None,vsql)
        generic_module.mark_step_complete(generic_module.module_name,35)    
        
    vsql="""select   
        BusinessUnitID,  
        duration,  
        QuoteDate,  
        RowNum,  
        QuoteCount,  
        RankInMonth,  
        isOutlier,  
        coalesce  
        (  
            (  
                select   
                    avg(r.QuoteCount - r.BotCount)  
                from  
                    cbabq_stage.ETL113_process_cdgquote_duration_outlier r  
                where  
                    r.BusinessUnitID = t.BusinessUnitID and  
                    r.duration = t.duration and  
                    r.RowNum < t.RowNum and  
                    r.RowNum >= t.RowNum - 90 and  
                    r.isOutlier = 0  
                    and  
                    (  
                        BusinessUnitID not in (32) or   
                        (  
                            BusinessUnitID = 32 and  
                            (  
                                (  
                                    t.QuoteDate >= '2016-04-19' and  
                                    r.QuoteDate >= '2016-04-19'  
                                ) or  
                                t.QuoteDate < '2016-04-19'  
                            )  
                        )  
                    )  
            ),  
            t.QuoteCount  
        ) durationMA  
    from  
        cbabq_stage.ETL113_process_cdgquote_duration_outlier t"""
    #print(vsql)   
    if not generic_module.is_step_complete(generic_module.module_name,36):
        generic_module.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cbabq_stage','ETL113_process_cdgquote_duration_ma',None,vsql)
        generic_module.mark_step_complete(generic_module.module_name,36)          
    
    vsql="""select    
        BusinessUnitID,  
        leadtime,  
        QuoteDate,  
        row_number() over (partition by BusinessUnitID,leadtime order by QuoteDate) RowNum,  
        cast(count(distinct AnalyticsSessionID) as float64) QuoteCount,  
        cast(count(distinct case when BotFlag = 1 then AnalyticsSessionID else null end) as float64) BotCount  
    from  
        cbabq_stage.ETL113_process_cdgquote  
    group by  
        BusinessUnitID,  
        leadtime,  
        QuoteDate"""
    #print(vsql)   
    if not generic_module.is_step_complete(generic_module.module_name,37):
        generic_module.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cbabq_stage','ETL113_process_cdgquote_leadtime',None,vsql)
        generic_module.mark_step_complete(generic_module.module_name,37)          
    
    vsql="""select   
        *,  
        substr(cast(QuoteDate as string),1,7) QuoteMonth,  
        dense_rank() over (partition by BusinessUnitID,leadtime,substr(cast(QuoteDate as string),1,7) order by QuoteCount desc) RankInMonth,  
        count(QuoteDate) over (partition by BusinessUnitID,leadtime,substr(cast(QuoteDate as string),1,7)) RecordNum  
    from  
        cbabq_stage.ETL113_process_cdgquote_leadtime"""
    #print(vsql)   
    if not generic_module.is_step_complete(generic_module.module_name,38):
        generic_module.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cbabq_stage','ETL113_process_cdgquote_leadtime_rank',None,vsql)
        generic_module.mark_step_complete(generic_module.module_name,38)          
    
    vsql="""select   
        *,  
        case  
            when RecordNum < 3 then 0  
            when RankInMonth <= 0.15 * RecordNum then 1  
            when RankInMonth >= 0.75 * RecordNum then 1  
            else 0  
        end isOutlier  
    from  
        cbabq_stage.ETL113_process_cdgquote_leadtime_rank"""
    #print(vsql)   
    if not generic_module.is_step_complete(generic_module.module_name,39):
        generic_module.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cbabq_stage','ETL113_process_cdgquote_leadtime_outlier',None,vsql)
        generic_module.mark_step_complete(generic_module.module_name,39)          
        
    vsql="""select    
        BusinessUnitID,   
        leadtime,   
        QuoteDate,   
        RowNum,   
        QuoteCount,   
        RankInMonth,   
        isOutlier,   
        coalesce   
        (   
            (   
                select    
                    avg(r.QuoteCount - r.BotCount)   
                from   
                    cbabq_stage.ETL113_process_cdgquote_leadtime_outlier r   
                where   
                    r.BusinessUnitID = t.BusinessUnitID and   
                    r.leadtime = t.leadtime and   
                    r.RowNum < t.RowNum and   
                    r.RowNum >= t.RowNum - 90 and   
                    r.isOutlier = 0   
                    and   
                    (   
                        BusinessUnitID not in (32) or    
                        (   
                            BusinessUnitID = 32 and   
                            (   
                                (   
                                    t.QuoteDate >= '2016-04-19' and   
                                    r.QuoteDate >= '2016-04-19'   
                                ) or   
                                t.QuoteDate < '2016-04-19'   
                            )   
                        )   
                    )   
            ),   
            t.QuoteCount   
        ) leadtimeMA   
    from cbabq_stage.ETL113_process_cdgquote_leadtime_outlier t"""
    #print(vsql)   
    if not generic_module.is_step_complete(generic_module.module_name,40):
        generic_module.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cbabq_stage','ETL113_process_cdgquote_leadtime_ma',None,vsql)
        generic_module.mark_step_complete(generic_module.module_name,40)                   
        
        
    vsql="""select     
        BusinessUnitID,   
        age,   
        QuoteDate,   
        row_number() over (partition by BusinessUnitID,age order by QuoteDate) RowNum,   
        cast(count(distinct AnalyticsSessionId) as float64) QuoteCount,   
        cast(count(distinct case when BotFlag = 1 then AnalyticsSessionId else null end) as float64) BotCount   
    from   
        cbabq_stage.ETL113_process_cdgquote   
    group by   
        BusinessUnitID,   
        age,   
        QuoteDate"""
    #print(vsql)   
    if not generic_module.is_step_complete(generic_module.module_name,41):
        generic_module.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cbabq_stage','ETL113_process_cdgquote_age',None,vsql)
        generic_module.mark_step_complete(generic_module.module_name,41)                   
        
        
    vsql="""select    
        *,  
        substr(cast(QuoteDate as string),1,7) QuoteMonth,  
        dense_rank() over (partition by BusinessUnitID,age,substr(cast(QuoteDate as string),1,7) order by QuoteCount desc) RankInMonth,  
        count(QuoteDate) over (partition by BusinessUnitID,age,substr(cast(QuoteDate as string),1,7)) RecordNum  
    from  
        cbabq_stage.ETL113_process_cdgquote_age"""
    #print(vsql)   
    if not generic_module.is_step_complete(generic_module.module_name,42):
        generic_module.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cbabq_stage','ETL113_process_cdgquote_age_rank',None,vsql)
        generic_module.mark_step_complete(generic_module.module_name,42)    
        
        
        
    vsql="""select    
        *,  
        case  
            when RecordNum < 3 then 0  
            when RankInMonth <= 0.15 * RecordNum then 1  
            when RankInMonth >= 0.75 * RecordNum then 1  
            else 0  
        end isOutlier  
    from  
        cbabq_stage.ETL113_process_cdgquote_age_rank"""
    #print(vsql)   
    if not generic_module.is_step_complete(generic_module.module_name,43):
        generic_module.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cbabq_stage','ETL113_process_cdgquote_age_outlier',None,vsql)
        generic_module.mark_step_complete(generic_module.module_name,43)    
        
    
    vsql="""select    
        BusinessUnitID,  
        age,  
        QuoteDate,  
        RowNum,  
        QuoteCount,  
        RankInMonth,  
        isOutlier,  
        coalesce  
        (  
            (  
                select   
                    avg(r.QuoteCount - r.BotCount)  
                from  
                    cbabq_stage.ETL113_process_cdgquote_age_outlier r  
                where  
                    r.BusinessUnitID = t.BusinessUnitID and  
                    r.age = t.age and  
                    r.RowNum < t.RowNum and  
                    r.RowNum >= t.RowNum - 90 and  
                    r.isOutlier = 0  
                    and  
                    (  
                        BusinessUnitID not in (32) or   
                        (  
                            BusinessUnitID = 32 and  
                            (  
                                (  
                                    t.QuoteDate >= '2016-04-19' and  
                                    r.QuoteDate >= '2016-04-19'  
                                ) or  
                                t.QuoteDate < '2016-04-19'  
                            )  
                        )  
                    )  
            ),  
            t.QuoteCount  
        ) ageMA  
    from  
        cbabq_stage.ETL113_process_cdgquote_age_outlier t"""
    #print(vsql)   
    if not generic_module.is_step_complete(generic_module.module_name,44):
        generic_module.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cbabq_stage','ETL113_process_cdgquote_age_ma',None,vsql)
        generic_module.mark_step_complete(generic_module.module_name,44) 
        
    vsql="""select    
        BusinessUnitID,  
        transactionhour,  
        QuoteDate,  
        row_number() over (partition by BusinessUnitID,transactionhour order by QuoteDate) RowNum,  
        cast(count(distinct AnalyticsSessionId) as float64) QuoteCount,  
        cast(count(distinct case when BotFlag = 1 then AnalyticsSessionId else null end) as float64) BotCount  
    from  
        cbabq_stage.ETL113_process_cdgquote  
    group by  
        BusinessUnitID,  
        transactionhour,  
        QuoteDate"""
    #print(vsql)   
    if not generic_module.is_step_complete(generic_module.module_name,45):
        generic_module.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cbabq_stage','ETL113_process_cdgquote_transactionhour',None,vsql)
        generic_module.mark_step_complete(generic_module.module_name,45) 
        
        
        
        
    vsql="""select    
        *,  
        substr(cast(QuoteDate as string),1,7) QuoteMonth,  --changed for bigquery  
        dense_rank() over (partition by BusinessUnitID,transactionhour,substr(cast(QuoteDate as string),1,7) order by QuoteCount desc) RankInMonth,  --changed for bigquery(aggregate function changed)   
        count(QuoteDate) over (partition by BusinessUnitID,transactionhour,substr(cast(QuoteDate as string),1,7)) RecordNum--changed for bigquery     
    from  
        cbabq_stage.ETL113_process_cdgquote_transactionhour"""
    #print(vsql)
    if not generic_module.is_step_complete(generic_module.module_name,46):
        generic_module.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cbabq_stage','ETL113_process_cdgquote_transactionhour_rank',None,vsql)
        generic_module.mark_step_complete(generic_module.module_name,46) 
    
    vsql="""  select 
        *,
        case
            when RecordNum < 3 then 0
            when RankInMonth <= 0.15 * RecordNum then 1
            when RankInMonth >= 0.75 * RecordNum then 1
            else 0
        end isOutlier
    from
        cbabq_stage.ETL113_process_cdgquote_transactionhour_rank"""
    #print(vsql)
    if not generic_module.is_step_complete(generic_module.module_name,47):
        generic_module.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cbabq_stage','ETL113_process_cdgquote_transactionhour_outlier',None,vsql)
        generic_module.mark_step_complete(generic_module.module_name,47) 


    #create table ETL113_process_cdgquote_transactionhour_ma
    vsql="""select 
        BusinessUnitID,
        --OutletGroup,
        transactionhour,
        QuoteDate,
        RowNum,
        QuoteCount,
        RankInMonth,
        isOutlier,
        coalesce
        (
            (
                select 
                    avg(r.QuoteCount - r.BotCount)
                from
                    cbabq_stage.ETL113_process_cdgquote_transactionhour_outlier r
                where
                    r.BusinessUnitID = t.BusinessUnitID and
                    --r.OutletGroup = t.OutletGroup and
                    r.transactionhour = t.transactionhour and
                    --hard-coded MA window, no sub-sub-query in redshift
                    r.RowNum < t.RowNum and
                    r.RowNum >= t.RowNum - 90 and
                    r.isOutlier = 0
                    --migration aware part
                    and
                    (
                        BusinessUnitID not in (32) or 
                        (
                            BusinessUnitID = 32 and
                            (
                                (
                                    t.QuoteDate >= '2016-04-19' and
                                    r.QuoteDate >= '2016-04-19'
                                ) or
                                t.QuoteDate < '2016-04-19'
                            )
                        )
                    )
            ),
            t.QuoteCount
        ) transactionhourMA
    from
        cbabq_stage.ETL113_process_cdgquote_transactionhour_outlier t"""
    #print(vsql)
    if not generic_module.is_step_complete(generic_module.module_name,48):
        generic_module.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cbabq_stage','ETL113_process_cdgquote_transactionhour_ma',None,vsql)
        generic_module.mark_step_complete(generic_module.module_name,48) 

    #create table ETL113_process_ma_cdgquote
    vsql=""" select
        a.platformversion,
        a.analyticssessionid,
        a.BusinessUnitID,
        a.QuoteDate,
        case 
            when DestinationMA = 0 then 0 
            else (b.QuoteCount - DestinationMA) / DestinationMA
        end as DestinationMA,
        case 
            when DurationMA = 0 then 0 
            /*specific treatment for RACV*/
            when (a.BusinessUnitID = 14) and (mod(a.Duration,7) = 0) then (c.QuoteCount - (DurationMA / 8)) / (DurationMA / 8)--mod operator used for bigquery
            when (a.BusinessUnitID = 14) and (mod(a.Duration,30) = 0) then (c.QuoteCount - (DurationMA / 8)) / (DurationMA / 8)--mod operator used for bigquery

            when (a.Duration >= 28) and (mod(a.Duration,7) = 0) then (c.QuoteCount - (DurationMA / 8)) / (DurationMA / 8)
            when (a.Duration > 30) and (mod(a.Duration,30) = 0) then (c.QuoteCount - (DurationMA / 8)) / (DurationMA / 8)
            else (c.QuoteCount - DurationMA) / DurationMA
        end as DurationMA,
        case 
            when LeadTimeMA = 0 then 0 
            else (d.QuoteCount - LeadTimeMA) / LeadTimeMA
        end as LeadTimeMA,
        case 
            when AgeMA = 0 then 0 
            /*specific treatment for RACV*/
            when (a.BusinessUnitID = 14) and (a.Age >= 70) then (e.QuoteCount - (AgeMA / 4)) / (AgeMA / 4)
            else (e.QuoteCount - AgeMA) / AgeMA
        end as AgeMA,
        case 
            when TransactionHourMA = 0 then 0 
            /*specific treatment for migrated business unit (impulse 1 -> 2) due to timezone diff*/
            /*MB*/
            when (a.BusinessUnitID in (32)) then (f.QuoteCount - TransactionHourMA) / TransactionHourMA / 6
            else (f.QuoteCount - TransactionHourMA) / TransactionHourMA
        end as TransactionHourMA,
        ssq.SameSessionQuoteCount,
        ConvertedFlag
    from
        cbabq_stage.ETL113_process_cdgquote a
        left join cbabq_stage.ETL113_process_cdgquote_destination_ma b on 
            a.BusinessUnitID = b.BusinessUnitID and
            --a.OutletGroup = b.OutletGroup and
            a.QuoteDate = b.QuoteDate and
            a.Destination = b.Destination
        left join cbabq_stage.ETL113_process_cdgquote_duration_ma c on 
            a.BusinessUnitID = c.BusinessUnitID and
            --a.OutletGroup = c.OutletGroup and
            a.QuoteDate = c.QuoteDate and
            a.duration = c.duration
        left join cbabq_stage.ETL113_process_cdgquote_leadtime_ma d on 
            a.BusinessUnitID = d.BusinessUnitID and
            --a.OutletGroup = d.OutletGroup and
            a.QuoteDate = d.QuoteDate and
            a.leadtime = d.leadtime
        left join cbabq_stage.ETL113_process_cdgquote_age_ma e on 
            a.BusinessUnitID = e.BusinessUnitID and
            --a.OutletGroup = e.OutletGroup and
            a.QuoteDate = e.QuoteDate and
            a.age = e.age
        left join cbabq_stage.ETL113_process_cdgquote_transactionhour_ma f on 
            a.BusinessUnitID = f.BusinessUnitID and
            --a.OutletGroup = f.OutletGroup and
            a.QuoteDate = f.QuoteDate and
            a.transactionhour = f.transactionhour
        left join
        (
            select 
               -- CampaignSessionID, 
                AnalyticsSessionID, 
                count(1) SameSessionQuoteCount
            from
                cbabq_stage.ETL113_process_cdgquote
            where
               isPrimaryTraveller = 1
            group by
                --CampaignSessionID
                AnalyticsSessionID
        ) ssq on 
            --a.CampaignSessionID = ssq.CampaignSessionID
            a.AnalyticsSessionID=ssq.AnalyticsSessionID
    where
        --datetime(a.QuoteDate) >= datetime_add(current_datetime("Australia/Sydney"), interval - 15 day)
        datetime(a.QuoteDate) >= datetime_add(cast(@start_date as datetime), interval - 15 day)
        and datetime(a.QuoteDate) <= cast(@end_date as datetime)
        """
    #print(vsql)
    if not generic_module.is_step_complete(generic_module.module_name,49):
        generic_module.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cbabq_stage','ETL113_process_ma_cdgquote',None,vsql)
        generic_module.mark_step_complete(generic_module.module_name,49) 
        
        
        
    vsql=""" select
        platformversion,
        analyticssessionid,
        DestinationMA,
        DurationMA,
        LeadTimeMA,
        AgeMA,
        TransactionHourMA,
        SameSessionQuoteCount,
        ConvertedFlag,
        max(
            case 
                when ConvertedFlag = 1 then 0
                
                /*whitelist impulse platform migration*/
                /*MB*/
                when BusinessUnitID = 32 and QuoteDate between '2016-04-19' and '2016-05-31' then 0
                /*AHM*/
                when BusinessUnitID = 75 and QuoteDate < '2016-12-01' then 0

                when
                    (
                        (
                            case 
                                when LeadTimeMA >= 3*6 then 2 
                                when LeadTimeMA >= 3 then 1 
                                when LeadTimeMA > 1 then 0.125
                                else 0 
                            end
                        ) * 2.0 + 
                        (
                            case 
                                when DurationMA >= 3*8 then 3
                                when DurationMA >= 3*6 then 2 
                                when DurationMA >= 3*3 then 1.5
                                when DurationMA >= 3 then 1 
                                when DurationMA > 1 then 0.25
                                else 0 
                            end
                        ) * 1.0 + 
                        (
                            case
                                when AgeMA >= 3*6 then 3
                                when AgeMA >= 3*3 then 2 
                                when AgeMA >= 3 then 1 
                                when AgeMA > 1 then 0.25
                                else 0 
                            end
                        ) * 1 + 
                        (
                            case 
                                when DestinationMA >= 3*6 then 2 
                                when DestinationMA >= 3 then 1 
                                when DestinationMA > 1 then 0.5
                                else 0 
                            end
                        ) * 0.5 +
                        (
                            case 
                                when TransactionHourMA >= 3*6 then 2 
                                when TransactionHourMA >= 3 then 1 
                                else 0 
                            end
                        ) * 0.25
                    ) > 2 then 1
                else 0
            end 
        ) BotFlag
    from
        cbabq_stage.ETL113_process_ma_cdgquote a
    group by
        platformversion,
        analyticssessionid,
        DestinationMA,
        DurationMA,
        LeadTimeMA,
        AgeMA,
        TransactionHourMA,
        SameSessionQuoteCount,
        ConvertedFlag"""
    #print(vsql)
    if not generic_module.is_step_complete(generic_module.module_name,50):
        generic_module.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cbabq_stage','ETL113_process_flagged_cdgquote',None,vsql)
        generic_module.mark_step_complete(generic_module.module_name,50) 
        
    vsql="""SELECT
              *
            FROM
              cbabq_prod.impulse_quote_ma
            WHERE concat(cast(analyticssessionid as string),'-',cast(platformversion as string)) not in
             (select concat(cast(analyticssessionid as string),'-',cast(platformversion as string)) from cbabq_stage.ETL113_process_flagged_cdgquote)
            UNION ALL
            SELECT
              *,cast(current_datetime("Australia/Sydney") as timestamp)
            --current_timestamp()
            FROM
              cbabq_stage.ETL113_process_flagged_cdgquote"""
    #print(vsql)
    if not generic_module.is_step_complete(generic_module.module_name,51):
        generic_module.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cbabq_prod','impulse_quote_ma',None,vsql)
        generic_module.mark_step_complete(generic_module.module_name,51) 
        
    vsql="""with
            stg_bot_impulse as (
             select
	            PlatformVersion,
	            AnalyticsSessionID,
	            DestinationMA as DestinationMAFactor,
	            DurationMA as DurationMAFactor,
	            LeadTimeMA as LeadTimeMAFactor,
	            AgeMA as AgeMAFactor,
	            TransactionHourMA as TransactionHourMAFactor,
	            SameSessionQuoteCount,
	            ConvertedFlag,
	            BotFlag
            from
                 cbabq_prod.impulse_quote_ma
            where
                date(UpdateTime) >= date(current_datetime("Australia/Sydney")) 
               and BotFlag=1)
            select PlatformVersion,AnalyticsSessionID,DestinationMAFactor,DurationMAFactor,LeadTimeMAFactor,AgeMAFactor,TransactionHourMAFactor,SameSessionQuoteCount,ConvertedFlag,BotFlag from cbabq_prod.botQuoteImpulse where  concat(cast(analyticssessionid as string),'-',cast(platformversion as string)) not in (select concat(cast(analyticssessionid as string),'-',cast(platformversion as string)) from stg_bot_impulse)
            union all
            select * from stg_bot_impulse"""
    #print(vsql)
    if not generic_module.is_step_complete(generic_module.module_name,'51a'):
        generic_module.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cbabq_prod','botQuoteImpulse',None,vsql)
        generic_module.mark_step_complete(generic_module.module_name,'51a') 


    #create table ETL113_etl_impulse2cdgQuote2        
    vsql="""select 
        '' GroupIndex,
        cast(q.QuoteDateUTC as date) QuoteDate,
        '' OutletAlphaKey,
        '' CurrencyCode,
        --ifnull(trim(q.RegionName), '') Area,--pull this info from cdgRegion region table for this.
		ifnull(trim(cr.Region), '') Area,--pull this info from cdgRegion region table for this.
        --ifnull(trim(q.Destination), '') Destination,
		ifnull(trim(icqd.DestinationCountryCode), '') Destination,
        '' as PurchasePath,
        --ifnull(trim(pk.ProductKey), '') ProductKey,
	   	ifnull(trim(cast(q.quoteProductid as string)),'')  ProductKey,
        ifnull(trim(p.ProductCode), '') ProductCode,
        ifnull(trim(p.Product), '') ProductName,
        ifnull(trim(p.PlanCode), '') PlanCode,
        --ifnull(datediff(day, q.TripStart, q.TripEnd) + 1, 0) Duration,
        --ifnull(datetime_diff(q.TripEnd,q.TripStart,day ) + 1, 0) Duration,
		q.QuoteDuration Duration,
        /*case
            --when ifnull(datediff(day, q.QuoteDate, q.TripStart), 0) < 0 then 0
             when ifnull(datetime_diff(q.TripStart,q.QuoteDate,day), 0) < 0 then 0
            --else ifnull(datediff(day, q.QuoteDate, q.TripStart), 0)
            	else ifnull(datetime_diff(q.TripStart,q.QuoteDate,day), 0)
        end LeadTime,*/
		timestamp_diff(q.tripstartdate,q.quotedateUTC,day) as LeadTime,                                       
        trv.TravellerAge as PrimaryCustomerAge,
        trvage.YoungestAge,
        trvage.OldestAge,
        --ifnull(q.ChildCount, 0) NumberOfChildren,
		ifnull(ss.numchildren, 0) NumberOfChildren,
        --ifnull(q.AdultCount, 0) NumberOfAdults,
		ifnull(ss.numadults, 0) NumberOfAdults,
        --ifnull(q.TotalGrossPremium, 0) QuotedPrice,
		ifnull(q.quoteGross, 0) QuotedPrice,
        count(distinct q.SessionID) as  QuoteCount,
        --case when q.factQuoteID is null then 0
		case when q.savedQuoteID is null then 0
             else 1
        end QuoteSessionCount,
        --case when q.TotalGrossPremium is not null then 1
		case when q.quoteGross is not null then 1
			 else 0
		end as QuoteWithPriceCount,
        --case when s.IsPolicyPurchased = 1 then 1
		case when q.policies is not null then 1
			 else 0
		end as ConvertedQuoteCount,
        trv.TravellerAge,
        trv.IsPrimaryTraveller,
        ss.Domain,
		--q.Domain,
        --q.BusinessUnitName as BusinessUnit,
		cbu.BusinessUnit as BusinessUnit,
        --'' as Channel,
		q.cbaChannelId as Channel,
		--s.AffiliateCode as AlphaCode
		q.issuerAffiliateCode as AlphaCode
    --into #cdgquote2
    from
		--[db-au-cmdwh].dbo.cdgfactSession s
		cbabq_prod.impulse_cba_quotes q
    --    left outer join cbabq_prod.impulse_quote_ma s
		--on s.AnalyticsSessionId=q.sessionid
		inner join cbabq_prod.impulse_cba_sessions ss
		on ss.sessiontoken=q.sessiontoken
		left outer join										--isPrimaryTraveller is assumed to be the first entry denoted by the minimum session ID
		(
			select SessionID,TravellerAge,1 as isPrimaryTraveller from (
			select --top 1
				SessionID,age as TravellerAge,row_number() over (partition by sessionid order by travelleridentifier) as row_number
        --t.Age as TravellerAge,
			from
				cbabq_prod.impulse_cba_quote_travellers t
			) where row_number =1
		) trv
    on trv.SessionID = q.SessionId
		--outer apply													
		left outer join
		(
			select t.SessionID,
				min(t.Age) as YoungestAge,
				max(t.Age) as OldestAge
			from
				cbabq_prod.impulse_cba_quote_travellers t
			--where
				--t.SessionID = s.factSessionID			
				group by t.SessionID
		) trvage
		on trvage.SessionID = q.SessionId
        --outer apply													
        left outer join
        (
            select --top 1 
            qp.ProductID,
                min(ProductKey) ProductKey
            from
                cmbq_prod.usrCDGQuoteProduct qp
                group by qp.ProductID
            --where
                --qp.ProductID = q.ProductID
        ) pk
        on pk.ProductID = q.QuoteProductID
		left outer join
		cmbq_prod.cdgBusinessUnit cbu
		on cbu.BusinessUnitId=cast(q.businessunitid as int64)
		left outer join
		cmbq_prod.cdgRegion cr
		on cr.RegionId=cast(q.RegionId as int64)
		left outer join
		cbabq_prod.impulse_cba_quote_destinations icqd
		on icqd.sessionId=q.sessionId
		left outer join
		cmbq_prod.cdgProduct p
		on p.productId=q.QuoteProductID
    where
       date(q.QuoteDateUTC) >= cast(@start_date as date) 
       and date(q.QuoteDateUTC) <= cast(@end_date as date) 
		--s.botflag=1        
    and q.SessionID in 
        (
            select 
                --null
                --r.factSessionID
                qb.AnalyticsSessionID
            from
                cbabq_prod.botQuoteImpulse qb
                --inner join cmbq_prod.cdgfactSession r on
                  --  qb.PlatformVersion = 3 and
                    --r.factSessionID = qb.AnalyticsSessionID and
                    where
                    qb.BotFlag = 1
            --where
              --  r.factSessionID = q.SessionID
        )
	group by
        cast(q.QuoteDateUTC as date),
        --ifnull(trim(q.CurrencyCode), ''),
		        --'' CurrencyCode,
        --ifnull(trim(q.RegionName), ''),
				ifnull(trim(cr.Region), '') ,--pull this info from cdgRegion region table for this.
        --ifnull(trim(q.Destination), ''),
				ifnull(trim(icqd.DestinationCountryCode),''),
        --ifnull(trim(pk.ProductKey), ''),
				ifnull(trim(cast(q.quoteProductid as string)),'') ,
        --ifnull(trim(q.ProductCode), ''),
        --ifnull(trim(q.ProductName), ''),
        --ifnull(trim(q.PlanCode), ''),
        ifnull(trim(p.ProductCode), '') ,
        ifnull(trim(p.Product), '') ,
        ifnull(trim(p.PlanCode), '') ,
        --ifnull(datediff(day, q.TripStart, q.TripEnd) + 1, 0),
        --ifnull(datetime_diff(q.TripEnd,q.TripStart,day) + 1, 0),
		q.QuoteDuration ,
        /*case
            --when ifnull(datediff(day, q.QuoteDate, q.TripStart), 0) < 0 then 0
            	when ifnull(datetime_diff(q.TripStart,q.QuoteDate,day), 0) < 0 then 0
            --else ifnull(datediff(day, q.QuoteDate, q.TripStart), 0)
            	else ifnull(datetime_diff(q.TripStart,q.QuoteDate,day), 0)
        end,*/
		timestamp_diff(q.tripstartdate,q.quotedateUTC,day)  ,                                       
        trv.TravellerAge,
        trvage.YoungestAge,
        trvage.OldestAge,
        --ifnull(q.ChildCount, 0),
		ifnull(ss.numchildren, 0) ,
        --ifnull(q.AdultCount, 0),
        ifnull(ss.numadults, 0),
        --ifnull(q.TotalGrossPremium, 0),
		ifnull(q.quoteGross, 0) ,
        --case when q.factQuoteID is null then 0
		case when q.savedQuoteID is null then 0
             else 1
        end,
        --case when q.TotalGrossPremium is not null then 1
		case when q.quoteGross is not null then 1
			 else 0
		end,
        --case when s.IsPolicyPurchased = 1 then 1
		case when q.policies is not null then 1
			 else 0
		end,
        trv.TravellerAge,
        trv.IsPrimaryTraveller,
        ss.Domain,
		--q.Domain,
        --q.BusinessUnitName,
		cbu.BusinessUnit  ,
		q.cbaChannelId,
		--s.AffiliateCode
		q.issuerAffiliateCode"""
    #print(vsql)
    if not generic_module.is_step_complete(generic_module.module_name,52):
        #generic_module.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cbabq_stage','ETL113_impulse_QuoteSummaryBot',None,vsql)
        generic_module.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cbabq_stage','ETL113_etl_impulse2cdgQuote2',None,vsql)
        generic_module.mark_step_complete(generic_module.module_name,52) 
        
    vsql="""--create rolled2 table with impulse version 2 data
  select 
        QuoteDate,
        3 QuoteSource,
        o.CountryKey,
        o.CompanyKey,
        t.OutletAlphaKey,
        --convert(varchar(10), null) StoreCode,
        cast(null as string) StoreCode,
        u.UserKey,
        --convert(varchar(41), null) CRMUserKey,
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
        NumberOfChildren,
        NumberOfAdults,
        NumberOfChildren + NumberOfAdults NumberOfPersons,
        QuoteCount,
        QuoteSessionCount,
        QuoteWithPriceCount,
        ConvertedQuoteCount,
        QuotedPrice,
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
    --into #rolled2
    from
		--#cdgQuote2 t
		cbabq_stage.ETL113_etl_impulse2cdgQuote2 t
        --cross apply 
        --join --commented for testing
         left outer join
        /*(
            select top 1
                o.CountryKey,
                o.CompanyKey,
                o.OutletKey
            from*/
                cbabq_prod.penOutlet o --with(nolock)
            --where
            on
                o.OutletAlphaKey = t.OutletAlphaKey and
                o.OutletStatus = 'Current'
        --) o
        --outer apply
        left outer join
        /*(
            select top 1 
                UserKey
            from*/
            (select distinct OutletKey,UserKey from cbabq_prod.penUser where Login = 'webuser') u --with(nolock)
            --where                
            on
                u.OutletKey = o.OutletKey --and
                --u.[Login] = 'webuser'
        --) u"""
    #print(vsql)
    if not generic_module.is_step_complete(generic_module.module_name,53):
        generic_module.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cbabq_stage','ETL113_etl_rolled_impulse2',None,vsql)
        generic_module.mark_step_complete(generic_module.module_name,53) 
        
    vsql="""insert into cbabq_stage.ETL113_etl_penQuoteSummaryBot
            (QuoteDate	,
            QuoteSource	,
            CountryKey	,
            CompanyKey	,
            OutletAlphaKey	,
            StoreCode	,
            UserKey	,
            CRMUserKey	,
            SaveStep	,
            CurrencyCode	,
            Area	,
            Destination	,
            PurchasePath	,
            ProductKey	,
            ProductCode	,
            ProductName	,
            PlanCode	,
            PlanName	,
            PlanType	,
            MaxDuration	,
            Duration	,
            LeadTime	,
            Excess	,
            CompetitorName	,
            CompetitorGap	,
            PrimaryCustomerAge	,
            PrimaryCustomerSuburb	,
            PrimaryCustomerState	,
            YoungestAge	,
            OldestAge	,
            NumberOfChildren	,
            NumberOfAdults	,
            NumberOfPersons	,
            QuotedPrice	,
            QuoteSessionCount	,
            QuoteCount	,
            QuoteWithPriceCount	,
            SavedQuoteCount	,
            ConvertedCount	,
            ExpoQuoteCount	,
            AgentSpecialQuoteCount	,
            PromoQuoteCount	,
            UpsellQuoteCount	,
            PriceBeatQuoteCount	,
            QuoteRenewalCount	,
            CancellationQuoteCount	,
            LuggageQuoteCount	,
            MotorcycleQuoteCount	,
            WinterQuoteCount	,
            EMCQuoteCount	
            )
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
                    cast(PlanName as string),                                         
                    PlanType,                                                                   
                    MaxDuration,     
                    --Duration,                                                      
                    cast(Duration as int64),                --changed on 20190305 by Ratnesh
                    LeadTime,                                                                   
                    Excess,                                                                     
                    cast(CompetitorName as string),                                   
                    CompetitorGap,                                                              
                    PrimaryCustomerAge,                                                         
                    cast(PrimaryCustomerSuburb as string),                                                      
                    cast(PrimaryCustomerState as string),                                                       
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
                    --#rolled
                    cbabq_stage.ETL113_etl_rolled_impulse2"""
    #print(vsql)
    if not generic_module.is_step_complete(generic_module.module_name,54):
        #generic_module.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cbabq_stage','ETL113_etl_rolled_impulse2',None,vsql)
        query_output=generic_module.execute_sql_bigquery_and_return_result('cover-more-data-and-analytics','cbabq_workspace',vsql,write_disposition=None)
        #query_output.result()
        #print(query_output)
        #for i in query_output:
        #    print(i)
        generic_module.mark_step_complete(generic_module.module_name,54) 
        
    #downloading the QuoteSummaryBot Table. This will be a non partitioned small sized table containing aggregated data.
    if not generic_module.is_step_complete(generic_module.module_name,55):
        bq_query_result=generic_module.execute_sql_bigquery_and_return_result('cover-more-data-and-analytics','cbabq_workspace','select * from cbabq_stage.ETL113_etl_penQuoteSummaryBot')
        df_bq_query_result=bq_query_result.to_dataframe()
        #print(df_bq_query_result.head())
        #for row in bq_query_result:
        #    print(row)
        engine = create_engine('mssql+pymssql://@ULDWH02:1433/db-au-stage', pool_recycle=3600, pool_size=5)
        df_bq_query_result.to_sql('etl_penQuoteSummaryBotCBA', engine, if_exists='replace',index=False,chunksize=100)
        generic_module.mark_step_complete(generic_module.module_name,55)
        
        
    if not generic_module.is_step_complete(generic_module.module_name,56):
        cursor_ref=generic_module.create_sql_cursor('uldwh02','db-au-stage')#processed for last 14 days.
        """vsql=--declare 
               -- @start date
                
           -- select 
                @start = dateadd(day, -14, cast('+generic_module.start_date+' as date))"""
        vsql="""exec [db-au-stage].dbo.etlsp_ETL113_factQuoteSummaryBotCBA   @DateRange = '_User Defined',
                                        @StartDate = '"""+generic_module.start_date+"""',
                                        @EndDate ='"""+generic_module.end_date+"""'"""
        print(vsql)
        cursor_ref.execute(vsql);
        cursor_ref.commit()
        generic_module.mark_step_complete(generic_module.module_name,56)
        
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