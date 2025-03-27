"""
Author : Ratnesh
Dept   : Data Analytics & Innovation, Covermore Group
Date   : 2018-02-01
Usage  : "extract_bq_data.py <run_mode> <start_date> <end_date>"
          "python extract_bq_data.py interval=last3days"
          "python extract_bq_data.py start_date=2018-12-01 end_date=2019-01-01" #this will extract data for the month of December-2018
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
#import voice_analytics_generic_module112
import os
#import uuid
from sqlalchemy import create_engine

#enable following line if running on local workstation.
#sys.path.append(r'\\ULDWH02\ETL\Python Scripts')
#sys.path.append(r'\\aust.covermore.com.au\user_data\NorthSydney_Users\ratneshs\Home\Projects\generic\\')
#import environment_settings112
import generic_module112
generic_module112.set_module(os.path.basename(__file__))

#-------------------------------------------------------------------------------------
#Main processing block
try:
    print('***************************************************************************')
    print('Main processing block started at:'+str(datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S')))
    generic_module112.validate_parameters(0)#There should be atleast one parameter value passed.
    generic_module112.parse_parameters(**dict(arg.split('=') for arg in sys.argv[1:]))#Parse rest of the parameters.
    #generic_module112.connect_db()
    ####################generic_module112.extract_historical_oneoff_bq_data()#ONLY ENABLE after a written APPROVAL.
    #generic_module112.extract_bq_data()

    #create table ETL112_process_penguinquote
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
        cmbq_prod.penQuote q
        left join cmbq_prod.penguin_quote_ma b on
            q.QuoteKey = b.QuoteKey
            left outer join cmbq_prod.penOutlet o
            on q.OutletAlphaKey=o.OutletAlphaKey
            left outer join
             (select * from (    select 
                QuoteCountryKey,Age TravellerAge,row_number() over(partition by QuoteCountryKey order by QuoteCustomerID) row_number
            from
                cmbq_prod.penQuoteCustomer --with(nolock)
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
    if not generic_module112.is_step_complete(generic_module112.module_name,1):
        generic_module112.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cmbq_stage','ETL112_process_penguinquote',None,vsql)
        generic_module112.mark_step_complete(generic_module112.module_name,1)
    
    #create table ETL112_process_penguinquote_destination
    vsql="""select 
        BusinessUnitID,
        OutletGroup,
        Destination,
        QuoteDate,
        row_number() over (partition by BusinessUnitID,OutletGroup,Destination order by QuoteDate) RowNum,
        count(distinct QuoteKey)  QuoteCount,
        count(distinct case when BotFlag = 1 then QuoteKey else null end) BotCount
    from
        cmbq_stage.ETL112_process_penguinquote
    group by
        BusinessUnitID,
        OutletGroup,
        Destination,
        QuoteDate"""
    #print(vsql)
    if not generic_module112.is_step_complete(generic_module112.module_name,2):
        generic_module112.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cmbq_stage','ETL112_process_penguinquote_destination',None,vsql)
        generic_module112.mark_step_complete(generic_module112.module_name,2)
    
    #create table ETL112_process_penguinquote_destination_rank
    vsql="""select 
        *,
        substr(cast(QuoteDate as string),1,7) QuoteMonth,--changed for bigquery
        dense_rank() over (partition by BusinessUnitID,OutletGroup,Destination,substr(cast(QuoteDate as string),1,7) order by QuoteCount desc) RankInMonth,--changed for bigquery (aggregate function changed)
        count(QuoteDate) over (partition by BusinessUnitID,OutletGroup,Destination,substr(cast(QuoteDate as string),1,7)) RecordNum--changed for bigquery
    from
        cmbq_stage.ETL112_process_penguinquote_destination"""
    #print(vsql)
    if not generic_module112.is_step_complete(generic_module112.module_name,3):
        generic_module112.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cmbq_stage','ETL112_process_penguinquote_destination_rank',None,vsql)
        generic_module112.mark_step_complete(generic_module112.module_name,3)    
       
    
    #create table ETL112_process_penguinquote_destination_outlier
    vsql="""select 
        *,
        case
            when RecordNum < 3 then 0
            when RankInMonth <= 0.15 * RecordNum then 1
            when RankInMonth >= 0.75 * RecordNum then 1
            else 0
        end isOutlier
    from
        cmbq_stage.ETL112_process_penguinquote_destination_rank"""
    #print(vsql)
    if not generic_module112.is_step_complete(generic_module112.module_name,4):
        generic_module112.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cmbq_stage','ETL112_process_penguinquote_destination_outlier',None,vsql)
        generic_module112.mark_step_complete(generic_module112.module_name,4)


    #create table ETL112_process_penguinquote_destination_ma
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
                    cmbq_stage.ETL112_process_penguinquote_destination_outlier r
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
        cmbq_stage.ETL112_process_penguinquote_destination_outlier t"""
    #print(vsql)
    if not generic_module112.is_step_complete(generic_module112.module_name,5):
        generic_module112.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cmbq_stage','ETL112_process_penguinquote_destination_ma',None,vsql)
        generic_module112.mark_step_complete(generic_module112.module_name,5)        



    #create table ETL112_process_penguinquote_duration
    vsql=""" select 
        BusinessUnitID,
        OutletGroup,
        duration,
        QuoteDate,
        row_number() over (partition by BusinessUnitID,OutletGroup,duration order by QuoteDate) RowNum,
        cast(count(distinct QuoteKey) as float64) QuoteCount,
        cast(count(distinct case when BotFlag = 1 then QuoteKey else null end) as float64) BotCount
    from
        cmbq_stage.ETL112_process_penguinquote
    group by
        BusinessUnitID,
        OutletGroup,
        duration,
        QuoteDate"""
    #print(vsql)
    if not generic_module112.is_step_complete(generic_module112.module_name,6):
        generic_module112.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cmbq_stage','ETL112_process_penguinquote_duration',None,vsql)
        generic_module112.mark_step_complete(generic_module112.module_name,6)

    #create table ETL112_process_penguinquote_duration_rank
    vsql="""select 
        *,
        substr(cast(QuoteDate as string),1,7) QuoteMonth,--changed for bigquery
        dense_rank() over (partition by BusinessUnitID,OutletGroup,duration,substr(cast(QuoteDate as string),1,7) order by QuoteCount desc) RankInMonth,--changed for bigquery aggregate function changed
        count(QuoteDate) over (partition by BusinessUnitID,OutletGroup,duration,substr(cast(QuoteDate as string),1,7)) RecordNum--cahnged for bigquery
    from
        cmbq_stage.ETL112_process_penguinquote_duration"""
    #print(vsql)
    if not generic_module112.is_step_complete(generic_module112.module_name,7):
        generic_module112.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cmbq_stage','ETL112_process_penguinquote_duration_rank',None,vsql)
        generic_module112.mark_step_complete(generic_module112.module_name,7)


    #create table ETL112_process_penguinquote_duration_outlier
    vsql="""select 
        *,
        case
            when RecordNum < 3 then 0
            when RankInMonth <= 0.15 * RecordNum then 1
            when RankInMonth >= 0.75 * RecordNum then 1
            else 0
        end isOutlier
    from
        cmbq_stage.ETL112_process_penguinquote_duration_rank"""
    #print(vsql)
    if not generic_module112.is_step_complete(generic_module112.module_name,8):
        generic_module112.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cmbq_stage','ETL112_process_penguinquote_duration_outlier',None,vsql)
        generic_module112.mark_step_complete(generic_module112.module_name,8)
        
    #create table ETL112_process_penguinquote_duration_ma
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
                    cmbq_stage.ETL112_process_penguinquote_duration_outlier r
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
        cmbq_stage.ETL112_process_penguinquote_duration_outlier t"""
    #print(vsql)
    if not generic_module112.is_step_complete(generic_module112.module_name,9):
        generic_module112.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cmbq_stage','ETL112_process_penguinquote_duration_ma',None,vsql)
        generic_module112.mark_step_complete(generic_module112.module_name,9)
        
    #create table ETL112_process_penguinquote_leadtime
    vsql="""   select 
        BusinessUnitID,
        OutletGroup,
        leadtime,
        QuoteDate,
        row_number() over (partition by BusinessUnitID,OutletGroup,leadtime order by QuoteDate) RowNum,
        cast(count(distinct QuoteKey) as float64) QuoteCount,
        cast(count(distinct case when BotFlag = 1 then QuoteKey else null end) as float64) BotCount
    from
        cmbq_stage.ETL112_process_penguinquote
    group by
        BusinessUnitID,
        OutletGroup,
        leadtime,
        QuoteDate"""
    #print(vsql)
    if not generic_module112.is_step_complete(generic_module112.module_name,10):
        generic_module112.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cmbq_stage','ETL112_process_penguinquote_leadtime',None,vsql)
        generic_module112.mark_step_complete(generic_module112.module_name,10)
        
    #create table ETL112_process_penguinquote_leadtime_rank
    vsql="""   select 
        *,
        substr(cast(QuoteDate as string),1,7) QuoteMonth,--changed for bigquery
        dense_rank() over (partition by BusinessUnitID,OutletGroup,leadtime,substr(cast(QuoteDate as string),1,7) order by QuoteCount desc) RankInMonth,--changed for big query (aggregate function changed)
        count(QuoteDate) over (partition by BusinessUnitID,OutletGroup,leadtime,substr(cast(QuoteDate as string),1,7)) RecordNum--changed for bigquery
    from
        cmbq_stage.ETL112_process_penguinquote_leadtime"""
    #print(vsql)
    if not generic_module112.is_step_complete(generic_module112.module_name,11):
        generic_module112.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cmbq_stage','ETL112_process_penguinquote_leadtime_rank',None,vsql)
        generic_module112.mark_step_complete(generic_module112.module_name,11)
        
    #create table ETL112_process_penguinquote_leadtime_outlier
    vsql="""select 
        *,
        case
            when RecordNum < 3 then 0
            when RankInMonth <= 0.15 * RecordNum then 1
            when RankInMonth >= 0.75 * RecordNum then 1
            else 0
        end isOutlier
    from
        cmbq_stage.ETL112_process_penguinquote_leadtime_rank"""
    #print(vsql)
    if not generic_module112.is_step_complete(generic_module112.module_name,12):
        generic_module112.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cmbq_stage','ETL112_process_penguinquote_leadtime_outlier',None,vsql)
        generic_module112.mark_step_complete(generic_module112.module_name,12)
        
    #create table ETL112_process_penguinquote_leadtime_ma
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
                    cmbq_stage.ETL112_process_penguinquote_leadtime_outlier r
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
        cmbq_stage.ETL112_process_penguinquote_leadtime_outlier t"""
    #print(vsql)
    if not generic_module112.is_step_complete(generic_module112.module_name,13):
        generic_module112.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cmbq_stage','ETL112_process_penguinquote_leadtime_ma',None,vsql)
        generic_module112.mark_step_complete(generic_module112.module_name,13)
        
    #create table ETL112_process_penguinquote_age
    vsql="""    select 
        BusinessUnitID,
        OutletGroup,
        age,
        QuoteDate,
        row_number() over (partition by BusinessUnitID,OutletGroup,age order by QuoteDate) RowNum,
        cast(count(distinct QuoteKey) as float64) QuoteCount,
        cast(count(distinct case when BotFlag = 1 then QuoteKey else null end) as float64) BotCount
    from
        cmbq_stage.ETL112_process_penguinquote
    group by
        BusinessUnitID,
        OutletGroup,
        age,
        QuoteDate"""
    #print(vsql)
    if not generic_module112.is_step_complete(generic_module112.module_name,14):
        generic_module112.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cmbq_stage','ETL112_process_penguinquote_age',None,vsql)
        generic_module112.mark_step_complete(generic_module112.module_name,14)
        
    #create table ETL112_process_penguinquote_age_rank
    vsql="""    select 
        *,
        substr(cast(QuoteDate as string),1,7) QuoteMonth,--changed for bigquery
        dense_rank() over (partition by BusinessUnitID,OutletGroup,age,substr(cast(QuoteDate as string),1,7) order by QuoteCount desc) RankInMonth,--changed for bigquery (aggregate function changed)
        count(QuoteDate) over (partition by BusinessUnitID,OutletGroup,age,substr(cast(QuoteDate as string),1,7)) RecordNum--changed for bigquery
    from
        cmbq_stage.ETL112_process_penguinquote_age"""
    #print(vsql)
    if not generic_module112.is_step_complete(generic_module112.module_name,15):
        generic_module112.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cmbq_stage','ETL112_process_penguinquote_age_rank',None,vsql)
        generic_module112.mark_step_complete(generic_module112.module_name,15)
        
    #create table ETL112_process_penguinquote_age_outlier
    vsql="""select 
        *,
        case
            when RecordNum < 3 then 0
            when RankInMonth <= 0.15 * RecordNum then 1
            when RankInMonth >= 0.75 * RecordNum then 1
            else 0
        end isOutlier
    from
        cmbq_stage.ETL112_process_penguinquote_age_rank"""
    #print(vsql)
    if not generic_module112.is_step_complete(generic_module112.module_name,16):
        generic_module112.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cmbq_stage','ETL112_process_penguinquote_age_outlier',None,vsql)
        generic_module112.mark_step_complete(generic_module112.module_name,16)


    #create table ETL112_process_penguinquote_age_ma
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
                    cmbq_stage.ETL112_process_penguinquote_age_outlier r
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
        cmbq_stage.ETL112_process_penguinquote_age_outlier t"""
    #print(vsql)
    if not generic_module112.is_step_complete(generic_module112.module_name,17):
        generic_module112.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cmbq_stage','ETL112_process_penguinquote_age_ma',None,vsql)
        generic_module112.mark_step_complete(generic_module112.module_name,17)
        
    #create table ETL112_process_penguinquote_transactionhour
    vsql=""" select 
        BusinessUnitID,
        OutletGroup,
        transactionhour,
        QuoteDate,
        row_number() over (partition by BusinessUnitID,OutletGroup,transactionhour order by QuoteDate) RowNum,
        cast(count(distinct QuoteKey) as float64) QuoteCount,
        cast(count(distinct case when BotFlag = 1 then QuoteKey else null end) as float64) BotCount
    from
        cmbq_stage.ETL112_process_penguinquote
    group by
        BusinessUnitID,
        OutletGroup,
        transactionhour,
        QuoteDate"""
    #print(vsql)
    if not generic_module112.is_step_complete(generic_module112.module_name,18):
        generic_module112.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cmbq_stage','ETL112_process_penguinquote_transactionhour',None,vsql)
        generic_module112.mark_step_complete(generic_module112.module_name,18)

    #create table ETL112_process_penguinquote_transactionhour_rank
    vsql=""" select 
        *,
        substr(cast(QuoteDate as string),1,7) QuoteMonth,--changed for bigquery
        dense_rank() over (partition by BusinessUnitID,OutletGroup,transactionhour,substr(cast(QuoteDate as string),1,7) order by QuoteCount desc) RankInMonth,--changed for bigquery(aggregate function changed)
        count(QuoteDate) over (partition by BusinessUnitID,OutletGroup,transactionhour,substr(cast(QuoteDate as string),1,7)) RecordNum--changed for bigquery
    from
        cmbq_stage.ETL112_process_penguinquote_transactionhour"""
    #print(vsql)
    if not generic_module112.is_step_complete(generic_module112.module_name,19):
        generic_module112.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cmbq_stage','ETL112_process_penguinquote_transactionhour_rank',None,vsql)
        generic_module112.mark_step_complete(generic_module112.module_name,19)

    #create table ETL112_process_penguinquote_transactionhour_outlier
    vsql="""    select 
        *,
        case
            when RecordNum < 3 then 0
            when RankInMonth <= 0.15 * RecordNum then 1
            when RankInMonth >= 0.75 * RecordNum then 1
            else 0
        end isOutlier
    from
        cmbq_stage.ETL112_process_penguinquote_transactionhour_rank"""
    #print(vsql)
    if not generic_module112.is_step_complete(generic_module112.module_name,20):
        generic_module112.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cmbq_stage','ETL112_process_penguinquote_transactionhour_outlier',None,vsql)
        generic_module112.mark_step_complete(generic_module112.module_name,20)

    #create table ETL112_process_penguinquote_transactionhour_ma
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
                    cmbq_stage.ETL112_process_penguinquote_transactionhour_outlier r
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
        cmbq_stage.ETL112_process_penguinquote_transactionhour_outlier t"""
    #print(vsql)
    if not generic_module112.is_step_complete(generic_module112.module_name,21):
        generic_module112.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cmbq_stage','ETL112_process_penguinquote_transactionhour_ma',None,vsql)
        generic_module112.mark_step_complete(generic_module112.module_name,21)        
    
    
    #create table ETL112_ma_penguinquote
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
        cmbq_stage.ETL112_process_penguinquote a
        left join cmbq_stage.ETL112_process_penguinquote_destination_ma b on 
            a.BusinessUnitID = b.BusinessUnitID and
            a.QuoteDate = b.QuoteDate and
            a.OutletGroup = b.OutletGroup and
            a.Destination = b.Destination
        left join cmbq_stage.ETL112_process_penguinquote_duration_ma c on 
            a.BusinessUnitID = c.BusinessUnitID and
            a.QuoteDate = c.QuoteDate and
            a.OutletGroup = c.OutletGroup and
            a.duration = c.duration
        left join cmbq_stage.ETL112_process_penguinquote_leadtime_ma d on 
            a.BusinessUnitID = d.BusinessUnitID and
            a.QuoteDate = d.QuoteDate and
            a.OutletGroup = d.OutletGroup and
            a.leadtime = d.leadtime
        left join cmbq_stage.ETL112_process_penguinquote_age_ma e on 
            a.BusinessUnitID = e.BusinessUnitID and
            a.QuoteDate = e.QuoteDate and
            a.OutletGroup = e.OutletGroup and
            a.age = e.age
        left join cmbq_stage.ETL112_process_penguinquote_transactionhour_ma f on 
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
                cmbq_stage.ETL112_process_penguinquote
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
    if not generic_module112.is_step_complete(generic_module112.module_name,22):
        generic_module112.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cmbq_stage','ETL112_ma_penguinquote',None,vsql)
        generic_module112.mark_step_complete(generic_module112.module_name,22)    
    
    #create table ETL112_flagged_penguinquote
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
        cmbq_stage.ETL112_ma_penguinquote a
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
    if not generic_module112.is_step_complete(generic_module112.module_name,23):
        generic_module112.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cmbq_stage','ETL112_flagged_penguinquote',None,vsql)
        generic_module112.mark_step_complete(generic_module112.module_name,23)
  
    
    #create table cmbq_prod.penguin_quote_ma
    vsql="""SELECT
              *
            FROM
              cmbq_prod.penguin_quote_ma
            WHERE
              quotekey NOT IN (
              SELECT
                quotekey
              FROM
                cmbq_stage.ETL112_flagged_penguinquote)
            UNION ALL
            SELECT
              *,cast(current_datetime("Australia/Sydney") as timestamp)--reverted by ratnesh on 12nov18 to keep all time as local
            --current_timestamp()
            FROM
              cmbq_stage.ETL112_flagged_penguinquote"""
    #print(vsql)
    if not generic_module112.is_step_complete(generic_module112.module_name,24):
        generic_module112.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cmbq_prod','penguin_quote_ma',None,vsql)
        generic_module112.mark_step_complete(generic_module112.module_name,24)
        
    #create table cmbq_prod.botQuotePenguin
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
                             cmbq_prod.penguin_quote_ma
                        where date(UpdateTime) >= date(current_datetime("Australia/Sydney"))
                        and BotFlag=1)
            select QuoteKey,DestinationMAFactor,DurationMAFactor,LeadTimeMAFactor,AgeMAFactor,TransactionHourMAFactor,SameSessionQuoteCount,ConvertedFlag,BotFlag from cmbq_prod.botQuotePenguin where QuoteKey not in (select QuoteKey from stg_bot_penguin)
            union all
            select * from stg_bot_penguin"""
    #print(vsql)
    if not generic_module112.is_step_complete(generic_module112.module_name,25):
        generic_module112.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cmbq_prod','botQuotePenguin',None,vsql)
        generic_module112.mark_step_complete(generic_module112.module_name,25)    

    #create table ETL112_etl_penQuoteSummaryBot
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
        cmbq_prod.penQuote q
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
                cmbq_prod.penQuoteCompetitor )
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
                cmbq_prod.penQuoteCustomer qc
                --left join [db-au-cmdwh]..penCustomer c on
                left join cmbq_prod.penCustomer c on
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
                cmbq_prod.penQuoteAddOn --qa
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
                cmbq_prod.penUser pu
                inner join cmbq_prod.penOutlet o on
                    o.OutletKey = pu.OutletKey and
                    o.OutletStatus = 'Current'
            where
                pu.UserStatus = 'Current' --and
        ) where row_number=1)  u*/
        (select distinct pu.UserKey,pu.OutletAlphaKey,pu.Login from 
                cmbq_prod.penUser pu
                inner join cmbq_prod.penOutlet o on
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
                cmbq_prod.penCRMUser)
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
                cmbq_prod.botQuotePenguin qb
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
    if not generic_module112.is_step_complete(generic_module112.module_name,26):
        generic_module112.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cmbq_stage','ETL112_etl_penQuoteSummaryBot',None,vsql)
        generic_module112.mark_step_complete(generic_module112.module_name,26)    
    
    
    #create table ETL112_process_penguinquote
    """vsql=
    #print(vsql)
    if not generic_module112.is_step_complete(generic_module112.module_name,27):
        generic_module112.create_bigquery_table_from_queryresult('cover-more-data-and-analytics','cmbq_stage','ETL112_process_penguinquote',None,vsql)
        generic_module112.mark_step_complete(generic_module112.module_name,27)"""
    
        
    ##downloading the QuoteSummaryBot Table. This will be a non partitioned small sized table containing aggregated data.
    #if not generic_module112.is_step_complete(generic_module112.module_name,55):
    #    bq_query_result=generic_module112.execute_sql_bigquery_and_return_result('cover-more-data-and-analytics','cmbq_workspace','select * from cmbq_stage.ETL112_etl_penQuoteSummaryBot')
    #    df_bq_query_result=bq_query_result.to_dataframe()
    #    #print(df_bq_query_result.head())
    #    #for row in bq_query_result:
    #    #    print(row)
    #    engine = create_engine('mssql+pymssql://@ULDWH02:1433/db-au-stage', pool_recycle=3600, pool_size=5)
    #    #df_bq_query_result.to_sql('etl_penQuoteSummaryBotCBA', engine, if_exists='replace',index=False,chunksize=100)
    #    df_bq_query_result.to_sql('etl_penQuoteSummaryBot', engine, if_exists='replace',index=False,chunksize=100)
    #    generic_module112.mark_step_complete(generic_module112.module_name,55)
    #
    #    
    ##generic_module112.cnxn.close()#closing cursor, doesnt clear lock. Closing connection is important to release the lock.
    ##generic_module112.update_cnxn.close()
    #print('Main processing block finished at:'+str(datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S')))

except Exception as excp:
    #generic_module112.cnxn.close()#closing cursor, doesnt clear lock. Closing connection is important to release the lock.
    #raise Exception("Error while executing extract_bq_data(). Full error description is:"+str(excp))
    raise
#finally:
    #cursor.close()
    #generic_module112.cnxn.close()#closing cursor, doesnt clear lock. Closing connection is important to release the lock.