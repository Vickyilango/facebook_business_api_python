"""
Created on Thu Nov  7 18:30:09 2019

@author: VIGNESHWAR.I
"""

# Import all the facebook mumbo jumbo
from facebookads.api import FacebookAdsApi
from facebookads.adobjects.adsinsights import AdsInsights
from facebookads.adobjects.adaccount import AdAccount
from facebookads.adobjects.business import Business
from facebook_business.adobjects.adaccountuser import AdAccountUser
from facebook_business.adobjects.adreportrun import AdReportRun

# Import th csv writer and the date/time function
import datetime
import csv
import time


# Set the info to get connected to the API. Do NOT share this info
my_app_id = '<Your app id>'
my_app_secret = '<Your app secret>'
my_access_token = '<user token>'
# Start the connection to the facebook API
FacebookAdsApi.init(my_app_id, my_app_secret, my_access_token)

# Create a business object for the business account
business = Business('<Business Page Acoount Name>')

# Get today's date for the filename, and the csv data
today = datetime.datetime.now() 
todaydate = today.strftime('%m/%d/%Y')
todaydatehyphen = todaydate.strftime('%m-%d-%Y')

# Define the destination filename
filename = todaydatehyphen + '_fb.csv'
filelocation = "/home/VIGNESHWAR.I/"+ filename

# Get all ad accounts on the business account
me = AdAccountUser(fbid='me')
adAccounts = me.get_ad_accounts()
#accounts = business.get_owned_ad_accounts(fields=[AdAccount.Field.id])

# Open or create new file 
try:
    csvfile = open(filelocation , 'w')
except:
    print ("Cannot open file.")


# To keep track of rows added to file
rows = 0

try:
    # Create file writer
    filewriter = csv.writer(csvfile, delimiter=',')
except Exception as err:
    print(err)

#These are the params for async_job
params = {
    'time_range': {
        'since':  "2018-12-02", 
        'until': "2019-09-02"
        }, 
    'fields': [
                AdsInsights.Field.account_id,
                AdsInsights.Field.account_name,
                AdsInsights.Field.action_values,
                AdsInsights.Field.actions,
                AdsInsights.Field.ad_id,
                AdsInsights.Field.ad_name,
                AdsInsights.Field.adset_id,
                AdsInsights.Field.adset_name,
                AdsInsights.Field.campaign_id,
                AdsInsights.Field.campaign_name, 
                AdsInsights.Field.cost_per_action_type,
                AdsInsights.Field.cost_per_inline_link_click,
                AdsInsights.Field.cost_per_inline_post_engagement,
                AdsInsights.Field.cost_per_unique_click,
                AdsInsights.Field.cpm, 
                AdsInsights.Field.cpp, 
                AdsInsights.Field.ctr, 
                AdsInsights.Field.date_start,
                AdsInsights.Field.date_stop,
                AdsInsights.Field.frequency,
                AdsInsights.Field.impressions,
                AdsInsights.Field.inline_link_clicks,
                AdsInsights.Field.inline_post_engagement,
                AdsInsights.Field.reach,
                AdsInsights.Field.spend,
                AdsInsights.Field.unique_clicks,
                AdsInsights.Field.unique_ctr,
                AdsInsights.Field.video_p100_watched_actions,
                AdsInsights.Field.video_p25_watched_actions,
                AdsInsights.Field.video_p50_watched_actions,
                AdsInsights.Field.video_p75_watched_actions,
                AdsInsights.Field.video_p95_watched_actions,
                AdsInsights.Field.website_ctr
      ],
    'breakdowns': ['country'],
    'level': 'ad',
    'time_increment': 1
}
    
#Writing column names in the file
filewriter.writerow([
    "account_id",
    "account_name",
    "action_values",
    "actions_link_click",
    "actions_post_reaction",
    "actions_post",
    "actions_onsite_conversion_post_save",
    "actions_page_engagement",
    "actions_post_engagement",
    "ad_id",
    "ad_name",
    "adset_id",
    "adset_name",
    "campaign_id",
    "campaign_name", 
    "cost_per_action_type_link_click ",
    "cost_per_action_type_post_reaction",
    "cost_per_action_type_post",
    "cost_per_action_type_onsite_conversion_post_save",
    "cost_per_action_type_page_engagement",
    "cost_per_action_type_post_engagement",
    "cost_per_inline_link_click",
    "cost_per_inline_post_engagement",
    "cost_per_unique_click",
    "cpm", 
    "cpp", 
    "ctr", 
    "date_start",
    "date_stop",
    "frequency",
    "impressions",
    "inline_link_clicks",
    "inline_post_engagement",
    "reach",
    "spend",
    "unique_clicks",
    "unique_ctr",
    "video_p100_watched_views",
    "video_p25_watched_views",
    "video_p50_watched_views",
    "video_p75_watched_views",
    "video_p95_watched_views",
    "website_ctr_link_click",
    "date_of_report"
    ])

    
#Wait till all the data is pulled 
def wait_for_async_job(async_job):
    async_job.api_get()
    while async_job[AdReportRun.Field.async_status] != 'Job Completed' or async_job[AdReportRun.Field.async_percent_completion] < 100:
        time.sleep(1)
        async_job.api_get()
        
# Iterate through the adaccounts
for account in adAccounts:
    # Create an addaccount object from the adaccount id to make it possible to get insights
    tempaccount = AdAccount(account[AdAccount.Field.id])
    # Grab insight info for all ads in the adaccount
    ads = tempaccount.get_insights_async(fields = params.get('fields'), params=params) 
    wait_for_async_job(ads)
    x = ads.get_result()
    #campaigns = tempaccount.get_campaigns(fields=[Campaign.Field.name,Campaign.Field])
    # Iterate through all accounts in the business account
    for ad in x:
        # Set default values in case the insight info is empty
        date_of_report = todaydate
        account_id = ""
        account_name = ""
        action_values = ""
        actions = ""
        actions_link_click = ""
        actions_post_reaction = ""
        actions_post = ""
        actions_onsite_conversion_post_save = ""
        actions_page_engagement = ""
        actions_post_engagement = ""
        ad_id = ""
        ad_name = ""
        adset_id = ""
        adset_name = ""
        campaign_id = ""
        campaign_name = "" 
        cost_per_action_type = ""
        cost_per_action_type_link_click = ""
        cost_per_action_type_post_reaction = ""
        cost_per_action_type_post = ""
        cost_per_action_type_onsite_conversion_post_save = ""
        cost_per_action_type_page_engagement = ""
        cost_per_action_type_post_engagement = ""
        cost_per_inline_link_click = ""
        cost_per_inline_post_engagement = ""
        cost_per_unique_click = ""
        cpm = ""
        cpp = ""
        ctr = ""
        date_start = ""
        date_stop = ""
        frequency = ""
        impressions = ""
        inline_link_clicks = ""
        inline_post_engagement = ""
        reach = ""
        spend = ""
        unique_clicks = ""
        unique_ctr = ""
        video_p100_watched_actions = ""
        video_p25_watched_actions = ""
        video_p50_watched_actions = ""
        video_p75_watched_actions = ""
        video_p95_watched_actions = ""
        website_ctr = ""
        website_ctr_link_click = ""

        # Set values from insight data
        if ('account_id' in ad) :
            account_id = ad['account_id']
        if ('account_name' in ad) :
            account_name = ad['account_name']
        if ('action_values' in ad) :
            action_values= ad['action_values']
        if ('actions' in ad) :
            for i in ad['actions']:
                if i['action_type'] == 'link_click':
                    actions_link_click = i['value']
                if i['action_type'] == 'post_reaction':
                    actions_post_reaction = i['value']
                if i['action_type'] == 'post':
                    actions_post = i['value']
                if i['action_type'] == 'onsite_conversion.post_save':
                    actions_onsite_conversion_post_save = i['value']
                if i['action_type'] == 'page_engagement':
                    actions_page_engagement = i['value']
                if i['action_type'] == 'post_engagement':
                    actions_post_engagement = i['value']            
        if ('ad_id' in ad) :
            ad_id= ad['ad_id']
        if ('ad_name' in ad) :
            ad_name= ad['ad_name']
        if ('adset_id' in ad) :
            adset_id= ad['adset_id']
        if ('adset_name' in ad) :
            adset_name= ad['adset_name']
        if ('campaign_id' in ad) :
            campaign_id= ad['campaign_id']
        if ('campaign_name' in ad) :
            campaign_name= ad['campaign_name'] 
        if ('cost_per_action_type' in ad) :
            for i in ad['cost_per_action_type']:
                if i['action_type'] == 'link_click':
                    cost_per_action_type_link_click = i['value']
                if i['action_type'] == 'post_reaction':
                    cost_per_action_type_post_reaction = i['value']
                if i['action_type'] == 'post':
                    cost_per_action_type_post = i['value']
                if i['action_type'] == 'onsite_conversion.post_save':
                    cost_per_action_type_onsite_conversion_post_save = i['value']
                if i['action_type'] == 'page_engagement':
                    cost_per_action_type_page_engagement = i['value']
                if i['action_type'] == 'post_engagement':
                    cost_per_action_type_post_engagement = i['value']    
        if ('cost_per_inline_link_click' in ad) :
            cost_per_inline_link_click= ad['cost_per_inline_link_click']
        if ('cost_per_inline_post_engagement' in ad) :
            cost_per_inline_post_engagement= ad['cost_per_inline_post_engagement']
        if ('cost_per_unique_click' in ad) :
            cost_per_unique_click= ad['cost_per_unique_click']
        if ('cpm' in ad) :
            cpm= ad['cpm']
        if ('cpp' in ad) :
            cpp= ad['cpp']
        if ('ctr' in ad) :
            ctr= ad['ctr']
        if ('date_start' in ad) :
            date_start= ad['date_start']
        if ('date_stop' in ad) :
            date_stop= ad['date_stop']
        if ('frequency' in ad) :
            frequency= ad['frequency']
        if ('impressions' in ad) :
            impressions= ad['impressions']
        if ('inline_link_clicks' in ad) :
            inline_link_clicks= ad['inline_link_clicks']
        if ('inline_post_engagement' in ad) :
            inline_post_engagement= ad['inline_post_engagement']
        if ('reach' in ad) :
            reach= ad['reach']
        if ('spend' in ad) :
            spend= ad['spend']
        if ('unique_clicks' in ad) :
            unique_clicks= ad['unique_clicks']
        if ('unique_ctr' in ad) :
            unique_ctr= ad['unique_ctr']
        if ('video_p100_watched_actions' in ad) :
            for i in ad['video_p100_watched_actions']:
                if i['action_type'] == 'video_view':
                    video_p100_watched_actions = i['value']                
        if ('video_p25_watched_actions' in ad) :
            for i in ad['video_p25_watched_actions']:
                if i['action_type'] == 'video_view':
                    video_p25_watched_actions = i['value']             

        if ('video_p50_watched_actions' in ad) :
            for i in ad['video_p50_watched_actions']:
                if i['action_type'] == 'video_view':
                    video_p50_watched_actions = i['value']             

        if ('video_p75_watched_actions' in ad) :
            for i in ad['video_p75_watched_actions']:
                if i['action_type'] == 'video_view':
                    video_p75_watched_actions = i['value']   

        if ('video_p95_watched_actions' in ad) :
            for i in ad['video_p95_watched_actions']:
                if i['action_type'] == 'video_view':
                    video_p95_watched_actions = i['value'] 
                    
        if ('website_ctr' in ad) :
            for i in ad['website_ctr']:
                if i['action_type'] == 'link_click':
                    website_ctr_link_click = i['value']    
        if ('date_of_report' in ad) :
            date_of_report = ad['date_of_report']

        # Write all ad info to the file, and increment the number of rows that will display
        filewriter.writerow([
                account_id,
                account_name,
                action_values,
                actions_link_click ,
                actions_post_reaction ,
                actions_post,
                actions_onsite_conversion_post_save,
                actions_page_engagement,
                actions_post_engagement,
                ad_id,
                ad_name,
                adset_id,
                adset_name,
                campaign_id,
                campaign_name, 
                cost_per_action_type_link_click ,
                cost_per_action_type_post_reaction,
                cost_per_action_type_post,
                cost_per_action_type_onsite_conversion_post_save,
                cost_per_action_type_page_engagement,
                cost_per_action_type_post_engagement,
                cost_per_inline_link_click,
                cost_per_inline_post_engagement,
                cost_per_unique_click,
                cpm, 
                cpp, 
                ctr, 
                date_start,
                date_stop,
                frequency,
                impressions,
                inline_link_clicks,
                inline_post_engagement,
                reach,
                spend,
                unique_clicks,
                unique_ctr,
                video_p100_watched_actions,
                video_p25_watched_actions,
                video_p50_watched_actions,
                video_p75_watched_actions,
                video_p95_watched_actions,
                website_ctr_link_click,
                date_of_report
                ])
        rows += 1


csvfile.close()

# Print report
print (str(rows) + " rows added to the file " + filename)



