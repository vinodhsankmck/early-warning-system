from newsapi.newsapi_client import NewsApiClient
import re
import math
import copy
import logging
import json
import asyncio
import nest_asyncio
import datetime
from datetime import timedelta
from datetime import datetime
from dateutil.relativedelta import relativedelta
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
from util.decorator_wrapper_util import asyncio_wrapper_gather,run_asyncio_gather
from company_cleanup import company_name_cleanup

# Init
#nest_asyncio.apply()
newsapi = NewsApiClient(api_key='5d6c2c4cf944479099567612b975755e')
executor = ThreadPoolExecutor(1)
company_names_cleaned = pd.DataFrame(company_name_cleanup())
company_names_cleaned.dropna()
company_names_cleaned = company_names_cleaned.loc[company_names_cleaned['Country of Risk ISO Country Code'] == 'US']
#company_names_cleaned = company_names_cleaned.iloc[0:700] # Use loop to run for subset of companies if you are using a local environment
source_quality = {
    'top_us_domains':['abcnews.go.com', 'abc.net.au', 'aljazeera.com', 'arstechnica.com', 'apnews.com', 'afr.com',
                      'axios.com', 'bbc.co.uk', 'bbc.co.uk', 'bleacherreport.com', 'bloomberg.com', 'breitbart.com',
                      'businessinsider.com', 'uk.businessinsider.com', 'buzzfeed.com', 'cbc.ca', 'cbsnews.com', 'us.cnn.com',
                      'ccn.com', 'engadget.com', 'ew.com', 'espn.go.com', 'espncricinfo.com', 'business.financialpost.com',
                      'football-italia.net', 'fortune.com', 'fourfourtwo.com', 'foxnews.com', 'foxsports.com', 'news.google.com',
                      'news.google.com', 'news.google.com', 'news.google.com', 'news.google.com', 'news.ycombinator.com', 'ign.com',
                      'independent.co.uk', 'mashable.com', 'medicalnewstoday.com', 'msnbc.com', 'mtv.com', 'mtv.co.uk', 'news.nationalgeographic.com',
                      'nationalreview.com', 'nbcnews.com', 'news24.com', 'newscientist.com', 'news.com.au', 'newsweek.com', 'nymag.com',
                      'nextbigfuture.com', 'nfl.com', 'nhl.com', 'politico.com', 'polygon.com', 'recode.net', 'reddit.com', 'reuters.com',
                      'rte.ie', 'talksport.com', 'techcrunch.com', 'techradar.com', 'theamericanconservative.com', 'theglobeandmail.com',
                      'thehill.com', 'thehindu.com', 'huffingtonpost.com', 'irishtimes.com', 'jpost.com', 'theladbible.com', 'thenextweb.com',
                      'thesportbible.com', 'timesofindia.indiatimes.com', 'theverge.com', 'wsj.com', 'washingtonpost.com', 'washingtontimes.com',
                      'time.com', 'usatoday.com', 'news.vice.com', 'wired.com'],

    'trusted':['linkedin','twitter','businesswire','reuters','prnewswire',
                 'releasewire','wsj','theguardian','the guardian','nytimes','ft','economist',
                 'cnn','seekingalpha','technologyreview.com','semiconductors.org','wsts.org',
                 'bloomberg','bbc news','news.google.com','forbes','nasdaq','barrons',
                 'marketwatch','delloro','idc','gartner','techcrunch','wired','mashable','platts',
                 'bentekenergy','pira','bassoe','breakingenergy','eia','iea','ogj','offshoreenergytoday',
                 'offshore-mag','opec','pennenergy','rigzone','worldoil','vanguard','abc news','cnbc','cbc news','nbc',
                 'dallasnews','wall street journal','globenewswire','telegraph','huffington post','business insider',
                 'bostonglobe','fastcompany','politico','irish times','independent','seattletimes','new york times',
                 'latimes','indianexpress','fortune','japantoday','time','financial post','chicago tribune',
                 'columbia.edu','wired','today','nationalobserver','channelnewsasia','firstpost',
                 'timesofisrael','the jerusalem post','huffpost','oilprice','nzherald'],
    'non_trusted':[]
}

# Configuration
logging.getLogger().setLevel(logging.INFO)

default_params = {'q':None,'qintitle':None ,'sources':None, 'domains':','.join(p for p in source_quality['top_us_domains']), 
                  'exclude_domains':None,
                'from_param':None, 'to':None, 'language':None, 'sort_by':None, 'page':None,
                       'page_size':None}



def daterange(start_date, end_date):
    start_date_datetime = datetime.datetime.strptime(start_date,'%Y-%m-%d')
    end_date_datetime = datetime.datetime.strptime(end_date,'%Y-%m-%d')
    for n in range(int((end_date_datetime - start_date_datetime).days)):
        yield start_date_datetime + timedelta(n)
            
# Native Coroutine
async def get_everything_end_point_async(optional_params):
    # Parameters
    params = {
        #'language':'en',
        'page_size':100
    }
    # Add option
    optional_params.update(params)
    default_params.update(optional_params)
    #print(default_params['q'])
    loop = asyncio.get_event_loop()
    # Api call
    # get_everything is not a native co-routine, so it needs to run into a separate thread
    print(default_params['q'])
    everything = await loop.run_in_executor(executor,newsapi.get_everything,
                                                    *(default_params['q'],default_params['qintitle'], default_params['sources'],
                                                    default_params['domains'],default_params['exclude_domains'], default_params['from_param'],
                                                    default_params['to'],default_params['language'],
                                                    default_params['sort_by'],default_params['page'],
                                                    default_params['page_size']))
    # Return Json
    return everything

def get_everything_end_point(optional_params):
    # Parameters
    params = {
        #'language':'en',
        'page_size':100
    }
    # Add option
    optional_params.update(params)
    default_params.update(optional_params)
    # Api call
    everything = newsapi.get_everything(**default_params)
    # Return Json
    return everything

def get_unique_urls(data):
    ids = set()
    for article in data["articles"]:
        ids.add(article["url"])
    return ids

def get_article(json,id):
    for article in json["articles"]:
        if article["url"] == id:
            return article

def get_new_urls(data,json):
    # Get all url available already
    unique_urls = get_unique_urls(data)
    # logging.info("Unique URLs = {}".format(len(unique_urls)))
    # URLs in new file
    new_json_url = get_unique_urls(json)
    # Find the new urls
    new_uniques = []
    for url in new_json_url:
        if url not in unique_urls:
            new_uniques.append(url)
    # logging.info("Found {} new unique URLs".format(len(new_uniques)))
    return new_uniques

def consolidate_json(json_list):
    logging.debug('Consolidating JSON files')
    counter=0
    for json in tqdm(json_list):
        if counter==0:
            data = json
            # Counter
            counter += 1
        else:
            # Get new article using url as unique id
            new_urls = get_new_urls(data=data, json=json)
            # print(new_urls)
            # Get new unique URLs
            for url in new_urls:
                data["articles"].append(get_article(json,url))

    logging.debug('Number of final articles %s'%len(data['articles']))

    return data

def get_news(us_client):
    params = {}
    title = False
    sort = 'publishedAt'
    start_date = '2017-10-01' # Input the start date 
    end_date = '2022-12-31' # Input the end date 
    params['language'] = 'en'
    if us_client == 'US':
        company_names = company_names_cleaned.loc[company_names_cleaned['Country of Risk ISO Country Code'] == us_client]
        client_list = company_names['Keywords']
        # Sort by relevancy
        params['sort_by'] = sort
        #Creating logs
        # logs_df = pd.DataFrame(columns=['Client name','Total articles 2019','Total articles 2020','Total articles 2021','Total articles 2022'])
        #Get date range
        date_range_list = []
        date_range_list = pd.date_range(start_date,end_date,freq='MS').tolist()
        year_range_list = pd.date_range(start_date,end_date,freq='MS').strftime("%Y").tolist()
        year_set = set(year_range_list)
        unique_year_list = (list(year_set))
        # Extract for key subject
        client_list = [word.lower() for word in client_list]
        for word in tqdm(client_list):
            #if word == '"benefit cosmetics"':
            for year in unique_year_list:
                json_data = []
                logging.info('Looping over key words: %s' % word)
                logging.info('Looping over year: %s' % year)
                for month_date in tqdm(date_range_list):
                    if year == month_date.strftime("%Y"):
                        start_date = month_date
                        end_date = month_date + relativedelta(months=1)
                        params['from_param'] = start_date.strftime("%Y-%m-%d")
                        params['to'] = end_date.strftime("%Y-%m-%d")
                        year = start_date.strftime("%Y")
                        # Selecting key words to look for first
                        if title:
                            params['qintitle'] = word#re.sub('\W+',' ',word)
                        else:
                            params['q'] = word#re.sub('\W+',' ',word)
                        # print(params['q'])
                        # Get number of pages available
                        params['page']=1
                        request = get_everything_end_point(params)
                        json_data.append(request)
                        # Get 100 articles per page - max 100 pages per request
                        n_pages = min(10000,math.floor(request['totalResults']/10000)+1)
                        # A query can buffer 100 pages deep
                        logging.info('Looping over pages: %s pages' % n_pages)
                        list_params = []
                        for q in range(2,(n_pages+1)):
                            params['page'] = q
                            logging.info('Printing list parameters %s' % params['page'])
                            list_params.append(copy.deepcopy(params))
                        if list_params:
                            nest_asyncio.apply()
                            loop = asyncio.get_event_loop()
                            res = loop.run_until_complete(run_asyncio_gather(get_everything_end_point_async,list_params))
                            json_data.extend(res)
                        fp = "/Projects/citiews/credit_ews/Output/json_data/" + year + "/" + word.lower().replace(" ","_").replace('"','') + ".json"
                        has_items = bool(request['articles'])
                        # # print(has_items)
                        if has_items == True:
                            with open(fp, 'w') as outfile:
                                json.dump(consolidate_json(json_data),outfile)