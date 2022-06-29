from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import json
import re
import os 
import pandas as pd
import glob
from os import listdir

def get_vader_sentiment(doc):
    '''Leveraging vader sentiment analyzer
    VADER (Valence Aware Dictionary and sEntiment Reasoner) is a lexicon and
    rule-based sentiment analysis tool that is specifically attuned to
    sentiments expressed in social media. It is fully open-sourced under
    the [MIT License] .'''
    # Initialization
    analyzer = SentimentIntensityAnalyzer()
    # Scoring
    score = analyzer.polarity_scores(doc)['compound']

    return score

def client_file_upload():
    client_df = pd.read_excel("/Projects/citiews/credit_ews/Input/EWS pilot client names - Industry.xlsx")
    client_df = client_df[client_df['Domicile Country ISO Country Code'] == 'US']
    client_df = client_df.reset_index()
    client_df = client_df[['Client name','Keywords','Domicile Country ISO Country Code','RMI Industry - Level 1 Name' ,'RMI Industry - Level 2 Name','NAICS 1 Industry - Industry Code'
                           ,'NAICS 1 Industry - Industry Name']]
    client_df = client_df.rename(columns={'Client name':'client_name', 'Domicile Country ISO Country Code':'country_code','RMI Industry - Level 1 Name':'industry_name_level_1'
                                                      ,'RMI Industry - Level 2 Name':'industry_name_level_2','NAICS 1 Industry - Industry Code':'industry_code','NAICS 1 Industry - Industry Name':'industry_name'})
    
    return client_df

def read_news_data():
    dirpath = "/Users/vinodh_sankaran/Projects/citiews/credit_ews/Output/json_data"
    subfolders = [ f.name for f in os.scandir(dirpath) if f.is_dir()]
    client_df = client_file_upload()
    keyword_list = []
    client_df['Keywords'] = client_df['Keywords'].str.replace(' ','_').str.replace('"','')
    keyword_list = client_df['Keywords'].to_list()
    keyword_list = [s + '.json' for s in keyword_list]
    for name in keyword_list: #run for each company in loops if there is any memory issue
        # Create empty lists
        file_dict = {}
        file_name = []
        score = []
        year = []
        url = []
        published_dt = []
        for yr in subfolders:
            fpath = dirpath + "/" + yr
            files_in_path = os.listdir(fpath)
            # Looping through the files
            for file in files_in_path:
                if file == name:
                    # Opening JSON file
                    f = open(fpath + "/" + file)
                    # returns JSON object as
                    # a dictionary
                    data = json.load(f)
                    # Convert JSON data to string
                    json_string = json.dumps(data)   
                    articles = data["articles"]
                    for articles in articles:
                        compound_score = get_vader_sentiment(articles["content"])
                        score.append(compound_score)
                        published_dt.append(articles["publishedAt"])
                    # Calculate VADER Score
                    #print("Year " + yr + " News String\n" + json_string + "\n\n\n")
                    #compound_score = get_vader_sentiment(json_string)
                    #print(compound_score)
                    # Appending data into list
                        file_name.append(file.replace('.json',''))
                        year.append(yr)
                        url.append(articles["url"])
                    # Closing file
                    f.close()

        # Create a dict with required columns
        file_dict = {'file_name': file_name, 'year': year, 'news_published_date':published_dt, 'vader_compound_score': score, 'news_link_url':url}
        file_df = pd.DataFrame(file_dict)
        if not file_df.empty:
            file_df.to_csv("/Projects/citiews/credit_ews/Output/news_data/sentiment_analysis/" + name.replace('.json','') +".csv", sep = ',', index = False)

def unpivot_data():
    dirpath = "/Projects/citiews/credit_ews/Output/news_data/sentiment_analysis"
    files = listdir(dirpath)
    files = [f for f in files if f.endswith(".csv")]

# Unpivot Files
    for file in files:
        df = pd.read_csv(dirpath + "/" + file)
        df['news_published_date'] = df['news_published_date'].str[:10]
        df['news_published_date'] = pd.to_datetime(df.news_published_date).dt.to_period('M').dt.to_timestamp()
        agg_df = df.groupby(['file_name','year','news_published_date'],as_index=False)['vader_compound_score'].mean()
        #df_unpivoted = df.melt(id_vars = ['file_name'], var_name = 'year', value_name = 'compound_score')
        agg_df.to_csv("/Projects/citiews/credit_ews/Output/news_data/sentiment_analysis/Unpivot/" + file, index=False)
        del [[df,agg_df]]

def consolidated_data():
# Get CSV files list from a folder
    path = "/Projects/citiews/credit_ews/Output/news_data/sentiment_analysis/Unpivot"
    csv_files = glob.glob(path + "/*.csv")

# Read each CSV file into DataFrame
# This creates a list of dataframes
    df_list = (pd.read_csv(file) for file in csv_files)

# Concatenate all CSV Files
    big_df   = pd.concat(df_list, ignore_index=True)
    big_df.to_csv("/Projects/citiews/credit_ews/Output/news_data/sentiment_analysis/Final/news_data_sentiment_score.csv",index=False)