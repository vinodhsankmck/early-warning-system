from company_upload import client_file_upload
from signal_calculations import calc_signals_yoy_qoq

def evaluate_QoQ_forecast_vs_actual_signals():
    import pandas as pd
    import numpy as np
    # Importing forecasted and reported revenue by company and year
    revenue_df = pd.read_csv("/input/External Data/Evaluate Medtech/2. Archived Company Annual Revenue Forecasts Broken Out by Segments.csv")
    # Picking a subset of the data to get forecasted revenue by company by year
    forecast_df = revenue_df[revenue_df['Data Kind']=='Actual Data']
    forecast_df = forecast_df[forecast_df['Source']=='Forecast']
    
    # Calculating End of Month Date
    forecast_df['Date'] = pd.to_datetime(pd.Series(forecast_df['Date']))
    # Converting the event occuring date to end of quarter
    forecast_df['Quarter'] = forecast_df['Date'].dt.to_period("Q").dt.end_time
    forecast_df['Quarter'] = pd.to_datetime(forecast_df['Quarter']).dt.date
    # Converting to date format
    forecast_df['Date'] = forecast_df['Date'].dt.to_period("M").dt.end_time
    forecast_df['Date'] = pd.to_datetime(forecast_df['Date']).dt.date
    
    # Calculating reported revenue by company by year
    forecast_latest_df = forecast_df.groupby(['Company','Item','Year'], as_index=False)['Quarter','Date'].max()
    forecast_latest_df = forecast_latest_df.rename(columns={'Year':'Curr_Financial_Year','Quarter':'Latest_Forecast_Quarter'})
    forecast_latest_df = forecast_latest_df.sort_values(by=['Company','Item','Latest_Forecast_Quarter'])
    
    # Creating final data based on forecast
    final_df = forecast_df[['Company','Item','Year','Quarter']]
    
    # Calculating Forecasted revenue by company by year
    forecast_df = forecast_df[['Company','Item','Year','Quarter','Data','Date']]
    forecast_df = forecast_df.rename(columns={'Year':'Curr_Financial_Year','Data':'Forecasted_Revenue'})
        
    # Calculating previous forecast quarter
    company_list = forecast_latest_df['Company'].unique()
    prev_actual_qtr = []
    for company in company_list:
        temp_df1 = forecast_latest_df[forecast_latest_df['Company']==company]
        item_list = temp_df1['Item'].unique()
        for item in item_list:
            temp_df2 = temp_df1[temp_df1['Item']==item]
            temp_df2['Prev_Forecast_Quarter'] = temp_df2['Latest_Forecast_Quarter'].shift(1)
            temp_df2['Prev_Financial_Year'] = temp_df2['Curr_Financial_Year'].shift(1)
            prev_actual_qtr.append(temp_df2)
    forecast_latest_df = pd.concat(prev_actual_qtr)
    
    # Merging with forecasted data based on forecast date
    forecast_df = pd.merge(forecast_df,forecast_latest_df,how='inner', left_on=['Company','Item','Curr_Financial_Year','Quarter','Date'], right_on=['Company','Item','Curr_Financial_Year','Latest_Forecast_Quarter','Date'])
    forecast_df = forecast_df[['Company','Item','Prev_Financial_Year','Curr_Financial_Year','Prev_Forecast_Quarter','Latest_Forecast_Quarter','Forecasted_Revenue']]
    forecast_df = forecast_df.drop_duplicates()
    
    # Populating forecasted values by quarter
    merge_forecast_df = pd.merge(final_df,forecast_df,how='inner',on=['Company','Item'])
    merge_forecast_df['Prev_Forecast_Quarter'] = pd.to_datetime(merge_forecast_df['Prev_Forecast_Quarter']).dt.date
    merge_forecast_df['Latest_Forecast_Quarter'] = pd.to_datetime(merge_forecast_df['Latest_Forecast_Quarter']).dt.date
    merge_forecast_df = merge_forecast_df.loc[(merge_forecast_df['Year']==merge_forecast_df['Curr_Financial_Year']) & (merge_forecast_df['Quarter']>=merge_forecast_df['Prev_Forecast_Quarter']) & (merge_forecast_df['Quarter']<merge_forecast_df['Latest_Forecast_Quarter'])]
    merge_forecast_df = merge_forecast_df[['Company','Item','Quarter','Curr_Financial_Year','Forecasted_Revenue']]
    merge_forecast_df = merge_forecast_df.drop_duplicates()
    merge_forecast_df = merge_forecast_df.rename(columns={'Curr_Financial_Year':'Forecasted_Financial_Year'})
    
    # Importing reported revenue by company and year
    actual_df = revenue_df[revenue_df['Data Kind']=='Actual Data']
    actual_df = actual_df[actual_df['Source']=='Reported']
    actual_df['Date'] = pd.to_datetime(pd.Series(actual_df['Date']))
    
    # Calculating reported revenue by company by year
    actual_latest_df = actual_df.groupby(['Company','Item','Year'], as_index=False)['Date'].min()
    
    # Calculating the actual reporting end of quarter
    actual_latest_df['Actual_Announcement_Quarter'] = actual_latest_df['Date'].dt.to_period("Q").dt.end_time
    actual_latest_df['Actual_Announcement_Quarter'] = pd.to_datetime(actual_latest_df['Actual_Announcement_Quarter']).dt.date
    
    actual_latest_df['Date'] = actual_latest_df['Date'].dt.to_period("M").dt.end_time
    actual_latest_df['Date'] = pd.to_datetime(actual_latest_df['Date']).dt.date
    actual_latest_df = actual_latest_df.rename(columns={'Date':'Actual_Announcement_Date'})
    actual_latest_df['Curr_Financial_Year'] = actual_latest_df['Year']
    actual_latest_df = actual_latest_df.sort_values(by=['Company','Item','Actual_Announcement_Quarter'])
    
    # Calculating previous announcement quarter
    company_list = actual_latest_df['Company'].unique()
    prev_actual_qtr = []
    for company in company_list:
        temp_df1 = actual_latest_df[actual_latest_df['Company']==company]
        item_list = temp_df1['Item'].unique()
        for item in item_list:
            temp_df2 = temp_df1[temp_df1['Item']==item]
            temp_df2['Next_Acutal_Announcement_Quarter'] = temp_df2['Actual_Announcement_Quarter'].shift(-1)
            temp_df2['Next_Financial_Year'] = temp_df2['Curr_Financial_Year'].shift(-1)
            prev_actual_qtr.append(temp_df2)
    actual_latest_df = pd.concat(prev_actual_qtr)
    
    # Merging with reported data based on reported date
    actual_df['Date'] = actual_df['Date'].dt.to_period("M").dt.end_time
    actual_df['Date'] = pd.to_datetime(actual_df['Date']).dt.date
    actual_df = pd.merge(actual_df,actual_latest_df,how='inner', left_on=['Company','Item','Year','Date'], right_on=['Company','Item','Year','Actual_Announcement_Date'])
    actual_df = actual_df.rename(columns={'Data':'Actual_Revenue','Year':'Financial_Year'})
    actual_df = actual_df[['Company','Item','Financial_Year','Next_Financial_Year','Actual_Announcement_Quarter','Next_Acutal_Announcement_Quarter','Actual_Revenue']]
    
    # Merging forecast with final data
    merge_actual_df = pd.merge(final_df,actual_df,how='inner',on=['Company','Item'])
    merge_actual_df['Next_Financial_Year'] = np.where(merge_actual_df['Next_Financial_Year'].isnull(),merge_actual_df['Year'],merge_actual_df['Next_Financial_Year'])
    merge_actual_df['Acutal_Announcement_Quarter_Next_Year'] = merge_actual_df['Actual_Announcement_Quarter']+pd.DateOffset(months=12)
    merge_actual_df['Acutal_Announcement_Quarter_Next_Year'] = pd.to_datetime(merge_actual_df['Acutal_Announcement_Quarter_Next_Year']).dt.date
    merge_actual_df['Next_Acutal_Announcement_Quarter'] = np.where(merge_actual_df['Next_Acutal_Announcement_Quarter'].isnull(),merge_actual_df['Acutal_Announcement_Quarter_Next_Year'],merge_actual_df['Next_Acutal_Announcement_Quarter'])
    merge_actual_df = merge_actual_df.loc[(merge_actual_df['Year']-1== merge_actual_df['Financial_Year']) & (merge_actual_df['Quarter'] >= merge_actual_df['Actual_Announcement_Quarter']) & (merge_actual_df['Quarter'] < merge_actual_df['Next_Acutal_Announcement_Quarter'])]
    merge_actual_df = merge_actual_df[['Company','Item','Quarter','Financial_Year','Actual_Announcement_Quarter','Next_Acutal_Announcement_Quarter','Actual_Revenue']]
    merge_actual_df = merge_actual_df.drop_duplicates()
    merge_actual_df = merge_actual_df.rename(columns={'Financial_Year':'Reported_Financial_Year'})
    merge_actual_df = merge_actual_df[['Company','Item','Reported_Financial_Year','Quarter','Actual_Revenue']]
    
    # Merging Actual and Forecasted Data
    merge_df = pd.merge(merge_actual_df,merge_forecast_df,how='inner',on=['Company','Item','Quarter'])
    merge_df = merge_df[['Company','Item','Quarter','Forecasted_Financial_Year','Reported_Financial_Year','Forecasted_Revenue','Actual_Revenue']]
    merge_df['pct_change_actual_vs_forecasted'] = (merge_df['Actual_Revenue'] - merge_df['Forecasted_Revenue'])/merge_df['Forecasted_Revenue']
    merge_df = merge_df.sort_values(by=['Company','Item','Quarter'])
    # QoQ change in actual vs forecasted
    merge_df = calc_signals_yoy_qoq(merge_df,'pct_change_actual_vs_forecasted','QoQ_actual_vs_forecast','AvsF','Reported_Financial_Year')

# #     # Writing to CSV
    merge_df.to_csv("/output/External Data/Evaluate Medtech/evaluate_medtech_QoQ_pct_change_actual_vs_forecasted.csv", index=False)
    return merge_df[merge_df['Company']=='Amgen']
    