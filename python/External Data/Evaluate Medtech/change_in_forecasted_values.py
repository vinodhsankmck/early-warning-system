from company_upload import client_file_upload
from signal_calculations import calc_signals_yoy_qoq

def evaluate_forecasted_value_change_yoy_qoq():
    # Importing forecasted revenue by company and year by item
    forecast_df = pd.read_csv("/input/External Data/Evaluate Medtech/2. Archived Company Annual Revenue Forecasts Broken Out by Segments.csv")
    
    # Picking a subset of the data to get forecasted revenue by company by year
    forecast_df = forecast_df[forecast_df['Data Kind']=='Actual Data']
    forecast_df = forecast_df[forecast_df['Source']=='Forecast']
    forecast_df['Date'] = pd.to_datetime(pd.Series(forecast_df['Date']))
    
    # Calculating end of quarter based on date
    forecast_df['quarter'] = forecast_df['Date'].dt.to_period("Q").dt.end_time
    forecast_df['quarter'] = pd.to_datetime(forecast_df['quarter']).dt.date
    
    # Calculating max date by quarter
    max_quarter_df = forecast_df.groupby(['Company','Item','Year'], as_index=False)['Date'].max()
    max_date_quarter_df = forecast_df.groupby(['Company','Item','Year','quarter'], as_index=False)['Date'].max()
    
    # Sorting Data by Company, Item, Year
    max_quarter_df = max_quarter_df.sort_values(by=['Company','Item','Year','Date'])
    
    # Calculating the previous year forecasted quarter by quarter by year
    company_list = max_quarter_df['Company'].unique()
    forecast_date = []
    for company in company_list:
        temp_df1 = max_quarter_df[max_quarter_df['Company']==company]
        item_list = temp_df1['Item'].unique()
        for item in item_list:
            temp_df2 = temp_df1[temp_df1['Item']==item]
            temp_df2['Prev_Forecast_Date'] = temp_df2['Date'].shift(1)
            temp_df2['Prev_Financial_Year'] = temp_df2['Year'].shift(1)
            forecast_date.append(temp_df2)
    max_quarter_df = pd.concat(forecast_date)
    
    # Renaming required columns
    max_quarter_df = max_quarter_df.rename(columns={'Year':'Financial_Year','Date':'Forecast_Date'})
    
    # Converting data to quarterly values
    max_quarter_df['Forecast_Quarter'] = max_quarter_df['Forecast_Date'].dt.to_period("Q").dt.end_time
    max_quarter_df['Forecast_Quarter'] = pd.to_datetime(max_quarter_df['Forecast_Quarter']).dt.date
    max_quarter_df['Prev_Forecast_Quarter'] = max_quarter_df['Prev_Forecast_Date'].dt.to_period("Q").dt.end_time
    max_quarter_df['Prev_Forecast_Quarter'] = pd.to_datetime(max_quarter_df['Prev_Forecast_Quarter']).dt.date
    
    # Selecting required columns
    max_quarter_df = max_quarter_df[['Company','Item','Financial_Year','Prev_Financial_Year','Forecast_Date','Forecast_Quarter','Prev_Forecast_Date','Prev_Forecast_Quarter']]
    
    # Merging data based on quarter date
    forecast_quarter_df = pd.merge(forecast_df,max_date_quarter_df,how='inner',on=['Company','Item','Year','quarter',"Date"])
    forecast_quarter_df = pd.merge(forecast_quarter_df,max_quarter_df,how='inner',on=['Company','Item'])
    forecast_quarter_df = forecast_quarter_df[forecast_quarter_df['Prev_Forecast_Date'].notnull()]
    forecast_quarter_df = forecast_quarter_df.loc[(forecast_quarter_df['Year']==forecast_quarter_df['Financial_Year']) & (forecast_quarter_df['quarter'] >= forecast_quarter_df['Prev_Forecast_Quarter']) & (forecast_quarter_df['quarter'] < forecast_quarter_df['Forecast_Quarter'])]
    forecast_quarter_df = forecast_quarter_df[['Company','Item','Financial_Year','quarter','Data']]
    forecast_quarter_df = forecast_quarter_df.rename(columns={'Financial_Year':'Forecasted_Year','Data':'Forecasted_Revenue','quarter':'Quarter'})
    forecast_quarter_df = forecast_quarter_df.sort_values(by=['Company','Item','Quarter'],  ascending=True)
    
    # Calculating QoQ revenue change by company by item
    forecast_quarter_df = calc_signals_yoy_qoq(forecast_quarter_df,'Forecasted_Revenue','QoQ','Q','Forecasted_Year')
#     forecast_quarter_df = forecast_quarter_df[forecast_quarter_df['year_date'] >= 2015]
    
    # Writing QoQ changes to CSV
    forecast_quarter_df.to_csv("/output/External Data/Evaluate Medtech/evaluate_medtech_QoQ_change_forecasted_revenue.csv", index=False)
    
    # Merging data based on year date
    forecast_year_df = forecast_quarter_df[['Company','Item','Quarter','Forecasted_Year','Forecasted_Revenue']]
    max_year_df = forecast_year_df.groupby(['Company','Item','Forecasted_Year'], as_index=False)['Quarter'].max()
    forecast_year_df = pd.merge(forecast_year_df,max_year_df,how='inner',on=['Company','Item','Forecasted_Year','Quarter'])
    forecast_year_df = forecast_year_df[['Company','Item','Forecasted_Year','Forecasted_Revenue']]
    forecast_year_df = forecast_year_df.sort_values(by=['Company','Item','Forecasted_Year'],  ascending=True)
    
    # Calculating YoY revenue change by company by item
    forecast_year_df = calc_signals_yoy_qoq(forecast_year_df,'Forecasted_Revenue','YoY','Y','Forecasted_Year')
# #     forecast_year_df = forecast_year_df[forecast_year_df['year_date'] >= 2015]
    
#     # Writing YoY changes to CSV
    forecast_year_df.to_csv("/output/External Data/Evaluate Medtech/evaluate_medtech_YoY_change_forecasted_revenue.csv", index=False)
    return forecast_quarter_df