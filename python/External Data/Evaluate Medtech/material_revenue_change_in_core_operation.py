from company_upload import client_file_upload
from signal_calculations import calc_signals_yoy_qoq

def evaluate_material_change_signal():
    import pandas as pd
    import numpy as np
    # Importing revenue data by company and year by item
    core_op_df = pd.read_csv("/input/External Data/Evaluate Medtech/2. Archived Company Annual Revenue Forecasts Broken Out by Segments.csv")
    core_op_df = core_op_df[core_op_df['Data Kind']=='Actual Data']
    core_op_df = core_op_df[core_op_df['Source']=='Reported']
    core_op_df = core_op_df[core_op_df['Item'].isin(['Rx Sales from Products module','MedTech & Healthcare Supply'])]
    core_op_df['Date'] = pd.to_datetime(pd.Series(core_op_df['Date']))
    
    # Calculating end of quarter based on date
    core_op_df['quarter'] = core_op_df['Date'].dt.to_period("Q").dt.end_time
    core_op_df['quarter'] = pd.to_datetime(core_op_df['quarter']).dt.date
    
    # Calculating max date by quarter and year
    core_op_df['Date'] = core_op_df['Date'].dt.to_period("M").dt.end_time
    core_op_df['Date'] = pd.to_datetime(core_op_df['Date']).dt.date
    quarter_df = core_op_df.groupby(['Company','Item','Year','quarter'], as_index=False)['Date'].max()
    
    # Calculating reported revenue by company by year
    reported_latest_df = core_op_df.groupby(['Company','Item','Year'], as_index=False)['Date'].min()
    
    # Sorting based on Company Item and Year
    reported_latest_df = reported_latest_df.sort_values(['Company','Item','Year','Date'])
    
    # Calculating next financial year and reported date
    company_list = reported_latest_df['Company'].unique()
    next_year_data = []
    for company in company_list:
        temp_df = reported_latest_df
        temp_df = temp_df[temp_df['Company']==company]
        temp_df['Prev_Financial_Year'] = temp_df['Year'].shift(1)
        temp_df['Prev_Actual_Announcement_Date'] = temp_df['Date'].shift(1)
        next_year_data.append(temp_df)
    reported_latest_df = pd.concat(next_year_data)
    
    # Calculating the actual reporting end of quarter
    reported_latest_df['Prev_Actual_Announcement_Date'] = pd.to_datetime(pd.Series(reported_latest_df['Prev_Actual_Announcement_Date']))
    reported_latest_df['Prev_Actual_Announcement_Quarter'] = reported_latest_df['Prev_Actual_Announcement_Date'].dt.to_period("Q").dt.end_time
    reported_latest_df['Prev_Actual_Announcement_Quarter'] = pd.to_datetime(reported_latest_df['Prev_Actual_Announcement_Quarter']).dt.date
    reported_latest_df['Date'] = pd.to_datetime(pd.Series(reported_latest_df['Date']))
    reported_latest_df['Actual_Announcement_Quarter'] = reported_latest_df['Date'].dt.to_period("Q").dt.end_time
    reported_latest_df['Actual_Announcement_Quarter'] = pd.to_datetime(reported_latest_df['Actual_Announcement_Quarter']).dt.date
    reported_latest_df = reported_latest_df[reported_latest_df['Prev_Actual_Announcement_Quarter'].notnull()]
    
    # Merging data based on year date
    core_op_qtr_df = pd.merge(core_op_df,quarter_df,how='inner',on=['Company','Item','Year','quarter','Date'])
    core_op_qtr_df = core_op_qtr_df[['Company','Item','Year','quarter','Data']]
    
    # Merging with latest reported date data
    core_op_qtr_curr_df = pd.merge(core_op_qtr_df,reported_latest_df,how='left',on=['Company','Item'])
    core_op_qtr_curr_df = core_op_qtr_curr_df.rename(columns={'Data':'Reported_Revenue','Year_x':'Financial_Year'})
    
    # Filtering data based on quarter and financial year
    core_op_qtr_curr_df = core_op_qtr_curr_df.loc[(core_op_qtr_curr_df['Financial_Year']==core_op_qtr_curr_df['Prev_Financial_Year']) & (core_op_qtr_curr_df['quarter']>= core_op_qtr_curr_df['Prev_Actual_Announcement_Quarter']) & (core_op_qtr_curr_df['quarter'] < core_op_qtr_curr_df['Actual_Announcement_Quarter'])]
    
    # Selecting only required columns
    core_op_qtr_curr_df = core_op_qtr_curr_df[['Company','Item','Financial_Year','quarter','Prev_Actual_Announcement_Quarter','Reported_Revenue']]
    core_op_qtr_curr_df = core_op_qtr_curr_df.rename(columns={'Prev_Actual_Announcement_Quarter':'Actual_Announcement_Quarter'})
    core_op_qtr_curr_df = core_op_qtr_curr_df.sort_values(by=['Company','Item','quarter'])
    # Calculating YoY revenue change by company by item
    core_op_qtr_curr_df = calc_signals_yoy_qoq(core_op_qtr_curr_df,'Reported_Revenue','YoY_Reported_Revenue','Y','Financial_Year')
#     core_op_qtr_curr_df = core_op_qtr_curr_df.loc[(core_op_qtr_curr_df['Company']=='Amgen') & (core_op_qtr_curr_df['Item']=='Rx Sales from Products module')]
    core_op_qtr_curr_df.to_csv("/output/External Data/Evaluate Medtech/evaluate_medtech_YoY_change_core_operation.csv", index=False)
    
    return core_op_qtr_curr_df