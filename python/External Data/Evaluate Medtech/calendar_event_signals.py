from company_upload import client_file_upload
from signal_calculations import calc_signals_calendar_events

import pandas as pd
import numpy as np
from datetime import datetime

def evaluate_calendar_event_signals():
    # Importing calendar events by company
    events_df = pd.read_excel("/input/External Data/Evaluate Medtech/3. Calendar of Events Report.xlsx", sheet_name='Product Events', skiprows=4)
    # Assigning Event type for the analysis
    event_type = ['Closed (Positive)','Closed (Negative)']
    events_df = events_df[events_df['Event Status'].isin(event_type)]
    # Converting the event occuring date to end of quarter
    events_df['Source Date'] = events_df['Source Date'].dt.to_period("Q").dt.end_time
    events_df = events_df[['Company','Event Status','Source Date']]
    events_df = events_df.rename(columns={'Source Date':'as_of_date'})
    events_df['year'] = events_df['as_of_date'].dt.year
    # Creating positive and negative event flags based on the event status
    events_df = events_df.groupby(['Company','Event Status','as_of_date'],as_index=False).count()
    # Convert as of date to date
    events_df['as_of_date'] = events_df.as_of_date.dt.date
    events_df = events_df.rename(columns={'year':'total_events'})
    # Pivot data frame to create positive and negative event columns
    events_df['year'] = pd.DatetimeIndex(events_df['as_of_date']).year 
    events_df = pd.pivot_table(events_df,index=['Company','year','as_of_date'],columns='Event Status',values='total_events',aggfunc='max')
    events_df = events_df.rename(columns={'Closed (Negative)':'total_negative_events','Closed (Positive)':'total_positive_events'})
    # Calculating total events
    events_df['total_events'] = events_df['total_positive_events'].fillna(0) + events_df['total_negative_events'].fillna(0)
    # Reset index from Multi Index
    events_df = events_df.reset_index()
    # Fill NaN positive and negative events with 0
    events_df['total_positive_events'] = events_df['total_positive_events'].fillna(0)
    events_df['total_negative_events'] = events_df['total_negative_events'].fillna(0)
    # Calculating Negative event Flag
    events_df['negative_events_flag'] = np.where(events_df['total_negative_events'] > 0,1,0)
    # Calculating Negative event Percent
    events_df['pct_negative_events'] = events_df['total_negative_events']/events_df['total_events']
    # Sorting data by company and as of date
    events_df = events_df.sort_values(by=['Company','as_of_date'])
    
    # Creating signals based on the event status type
    negative_df = calc_signals_calendar_events(events_df,'total_negative_events','EVENT','negative_events',8)
    positive_df = calc_signals_calendar_events(events_df,'total_positive_events','EVENT','positive_events',8)
    negative_flag_df = calc_signals_calendar_events(events_df,'negative_events_flag','EVENT','negative_event_flag',8)
    negative_proportion_df = calc_signals_calendar_events(events_df,'pct_negative_events','EVENT','pct_negative_events',12)
    # Writing Negative events as CSV
    negative_df.to_csv("/output/External Data/Evaluate Medtech/evaluate_medtech_negative_event_signals.csv", index=False)
    # Writing Positive events as CSV
    positive_df.to_csv("/output/External Data/Evaluate Medtech/evaluate_medtech_positive_event_signals.csv", index=False)
    # Writing Negative Event Flag as CSV
    negative_flag_df.to_csv("/output/External Data/Evaluate Medtech/evaluate_medtech_negative_event_flag_signals.csv", index=False)
    # Writing Proportion of Negative events as CSV
    negative_proportion_df.to_csv("/output/External Data/Evaluate Medtech/evaluate_medtech_pct_of_negative_event_signals.csv", index=False)
    return events_df#['Company']
    