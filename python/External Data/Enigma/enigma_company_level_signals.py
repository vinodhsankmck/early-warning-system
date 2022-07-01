from company_upload import client_file_upload
from enigma_signal_calculations import enigma_calc_signals

def enigma_company_level_signals():
    import pandas as pd
    import numpy as np
    # Import Enigma Data
    trans_df = pd.read_excel("/input/External Data/Engima/SampleDataParentChild (v4).xlsx", sheet_name='Firmographics-MTX Data', skiprows=5)
    state_df = pd.read_csv("/input/External Data/Engima/state_code_mapping.csv")
    # Merging US state code data to filter to only US clients
    trans_df = pd.merge(trans_df,state_df,how='inner',left_on='STATE',right_on='Code')
    # Removing rows with NULL revenue
    trans_df = trans_df[trans_df['MONTHLY_CARD_REVENUE'].notnull()]
    # Selecting the required columns
    trans_df = trans_df[['COMPANY_NAME','STATE','CITY','TRANSACTION_MONTH','MONTHLY_CARD_REVENUE','MONTLHY_CARD_TRANSACTIONS','MONTHLY_SHOPPERS']]
    # Dropping all full duplicate rows
    trans_df = trans_df.drop_duplicates()
    # Importing EWS company information with NAICS
    client_df = client_file_upload()
    # Merging company data with Enigma data
    trans_df = pd.merge(client_df,trans_df,how='inner',left_on='client_name',right_on='COMPANY_NAME')
    # Calculating MoM Company Level growth for the past 12 months
    monthly_df = trans_df.sort_values(by=['COMPANY_NAME','STATE','CITY','TRANSACTION_MONTH'])
    # Calculating average basket size which is Revenue over # of Transactions
    monthly_df['avg_basket_size'] = monthly_df['MONTHLY_CARD_REVENUE']/monthly_df['MONTLHY_CARD_TRANSACTIONS']
    monthly_df = enigma_calc_signals(monthly_df,['MONTHLY_CARD_REVENUE','MONTLHY_CARD_TRANSACTIONS','avg_basket_size'],['MoM_revenue','MoM_transactions','MoM_basket_size'],'M')
    # Writing MoM Company Level changes to CSV
    monthly_df.to_csv("/output/External Data/Engima/enigma_MoM_company_level_change_signals.csv", index=False)
    
    # Converting the event occuring date to end of quarter
    trans_df['TRANSACTION_QUARTER'] = trans_df['TRANSACTION_MONTH'].dt.to_period("Q").dt.end_time
    trans_df['TRANSACTION_QUARTER'] = pd.to_datetime(trans_df['TRANSACTION_QUARTER']).dt.date
    trans_df['TRANSACTION_YEAR'] = pd.to_datetime(trans_df['TRANSACTION_QUARTER']).dt.date
    # Calculating QoQ Company Level growth for the past 8 quarters
    quarterly_df = trans_df.groupby(['COMPANY_NAME','STATE','CITY','TRANSACTION_QUARTER'], as_index=False)['MONTHLY_CARD_REVENUE','MONTLHY_CARD_TRANSACTIONS'].sum()
    quarterly_df = quarterly_df.rename(columns={'MONTHLY_CARD_REVENUE':'QUARTERLY_CARD_REVENUE','MONTLHY_CARD_TRANSACTIONS':'QUARTERLY_CARD_TRANSACTIONS'})
    quarterly_df = quarterly_df.sort_values(by=['COMPANY_NAME','STATE','CITY','TRANSACTION_QUARTER'])
    # Calculating average basket size which is Revenue over # of Transactions
    quarterly_df['avg_basket_size'] = quarterly_df['QUARTERLY_CARD_REVENUE']/quarterly_df['QUARTERLY_CARD_TRANSACTIONS']
    quarterly_df = enigma_calc_signals(quarterly_df,['QUARTERLY_CARD_REVENUE','QUARTERLY_CARD_TRANSACTIONS','avg_basket_size'],['QoQ_revenue','QoQ_transactions','QoQ_basket_size'],'Q')
    # Writing MoM Company Level changes to CSV
    quarterly_df.to_csv("/output/External Data/Engima/enigma_QoQ_company_level_change_signals.csv", index=False)
    
    # Calculating YoY Company Level growth for the past 8 quarters
    yearly_df = trans_df.groupby(['COMPANY_NAME','STATE','CITY','TRANSACTION_YEAR'], as_index=False)['MONTHLY_CARD_REVENUE','MONTLHY_CARD_TRANSACTIONS'].sum()
    yearly_df = yearly_df.rename(columns={'MONTHLY_CARD_REVENUE':'YEARLY_CARD_REVENUE','MONTLHY_CARD_TRANSACTIONS':'YEARLY_CARD_TRANSACTIONS'})
    yearly_df = yearly_df.sort_values(by=['COMPANY_NAME','STATE','CITY','TRANSACTION_YEAR'])
    # Calculating average basket size which is Revenue over # of Transactions
    yearly_df['avg_basket_size'] = yearly_df['YEARLY_CARD_REVENUE']/yearly_df['YEARLY_CARD_TRANSACTIONS']
    yearly_df = enigma_calc_signals(yearly_df,['YEARLY_CARD_REVENUE','YEARLY_CARD_TRANSACTIONS','avg_basket_size'],['YoY_revenue','YoY_transactions','YoY_basket_size'],'Y')
    # Writing MoM Company Level changes to CSV
    yearly_df.to_csv("/output/External Data/Engima/enigma_YoY_company_level_change_signals.csv", index=False)
    
    return yearly_df