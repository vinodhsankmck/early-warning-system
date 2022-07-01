from company_upload import client_file_upload

from fuzzymatcher import link_table, fuzzy_left_join

def euromonitor_obligor_level_yoy_market_proportion_change(current_year,rank):
    import pandas as pd
    import numpy as np
    # Creating data frame out of company level file
    client_df = client_file_upload()
    # Creating data frame with the Retail RSP data for different euromonitor categories
    company_shares_df = pd.read_excel("/input/External Data/Euromonitor/Euromonitor - EWS Company Shares.xlsx", skiprows=[0,2], sheet_name='Company Share GBOL')
    company_shares_df = company_shares_df[company_shares_df['Rank'] <= rank]
    # Splitting data for Consumer Appliance and health
    cons_df = company_shares_df[company_shares_df['Currency Conversion']=='Volume']
    cons_df = cons_df[cons_df['Subcategory'].isin(['Consumer Appliances','Consumer Electronics'])]
    company_shares_df = company_shares_df[company_shares_df['Currency Conversion']=='Local Currency']
    company_shares_df = company_shares_df[company_shares_df['Current/Constant']=='Current Prices']
    # Merging the Consumer Appliance and health category back with the original data frame
    company_shares_df = company_shares_df.append(cons_df)
    # Sort data based on Category and Year
    company_shares_df = company_shares_df.sort_values(by=['Category','Year','Country','Data Type'])
    company_shares_df['Year'] = company_shares_df['Year'].astype(int)
    # Renaming required columns
    company_shares_df = company_shares_df.rename(columns={'Subcategory':'euromonitor_category','Global Brand Owner':'company_name','Year':'year','Country':'country',
                                                    'Data Type':'data_type','Percent':'actual_value','Unit':'unit'})
    # Creating forecast flag to identify forecasted values (based on current year)
    company_shares_df['forecast_flag'] = np.where(company_shares_df['year'] > (current_year - 1),True,False)
    company_shares_df = company_shares_df[['year','euromonitor_category','company_name','country','data_type','forecast_flag','actual_value','unit']]
    category_list = company_shares_df['euromonitor_category'].unique()
    # Fuzzy Match between Company Data and Euromonitor Company Data
    fuzzy_df = fuzzymatcher.fuzzy_left_join(client_df, company_shares_df, 'client_name', 'company_name')
    # Selecting only rows with match score > 20%
    fuzzy_df = fuzzy_df[fuzzy_df['best_match_score']>.20]
    fuzzy_df = fuzzy_df.rename(columns={'euromonitor_category_left':'euromonitor_category'})
    # Defining client to company level mapping data frame
    company_map_df = fuzzy_df[['client_name','company_name']]
    company_map_df = company_map_df.drop_duplicates()
    company_map_df = pd.merge(company_map_df,client_df,how='inner',on='client_name')
    company_map_df = company_map_df.rename(columns={'client_name_x':'client_name'})
    company_map_df = company_map_df[['client_name','company_name','industry_code','industry_name']]
    fuzzy_df = fuzzy_df[['euromonitor_category','company_name']]
    fuzzy_df = fuzzy_df.drop_duplicates()
    # Merging Company data with Euromonitor Data
    merge_df = pd.merge(fuzzy_df,company_shares_df,how='inner', on='company_name')
    merge_df = merge_df.drop('euromonitor_category_x',1)
    merge_df = merge_df.rename(columns={'euromonitor_category_y':'euromonitor_category','company_name_x':'company_name'})
    # Sort values by client and data type
    merge_df = merge_df.sort_values(by=['company_name','euromonitor_category','data_type','year'])
    merge_df = merge_df.drop_duplicates()
    company_list = merge_df['company_name'].unique()
    growth_data = []
    # Calculating YOY growth based on category and data type
    for company in company_list:
        growth_df = merge_df
        growth_df = growth_df[growth_df['company_name']==company]
        category_list = growth_df['euromonitor_category'].unique()
        for category in category_list:
            growth_df = growth_df[growth_df['euromonitor_category']==category]
            data_type_list = growth_df['data_type'].unique()
            for data in data_type_list:
                growth_signal_df = growth_df[growth_df['data_type']==data]
                growth_signal_df['yoy_growth_L1'] = growth_signal_df['actual_value'].pct_change(periods=1)
                growth_signal_df['yoy_growth_L2'] = growth_signal_df['actual_value'].pct_change(periods=2)
                growth_data.append(growth_signal_df)
    merge_df = pd.concat(growth_data)
    # Merging YOY data with client data
    merge_df = pd.merge(merge_df,company_map_df,how='inner',on='company_name')
    merge_df = merge_df.drop_duplicates()
    merge_df = merge_df.rename(columns={'company_name_x':'company_name'})
    merge_df = merge_df[['client_name','company_name','year','industry_code','industry_name','euromonitor_category','country','data_type','forecast_flag','yoy_growth_L1','yoy_growth_L2']]
    merge_df['yoy_growth_L1'] = merge_df['yoy_growth_L1'].abs()
    merge_df['yoy_growth_L2'] = merge_df['yoy_growth_L2'].abs()
    merge_df = merge_df.sort_values(by=['client_name','euromonitor_category','data_type','year'])
    # Writing the final data to the output path 
    merge_df.to_csv("/output/External Data/Euromonitor/euromonitor_obligor_level_yoy_market_proportion_change.csv",index=False)
    return merge_df