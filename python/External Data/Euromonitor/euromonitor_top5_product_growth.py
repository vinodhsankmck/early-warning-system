from company_upload import client_file_upload

def euromonitor_top5_product_level_signal_yoy(current_year):
    import pandas as pd
    import numpy as np
    # Creating data frame out of company level file
    client_df = client_file_upload()
    # Creating data frame with the Retail RSP data for different euromonitor categories
    company_shares_df = pd.read_excel("/input/External Data/Euromonitor/Euromonitor - EWS Company Shares.xlsx", skiprows=[0,2], sheet_name='Company Share GBOL')
    company_shares_df = company_shares_df[company_shares_df['Rank'].isin([1,2,3,4,5])]
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
    company_shares_df = company_shares_df.rename(columns={'Subcategory':'euromonitor_category','Year':'year','Country':'country',
                                                    'Data Type':'data_type','Percent':'actual_value','Unit':'unit'})
    # Creating forecast flag to identify forecasted values (based on current year)
    company_shares_df['forecast_flag'] = np.where(company_shares_df['year'] > (current_year - 1),True,False)
    company_shares_df = company_shares_df[['year','euromonitor_category','country','data_type','forecast_flag','actual_value','unit']]
    company_shares_df = company_shares_df.groupby(['year','euromonitor_category','country','data_type','forecast_flag','unit'], as_index=False)['actual_value'].sum()
    category_list = company_shares_df['euromonitor_category'].unique()
    growth_data = []
    # Calculating YOY growth based on category and data type
    for category in category_list:
        growth_df = company_shares_df
        growth_df = growth_df[growth_df['euromonitor_category']==category]
        data_type_list = growth_df['data_type'].unique()
        for data in data_type_list:
            growth_signal_df = growth_df[growth_df['data_type']==data]
            growth_signal_df['yoy_growth_L1'] = growth_signal_df['actual_value'].pct_change(periods=1)
            growth_signal_df['yoy_growth_L2'] = growth_signal_df['actual_value'].pct_change(periods=2)
            growth_data.append(growth_signal_df)
    company_shares_df = pd.concat(growth_data)
    # Merging Company data with Euromonitor Market Data
    merge_df = pd.merge(client_df,company_shares_df,how='inner', on='euromonitor_category')
    merge_df = merge_df.rename(columns={'euromonitor_category_x':'euromonitor_category'})
    merge_df = merge_df[['client_name','year','industry_code','industry_name','euromonitor_category','country','data_type','forecast_flag','yoy_growth_L1','yoy_growth_L2']]
    # Sort values by client and data type
    merge_df = merge_df.sort_values(by=['client_name','data_type','year'])
    merge_df['yoy_growth_L1'] = merge_df['yoy_growth_L1'].abs()
    merge_df['yoy_growth_L2'] = merge_df['yoy_growth_L2'].abs()
    # Writing the final data to the output path 
    merge_df.to_csv("/output/External Data/Euromonitor/euromonitor_top 5_company_yoy_growth_signal.csv",index=False)
    return merge_df