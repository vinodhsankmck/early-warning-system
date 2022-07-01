from company_upload import client_file_upload

def euromonitor_yoy_product_category_growth_signal(current_year):
    import pandas as pd
    import numpy as np
    # Creating data frame out of company level file
    client_df = client_file_upload()
    data_type = ['Retail Value RSP','Socio-economic indicators']
    # Creating data frame with the Retail RSP data for different euromonitor categories
    market_data_df = pd.read_excel("/input/External Data/Euromonitor/Euromonitor - EWS Industries, Economies and Consumers.xlsx", skiprows=[0,2], sheet_name='Market Sizes')
    # Splitting Alcoholic Drinks category due to multiple data types
    alcoholic_drinks_df1 = market_data_df[market_data_df['Subcategory']=='Alcoholic Drinks']
    alcoholic_drinks_df2 = alcoholic_drinks_df1[alcoholic_drinks_df1['Data Type']=='Total Volume']
    market_data_df = market_data_df[market_data_df['Currency Conversion']=='Local Currency']
    market_data_df = market_data_df[market_data_df['Current/Constant']=='Historic Current Prices, Forecast Current Prices']
    alcoholic_drinks_df1 = alcoholic_drinks_df1[alcoholic_drinks_df1['Currency Conversion']=='Local Currency']
    alcoholic_drinks_df1 = alcoholic_drinks_df1[alcoholic_drinks_df1['Current/Constant']=='Historic Current Prices, Forecast Current Prices']
    market_data_df = market_data_df[market_data_df['Data Type'].isin(data_type)]
    # Merging the Alcoholic Drinks category back with the original data frame
    market_data_df = market_data_df.append(alcoholic_drinks_df1)
    market_data_df = market_data_df.append(alcoholic_drinks_df2)
    market_data_df = market_data_df.sort_values(by=['Category','Year','Country','Data Type'])
    market_data_df['Year'] = market_data_df['Year'].astype(int)
    # Renaming required columns
    market_data_df = market_data_df.rename(columns={'Subcategory':'euromonitor_category','Year':'year','Country':'country',
                                                    'Data Type':'data_type','Actual':'actual_value','GrowthYoY':'yoy_growth_L1','Unit':'unit'})
    # Creating forecast flag to identify forecasted values (based on current year)
    market_data_df['forecast_flag'] = np.where(market_data_df['year'] > (current_year - 1),True,False)
    market_data_df = market_data_df[['year','euromonitor_category','country','data_type','forecast_flag','actual_value','unit','yoy_growth_L1']]
    category_list = market_data_df['euromonitor_category'].unique()
    growth_data = []
    # Calculating YOY growth based on category and data type
    for category in category_list:
        growth_df = market_data_df
        growth_df = growth_df[growth_df['euromonitor_category']==category]
        data_type_list = growth_df['data_type'].unique()
        for data in data_type_list:
            growth_signal_df = growth_df[growth_df['data_type']==data]
            growth_signal_df['yoy_growth_L2'] = growth_signal_df['actual_value'].pct_change(periods=2)
            growth_data.append(growth_signal_df)
    market_data_df = pd.concat(growth_data)
    # Merging Company data with Euromonitor Market Data
    merge_df = pd.merge(client_df,market_data_df,how='inner', on='euromonitor_category')
    merge_df = merge_df.rename(columns={'euromonitor_category_x':'euromonitor_category'})
    merge_df = merge_df[['client_name','year','industry_code','industry_name','euromonitor_category','country','data_type','forecast_flag','yoy_growth_L1','yoy_growth_L2']]
    # Re-calculating YOY Growth L1 since the value is already in percent units
    merge_df['yoy_growth_L1'] = merge_df['yoy_growth_L1'].astype(float)/100
    merge_df = merge_df.sort_values(by=['client_name','data_type','year'])
    # Writing the final data to the output path 
    merge_df.to_csv("/output/External Data/Euromonitor/euromonitor_yoy_product_category_growth_signal.csv",index=False)
    return merge_df