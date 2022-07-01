def calc_signals_calendar_events(df,metric_name,signal_type,signal_name,level):
    import pandas as pd
    import numpy as np
    signal_df = df
    signal_data = []
    level = level + 1
    if signal_type == 'Q':
        company_list = signal_df['Company'].unique()
        for company in company_list:
            temp_df = signal_df[signal_df['Company']==company]
            for lvl in range(1,level):
                temp_df[signal_name + '_qoq_growth_L' + str(lvl)] = temp_df[metric_name].pct_change(periods=lvl)
                signal_data.append(temp_df)
        signal_df = pd.concat(signal_data)
    elif signal_type == 'PCT':
        company_list = signal_df['Company'].unique()
        for company in company_list:
            temp_df = signal_df[signal_df['Company']==company]
            for lvl in range(1,level):
                temp_df[signal_name + '_qoq_growth_L' + str(lvl)] = abs(temp_df[metric_name] - temp_df[metric_name].shift(lvl))
                signal_data.append(temp_df)
    elif signal_type == 'EVENT':
        company_list = signal_df['Company'].unique()
        for company in company_list:
            temp_df = signal_df[signal_df['Company']==company]
            for lvl in range(1,level):
                temp_df[signal_name + '_qoq_L' + str(lvl)] = temp_df[metric_name].shift(lvl)
                signal_data.append(temp_df)
        signal_df = pd.concat(signal_data)
    # Replace inf and -inf to Blank
    signal_df.replace([np.inf,-np.inf],'', inplace=True)
    # Remove Duplicates
    signal_df = signal_df.drop_duplicates()
    # Merge with EWS Company Data
    company_df = client_file_upload()
    merge_df = pd.merge(company_df,signal_df,how='inner',on='Company')
    return merge_df

def calc_signals_yoy_qoq(df,metric_name,signal_name,signal_type,year_type):
    import pandas as pd
    import numpy as np
    signal_df = df
    signal_data = []
#     level = level + 1
    company_list = signal_df['Company'].unique()
    for company in company_list:
        temp_df1 = signal_df[signal_df['Company']==company]
        item_list = temp_df1['Item'].unique()
        for item in item_list:
            temp_df2 = temp_df1[temp_df1['Item']==item]
            year_list = temp_df2[year_type].unique()
            if signal_type == 'AvsF':
                    temp_df2[signal_name + '_L1'] = temp_df2[metric_name].shift(1)
                    temp_df2[signal_name + '_L2'] = temp_df2[metric_name].shift(2)
                    temp_df2[signal_name + '_L3'] = temp_df2[metric_name].shift(3)
                    temp_df2[signal_name + '_L4'] = temp_df2[metric_name].shift(4)
                    signal_data.append(temp_df2)
            for year in year_list:
                temp_df3 = temp_df2[temp_df2[year_type]==year]
            if signal_type == 'Q':
                temp_df2[signal_name + '_growth_L1'] = temp_df2[metric_name].pct_change(periods=1)
                temp_df2[signal_name + '_growth_L2'] = temp_df2[metric_name].pct_change(periods=2)
                temp_df2[signal_name + '_growth_L3'] = temp_df2[metric_name].pct_change(periods=3)
                temp_df2[signal_name + '_growth_L4'] = temp_df2[metric_name].pct_change(periods=4)
                temp_df2[signal_name + '_growth_L5'] = temp_df2[metric_name].pct_change(periods=5)
                temp_df2[signal_name + '_growth_L6'] = temp_df2[metric_name].pct_change(periods=6)
                temp_df2[signal_name + '_growth_L7'] = temp_df2[metric_name].pct_change(periods=7)
                temp_df2[signal_name + '_growth_L8'] = temp_df2[metric_name].pct_change(periods=8)
                signal_data.append(temp_df2)
#                 for level in range(1,level):
#                     temp_df3[signal_name + '_growth_L' + str(level)] = temp_df3[metric_name].pct_change(periods=level)
            if signal_type == 'Y':
                temp_df2[signal_name + '_growth_L1'] = temp_df2[metric_name].pct_change(periods=1)
                temp_df2[signal_name + '_growth_L2'] = temp_df2[metric_name].pct_change(periods=2)
                signal_data.append(temp_df2)
    signal_df = pd.concat(signal_data)
    # Replace inf and -inf to Blank
    signal_df.replace([np.inf,-np.inf],'', inplace=True)
    # Remove Duplicates
    signal_df = signal_df.drop_duplicates()
    # Merge with EWS Company Data
    company_df = client_file_upload()
    merge_df = pd.merge(company_df,signal_df,how='inner',on='Company')
    return merge_df