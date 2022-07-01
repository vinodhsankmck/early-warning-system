from company_file_upload import client_file_upload

###################Retail Trade Survey - Sales Monthly#########################

def rts_monthly_sales_cleanup():
    client_df = client_file_upload()
    sales_df = pd.read_excel("/lookup/macroeconomics/retail_trade_survey/sales.xlsx")
    #unpivot
    sales_unpivot_df = pd.melt(sales_df, id_vars=['naics_code','industry_name'], value_vars = sales_df.loc[:,sales_df.columns != 'industry_name'],var_name='month')
    
    #rename unnamed columns
    sales_unpivot_df = sales_unpivot_df.rename(columns={'value':'sales'})
    
    #aggregate data at NAICS 5 Digit level
    sales_unpivot_df['naics_code'] = sales_unpivot_df.naics_code.astype('str')
    sales_unpivot_df['naics_code'] = sales_unpivot_df['naics_code'].str[:5]
    sales_unpivot_df['naics_code'] = sales_unpivot_df.naics_code.astype('int64')
    client_df_merge_naics_5 = pd.merge(client_df,sales_unpivot_df, how='left', left_on='industry_code', right_on='naics_code')
    client_df_merge_naics_5 = client_df_merge_naics_5.rename(columns={'industry_name_x':'industry_name','sales':'sales_naics_5'})
    client_df_merge_naics_5 = client_df_merge_naics_5[['month','client_name','country_code','industry_name_level_1' ,'industry_name_level_2','industry_code'
                           ,'industry_name','NAICS 2 Digit','NAICS 3 Digit','NAICS 4 Digit','sales_naics_5']]
    client_df_merge_naics_5 = client_df_merge_naics_5.groupby(['month','client_name','country_code','industry_name_level_1' ,'industry_name_level_2','industry_code'
                           ,'industry_name','NAICS 2 Digit','NAICS 3 Digit','NAICS 4 Digit'], as_index=False)['sales_naics_5'].max()

    #aggregate data at NAICS 4 Digit level
    sales_unpivot_df['naics_code'] = sales_unpivot_df.naics_code.astype('str')
    sales_unpivot_df['naics_code'] = sales_unpivot_df['naics_code'].str[:4]
    sales_unpivot_df['naics_code'] = sales_unpivot_df.naics_code.astype('int64')
    client_df_merge_naics_4 = pd.merge(client_df,sales_unpivot_df, how='left', left_on='NAICS 4 Digit', right_on='naics_code')
    client_df_merge_naics_4 = client_df_merge_naics_4.rename(columns={'industry_name_x':'industry_name','sales':'sales_naics_4'})
    client_df_merge_naics_4 = client_df_merge_naics_4[['month','client_name','country_code','industry_name_level_1' ,'industry_name_level_2','industry_code'
                           ,'industry_name','NAICS 2 Digit','NAICS 3 Digit','NAICS 4 Digit','sales_naics_4']]
    client_df_merge_naics_4 = client_df_merge_naics_4.groupby(['month','client_name','country_code','industry_name_level_1' ,'industry_name_level_2','industry_code'
                           ,'industry_name','NAICS 2 Digit','NAICS 3 Digit','NAICS 4 Digit'], as_index=False)['sales_naics_4'].max()
    
    #aggregate data at NAICS 3 Digit level
    sales_unpivot_df['naics_code'] = sales_unpivot_df.naics_code.astype('str')
    sales_unpivot_df['naics_code'] = sales_unpivot_df['naics_code'].str[:3]
    sales_unpivot_df['naics_code'] = sales_unpivot_df.naics_code.astype('int64')
    client_df_merge_naics_3 = pd.merge(client_df,sales_unpivot_df, how='left', left_on='NAICS 3 Digit', right_on='naics_code')
    client_df_merge_naics_3 = client_df_merge_naics_3.rename(columns={'industry_name_x':'industry_name','sales':'sales_naics_3'})
    client_df_merge_naics_3 = client_df_merge_naics_3[['month','client_name','country_code','industry_name_level_1' ,'industry_name_level_2','industry_code'
                           ,'industry_name','NAICS 2 Digit','NAICS 3 Digit','NAICS 4 Digit','sales_naics_3']]
    client_df_merge_naics_3 = client_df_merge_naics_3.groupby(['month','client_name','country_code','industry_name_level_1' ,'industry_name_level_2','industry_code'
                           ,'industry_name','NAICS 2 Digit','NAICS 3 Digit','NAICS 4 Digit'], as_index=False)['sales_naics_3'].max()
    
    #extracting final data set
    
    client_df_merge_naics_5 = client_df_merge_naics_5[['month','client_name','industry_code','sales_naics_5']]
    client_df_merge_naics_4 = client_df_merge_naics_4[['month','client_name','industry_code','sales_naics_4']]
    client_df_merge_naics_3 = pd.merge(client_df_merge_naics_3,client_df_merge_naics_4, how='left', left_on=['month','client_name','industry_code'], right_on=['month','client_name','industry_code'])
    client_df_merge_naics_3 = pd.merge(client_df_merge_naics_3,client_df_merge_naics_5, how='left', left_on=['month','client_name','industry_code'], right_on=['month','client_name','industry_code'])
    client_df_merge_naics_3 = client_df_merge_naics_3[['month','client_name','country_code','industry_name_level_1' ,'industry_name_level_2','industry_code'
                           ,'industry_name','NAICS 2 Digit','NAICS 3 Digit','NAICS 4 Digit','sales_naics_3','sales_naics_4','sales_naics_5']]
    client_df_merge_naics_3 = client_df_merge_naics_3.sort_values(by=['month','client_name','industry_name'], ascending=[False,True,True])
    
    client_df_merge_naics_3.to_csv("/output/macroeconomics/retail_trade_survey/external_data_sales.csv", index=False)
    #return client_df_merge_naics_3
# rts_monthly_sales_cleanup()

###################Retail Trade Survey - Inventory Monthly#########################

def rts_monthly_inventory_cleanup():
    client_df = client_file_upload()
    inventory_df = pd.read_excel("/lookup/macroeconomics/retail_trade_survey/inventory.xlsx")
    #unpivot
    inventory_unpivot_df = pd.melt(inventory_df, id_vars=['naics_code','industry_name'], value_vars = inventory_df.loc[:,inventory_df.columns != 'industry_name'],var_name='month')
    
    #rename unnamed columns
    inventory_unpivot_df = inventory_unpivot_df.rename(columns={'value':'inventory'})
    
    #aggregate data at NAICS 5 Digit level
    inventory_unpivot_df['naics_code'] = inventory_unpivot_df.naics_code.astype('str')
    inventory_unpivot_df['naics_code'] = inventory_unpivot_df['naics_code'].str[:5]
    inventory_unpivot_df['naics_code'] = inventory_unpivot_df.naics_code.astype('int64')
    client_df_merge_naics_5 = pd.merge(client_df,inventory_unpivot_df, how='left', left_on='industry_code', right_on='naics_code')
    client_df_merge_naics_5 = client_df_merge_naics_5.rename(columns={'industry_name_x':'industry_name','inventory':'inventory_naics_5'})
    client_df_merge_naics_5 = client_df_merge_naics_5[['month','client_name','country_code','industry_name_level_1' ,'industry_name_level_2','industry_code'
                           ,'industry_name','NAICS 2 Digit','NAICS 3 Digit','NAICS 4 Digit','inventory_naics_5']]
    client_df_merge_naics_5 = client_df_merge_naics_5.groupby(['month','client_name','country_code','industry_name_level_1' ,'industry_name_level_2','industry_code'
                           ,'industry_name','NAICS 2 Digit','NAICS 3 Digit','NAICS 4 Digit'], as_index=False)['inventory_naics_5'].max()

    #aggregate data at NAICS 4 Digit level
    inventory_unpivot_df['naics_code'] = inventory_unpivot_df.naics_code.astype('str')
    inventory_unpivot_df['naics_code'] = inventory_unpivot_df['naics_code'].str[:4]
    inventory_unpivot_df['naics_code'] = inventory_unpivot_df.naics_code.astype('int64')
    client_df_merge_naics_4 = pd.merge(client_df,inventory_unpivot_df, how='left', left_on='NAICS 4 Digit', right_on='naics_code')
    client_df_merge_naics_4 = client_df_merge_naics_4.rename(columns={'industry_name_x':'industry_name','inventory':'inventory_naics_4'})
    client_df_merge_naics_4 = client_df_merge_naics_4[['month','client_name','country_code','industry_name_level_1' ,'industry_name_level_2','industry_code'
                           ,'industry_name','NAICS 2 Digit','NAICS 3 Digit','NAICS 4 Digit','inventory_naics_4']]
    client_df_merge_naics_4 = client_df_merge_naics_4.groupby(['month','client_name','country_code','industry_name_level_1' ,'industry_name_level_2','industry_code'
                           ,'industry_name','NAICS 2 Digit','NAICS 3 Digit','NAICS 4 Digit'], as_index=False)['inventory_naics_4'].max()
    
    #aggregate data at NAICS 3 Digit level
    inventory_unpivot_df['naics_code'] = inventory_unpivot_df.naics_code.astype('str')
    inventory_unpivot_df['naics_code'] = inventory_unpivot_df['naics_code'].str[:3]
    inventory_unpivot_df['naics_code'] = inventory_unpivot_df.naics_code.astype('int64')
    client_df_merge_naics_3 = pd.merge(client_df,inventory_unpivot_df, how='left', left_on='NAICS 3 Digit', right_on='naics_code')
    client_df_merge_naics_3 = client_df_merge_naics_3.rename(columns={'industry_name_x':'industry_name','inventory':'inventory_naics_3'})
    client_df_merge_naics_3 = client_df_merge_naics_3[['month','client_name','country_code','industry_name_level_1' ,'industry_name_level_2','industry_code'
                           ,'industry_name','NAICS 2 Digit','NAICS 3 Digit','NAICS 4 Digit','inventory_naics_3']]
    client_df_merge_naics_3 = client_df_merge_naics_3.groupby(['month','client_name','country_code','industry_name_level_1' ,'industry_name_level_2','industry_code'
                           ,'industry_name','NAICS 2 Digit','NAICS 3 Digit','NAICS 4 Digit'], as_index=False)['inventory_naics_3'].max()
    
    #extracting final data set
    
    client_df_merge_naics_5 = client_df_merge_naics_5[['month','client_name','industry_code','inventory_naics_5']]
    client_df_merge_naics_4 = client_df_merge_naics_4[['month','client_name','industry_code','inventory_naics_4']]
    client_df_merge_naics_3 = pd.merge(client_df_merge_naics_3,client_df_merge_naics_4, how='left', left_on=['month','client_name','industry_code'], right_on=['month','client_name','industry_code'])
    client_df_merge_naics_3 = pd.merge(client_df_merge_naics_3,client_df_merge_naics_5, how='left', left_on=['month','client_name','industry_code'], right_on=['month','client_name','industry_code'])
    client_df_merge_naics_3 = client_df_merge_naics_3[['month','client_name','country_code','industry_name_level_1' ,'industry_name_level_2','industry_code'
                           ,'industry_name','NAICS 2 Digit','NAICS 3 Digit','NAICS 4 Digit','inventory_naics_3','inventory_naics_4','inventory_naics_5']]
    client_df_merge_naics_3 = client_df_merge_naics_3.sort_values(by=['month','client_name','industry_name'], ascending=[False,True,True])
    
    client_df_merge_naics_3.to_csv("/output/macroeconomics/retail_trade_survey/external_data_inventory.csv", index=False)
    #return client_df_merge_naics_3
rts_monthly_inventory_cleanup()
