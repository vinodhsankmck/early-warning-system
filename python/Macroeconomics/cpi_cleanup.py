from company_file_upload import client_file_upload

#######################Consumer Price Index############################
def cpi_cleanup():
    client_df = client_file_upload()
    cpi_df = pd.read_csv("/output/macroeconomics/consumer_price_index/cpi_data.csv", parse_dates = {"mm_yy" : ["year","month"]}, keep_date_col=True)
    client_df_merge = pd.merge(client_df,cpi_df, how='left', left_on='item_code_cpi_BEA', right_on='item_code')
    client_df_merge = client_df_merge[['client_name','country_code','industry_name_level_1' ,'industry_name_level_2','industry_code'
                           ,'industry_name','item_code_cpi_BEA','item_name_cpi_BEA','region_name','mm_yy','CPI']]
    client_df_merge = client_df_merge.rename(columns={'item_name_cpi_BEA':'item_name','CPI':'consumer_price_index'})
    client_df_merge = client_df_merge.sort_values(by=['client_name','item_name','region_name','mm_yy'], ascending=[True,True,True,False])
    client_df_merge.to_csv("/output/macroeconomics/consumer_price_index/external_data_cpi_final.csv", index=False)