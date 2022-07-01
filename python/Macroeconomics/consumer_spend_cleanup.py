from company_file_upload import client_file_upload

#######################Consumer Spend############################
def consumer_spend_cleanup():
    client_df = client_file_upload()
    cp_df = pd.read_csv("/output/macroeconomics/consumer_spend/consumer_spend.csv")
    client_df_merge = pd.merge(client_df,cp_df, how='left', left_on='item_code_consumer_spend_BEA', right_on='item_code')
    client_df_merge = client_df_merge[['client_name','country_code','industry_name_level_1' ,'industry_name_level_2','industry_code'
                           ,'industry_name','item_name_consumer_spend_BEA','demographics_text','year','value']]
    client_df_merge = client_df_merge.rename(columns={'item_name_consumer_spend_BEA':'item_name','demographics_text':'demographics','value':'consumer_spend'})
    client_df_merge = client_df_merge.sort_values(by=['client_name','item_name','demographics','year'], ascending=[True,True,True,False])
    client_df_merge.to_csv("/output/macroeconomics/consumer_spend/external_data_consumer_spend.csv", index=False)
    
#######################Consumer Spend Quarterly############################
def consumer_spend_quarterly_cleanup():
    client_df = client_file_upload()
    cp_df = pd.read_excel("/lookup/macroeconomics/consumer_spend/consumer_spend_quarterly.xlsx")
    # Unpivot Data
    cp_unpivot_df = pd.melt(cp_df, id_vars=['product_type'], value_vars = cp_df.loc[:,cp_df.columns != 'product_type'],var_name='quarter')

    #rename unnamed columns
    cp_unpivot_df = cp_unpivot_df.rename(columns={'value':'consumer_spend'})
 
    client_df_merge = pd.merge(client_df,cp_unpivot_df, how='left', left_on='item_name_consumer_spend_quarterly_BEA', right_on='product_type')
    client_df_merge = client_df_merge[['client_name','item_name_consumer_spend_quarterly_BEA','quarter','consumer_spend']]
    client_df_merge = client_df_merge.rename(columns={'item_name_consumer_spend_quarterly_BEA':'item_name'})
    client_df_merge = client_df_merge.sort_values(by=['client_name','item_name','quarter'], ascending=[True,True,False])
    client_df_merge.to_csv("/output/macroeconomics/consumer_spend/external_data_consumer_spend_quarterly.csv", index=False)
    return client_df_merge
