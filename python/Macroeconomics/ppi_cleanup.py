from company_file_upload import client_file_upload

#######################Producer Price Index############################
def ppi_cleanup():
    client_df = client_file_upload()
    ppi_df = pd.read_csv("/output/macroeconomics/producer_price_index/ppi.csv", parse_dates = {"mm_yy" : ["year","month"]}, keep_date_col=True)
    # naics_df = pd.read_csv("/lookup/macroeconomics/producer_price_index/naics_code_description_mapping.csv",encoding='unicode_escape')
    ppi_df = ppi_df[['naics_code','mm_yy','value']]
    ppi_df['naics_code'] = ppi_df['naics_code'].str.replace('-','').str.replace('X','')
    ppi_df['naics_code']=ppi_df.naics_code.astype('int64')
    client_df_merge_naics_5 = pd.merge(client_df,ppi_df, how='left', left_on='industry_code', right_on='naics_code')
    client_df_merge_naics_5 = client_df_merge_naics_5[['mm_yy','client_name','country_code','industry_name_level_1' ,'industry_name_level_2','industry_code'
                           ,'industry_name','NAICS 2 Digit','NAICS 3 Digit','NAICS 4 Digit','value']]
    client_df_merge_naics_5 = client_df_merge_naics_5.rename(columns={'value':'ppi_naics_5'})
    client_df_merge_naics_5.drop_duplicates(subset=['mm_yy','client_name'])
    client_df_merge_naics_5 = client_df_merge_naics_5.groupby(['mm_yy','client_name','country_code','industry_name_level_1' ,'industry_name_level_2','industry_code'
                           ,'industry_name','NAICS 2 Digit','NAICS 3 Digit','NAICS 4 Digit'], as_index=False)['ppi_naics_5'].max()
    ppi_df['naics_code'] = ppi_df.naics_code.astype('str')
    ppi_df['naics_code'] = ppi_df['naics_code'].str[:4]
    ppi_df['naics_code'] = ppi_df.naics_code.astype('int64')
    client_df_merge_naics_4 = pd.merge(client_df,ppi_df, how='left', left_on='NAICS 4 Digit', right_on='naics_code')
    client_df_merge_naics_4 = client_df_merge_naics_4.rename(columns={'value':'ppi_naics_4'})
    client_df_merge_naics_4 = client_df_merge_naics_4[['mm_yy','client_name','country_code','industry_name_level_1' ,'industry_name_level_2','industry_code'
                           ,'industry_name','NAICS 2 Digit','NAICS 3 Digit','NAICS 4 Digit','ppi_naics_4']]
    client_df_merge_naics_4 = client_df_merge_naics_4.groupby(['mm_yy','client_name','country_code','industry_name_level_1' ,'industry_name_level_2','industry_code'
                           ,'industry_name','NAICS 2 Digit','NAICS 3 Digit','NAICS 4 Digit'], as_index=False)['ppi_naics_4'].max()
    ppi_df['naics_code'] = ppi_df.naics_code.astype('str')
    ppi_df['naics_code'] = ppi_df['naics_code'].str[:3]
    ppi_df['naics_code'] = ppi_df.naics_code.astype('int64')
    client_df_merge_naics_3 = pd.merge(client_df,ppi_df, how='left', left_on='NAICS 3 Digit', right_on='naics_code')
    client_df_merge_naics_3 = client_df_merge_naics_3.rename(columns={'value':'ppi_naics_3'})
    client_df_merge_naics_3 = client_df_merge_naics_3[['mm_yy','client_name','country_code','industry_name_level_1' ,'industry_name_level_2','industry_code'
                           ,'industry_name','NAICS 2 Digit','NAICS 3 Digit','NAICS 4 Digit','ppi_naics_3']]
    client_df_merge_naics_3 = client_df_merge_naics_3.groupby(['mm_yy','client_name','country_code','industry_name_level_1' ,'industry_name_level_2','industry_code'
                           ,'industry_name','NAICS 2 Digit','NAICS 3 Digit','NAICS 4 Digit'], as_index=False)['ppi_naics_3'].max()
    ppi_df['naics_code'] = ppi_df.naics_code.astype('str')
    ppi_df['naics_code'] = ppi_df['naics_code'].str[:2]
    ppi_df['naics_code'] = ppi_df.naics_code.astype('int64')
    client_df_merge_naics_2 = pd.merge(client_df,ppi_df, how='left', left_on='NAICS 2 Digit', right_on='naics_code')
    client_df_merge_naics_2 = client_df_merge_naics_2.rename(columns={'value':'ppi_naics_2'})
    client_df_merge_naics_2 = client_df_merge_naics_2[['mm_yy','client_name','country_code','industry_name_level_1' ,'industry_name_level_2','industry_code'
                           ,'industry_name','NAICS 2 Digit','NAICS 3 Digit','NAICS 4 Digit','ppi_naics_2']]
    client_df_merge_naics_2 = client_df_merge_naics_2.groupby(['mm_yy','client_name','country_code','industry_name_level_1' ,'industry_name_level_2','industry_code'
                           ,'industry_name','NAICS 2 Digit','NAICS 3 Digit','NAICS 4 Digit'], as_index=False)['ppi_naics_2'].max() 
    client_df_merge_naics_5 = client_df_merge_naics_5[['mm_yy','client_name','industry_code','ppi_naics_5']]
    client_df_merge_naics_4 = client_df_merge_naics_4[['mm_yy','client_name','industry_code','ppi_naics_4']]
    client_df_merge_naics_3 = client_df_merge_naics_3[['mm_yy','client_name','industry_code','ppi_naics_3']]
    client_df_merge_naics_2 = pd.merge(client_df_merge_naics_2,client_df_merge_naics_3, how='left', left_on=['mm_yy','client_name','industry_code'], right_on=['mm_yy','client_name','industry_code'])
    client_df_merge_naics_2 = pd.merge(client_df_merge_naics_2,client_df_merge_naics_4, how='left', left_on=['mm_yy','client_name','industry_code'], right_on=['mm_yy','client_name','industry_code'])
    client_df_merge_naics_2 = pd.merge(client_df_merge_naics_2,client_df_merge_naics_5, how='left', left_on=['mm_yy','client_name','industry_code'], right_on=['mm_yy','client_name','industry_code'])
    client_df_merge_naics_2 = client_df_merge_naics_2[['mm_yy','client_name','country_code','industry_name_level_1' ,'industry_name_level_2','industry_code'
                           ,'industry_name','NAICS 2 Digit','NAICS 3 Digit','NAICS 4 Digit','ppi_naics_2','ppi_naics_3','ppi_naics_4','ppi_naics_5']]
    client_df_merge_naics_2 = client_df_merge_naics_2.sort_values(by=['mm_yy','client_name','industry_name'], ascending=[False,True,True])
    client_df_merge_naics_2.to_csv("/output/macroeconomics/producer_price_index/external_data_ppi.csv", index=False)
# ppi_cleanup()    