from company_file_upload import client_file_upload

#######################Country Level Wages############################
def wages_cleanup():
    client_df = client_file_upload()
    wages_df = pd.read_csv("/output/macroeconomics/wages/US wide Employment & Wages.csv", parse_dates = {"mm_yy" : ["year","month"]}, keep_date_col=True)
    # naics_df = pd.read_csv("/lookup/macroeconomics/wages/naics_code_description_mapping.csv",encoding='unicode_escape')
    wages_df = wages_df[['mm_yy','industry_code','own_code','own_title','value']]
    client_df_merge_naics_5 = pd.merge(client_df,wages_df, how='left', left_on='industry_code', right_on='industry_code')
    client_df_merge_naics_5 = client_df_merge_naics_5[['mm_yy','client_name','country_code','industry_name_level_1' ,'industry_name_level_2','industry_code'
                           ,'industry_name','NAICS 2 Digit','NAICS 3 Digit','NAICS 4 Digit','own_title','value']]
    client_df_merge_naics_5 = client_df_merge_naics_5.rename(columns={'value':'wages_naics_5'})
    client_df_merge_naics_5.drop_duplicates(subset=['mm_yy','client_name','own_title'])
    client_df_merge_naics_5 = client_df_merge_naics_5.groupby(['mm_yy','client_name','country_code','industry_name_level_1' ,'industry_name_level_2','industry_code'
                           ,'industry_name','NAICS 2 Digit','NAICS 3 Digit','NAICS 4 Digit','own_title'], as_index=False)['wages_naics_5'].max()
    wages_df['industry_code'] = wages_df.industry_code.astype('str')
    wages_df['industry_code'] = wages_df['industry_code'].str[:2]
    wages_df['industry_code'] = wages_df.industry_code.astype('int64')
    client_df_merge_naics_2 = pd.merge(client_df,wages_df, how='left', left_on='NAICS 2 Digit', right_on='industry_code')
    client_df_merge_naics_2 = client_df_merge_naics_2.rename(columns={'industry_code_x':'industry_code','value':'wages_naics_2'})
    client_df_merge_naics_2 = client_df_merge_naics_2[['mm_yy','client_name','country_code','industry_name_level_1' ,'industry_name_level_2','industry_code'
                           ,'industry_name','NAICS 2 Digit','NAICS 3 Digit','NAICS 4 Digit','own_title','wages_naics_2']]
    client_df_merge_naics_2 = client_df_merge_naics_2.groupby(['mm_yy','client_name','country_code','industry_name_level_1' ,'industry_name_level_2','industry_code'
                           ,'industry_name','NAICS 2 Digit','NAICS 3 Digit','NAICS 4 Digit','own_title'], as_index=False)['wages_naics_2'].max() 
    client_df_merge_naics_5 = client_df_merge_naics_5[['mm_yy','client_name','industry_code','own_title','wages_naics_5']]
    client_df_merge_naics_2 = pd.merge(client_df_merge_naics_2,client_df_merge_naics_5, how='left', left_on=['mm_yy','client_name','industry_code','own_title'], right_on=['mm_yy','client_name','industry_code','own_title'])
    client_df_merge_naics_2 = client_df_merge_naics_2[['mm_yy','client_name','country_code','industry_name_level_1' ,'industry_name_level_2','industry_code'
                           ,'industry_name','NAICS 2 Digit','NAICS 3 Digit','NAICS 4 Digit','own_title','wages_naics_2','wages_naics_5']]
    client_df_merge_naics_2 = client_df_merge_naics_2.sort_values(by=['mm_yy','client_name','industry_name','own_title'], ascending=[False,True,True,True])
    client_df_merge_naics_2.to_csv("/output/macroeconomics/wages/external_data_employment_and_wages.csv", index=False)
# wages_cleanup()  