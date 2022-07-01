from company_file_upload import client_file_upload

#######################GDP by Industry############################
def gdp_cleanup():
    client_df = client_file_upload()
    gdp_df = pd.read_csv("/lookup/macroeconomics/gdp/gross_domestic_product_by_industry.csv", skiprows = 4)
    naics_df = pd.read_csv("/lookup/macroeconomics/gdp/gdp_industry_name_code_mapping.csv",encoding='unicode_escape')
    #drop line column
    gdp_df.drop(['Line'], axis = 1, inplace=True)

    #rename unnamed column to industry
    gdp_df = gdp_df.rename(columns={gdp_df.columns[0]: 'industry_name'})

    #start from row 3 to 89
    gdp_df = gdp_df[3:89]
    
    #unpivot
    gdp_unpivot_df = pd.melt(gdp_df, id_vars='industry_name', value_vars = gdp_df.loc[:,gdp_df.columns != 'industry_name'],var_name='year')

    #convert year column to string datatype
    gdp_unpivot_df['year'] = gdp_unpivot_df['year'].astype(str)

    #assign quarters based on the year column end eg 2015 will be Q1, 2015.1 will be Q2, 2015.3 will be Q3 etc
    gdp_unpivot_df['quarter'] = gdp_unpivot_df['year'].apply(lambda i : '04-01' if i.endswith('1') else '07-01' if i.endswith('2') else ('10-01' if i.endswith('3') else '01-01'))

    #change year from 2015.1, 2015.2 to 2015 etc
    gdp_unpivot_df['year'] = gdp_unpivot_df['year'].str[:4]
    
    #derive first date of quarter
    cols=["year","quarter"]
    gdp_unpivot_df['first_date_of_quarter'] = gdp_unpivot_df[cols].apply(lambda x: '-'.join(x.values.astype(str)), axis="columns")
    
    #remove leading spaces
    gdp_unpivot_df['industry_name'] = gdp_unpivot_df['industry_name'].str.strip()
    
    #rename column
    gdp_unpivot_df = gdp_unpivot_df.rename(columns={'value': 'gdp_value'})

    #naics code to industry name mapping
    gdp_unpivot_df = pd.merge(gdp_unpivot_df,naics_df, how='left', left_on='industry_name' ,right_on='industry_name')
    
    #reorder columns as desired
    gdp_unpivot_df = gdp_unpivot_df[["first_date_of_quarter", "naics_code", "industry_name", "gdp_value"]]
    
    #aggregate data at NAICS 4 Digit level
    gdp_unpivot_df['naics_code'] = gdp_unpivot_df.naics_code.astype('str')
    gdp_unpivot_df['naics_code'] = gdp_unpivot_df['naics_code'].str[:4]
    gdp_unpivot_df['naics_code'] = gdp_unpivot_df.naics_code.astype('int64')
    client_df_merge_naics_4 = pd.merge(client_df,gdp_unpivot_df, how='left', left_on='NAICS 4 Digit', right_on='naics_code')
    client_df_merge_naics_4 = client_df_merge_naics_4.rename(columns={'industry_name_x':'industry_name','gdp_value':'gdp_naics_4'})
    client_df_merge_naics_4 = client_df_merge_naics_4[['first_date_of_quarter','client_name','country_code','industry_name_level_1' ,'industry_name_level_2','industry_code'
                           ,'industry_name','NAICS 2 Digit','NAICS 3 Digit','NAICS 4 Digit','gdp_naics_4']]
    client_df_merge_naics_4 = client_df_merge_naics_4.groupby(['first_date_of_quarter','client_name','country_code','industry_name_level_1' ,'industry_name_level_2','industry_code'
                           ,'industry_name','NAICS 2 Digit','NAICS 3 Digit','NAICS 4 Digit'], as_index=False)['gdp_naics_4'].max()
    
    #aggregate data at NAICS 3 Digit level
    gdp_unpivot_df['naics_code'] = gdp_unpivot_df.naics_code.astype('str')
    gdp_unpivot_df['naics_code'] = gdp_unpivot_df['naics_code'].str[:3]
    gdp_unpivot_df['naics_code'] = gdp_unpivot_df.naics_code.astype('int64')
    client_df_merge_naics_3 = pd.merge(client_df,gdp_unpivot_df, how='left', left_on='NAICS 3 Digit', right_on='naics_code')
    client_df_merge_naics_3 = client_df_merge_naics_3.rename(columns={'industry_name_x':'industry_name','gdp_value':'gdp_naics_3'})
    client_df_merge_naics_3 = client_df_merge_naics_3[['first_date_of_quarter','client_name','country_code','industry_name_level_1' ,'industry_name_level_2','industry_code'
                           ,'industry_name','NAICS 2 Digit','NAICS 3 Digit','NAICS 4 Digit','gdp_naics_3']]
    client_df_merge_naics_3 = client_df_merge_naics_3.groupby(['first_date_of_quarter','client_name','country_code','industry_name_level_1' ,'industry_name_level_2','industry_code'
                           ,'industry_name','NAICS 2 Digit','NAICS 3 Digit','NAICS 4 Digit'], as_index=False)['gdp_naics_3'].max()
    
    #aggregate data at NAICS 2 Digit level
    gdp_unpivot_df['naics_code'] = gdp_unpivot_df.naics_code.astype('str')
    gdp_unpivot_df['naics_code'] = gdp_unpivot_df['naics_code'].str[:2]
    gdp_unpivot_df['naics_code'] = gdp_unpivot_df.naics_code.astype('int64')
    client_df_merge_naics_2 = pd.merge(client_df,gdp_unpivot_df, how='left', left_on='NAICS 2 Digit', right_on='naics_code')
    client_df_merge_naics_2 = client_df_merge_naics_2.rename(columns={'industry_name_x':'industry_name','gdp_value':'gdp_naics_2'})
    client_df_merge_naics_2 = client_df_merge_naics_2[['first_date_of_quarter','client_name','country_code','industry_name_level_1' ,'industry_name_level_2','industry_code'
                           ,'industry_name','NAICS 2 Digit','NAICS 3 Digit','NAICS 4 Digit','gdp_naics_2']]
    client_df_merge_naics_2 = client_df_merge_naics_2.groupby(['first_date_of_quarter','client_name','country_code','industry_name_level_1' ,'industry_name_level_2','industry_code'
                           ,'industry_name','NAICS 2 Digit','NAICS 3 Digit','NAICS 4 Digit'], as_index=False)['gdp_naics_2'].max() 
    
    #extracting final data set
    
    client_df_merge_naics_4 = client_df_merge_naics_4[['first_date_of_quarter','client_name','industry_code','gdp_naics_4']]
    client_df_merge_naics_3 = client_df_merge_naics_3[['first_date_of_quarter','client_name','industry_code','gdp_naics_3']]
    client_df_merge_naics_2 = pd.merge(client_df_merge_naics_2,client_df_merge_naics_3, how='left', left_on=['first_date_of_quarter','client_name','industry_code'], right_on=['first_date_of_quarter','client_name','industry_code'])
    client_df_merge_naics_2 = pd.merge(client_df_merge_naics_2,client_df_merge_naics_4, how='left', left_on=['first_date_of_quarter','client_name','industry_code'], right_on=['first_date_of_quarter','client_name','industry_code'])
    client_df_merge_naics_2 = client_df_merge_naics_2[['first_date_of_quarter','client_name','country_code','industry_name_level_1' ,'industry_name_level_2','industry_code'
                           ,'industry_name','NAICS 2 Digit','NAICS 3 Digit','NAICS 4 Digit','gdp_naics_2','gdp_naics_3','gdp_naics_4']]
    client_df_merge_naics_2 = client_df_merge_naics_2.sort_values(by=['first_date_of_quarter','client_name','industry_name'], ascending=[False,True,True])
    
    #write the final dataset as csv to the destination path
    client_df_merge_naics_2.to_csv("/output/macroeconomics/gdp/external_data_gdp.csv", index=False)
    
# gdp_cleanup()   