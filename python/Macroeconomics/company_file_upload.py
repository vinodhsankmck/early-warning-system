#######################Initialization############################
import pandas as pd

#######################Client File Upload############################

def client_file_upload():
    client_df = pd.read_excel("/lookup/company file/EWS pilot client names - Industry.xlsx", sheet_name='Sheet1')
    client_df = client_df[client_df['Domicile Country ISO Country Code'] == 'US']
    client_df = client_df.reset_index()
    client_df = client_df[['Client name','Domicile Country ISO Country Code','RMI Industry - Level 1 Name' ,'RMI Industry - Level 2 Name','NAICS 1 Industry - Industry Code'
                           ,'NAICS 1 Industry - Industry Name','NAICS 2 Digit','NAICS 3 Digit','NAICS 4 Digit','item_code_consumer_spend_BEA','item_name_consumer_spend_BEA','item_name_consumer_spend_quarterly_BEA','item_code_cpi_BEA','item_name_cpi_BEA']]
    client_df = client_df.rename(columns={'Client name':'client_name', 'Domicile Country ISO Country Code':'country_code','RMI Industry - Level 1 Name':'industry_name_level_1'
                                                      ,'RMI Industry - Level 2 Name':'industry_name_level_2','NAICS 1 Industry - Industry Code':'industry_code','NAICS 1 Industry - Industry Name':'industry_name'})
    
    return client_df