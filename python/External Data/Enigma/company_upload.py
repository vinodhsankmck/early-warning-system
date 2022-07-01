#######################Company File Upload############################

def client_file_upload():
    import pandas as pd
    import numpy as np
    client_df = pd.read_excel("/lookup/company_file/EWS pilot client names - Industry.xlsx", sheet_name='Sheet1')
    client_df = client_df[client_df['Domicile Country ISO Country Code'] == 'US']
    client_df = client_df.reset_index()
    client_df = client_df[['Client name','NAICS 1 Industry - Industry Code','NAICS 1 Industry - Industry Name']]
    client_df = client_df.rename(columns={'Client name':'client_name', 'NAICS 1 Industry - Industry Code':'industry_code','NAICS 1 Industry - Industry Name':'industry_name'})
    
    return client_df