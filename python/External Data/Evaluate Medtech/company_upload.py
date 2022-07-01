#######################Company File Mapping############################
import fuzzymatcher
def client_file_upload():
    import pandas as pd
    import numpy as np
    from fuzzymatcher import link_table, fuzzy_left_join
    client_df = pd.read_excel("/Users/vinodh_sankaran/Projects/citiews/credit_ews/Input/EWS pilot client names - Industry.xlsx", sheet_name='Sheet1')
    # Importing forecasted and reported revenue by company and year
    eval_company_df = pd.read_csv("/Users/vinodh_sankaran/Projects/citiews/credit_ews/Input/vendor data - external/evaluate medtech/2. Archived Company Annual Revenue Forecasts Broken Out by Segments.csv")
    eval_company_df = eval_company_df[['Company']]
    eval_company_df = eval_company_df.drop_duplicates()
    client_df = client_df[client_df['Domicile Country ISO Country Code'] == 'US']
    client_df = client_df.reset_index()
    client_df = client_df[['Client name','NAICS 1 Industry - Industry Code','NAICS 1 Industry - Industry Name']]
    client_df = client_df.rename(columns={'Client name':'client_name', 'NAICS 1 Industry - Industry Code':'industry_code','NAICS 1 Industry - Industry Name':'industry_name'})
    # Fuzzy Match between Company Data and Evaluate Company Data
    fuzzy_df = fuzzymatcher.fuzzy_left_join(client_df, eval_company_df, 'client_name', 'Company')
    # Selecting only rows with match score > 20%
    fuzzy_df = fuzzy_df[fuzzy_df['best_match_score'] >= -0.01]
#     fuzzy_df = fuzzy_df.rename(columns={'euromonitor_category_left':'euromonitor_category'})
    fuzzy_df = fuzzy_df[['client_name','Company','industry_code','industry_name']]
    fuzzy_df = fuzzy_df.sort_values(by=['client_name'])
    return fuzzy_df