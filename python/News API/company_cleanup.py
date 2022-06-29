# Company Name Clensing

import pandas as pd
import cleanco
from cleanco import basename

def company_name_cleanup():
    company_name = pd.read_excel("/Projects/citiews/credit_ews/Input/EWS pilot client names - Industry.xlsx")
    #to_exclude = ['inc','llc','ltd','kft','lac','dac','co','as','&','kg','international']
    company_name = company_name.reset_index()
    company_basename_lst = []
    for index,row in company_name.iterrows():
        company_basename_lst.append(basename(str(row['Client name'])))
    company_basename = pd.DataFrame(company_basename_lst, columns = ['Client name cleaned'])
    company_names = pd.merge(company_name,company_basename, left_index=True, right_index=True)
    company_names.to_csv("/Projects/citiews/credit_ews/Output/file_cleanup/EWS pilot client names cleaned.csv", encoding='utf-8', index=False)
    company_names_cleaned = company_names
    return company_names_cleaned