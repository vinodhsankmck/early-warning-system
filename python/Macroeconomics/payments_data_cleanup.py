#######################PHARMA DATA CLEANUP############################

def pharma_payments_data_cleanup():
    import pandas as pd
    import numpy as np
    year = ['2015','2016','2017','2018','2019','2020']
#     year = ['2020']
    payments_data = []
    for year in year:
        payments_df = pd.read_csv("/lookup/macroeconomics/pharma payments/OP_DTL_GNRL_PGYR" + year + "_P01212022.csv")
        payments_df = payments_df[payments_df['Recipient_Country']=='United States']
        print(payments_df['Physician_Primary_Type'].unique())
        payments_df = payments_df[payments_df['Physician_Primary_Type']=='Doctor of Dentistry']
        payments_df = payments_df.rename(columns={'Recipient_City':'city','Recipient_State':'state_code','Physician_Primary_Type':'physician_type','Total_Amount_of_Payment_USDollars':'total_amount','Date_of_Payment':'payment_month'})
        payments_df = payments_df[['payment_month','physician_type','state_code','city','total_amount']]
        payments_df['payment_month'] = payments_df['payment_month'].str[-4:] + "-" + payments_df['payment_month'].str[:2] + "-01"
        payments_df = payments_df.groupby(['payment_month','physician_type','state_code'], as_index=False)['total_amount'].sum()
        payments_us_wide_df = payments_df.groupby(['payment_month','physician_type'], as_index=False)['total_amount'].sum()
        payments_us_wide_df['state_code'] = 'US'
        payments_us_wide_df = payments_us_wide_df[['payment_month','physician_type','state_code','total_amount']]
        merge_df = payments_df.append(payments_us_wide_df, ignore_index=True)
        payments_data.append(merge_df)
    final_df = pd.concat(payments_data)
    final_df = final_df.sort_values(by=['payment_month','physician_type','state_code'], ascending=True)
    final_df.to_csv("/output/macroeconomics/pharma payments/external_data_pharma_dentistry_payments_data_by_state.csv",index=False)
    return payments_df