#######################PLACES DATA CLEANUP############################

def places_us_wide_data_extraction(df):
    places_df = df
    places_df['calc_value'] = places_df['total_population'].astype(float) * places_df['value'].astype(float)
    places_calc_df = places_df.groupby(['year','measure_id','measure_short_desc','measure_desc'], as_index=False)['calc_value','total_population'].sum()
    places_calc_df['state_code'] = 'US'
    places_calc_df['state_name'] = 'US'
    places_calc_df['city'] = 'US'
    places_calc_df['value'] = places_calc_df['calc_value']/places_calc_df['total_population']
    places_calc_df = places_calc_df[['year','state_code','state_name','city','measure_id','measure_short_desc','measure_desc','total_population','value']]
    places_df = places_df.drop('calc_value',1)
    return places_calc_df

def places_data_cleanup():
    import pandas as pd
    year = ['2016','2017','2018','2019']
    places_data = []
    for year in year:
        places_df = pd.read_csv("/lookup/macroeconomics/places/500_Cities__Local_Data_for_Better_Health__" + year + "_release.csv")
#         places_df = places_df[places_df['MeasureId']=='DENTAL']
        places_df = places_df[places_df['GeographicLevel']=='City']
        places_df = places_df[places_df['DataValueTypeID']=='AgeAdjPrv']
        if year != '2019':
            exclude_year = (int(year)-2)
            places_df = places_df[places_df['Year'] != exclude_year]
#         places_df = places_df[places_df['Short_Question_Text']=='Dental Visit']
        if year in ('2016','2017'):
            places_df = places_df[['Year','StateAbbr','StateDesc','CityName','Measure','Data_Value','Population2010','MeasureId','Short_Question_Text']]
            places_df = places_df.rename(columns={'Year':'year','StateAbbr':'state_code','StateDesc':'state_name','CityName':'city','Measure':'measure_desc',
                                              'Data_Value':'value','Population2010':'total_population','MeasureId':'measure_id','Short_Question_Text':'measure_short_desc'})
        else:
            places_df = places_df[['Year','StateAbbr','StateDesc','CityName','Measure','Data_Value','PopulationCount','MeasureId','Short_Question_Text']]
            places_df = places_df.rename(columns={'Year':'year','StateAbbr':'state_code','StateDesc':'state_name','CityName':'city','Measure':'measure_desc',
                                              'Data_Value':'value','PopulationCount':'total_population','MeasureId':'measure_id','Short_Question_Text':'measure_short_desc'})
        places_us_wide_df = places_us_wide_data_extraction(places_df)
        merge_df = places_df.append(places_us_wide_df, ignore_index=True)
        merge_df = merge_df[['year','state_code','state_name','measure_id','measure_short_desc','measure_desc','total_population','value']]
        merge_df['calc_value'] = merge_df['total_population'].astype(float) * merge_df['value'].astype(float)
        merge_df = merge_df.groupby(['year','state_code','state_name','measure_id','measure_short_desc','measure_desc'], as_index=False)['calc_value','total_population'].sum()
        merge_df['value'] = merge_df['calc_value']/merge_df['total_population']
        merge_df = merge_df.drop('calc_value',1)
        places_data.append(merge_df)
    final_df = pd.concat(places_data)
    final_df = final_df.sort_values(by=['year','state_code','measure_id'], ascending=True)
    final_df.to_csv("/output/macroeconomics/places/external_data_places_data_by_state.csv",index=False)
    # return merge_df.head(10)