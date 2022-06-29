from get_data import get_data,make_chunk

def get_wages_data(area_codes_list, start_year, end_year):
    json_data = get_data(area_codes_list, start_year, end_year)
    # print(json_data)
    area_code_df = pd.read_excel("/lookup/macroeconomics/wages/area_codes.xlsx", dtype=str)
    data_type_code_df = pd.read_csv("/lookup/macroeconomics/wages/data_type_codes.csv", dtype=str)
    industry_code_df = pd.read_excel("/lookup/macroeconomics/wages/industry_codes.xlsx", dtype=str)
    ownership_code_df = pd.read_csv("/lookup/macroeconomics/wages/ownership_codes.csv", dtype=str)
    result = []
    for series in json_data['Results']['series']:
        seriesId = series['seriesID']
        for item in series['data']:
            year = item['year']
            period = item['period']
            value = item['value']
            result.append({
                'area_code': str(seriesId[3:8]),
                'data_type_code': str(seriesId[8:9]),
                'own_code': str(seriesId[10:11]),
                'industry_code': str(seriesId[11:]),
                'year': year,
                'month': str(period[1:]),
                'value': value
            })
    result_df = pd.DataFrame(result)
    # print(result_df)
    if not result_df.empty:
        result_df = result_df.merge(area_code_df, how='left', on='area_code')
        result_df = result_df.merge(data_type_code_df, how='left', on='data_type_code')
        result_df = result_df.merge(industry_code_df, how='left', on='industry_code')
        result_df = result_df.merge(ownership_code_df, how='left', on='own_code')
    return result_df

def get_wages_data_in_chunk(start_year,end_year):
    result_df = None
    series_id_list = []
    area_code_df = pd.read_excel("/lookup/macroeconomics/wages/area_codes.xlsx", dtype=str)
    data_type_code_df = pd.read_csv("/lookup/macroeconomics/wages/data_type_codes.csv", dtype=str)
    industry_code_df = pd.read_excel("/lookup/macroeconomics/wages/industry_codes.xlsx", dtype=str)
    ownership_code_df = pd.read_csv("/lookup/macroeconomics/wages/ownership_codes.csv", dtype=str)
    # Getting all the list of series id to loop through
    for s,area in area_code_df.iterrows():    
        for m,dtype in data_type_code_df.iterrows():
            for i,ind in industry_code_df.iterrows():
                for i,own in ownership_code_df.iterrows():
                    series_id_list.append('ENU' + area['area_code'] + dtype['data_type_code'] + '0' + own['own_code'] + ind['industry_code'])
    chunk_list = make_chunk(series_id_list)
    result = []
    for idx, area_code_list_sub in enumerate(chunk_list):
        # print(idx, area_code_list_sub)
        df = get_wages_data(area_code_list_sub, start_year, end_year)
        if result_df is not None:
            result_df = result_df.append(df, ignore_index=True)
        else:
            result_df = df
    result_df = result_df.sort_values(by=['year', 'month', 'area_code', 'data_type_code', 'data_type_code', 'own_code'], ascending=False)
    return result_df

def run(start_year,end_year):
    results_df = get_wages_data_in_chunk(start_year,end_year)
    results_df.to_csv("/output/macroeconomics/wages/external_data_wages.csv", index=False)

# run("2015","2022")