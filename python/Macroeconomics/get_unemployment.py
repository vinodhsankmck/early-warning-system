from get_data import get_data,make_chunk

def get_unemployment_data(area_codes_list, start_year, end_year):
    json_data = get_data(area_codes_list, start_year, end_year)
    area_code_df = pd.read_csv("/lookup/macroeconomics/unemployment/area_code.csv")
    measure_df = pd.read_excel("/lookup/macroeconomics/unemployment/measure.xlsx", dtype=str)
    result = []
    for series in json_data['Results']['series']:
        seriesId = series['seriesID']
        for item in series['data']:
            year = item['year']
            period = item['period']
            value = item['value']
            result.append({
                'area_code': str(seriesId[3:18]),
                'measure_code': str(seriesId[18:]),
                'year': year,
                'month': str(period[1:]),
                'value': value
            })
    result_df = pd.DataFrame(result)
    # print(result_df)
    result_df = result_df.merge(area_code_df, how='left', on='area_code')
    result_df = result_df.merge(measure_df, how='left', on='measure_code')
    return result_df

def get_unemployment_data_in_chunk(start_year,end_year):
    result_df = None
    series_id_list = []
    area_code_df = pd.read_csv("/lookup/macroeconomics/unemployment/area_code.csv")
    measure_df = pd.read_excel("/lookup/macroeconomics/unemployment/measure.xlsx", dtype=str)
    # Getting all the list of series id to loop through
    for s,area in area_code_df.iterrows():    
        for m,measure in measure_df.iterrows():
            series_id_list.append('LAU' + area['area_code'] + measure['measure_code'])
    chunk_list = make_chunk(series_id_list)
    result = []
    for idx, area_code_list_sub in enumerate(chunk_list):
        # print(idx, area_code_list_sub)
        df = get_unemployment_data(area_code_list_sub, start_year, end_year)
        if result_df is not None:
            result_df = result_df.append(df, ignore_index=True)
        else:
            result_df = df
    result_df = result_df.sort_values(by=['year', 'month', 'area_code', 'measure_code'], ascending=False)
    return result_df

def run(start_year,end_year):
    results_df = get_unemployment_data_in_chunk(start_year,end_year)
    results_df.to_csv("/output/macroeconomics/unemployment/external_data_unemployment.csv", index=False)

# run("2015","2022")