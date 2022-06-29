from get_data import get_data,make_chunk

def get_ppi_data(area_codes_list, start_year, end_year):
    json_data = get_data(area_codes_list, start_year, end_year)
    industry_df = pd.read_csv("/lookup/macroeconomics/producer_price_index/industry_codes.csv")
    result = []
    for series in json_data['Results']['series']:
        seriesId = series['seriesID']
        for item in series['data']:
            year = item['year']
            period = item['period']
            value = item['value']
            result.append({
                'naics_code': str(seriesId[3:9]),
                'year': year,
                'month': str(period[1:]),
                'value': value
            })
    result_df = pd.DataFrame(result)
    result_df = result_df.merge(industry_df, how='left', on='naics_code')
    return result_df

def get_ppi_data_in_chunk(start_year,end_year):
    result_df = None
    series_id_list = []
    industry_df = pd.read_csv("/lookup/macroeconomics/producer_price_index/industry_codes.csv")
    # Getting all the list of series id to loop through
    for s,ind in industry_df.iterrows():
        series_id_list.append('PCU' + ind['naics_code'] + ind['naics_code'])
    chunk_list = make_chunk(series_id_list)
    result = []
    for idx, area_code_list_sub in enumerate(chunk_list):
        df = get_ppi_data(area_code_list_sub, start_year, end_year)
        if result_df is not None:
            result_df = result_df.append(df, ignore_index=True)
        else:
            result_df = df
    result_df = result_df.sort_values(by=['year', 'month', 'naics_code'], ascending=False)
    return result_df

def run(start_year,end_year):
    results_df = get_ppi_data_in_chunk(start_year,end_year)
    results_df.to_csv("/output/macroeconomics/producer_price_index/external_data_ppi.csv", index=False)

# run("2015","2022")