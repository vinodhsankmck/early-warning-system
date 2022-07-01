from get_data import get_data,make_chunk

def get_cpi_data(area_codes_list, start_year, end_year):
    json_data = get_data(area_codes_list, start_year, end_year)
    item_file_df = pd.read_csv("/lookup/macroeconomics/consumer_price_index/item_list.csv")
    item_file_df = item_file_df[['item_code','item_name']]
    area_code_df = pd.read_excel("/lookup/macroeconomics/consumer_price_index/area_code_cpi.xls", dtype=str)
    result = []
    for series in json_data['Results']['series']:
        seriesId = series['seriesID']
        for item in series['data']:
            year = item['year']
            period = item['period']
            CPI = item['value']
            result.append({
                'item_code': str(seriesId[8:]),
                'area_code': str(seriesId[4:8]),
                'year': year,
                'month': str(period[1:]),
                'CPI': CPI
            })
    result_df = pd.DataFrame(result)
    result_df = result_df.merge(item_file_df, how='left', on='item_code')
    result_df = result_df.merge(area_code_df, how='left', on='area_code')
    return result_df

def get_cpi_data_in_chunk(area_code_list,start_year,end_year):
    result_df = None
    series_id_list = []
    item_file_df = pd.read_csv("/lookup/macroeconomics/consumer_price_index/item_list.csv")
    # Getting all the list of series id to loop through
    for area in area_code_list:    
        for i,row in item_file_df.iterrows():
            series_id_list.append('CUUR' + area + row['item_code'])
    chunk_list = make_chunk(series_id_list)
    result = []
    for idx, area_code_list_sub in enumerate(chunk_list):
        # print(idx, area_code_list_sub)
        df = get_cpi_data(area_code_list_sub, start_year, end_year)
        if result_df is not None:
            result_df = result_df.append(df, ignore_index=True)
        else:
            result_df = df
    result_df = result_df.sort_values(by=['year', 'month', 'area_code', 'item_code'], ascending=False)
    return result_df

def run(start_year,end_year):
    results_df = get_cpi_data_in_chunk(['0000','0100','0200','0300','0400'],start_year,end_year)
    results_df.to_csv("/output/macroeconomics/consumer_price_index/cpi_data.csv", index=False)


# run("2015","2022")