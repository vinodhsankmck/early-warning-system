from get_data import get_data,make_chunk

def get_consumer_exp_data(area_codes_list, start_year, end_year):
    json_data = get_data(area_codes_list, start_year, end_year)
    item_code_df = pd.read_csv("/lookup/macroeconomics/consumer_spend/item_codes.csv")
    demographics_df = pd.read_csv("/lookup/macroeconomics/consumer_spend/demographics_code.csv")
    result = []
    for series in json_data['Results']['series']:
        seriesId = series['seriesID']
        for item in series['data']:
            year = item['year']
            period = item['period']
            value = item['value']
            c1 = 'LB'
            c2 = '01M'
            pos1 = seriesId.find(c1)
            pos2 = seriesId.find(c2)
            result.append({
                'item_code': str(seriesId[3:pos1]),
                'demographics_code': str(seriesId[pos1:pos2]),
                'year': year,
                # 'month': str(period[1:]),
                'value': value
            })
    result_df = pd.DataFrame(result)
    # print(result_df)
    if not result_df.empty:
        result_df = result_df.merge(item_code_df, how='left', on='item_code')
        result_df = result_df.merge(demographics_df, how='left', on='demographics_code')
    return result_df

def get_consumer_exp_data_in_chunk(start_year,end_year):
    result_df = None
    series_id_list = []
    item_code_df = pd.read_csv("/lookup/macroeconomics/consumer_spend/item_codes.csv")
    demographics_df = pd.read_csv("/lookup/macroeconomics/consumer_spend/demographics_code.csv")
    # Getting all the list of series id to loop through
    for s,item in item_code_df.iterrows():    
        for m,demo in demographics_df.iterrows():
            series_id_list.append('CXU' + item['item_code'] + demo['demographics_code'] + '01M')
    chunk_list = make_chunk(series_id_list)
    # chunk_list = [['CXUALCBEVGLB0101M', 'CXUALCBEVGLB0201M', 'CXUALCBEVGLB0401M']]
    result = []
    for idx, area_code_list_sub in enumerate(chunk_list):
        # print(idx, area_code_list_sub)
        df = get_consumer_exp_data(area_code_list_sub, start_year, end_year)
        if result_df is not None:
            result_df = result_df.append(df, ignore_index=True)
        else:
            result_df = df
    result_df = result_df.sort_values(by=['year'], ascending=False)
    return result_df

def run(start_year,end_year):
    results_df = get_consumer_exp_data_in_chunk(start_year,end_year)
    results_df.to_csv("/output/macroeconomics/unemployment/external_data_consumer_spend.csv", index=False)

# run("2015","2022")