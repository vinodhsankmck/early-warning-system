# Used to count # of news per company per year

import json
import re
import os 
import pandas as pd

dirpath = "/Projects/citiews/credit_ews/Output/json_data"
subfolders = [ f.name for f in os.scandir(dirpath) if f.is_dir()]
    
# Create empty lists
file_name = []
total_news = []
year = []
for yr in subfolders:
    fpath = dirpath + "/" + yr
    files_in_path = os.listdir(fpath)
    # Looping through the files
    for file in files_in_path:
        if file != '.DS_Store':
            # Opening JSON file
            f = open(fpath + "/" + file)
            # returns JSON object as
            # a dictionary
            data = json.load(f)
            # Convert JSON data to string
            json_string = json.dumps(data)    

            # Count total number of news
            total_number_of_news = sum(1 for _ in re.finditer(r'\b%s\b' % re.escape('publishedAt'), json_string))

            # Appending data into list
            file_name.append(file.replace('.json',''))
            year.append(yr)
            total_news.append(total_number_of_news)

            # Closing file
            f.close()
    
# Create a dict with 2 columns
file_dict = {'file_name': file_name, 'year': year, 'total_news': total_news}
file_df = pd.DataFrame(file_dict)
pivot_file_df = file_df.pivot(index = 'file_name', columns = 'year', values = 'total_news')
pivot_file_df.to_csv("/Projects/citiews/credit_ews/Output/news_by_companies.csv", sep = '\t', index = True)
print("CSV written successfully")