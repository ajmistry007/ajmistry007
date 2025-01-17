#library imports

import os
from datetime import datetime
import pandas as pd

 
# source file location
folder_path = 'C:\\Users\\cms_data'

# This is timestamp of last successful job execution. This will be used to compare with file modification time to process only new files.
timestamp = '2025-01-01 00:00:00'

 

# Convert the given timestamp to a datetime if its string
target_time = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
dataframes = []

 
# Reading files from source folder
for filename in os.listdir(folder_path):
    file_path = os.path.join(folder_path, filename)
    # Check if it's a file
    if os.path.isfile(file_path):
        # Get the file's modification datetime
        file_mod_time = datetime.fromtimestamp(os.path.getmtime(file_path))
        # Compare file modification time with last processed time
        if file_mod_time >= datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S'):
            print("process data")
            df = pd.read_json(file_path)
            dataframes.append(df)

if len(dataframes) > 0:
    # capture current datrtime if data is available to process and then update this time to some file or database util table
    timestamp = datetime.datetime.now()
    # create dataframe from list
    combined_df = pd.concat(dataframes, ignore_index=True)  
    # filter data for theme = Hospitals in new dataframe        
    df_hospitals = combined_df.loc[combined_df['theme'].str[0] == "Hospitals"]
    # Update column names to snake case and remove special characters
    df_hospitals.columns = [x.lower() for x in df_hospitals.columns]
    df_hospitals.columns = df_hospitals.columns.str.replace("[ ]", "_", regex=True)
    df_hospitals.columns = df_hospitals.columns.str.replace('[#,@,&]', '',regex=True)
    # Print the output.
    print("total records read :", combined_df.shape[0])
    print("total records extracted :",df_hospitals.shape[0])
    # target folder
    df_hospitals.to_csv('C:\\Users\\Hospitals_data.csv', index=False)
else:
    print("no new files to be processed")