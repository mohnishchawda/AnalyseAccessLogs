import os
import pandas as pd
import numpy as np

pd.set_option('display.max_rows', 10000)
pd.set_option('display.max_columns', 100)
pd.set_option('display.width', 10000)


# This Function captures all non-hidden files having log extension from a directory. Directories are excluded.
def get_file_list(directory):
    # Get all files (including hidden) and directories (including hidden)
    file_names = os.listdir(directory)
    file_list = []
    for dir_obj in file_names:
        # The below if statement gets rid of directories
        if os.path.isfile(os.path.join(os.path.abspath(directory), dir_obj)):
            # The below if statement only focuses on files having extension of .log
            if dir_obj.endswith(".log"):
                file_list.append(dir_obj)
    # Sort List
    file_list.sort()
    return file_list


# Main Logic
def analyse_access_log(file_name, url_list):
    # Capture Fields (columns) in the Log Files
    fields = 'date time s-ip cs-method cs-uri-stem cs-uri-query s-port cs-username c-ip cs(User-Agent) cs(Referer) sc-status sc-substatus sc-win32-status time-taken'.split(' ')
    # Convert Log data into a Data Frame or Table
    data_frame = pd.read_csv('access_logs/' + file_name, sep=r'\s+', comment='#', names=fields)
    # Filter Table by URL. URLS is a list containing the urls.
    data_frame = data_frame[data_frame['cs-uri-stem'].isin(url_list)]
    if not data_frame.empty:
        # Create a column that lists the HOUR of the Log
        data_frame['hour'] = data_frame['time'].str.split(':', n=2, expand=True)[0]
        # Shorten the Dataframe to include just 2 columns we are interested in.
        data_frame = data_frame[['cs-uri-stem', 'hour']]
        for url in url_list:
            # Group by on Table to capture the data
            df = data_frame[data_frame['cs-uri-stem'] == url_list[url_list.index(url)]].groupby(['hour']).agg('count').transpose()
            # Adding a column which include the URL name
            df['URL'] = url_list[url_list.index(url)]
            # Adding a column which include the file name
            df['file'] = file_name
            # The below piece of code concatenates sub-tables to create a table which contains consolidated data for all URLs in a Log.
            if url_list.index(url) == 0:
                data_frame_final = df
            else:
                data_frame_final = pd.concat([data_frame_final, df])
        # Replace Nan with 0 so that each column have same datatype
        data_frame_final = data_frame_final.replace(np.nan, 0)
        return data_frame_final


def analyse(input_directory, output_directory, url_list):
    # Get list of files in the input_directory having extension .log
    files_list = get_file_list(input_directory)
    for file in files_list:
        print('ANALYSING: ' + file)
        table = analyse_access_log(file, url_list)
        # The below piece of code concatenates sub-tables to create a table which contains consolidated data for all URLs in a Log.
        if files_list.index(file) == 0:
            table_final = table
        else:
            table_final = pd.concat([table_final, table])
    print('********************************************************************************************************************************')
    table_final = table_final.reset_index(drop=True)
    table_final = table_final.set_index(['file', 'URL'])
    # Dumps the final output table to a CSV File
    table_final.to_csv(output_directory + '/final.csv')
    print(table_final)
    print('********************************************************************************************************************************')


# Pass 3 Arguments:
# 1. Input Directory
# 2. Output Direcvtory
# 3. List of URLs to be analysed in the Access Log
analyse('access_logs', 'output', ['/app/reports', '/app/to-do', '/app/report-history'])
