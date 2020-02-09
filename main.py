import csv
import re
import sys
import numpy as np
import pandas as pd
import math
import struct
import plotly.graph_objects as go
from time import ctime
from datetime import date, datetime, time, timedelta




####### CHANGE THIS PATH #######
scid_filename = 'CLH20.scid'
depth_filenames = ['CLH20.2020-01-30.depth', 'CLH20.2020-01-31.depth']


def deserialize(excelDateAndTime):
    """
    Returns excel date time as Python datetime object
    """
    date_tok = int(excelDateAndTime)
    time_tok = round(86400*(excelDateAndTime - date_tok))
    d = date(1899, 12, 31) + timedelta(date_tok)
    t = time(*helper(time_tok, (24, 60, 60, 1000000)))
    return datetime.combine(d, t)

def getDateTime(datetime):
    
    chunks = str(datetime).split('-')
    date_year, time, contract_name = (chunks[0], chunks[1], chunks[2])
    
    year, month, date = (date_year[:4],date_year[4:6], date_year[6:8])
    hour, minute, sec = (time[:2],time[2:4], time[4:6])
    
    date_Str = year + '-' + month + '-' + date 
    time_str = hour + ':' + minute + ':' + sec 

    return date_Str + ' ' + time_str



def helper(factor, units):
    factor /= 86399.99
    result = list()
    for unit in units:
        value, factor = divmod(factor * unit, 1)
        result.append(int(value))
    result[3] = int(0)
    return result


def datetimeTwo(excelDateAndTime, tzAdjust):
    """
    Returns excel date time in format YYYYMMDD HHMMSS
    """
    t = str(deserialize(excelDateAndTime + tzAdjust.seconds/86400.0))
    return (re.sub(r'(:|-)', '', t)[:8], re.sub(r'(:|-)', '', t)[9:15])


def excelTimeAdjust(date_and_time, tzAdjust):
    return date_and_time + tzAdjust.seconds/86400.0


def getRecordsFromScid(filename, fileoutput, tzvar, dtformat):
    """
    Read in records from SierraChart .scid data filename
    dtformat determines the datetime representation in column 1 (and 2)
    """
    sizeHeader = 0x38
    sizeRecord = 0x28
    if tzvar == 99999:
        tzAdjust = datetime.fromtimestamp(0) - datetime.utcfromtimestamp(0)
    else:
        tzAdjust = timedelta(hours=tzvar)

    header = ('datetime', 'date', 'tick_date', 'O', 'H', 'L', 'C', 'V', 'T')

    loop_index = 1
    new_filename = fileoutput + str(loop_index) + '.tsv'
    ftsv = open(new_filename, 'w')
    fileout = csv.writer(ftsv, delimiter='\t')
    print('we need to split large content to multi TSV files')
    print('creating '+ new_filename +' file...')

    with open(filename, 'rb') as fscid:
        fscid.read(sizeHeader)  # discard header
        fileout.writerow(header)
        max_rows = 20000000
        loop_rows_limit = 400000

        for i in range(max_rows):

            if i / loop_rows_limit > loop_index:
                print('finished '+ new_filename +' file')
                loop_index += 1
                new_filename = fileoutput + str(loop_index) + '.tsv'
                ftsv = open(new_filename, 'w')
                print('creating '+ new_filename +' file...')
                fileout = csv.writer(ftsv, delimiter='\t')
                fileout.writerow(header)
                

            data = fscid.read(sizeRecord)
            if data != "" and len(data) == sizeRecord:
                dataRow = struct.unpack('d4f4I', data)
                if dtformat == 0:
                    adjustedTime = dataRow[0] + tzAdjust.seconds/86400.0
                    outrow = (str(adjustedTime), str(adjustedTime), getDateTime(adjustedTime),
                            str(dataRow[1]), str(dataRow[2]), str(dataRow[3]),
                            str(dataRow[4]), str(dataRow[5]), str(dataRow[6]))
                    fileout.writerow(outrow)
                elif dtformat == 1:
                    date, time = datetimeTwo(dataRow[0], tzAdjust)
                    outrow = (date+'-'+time+'-CLH20', date, getDateTime(date+'-'+time+'-CLH20'),
                            str(dataRow[1]), str(dataRow[2]), str(dataRow[3]),
                            str(dataRow[4]), str(dataRow[5]), str(dataRow[6]))
                    fileout.writerow(outrow)
            else:
                break
    print('finished converting SCID file to TSV file')
    return

def getRecordsFromDepth(filename, fileoutput, tzvar, dtformat):
    """
    Read in records from SierraChart .scid data filename
    dtformat determines the datetime representation in column 1 (and 2)
    """
    sizeHeader = 0x40
    sizeRecord = 0x18
    if tzvar == 99999:
        tzAdjust = datetime.fromtimestamp(0) - datetime.utcfromtimestamp(0)
    else:
        tzAdjust = timedelta(hours=tzvar)

    header = ('datetime', 'date', 'tick_date', 'flag', 'numorder', 'price', 'quantity', 'reserved')

    loop_index = 1
    new_filename = fileoutput + str(loop_index) + '.tsv'
    ftsv = open(new_filename, 'w')
    print('we need to split large content to multi TSV files')
    print('creating '+ new_filename +' file...')
    fileout = csv.writer(ftsv, delimiter='\t')

   
    with open(filename, 'rb') as fscid:
        fscid.read(sizeHeader)  # discard header
        fileout.writerow(header)
        max_rows = 10000000
        loop_rows_limit = 500000
        
        for i in range(max_rows):

            if i / loop_rows_limit > loop_index:
                print('finished '+ new_filename +' file')
                loop_index += 1
                new_filename = fileoutput + str(loop_index) + '.tsv'
                ftsv = open(new_filename, 'w')
                print('creating '+ new_filename +' file...')
                fileout = csv.writer(ftsv, delimiter='\t')
                fileout.writerow(header)

            data = fscid.read(sizeRecord)
            if data != "" and len(data) == sizeRecord:
                dataRow = struct.unpack('dc?hf2i', data)
                if dtformat == 0:
                    adjustedTime = dataRow[0] + tzAdjust.seconds/86400.0
                    outrow = (str(adjustedTime), str(adjustedTime), getDateTime(adjustedTime),
                              str(dataRow[2]), str(dataRow[3]),
                              str(dataRow[4]), str(dataRow[5]), str(dataRow[6]))
                    fileout.writerow(outrow)
                elif dtformat == 1:
                    date, time = datetimeTwo(dataRow[0], tzAdjust)
                    outrow = (date+'-'+time+'-CLH20', date, getDateTime(date+'-'+time+'-CLH20'),
                              str(dataRow[2]), str(dataRow[3]),
                              str(dataRow[4]), str(dataRow[5]), str(dataRow[6]))
                    fileout.writerow(outrow)
            else:
                break
    print('finished converting DEPTH file to TSV file')
    return

# merge valid SCID content and depth content

def tsv_to_df(file_path):
    return pd.read_csv(file_path, delimiter='\t',encoding='utf-8')

def groupby_dfs(dfs):
    temp = []
    for r in dfs:
        temp.append(r.groupby(['datetime', 'tick_date']).mean())
    return temp

def export_split(df, filename):
    
    total_cnt = len(df.index)
    loop_index = 0
    limit = 6000000
    loop_cnt = math.floor(total_cnt/limit)
    dfs = []
    print('predicted loop total number', loop_cnt)
    while(loop_index <= loop_cnt):
        if(loop_index == loop_cnt):
            sub_df = df.iloc[loop_index*limit:total_cnt]            
        else:
            sub_df = df.iloc[loop_index*limit:(loop_index+1)*limit]
        print(sub_df.head())
        sub_df_ex = sub_df.groupby(['datetime', 'tick_date']).mean()
        
        dfs.append(sub_df_ex)
        print('loop index', loop_index)
        loop_index += 1

    new_df = pd.concat(dfs, ignore_index=False)
    new_df_ex = new_df.groupby(['datetime', 'tick_date']).mean()
    new_df_ex.to_csv(filename+'.tsv', sep='\t')
    print('export '+filename+'.tsv')


# merge valid SCID_TSV files and DEPTH_TSV fils

def merge_scid_depth():
    
    print('getting file names which have valid data for merging SCID and DEPTH...')
    
    depth_files = ["CLH20.2020-01-301.tsv", "CLH20.2020-01-302.tsv", "CLH20.2020-01-303.tsv", "CLH20.2020-01-304.tsv", "CLH20.2020-01-305.tsv", "CLH20.2020-01-311.tsv", "CLH20.2020-01-312.tsv", "CLH20.2020-01-313.tsv", "CLH20.2020-01-314.tsv"]
    scid_files = ["CLH2010.tsv", "CLH2011.tsv", "CLH2012.tsv"]
    print('depth content TSVs ===> "CLH20.2020-01-301.tsv", "CLH20.2020-01-302.tsv", "CLH20.2020-01-303.tsv", "CLH20.2020-01-304.tsv", "CLH20.2020-01-305.tsv", "CLH20.2020-01-311.tsv", "CLH20.2020-01-312.tsv", "CLH20.2020-01-313.tsv", "CLH20.2020-01-314.tsv"')
    print('valid scid content TSVs ===> "CLH2010.tsv", "CLH2011.tsv", "CLH2012.tsv"')

    print('getting dataframes from DEPTH and SCID...')
    depth_frames = [ tsv_to_df(f) for f in depth_files ]
    scid_frames = [ tsv_to_df(f) for f in scid_files ]
    print('dataframes complete')

    print('merging dataframes')
    merged_depth_df = pd.concat(depth_frames, ignore_index=True)
    merged_scid_df = pd.concat(scid_frames, ignore_index=True)
    print('merged complete')

    del(depth_frames)
    del(scid_frames)
    print('delete unneccessary variables ===> depth_frames, scid_frames')
    
    print('getting unique datetime...')
    print(merged_scid_df.date.unique(), merged_depth_df.date.unique())
    
    for val in merged_scid_df.date.unique():
        print('joining dataframe regarding ' + str(val) + '....')
        temp = pd.merge(merged_scid_df[merged_scid_df.date == val], merged_depth_df[merged_depth_df.date == val], on=['datetime', 'date', 'tick_date'], how='left')
        print('merging completed. starting exporting to multi TSVs...')
        export_split(temp, str(val))
    
    print('getting core dataframe complete')


# get one dataframe from multi tsv files

def getOneDf():
    
    print('getting file names of all splitted TSV files...')
    scid_files_1 = ["CLH201.tsv", "CLH202.tsv","CLH203.tsv", "CLH204.tsv", "CLH205.tsv", "CLH206.tsv", "CLH207.tsv", "CLH208.tsv"]
    depth_files = ["20200130.tsv", "20200131.tsv", "20200201.tsv", "20200203.tsv", "20200204.tsv"]
    scid_files_2 = ["CLH2012.tsv","CLH2013.tsv","CLH2014.tsv"]
    print('first - scid content TSVs ===> "CLH201.tsv", "CLH202.tsv","CLH203.tsv", "CLH204.tsv", "CLH205.tsv", "CLH206.tsv", "CLH207.tsv", "CLH208.tsv"')
    print('second - depth content TSVs ===> "20200130.tsv", "20200131.tsv", "20200201.tsv", "20200203.tsv", "20200204.tsv"')
    print('first - valid depth content TSVs ===> "CLH2012.tsv","CLH2013.tsv","CLH2014.tsv"')

    depth_frames = [ tsv_to_df(f) for f in depth_files ]
    scid_frames_1 = [ tsv_to_df(f) for f in scid_files_1 ]
    scid_frames_2 = [ tsv_to_df(f) for f in scid_files_2 ]
    print('making dataframe array complete')

    print('strating groupby...')
    scid_frames_1_ex = groupby_dfs(scid_frames_1)
    scid_frames_2_ex = groupby_dfs(scid_frames_2)
    depth_frames_ex = groupby_dfs(depth_frames)
    print('groupby complete')

    del(depth_frames)
    del(scid_frames_1)
    del(scid_frames_2)
    print('delete unneccessary variables ===> depth_frames, scid_frames_1, scid_frames_2')

    print('strating joining...')
    merged_depth_df = pd.concat(depth_frames_ex)
    merged_scid_df_1 = pd.concat(scid_frames_1_ex)
    merged_scid_df_2 = pd.concat(scid_frames_2_ex)    
    print('joining complete')

    del(depth_frames_ex)
    del(scid_frames_1_ex)
    del(scid_frames_2_ex)
    print('delete unneccessary variables ===> depth_frames_ex, scid_frames_1_ex, scid_frames_2_ex')

    print('strating merging into one dataframe...')
    frames = [merged_scid_df_1, merged_depth_df, merged_scid_df_2]
    full_df = pd.concat(frames)
    print('full merge complete')

    del(merged_scid_df_1)
    del(merged_depth_df)
    del(merged_scid_df_2)
    print('delete unneccessary variables ===> merged_scid_df_1, merged_depth_df, merged_scid_df_2')

    print('testing final dataframe...')
    print(full_df.tail(10))

    return full_df

#show Candlestick chart with Pandas dataframe using plotly 

def showChart(df):

    print('show chart')

    fig = go.Figure(data=[go.Candlestick(x=df['date'],
                    open=df['O'],
                    high=df['H'],
                    low=df['L'],
                    close=df['C'])])
    # fig.update_layout(
    #     title = 'Time Series with Custom Date-Time Format',
    #     xaxis_tickformat = '%d %B %Y'
    # )

    fig.show()

if __name__ == '__main__':
    """
    Takes a SierraChart scid file (input argument 1) and converts
      it to a tab separated file (tsv)
    Timezone conversion can follow the users local timezone, or a
      specified integer (input l or an integer but if the default
      filename is being used, '' must be specified for the filename)
    """
    tzvar = 0
    if len(sys.argv) > 1:
        if len(sys.argv[1]):
            filename = sys.argv[1]
        if len(sys.argv) > 2 and len(sys.argv[2]):
            print(sys.argv[2], type(sys.argv[2]))
            if sys.argv[2] == 'l':
                tzvar = 99999
                print(tzvar, type(tzvar))
            else:
                tzvar = float(sys.argv[2])
                print(tzvar, type(tzvar))

    print('Start converting...')
    print('converting SCID file...')
    # convert SCID file to multi TSV files
    fileoutput = scid_filename[:-5]
    getRecordsFromScid(scid_filename, fileoutput, tzvar, 1)

    print('converting DEPTH file...')
    # convert DEPTH file to multi TSV files
    for depth_fn in depth_filenames:
        fileoutput = depth_fn[:-6]
        getRecordsFromDepth(depth_fn, fileoutput, tzvar, 1)

    print('merging DEPTH and valid SCID content...')
    # merge valid scid content and depth content into multi tsv files
    merge_scid_depth()

    print('getting one dataframe...')
    # get one dataframe from multi tsv files
    final_df = getOneDf()

    print('starting drawing chart...')
    showChart(final_df)
    print('drawing chart finished')


