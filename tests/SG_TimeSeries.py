'''
    File: SG_TimeSeries
        Script to process the weekly pattern files from SafeGraph.  The pattern
    files contain location visit information.  In order to get a complete picture
    of the traffic these pattern files need to be merged with the SafeGraph
    Core Point of Interest files and their Suplemental Square Footage files.

    This script is intended to be a standard part of the data processing pipeline.

    Author: MBoals

    Date Created: 5-21-2020
    Last Update:

    Update Log:

'''

# Standard files for file system io
import os
import sys
import ast
import argparse
import glob
import zipfile

import pickle

import json

# Modules for AWS s3 access
import boto3
import s3fs

# Locally defined class definitions for data frame processing
from FrameJig import DFBuilder

def build_time_series_frame(files=[], path='', columns=[], 
        suffixes=[], join_keys=[], how='inner', **kwargs) :
    '''
        Read each of the files in the list from the designated path
        keep the columns and adjust the column names using the suffixes
        Then join the frames on the designated keys
    '''

    # Check that the suffixes and files lists are the same length
    if len(files) != len(suffixes) :
        # Raise an error
        raise ValueError

    big_df = None
    print(len(files))

    for idx, file_entry in enumerate(files) :
        # Use the DFBuilder - this allows for calling a with a list of lists for files
        print(file_entry, path, columns)
        if type(file_entry) != list :
            file_entry = [file_entry]

        new_df = DFBuilder(file_entry, path, columns, **kwargs).build()
        new_df['est_dwell'] = calc_dwells(new_df['bucketed_dwell_times'])

        print(new_df.columns)

        # append the suffix
        new_columns = [x + '_' + suffixes[idx] for x in new_df.columns]
        new_df.columns = new_columns

        # Now join this with the other dataframes
        if big_df is not None:
            print(big_df.head())
            print(new_df.head())
            big_df = big_df.merge(new_df, left_index=True, right_index=True, how=how)
        else:
            big_df = new_df

    return big_df

'''

    Simple utility functions

'''
def str_to_list(in_str) :
    '''
        Function: str_to_list - parse a string that represents a list into an actual list
            This function assumes that in_str is in the format of 
                "['token 1', 'token 2', ....'token n']"
            and returns the list
                ['token 1', 'token 2', ..., 'token n']

        Arguments:
            in_str - string version of list.

        Return: 
            list of strings from parsed input string.
    '''
    out_lst = \
        in_str.replace('[', '').replace(']', '').replace("'", "").replace(' ','').split(',')
    return out_lst


def dict_check(key, this_dict, default) :
    '''
        Function: dict_check - check a dictionary for the existance of a key.  If the key
            exists then return the value for that key.  If the key doesn't exist
            return the default argument.

        Arguments:
            key - key that we're checking for in the dictionary.
            this_dict - dictionary that needs checking.
            default - default value to return if the key is not in the dictionary.

        Return:
            value associated with the key argument or default if key isn't in dict.
    '''
    if key in this_dict.keys() :
        return(this_dict[key])
    else:
        return(default)

# This function will split the bucketed dwell field and calculate an estimated mean
def calc_dwells(dwell_col) :
    total_dwell_list = []
    dwell_buckets = {}
    bucket_multipliers = {'<5' : 2.5, '5-20' : 12.5, '21-60' : 40.5, '61-240' : 150.5, '>240' : 240}
    
    # run through each row in the series
    
    for row in dwell_col :
        dwell_buckets = ast.literal_eval(row)
        
        total_dwell = 0

        # Now use the dictionary to calculate the total dwell time
        for bucket in dwell_buckets :
            total_dwell += dwell_buckets[bucket] * bucket_multipliers[bucket]

        total_dwell_list.append(total_dwell)
    
    # return the list of total dwell times
    return(total_dwell_list)

'''

    Primary process is below

'''

def pattern_proc() :
    '''
        Function: pattern_proc - sequence of steps to process the core poi and 
            pattern files.  This function starts by reading and parsing a configuration
            file.  The file is in a standard json format (for convenience).
            The entries in the config file specify the input files to read
            and how to process them.

        Arguments: None

        Return: None
    '''
    data_path = '/home/michael/Documents/Data/safegraph/v2/main-file/'

    data_files = [x.split('/')[-1] for x in sorted(glob.glob(data_path+'*.csv.gz'))]
    print(len(data_files))
    suffixes = []
    for file_name in data_files :
        tokens = file_name.split('/')
        suffixes.append(tokens[-1][:10])

    big_df = build_time_series_frame(files=data_files[80:], path=data_path, 
        suffixes=suffixes[80:],
        columns=['raw_visit_counts', 'bucketed_dwell_times'],
        join_keys=[], how='left', compression='gzip', index_col=0)

    big_df.to_csv(data_path+'full_weekly_time_series.csv')

if __name__ == '__main__':
    pattern_proc()
