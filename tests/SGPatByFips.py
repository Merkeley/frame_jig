'''

    File: SGPatByFips

    Description: Process safegraph poi information and store the processed
        data in an sql database for use in analysis.

        This is part of a multifile, multi-step procedure for processing
        components of the safegraph and covid data from analysis.


'''
# Imports
import os
import sys
import ast
import argparse
import glob
import csv
import zipfile
import gzip

import pandas as pd

import pickle

import json

import pprint

import mysql.connector
from mysql.connector import errorcode


# Define the configuration parameters we need
default_config = {
        'system' : {
            'source_mode' : 'local',
            'output' : 'weekly_output',
            'output_path' : './'
            },
        'pattern' : {
            'new_file_list' : 'new_sync_files.txt',
            'keep_cols' : "['safegraph_place_id', 'date_range_end', 'raw_visit_counts', 'bucketed_dwell_times']",
            'path' : '/home/michael/Documents/Data/safegraph/v2/'
            },
        'SQL' : { # All the sql configuration info
            'mode' : 'local',
            'host' : 'localhost',
            'database' : 'Covid',
            'user_env_var' : 'Covid_SQL_user', # Name of environment variable
            'pwd_env_var' : 'Covid_SQL_pwd', # Name of environment variable
            }
    }

# Define the default configurations for the sql tables
TABLES = {}

TABLES['traffic'] = (
    "CREATE TABLE `traffic` ("
    " `safegraph_place_id` varchar(60) NOT NULL,"
    " `date` DATE NOT NULL,"
    " `visit_count` INT NOT NULL,"
    " `median_dwell` FLOAT NOT NULL,"
    " `est_total_dwell` FLOAT NOT NULL,"
    " `id` INT NOT NULL AUTO_INCREMENT,"
    " PRIMARY KEY (`id`)"
    ") ENGINE=InnoDB")

add_traffic = ("INSERT INTO traffic (safegraph_place_id, date, visit_count, "
            "median_dwell, est_total_dwell ) "
            "VALUES (%s, %s, %s, %s, %s )")
id_test = ("SELECT * FROM traffic WHERE safegraph_place_id='%s'")

def create_table(cursor, table_dict) :
    try:
        cursor.execute(table_dict)
    except mysql.connector.Error as err:
        if err.errno != errorcode.ER_TABLE_EXISTS_ERROR:
            print(err.msg)
            return False
        else:
            # Table exists
            return True

    return True

def db_connect(user, password, host, database) :
    '''
    Function: db_connect
        Simple wrapper around database connection script
    Parameters:
        user - username for database
        password - password for user
        host - host location for database
        database - database to use
    Return:
        cnx - database connection handle.  None is connection fails
    '''

    try:
        cnx = mysql.connector.connect(user=user, password=password, \
                database=database, host=host, use_pure=False)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print('Connection Failed: User name or password error')
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print('Database does not exist')
        else :
            print(err)

        return None
    else :
        return cnx

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


def read_config(filename, cfg_dict) :
    # Open the file and load the json data
    with open(filename) as fp:
        run_config = json.load(fp)

    out_dict = {}

    # Look through the json data for the cfg values we need
    for next_key in cfg_dict.keys() :
        if next_key in run_config.keys() :
            out_dict[next_key] = {}
            # For each of the keys in the cfg
            for sub_key in cfg_dict[next_key] :
                # Create a new dictionary of just the values we want
                out_dict[next_key][sub_key] = \
                    dict_check(sub_key, run_config[next_key], cfg_dict[next_key][sub_key])

    # Return the smaller dictionary
    return(out_dict)

def est_total_dwell(bucket_str) :
    bucket_multipliers = {'<5' : 2.5, '5-20' : 12.5, '21-60' : 40.5, '61-240' : 150.5, '>240' : 240}

    # run through each row in the series

    dwell_buckets = ast.literal_eval(bucket_str)

    total_dwell = 0

    # Now use the dictionary to calculate the total dwell time
    for bucket in dwell_buckets :
        total_dwell += dwell_buckets[bucket] * bucket_multipliers[bucket]

    # return the list of total dwell times
    return(total_dwell)

def main() :

    # Read the configuration file
    core_config = read_config(opt.config, default_config)

    if opt.verbose :
        print('Using Configuration:')
        pprint.pprint(core_config)
    '''
    '''
    # Open the local connection if the configuration calls for local
    cnx = db_connect(user=os.environ.get(core_config['SQL']['user_env_var']),
                password=os.environ.get(core_config['SQL']['pwd_env_var']),
                host=core_config['SQL']['host'],
                database=core_config['SQL']['database'])

    if cnx is None :
        print('Error connecting to database')
        return

    cursor = cnx.cursor()

    '''
    '''
    # Make sure all the tables exist
    for table in TABLES :
        if create_table(cursor, TABLES[table]) == False :
            print('Unrecoverable error creating table')
            return

    # Open the poi file
    try:
        fp = open(core_config['pattern']['new_file_list'], 'r') 
    except:
        print('Unable to open {}'.format(core_config['pattern']['new_file_list']))
        raise

    with fp:
        file_list = [x.strip('\n') for x in fp]

    col_names = str_to_list(core_config['pattern']['keep_cols'])

    csv.register_dialect('safegraph', delimiter=',', quotechar='"')

    for pattern_file in file_list[5:] :
        #with gzip.open(core_config['pattern']['path']+\
        try:  
            fp = gzip.open(pattern_file, 'rt') 
        except :
            print('Unable to open {}'.format(pattern_file))
            cnx.close()
            raise

        with fp :
            print(pattern_file)

            # Read the header
            data_reader = csv.reader(fp, quotechar='"', delimiter=',', escapechar='\\', \
                    quoting=0, doublequote=True)
            try:
                header = next(data_reader)
            except csv.Error as err:
                print('Error reading csv file', err)
                cnx.close()
                raise

            print(header)

            # Figure out which columns we want to keep
            keep_idx = []
            for idx, name in enumerate(col_names) : 
                if name in header:
                    keep_idx.append(header.index(name))
                else :
                    print("Key Error: Unknown keep column specified")
                    cnx.close()
                    return()

            # For each line in the file
            line_saved = 0
            print('Storing data')
            #for line in data_reader :
            reading_done = False;
            while not reading_done:
                try:
                    line = next(data_reader)
                except StopIteration:
                    reading_done=True
                    continue
                except csv.Error as err:
                    print('!', end='', sep='', flush=True)
                    continue

                # Keep the desired columns
                col_data = [line[idx] for idx in keep_idx]

                # Clean the date
                date_idx = col_names.index("date_range_end")
                col_data[date_idx] = col_data[date_idx].split('T')[0]


                # Calculate the mean dwell from bucketed dwell times
                dwell_idx = col_names.index("bucketed_dwell_times")
                col_data.append(est_total_dwell(col_data[dwell_idx]))
                col_data.pop(dwell_idx)

                col_data_strings = [str(x)[:40] for x in col_data]

                # Check if this location is already in the sql table
                try: 
                    cursor.execute(add_traffic, tuple(col_data_strings))
                except mysql.connector.Error as err:
                    print(err)

                '''
                '''
                line_saved += 1

                if line_saved % 1000 == 0 :
                    print('.', end='', sep='', flush=True)
                    cnx.commit()

            # Make a final commit
            cnx.commit()

    # Close the database connection
    cnx.close()

if __name__ == '__main__':
    #
    # Use Argparse to get command line arguments
    #
    # Basic usage information.
    #
    parser = argparse.ArgumentParser(prog='SB_PatternProc', \
            description='Process SafeGraph pattern file', \
            usage='%(prog)s [options]')
    #
    # Argument for the config file
    #
    parser.add_argument('-c', '--cfg', metavar='file name', \
            type=str, default='cfg/SGProc.cfg',\
            dest='config',
            help='Configuration File Name')
    #
    # Argument for the verbose output
    #
    parser.add_argument('-v', \
            type=bool, default=False,\
            dest='verbose',
            help='Verbose Processing Mode')
    #
    # parse the arguments and save in opt
    #
    opt = parser.parse_args()

    main()
