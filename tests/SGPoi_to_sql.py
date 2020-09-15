'''

    File: SGPoi_to_sql

    Description: Process safegraph poi information and store the processed
        data in an sql database for use in analysis.

        This is part of a multifile, multi-step procedure for processing
        components of the safegraph and covid data from analysis.


'''
# Imports
import os
import sys
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
        'core_poi' : {
            'rebuild' : 'True',
            'mode' : 'local',
            'path' : '/home/michael/Documents/Data/safegraph/core_poi/',
            'files' : '*',
            'format' : 'gzip',
            'keep_cols' : "['safegraph_place_id', 'location_name',\
                'naics_code', 'lattitude', 'longitude',\
                'street_address', 'city', 'region', 'postal_code']",
            'supplemental_sqft' : "True",
            'supplemental_file' : "SafeGraphPlacesGeoSupplementalSquareFeet.csv.gz",
            'supplemental_format' : "gzip",
            'supplemental_cols' : "[*]",
            'add_places' : "True",
            'places_file' : "",
            'places_format' : ""
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

TABLES['core_poi'] = (
    "CREATE TABLE `core_poi` ("
    " `safegraph_place_id` varchar(60) NOT NULL,"
    " `name` varchar(40) NOT NULL,"
    " `naics` varchar(8) NOT NULL,"
    " `lat` FLOAT NOT NULL,"
    " `lon` FLOAT NOT NULL,"
    " `address` varchar(40) NOT NULL,"
    " `city` varchar(40) NOT NULL,"
    " `region` varchar(40) NOT NULL,"
    " `zipcode` varchar(40) NOT NULL,"
    " `area_square_feet` INT NOT NULL,"
    " `state` varchar(40) NOT NULL,"
    " `county_fips` varchar(8) NOT NULL,"
    " `state_fips` varchar(8) NOT NULL,"
    " `county` varchar(40) NOT NULL,"
    " `id` INT NOT NULL AUTO_INCREMENT,"
    " PRIMARY KEY (`id`)"
    ") ENGINE=InnoDB")

add_poi = ("INSERT INTO core_poi (safegraph_place_id, name, naics, lat, lon, address, "
            "city, region, zipcode, area_square_feet, state, state_fips, county_fips, "
            "county) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
id_test = ("SELECT * FROM core_poi WHERE safegraph_place_id='%s'")

def create_table(cursor, table_dict) :
    try:
        cursor.execute(table_dict)
    except mysql.connector.Error as err:
        if err.errno != errorcode.ER_TABLE_EXISTS_ERROR:
            print(err.msg)
        return False

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

def main() :

    # Read the configuration file
    core_config = read_config(opt.config, default_config)

    if opt.verbose :
        print('Using Configuration:')
        pprint.pprint(core_config)

    # Read the supplemental square footage data and store in a pandas dataframe
    if core_config['core_poi']['supplemental_sqft'] == 'True' :
        sqft_df = pd.read_csv(core_config['core_poi']['supplemental_file'], index_col=0)
        sqft_ids = set(sqft_df.index)
    elif opt.verbose :
        print('No supplemental square foot data')

    # Read the places data file and store in a pandas dataframe
    places_df = pd.read_csv(core_config['core_poi']['places_file'], \
                index_col=0)
    places_ids = set(places_df.index)

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

    # Make sure all the tables exist
    for table in TABLES :
        if create_table(cursor, TABLES[table]) == False :
            print('Unrecoverable error creating table')
            return
    '''
    '''

    # Create at csv writer
    with open('test.csv', 'w') as csv_out :
        out_writer = csv.writer(csv_out, delimiter=',',
                quotechar='"', quoting=csv.QUOTE_ALL)

        # Open the poi file
        file_list = str_to_list(core_config['core_poi']['files'])
        with gzip.open(core_config['core_poi']['path']+\
                file_list[0], 'rt') as fp :
            # Read the header
            data_reader = csv.reader(fp, delimiter=',')
            header = next(data_reader) 

        col_names = str_to_list(core_config['core_poi']['keep_cols'])

        if core_config['core_poi']['supplemental_sqft'] == 'True' :
             col_names.append('area_square_feet')
        col_names.extend(list(places_df.columns[:-1]))

        out_writer.writerow(col_names)

        for poi_file in file_list :
            with gzip.open(core_config['core_poi']['path']+\
                    poi_file, 'rt') as fp :
                # Read the header
                data_reader = csv.reader(fp, delimiter=',')
                header = next(data_reader) 

                # Figure out which columns we want to keep
                keep_idx = []
                for idx, name in enumerate(col_names) : 
                    if name in header:
                        keep_idx.append(header.index(name))

                # For each line in the file
                line_saved = 0
                print('Storing data')
                for line in data_reader :
                    # Keep the desired columns
                    col_data = [line[idx] for idx in keep_idx]

                    # Check if this location is in the supplemental square footage df
                    if col_data[0] in sqft_ids :
                        col_data.append(sqft_df.loc[col_data[0], 'area_square_feet'])
                    else:
                        continue

                    # Add the location info to the row
                    if col_data[0] in places_ids :
                        col_data.extend(places_df.loc[col_data[0], :].values[:-1])
                    else:
                        continue

                    col_data_strings = [str(x)[:40] for x in col_data]

                    #out_writer.writerow(col_data)

                    '''
                    '''
                    # Check if this location is already in the sql table
                    try: 
                        cursor.execute(add_poi, tuple(col_data_strings))
                    except mysql.connector.Error as err:
                        print(err)

                    line_saved += 1
                    if line_saved % 1000 == 0 :
                        print('.', end='', sep='', flush=True)
                        cnx.commit()

                # If not, add

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
