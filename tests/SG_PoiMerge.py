'''
    File: SG_PoiMerge
        Script to process the core points of interest files from SafeGraph.  
    The points of interest files contain detailed location information.  In order to get a complete picture
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


# Define classes for the different frame building processes
class CoreBuilderL(DFBuilder) :
    '''
        Core_builderL - Dataframe builder for Core POI dataframe with locally 
            stored data.  Define core poi specific cleaning method and file opening
            method.
    '''
    # Constructor simply calls the super class constructor
    def __init__(self, files=None, path=None, columns=None, **kwargs) :
        super().__init__(files, path, columns, **kwargs)


    # For the Core POI we'll just return the data frame
    def df_clean(self, frame) :
        return(frame)


    #
    # Overload the file_open method with functionality specific to the core poi files
    # Note that this is a generator method.  In some cases files are 
    # archived together in a compressed file format.  This overload should handle those
    # special cases and yield all values back to the build method.
    #
    def file_open(self, file_name, **kwargs) :
        # There are two possible local configurations for reading the local core file.
        # The file can be unzipped and the component files can be locally available
        # Or the core file from safegraph can be in its original form and we will
        # need to read the component files from the cosolidated zip file

        # If the file is a .csv file then just open the file and return the file pointer
        if file_name.endswith('csv') :
            try:
                fp = open(file_name, 'r')
                yield(fp)
                fp.close()
            except OSError:
                print('Error opening file {}'.format(file_name))
                raise

        elif file_name.endswith('zip') :
            # The zip file format can be a zip of zip files
            # Read the zip file and examine the contents
            try:
                z = zipfile.ZipFile(file_name)

            except OSError:
                print('Error opening {}'.format(file_name))
                raise

            # The zip format of the core_poi file often has multiple files
            # we're only interested in the core*.csv files
            z_contents = [x for x in z.namelist() \
                    if(x[:4] == 'core') and (x.endswith(('gz', 'csv')))]

            # Read each of the files in the zip file archive and
            # pass the pointer back to the build method.
            if len(z_contents) > 1 :
                for sub_file in z_contents :
                    try:
                        z_fp = z.open(sub_file)
                        yield(z_fp)
                        # Close the previous handle
                        z_fp.close()
                    except OSError:
                        print('Error: failed to open {}'.format(sub_file))
                        raise
                    except:
                        print('Unexpected Error {}'.format(sys.exc_info()[0]))
                        raise
        else:
            try:
                fp = open(file_name, 'rb')
                yield(fp)
                fp.close()
            except OSError:
                print('Error opening file {}'.format(file_name))
                raise
                return


class CoreBuilderR(DFBuilder) :
    '''
        Core_builderR - Dataframe builder for Core POI dataframe with remotely 
            stored data.  Define core poi specific cleaning method and file opening
            method.
    '''
    # Use the constructor for the super class
    def __init__(self, files=None, path=None, columns=None, **kwargs) :
        super().__init__(files, path, columns, **kwargs)

        # Open access to the S3 tools.  This implementation assumes that
        # AWS S3 access is configured in the .aws/config file
        try :
            self.s3 = s3fs.S3FileSystem(anon=False)
        except OSError:
            print('Unable to open connection to s3fs')
            raise
        except:
            print('Unexpected Error {}'.format(sys.exc_info()[0]))
            raise


    def df_clean(self, frame) :
        return(frame)

    #
    # Overload the file_open method with functionality specific to the core poi files
    #
    # Note that this is a generator method.  
    #
    # In some cases files are 
    # archived together in a compressed file format.  This overload should handle those
    # special cases and yield all values back to the build method.
    #
    def file_open(self, file_name, **kwargs) :
        # Define the file open method for the remote configuration
        #
        # There are two possible remote configurations for reading the remote core file.
        # The file can be unzipped and the component files can be remotely available
        # Or the core file from safegraph can be in its original form and we will
        # need to read the component files from the cosolidated zip file


        # If the file is a .csv file then just open the file and return the file pointer
        if file_name.endswith('csv') :
            try:
                fp = self.s3.open(file_name, 'r')
                yield(fp)
                fp.close()
            except OSError:
                print('Error opening file {}'.format(file_name))
                raise

        elif file_name.endswith('zip') :
            # The zip file format can be a zip of zip files
            # Read the zip file and examine the contents
            try:
                fp = self.s3.open(file_name)
                z = zipfile.ZipFile(fp)

            except OSError:
                print('Error opening {}'.format(file_name))
                raise

            # use zipfile to read the contents of the target
            z_contents = [x for x in z.namelist() \
                    if(x[:4] == 'core') and (x.endswith(('gz', 'csv')))]

            # get the base path of the original filename
            path = os.path.basename(file_name)

            if len(z_contents) > 1 :
                for sub_file in z_contents :
                    try:
                        z_fp = z.open(sub_file)
                        yield(z_fp)
                        z_fp.close()
                    except OSError:
                        print('Error: failed to open {}'.format(sub_file))
                        raise
                    except:
                        print('Unexpected Error {}'.format(sys.exc_info()[0]))
                        raise

            fp.close()
        else:
            # This condition will catch the 'gz' file format
            try:
                fp = self.s3.open(file_name, 'rb')
                yield(fp)
                fp.close()
            except OSError:
                print('Error opening file {}'.format(file_name))
                raise
                return

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


'''

    Primary process is below

'''

def core_poi_build(option_dict) :
    '''
        Function: pattern_proc - sequence of steps to process the core poi and 
            pattern files.  This function starts by reading and parsing a configuration
            file.  The file is in a standard json format (for convenience).
            The entries in the config file specify the input files to read
            and how to process them.

        Arguments: None

        Return: None
    '''
        # Get the rebuild setting
    core_rebuild = \
            dict_check('rebuild', option_dict, 'True') == 'True'

    # Get the pickle file usage setting
    core_save_pickle = \
            dict_check('save_pickle', option_dict, 'False') == 'True'

    # Get the name of the pickle file
    core_pickle_name = \
            dict_check('pickle_file', option_dict, '')

    # Check for a path to the pattern files - local or remote
    core_path = dict_check('path', option_dict, './')
    core_files = dict_check('files', option_dict, None)

    if core_files is None:
        print('Error: No core poi files specified')
        return
    else :
        core_files = str_to_list(core_files)

    # Look for a file format for the core poi file
    core_format = dict_check('format', option_dict, 'infer')

    # File format
    core_mode = dict_check('mode', option_dict, 'Unknown')

    # Columns to keep
    core_keep = dict_check('keep_cols', option_dict, '*')

    # Convert the text list to an actual list
    core_keep = str_to_list(core_keep)

    # Get the parameters for the suplemental files
    core_sqft = \
        dict_check('supplemental_sqft', option_dict, 'False') == 'True'

    core_sup_file = dict_check('supplemental_file', option_dict, '')

    core_places = dict_check('places_file', option_dict, None)
    if core_places is None:
        print('Warning: No core poi places file specified')

    core_output = dict_check('output_file', option_dict, None)
    if core_output is None:
        print('Warning: No core poi output file specified')

    # Use the frame builder class definition that corresponds to our mode
    if core_mode == 'Local' :
        core_poi_df = CoreBuilderL(files=core_files,\
                path=core_path, columns=core_keep, compression=core_format, index_col=0).build()
    elif core_mode == 'Remote':
        core_poi_df = CoreBuilderR(files=core_files,\
                path=core_path, columns=core_keep, compression=core_format, index_col=0).build()
    else:
        print('Unknown File Mode for Core POI')
        print("Mode must be 'Local' or 'Remote'")
        return

    # Read the places data (lat, lon) -> (state, county)
    if core_places is not None :
        print('Adding places information')
        places_df = DFBuilder(files=[core_places], path='', columns=['*'], index_col=0).build()

        # Merge the data frames
        full_df = core_poi_df.merge(places_df, left_index=True, right_index=True)
    else:
        print('No places information available')
        full_df = core_poi_df

    # If we're adding the supplemental square footage do that
    if core_sqft :
        # Build the dataframe
        print('Adding square footage')
        sqft_df = DFBuilder(files=[core_sup_file], columns=['*'],\
                compression='gzip', index_col=0).build()

    # Merge with the larger dataframe
    full_df = full_df.merge(sqft_df, left_index=True, right_index=True, how='left')

    if core_save_pickle :
        try:
            print('Saving Pickle File {}'.format(core_pickle_name))
            pickle.dump(full_df, open(core_pickle_name, 'wb'))
        except OSError :
            print('Unable to open file: {}'.format(core_pickle_name))

    if core_output is not None:
        try:
            print('Saving Output File {}'.format(core_output))
            full_df.to_csv(core_output)
        except OSError :
            print('Unable to open file: {}'.format(core_output))

    return full_df

def main():
    #
    # Argparse values are stored in opt (see the bottom of this file)
    #
    # If the user has specified a config file then process
    #
    if opt.config :
        #
        # Try to open the config file and parse using json module.
        #
        with open(opt.config) as fp:
            run_config = json.load(fp)

        # Run through the json dictionary and 
        #   set the run parameters based on the configuration
        if 'system' in run_config.keys() :
            output_file = dict_check('output', run_config['system'], '')
            output_path = dict_check('output_path', run_config['system'], './')
        else :
            source_mode = 'local'

        # Parse the parameters for the core poi files
        if 'core_poi' in run_config.keys() :
            core_poi_build(run_config['core_poi'])

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
    # parse the arguments and save in opt
    #
    opt = parser.parse_args()

    main()

