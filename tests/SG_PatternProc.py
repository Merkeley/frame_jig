'''
    File: SG_PatternProc
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


class PatternBuilderL(DFBuilder):
    '''
        class PatternBuilderL - configuration and methods for building the
            SafeGraph traffic pattern files.

    '''
    def __init__(self, files=None, path=None, columns=None, **kwargs) :
        super().__init__(files, path, columns, **kwargs)

    #
    # Overload the file_open method with functionality specific to the pattern files
    #
    # Note that this is a generator method.  
    #
    # In some cases files are 
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

            # use zipfile to read the contents of the target
            z_contents = [x for x in z.namelist() if (x.endswith(('gz', 'csv')))]

            # get the base path of the original filename
            path = os.path.basename(file_name)

            if len(z_contents) > 1 :
                for sub_file in z_contents[:-1] :
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

            try:
                z_fp = z.open(z_contents[-1])
                return(z_fp)
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
            except:
                print('Unexpected Error {}'.format(sys.exc_info()[0]))
                raise

class PatternBuilderR(DFBuilder):
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

    def df_clean(self, frame):
        return(frame)


    #
    # Overload the file_open method with functionality specific to the pattern files
    #
    # Note that this is a generator method.  
    #
    # In some cases files are 
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

            # use zipfile to read the contents of the target
            z_contents = [x for x in z.namelist() if (x.endswith(('gz', 'csv')))]

            # get the base path of the original filename
            path = os.path.basename(file_name)

            if len(z_contents) > 1 :
                for sub_file in z_contents[:-1] :
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

            try:
                z_fp = z.open(z_contents[-1])
                return(z_fp)
                z_fp.close()
            except OSError:
                print('Error: failed to open {}'.format(sub_file))
                raise
            except:
                print('Unexpected Error {}'.format(sys.exc_info()[0]))
                raise
        else:
            try:
                fp = self.s3.open(file_name)
                yield(fp)
                fp.close()
            except OSError:
                print('Error opening file {}'.format(file_name))
                raise
                return
            except:
                print('Unexpected Error {}'.format(sys.exc_info()[0]))
                raise

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

        # Parse the parameters for the pattern files
        if 'pattern' in run_config.keys() :
            # Rebuild specifies rebuilding the whole pattern file system
            pattern_rebuild = \
                    dict_check('rebuild', run_config['pattern'], 'True') == 'True'

            # Check for a path to the pattern files - local or remote
            pattern_path = dict_check('path', run_config['pattern'], './')
            pattern_files = dict_check('files', run_config['pattern'], None)
            if pattern_files is None:
                print('Error: No pattern files specified')
                return
            else :
                pattern_files = str_to_list(pattern_files)

            pattern_keep = dict_check('keep_cols', run_config['pattern'], '*')
            pattern_keep = str_to_list(pattern_keep)

            pattern_mode = dict_check('mode', run_config['pattern'], 'Unknown')

            # Look for a file format for the pattern files
            pattern_format = dict_check('format', run_config['core_poi'], 'infer')


        # Parse the parameters for the core poi files
        if 'core_poi' in run_config.keys() :
            # Get the rebuild setting
            core_rebuild = \
                    dict_check('rebuild', run_config['core_poi'], 'True') == 'True'

            # Get the pickle file usage setting
            core_save_pickle = \
                    dict_check('save_pickle', run_config['core_poi'], 'False') == 'True'

            # Get the name of the pickle file
            core_pickle_name = \
                    dict_check('pickle_file', run_config['core_poi'], '')

            # Check for a path to the pattern files - local or remote
            core_path = dict_check('path', run_config['core_poi'], './')
            core_files = dict_check('files', run_config['core_poi'], None)

            if core_files is None:
                print('Error: No core poi files specified')
                return
            else :
                core_files = str_to_list(core_files)

            # Look for a file format for the core poi file
            core_format = dict_check('format', run_config['core_poi'], 'infer')

            # File format
            core_mode = dict_check('mode', run_config['core_poi'], 'Unknown')

            # Columns to keep
            core_keep = dict_check('keep_cols', run_config['core_poi'], '*')
            # Convert the text list to an actual list
            core_keep = str_to_list(core_keep)

            # Get the parameters for the suplemental files
            core_sqft = \
                dict_check('supplemental_sqft', run_config['core_poi'], 'False') == 'True'
            core_sup_file = dict_check('supplemental_file', run_config['core_poi'], '')
            core_places = dict_check('places_file', run_config['core_poi'], None)
            if core_places is None:
                print('Warning: No core poi places file specified')
    else :
        # Look for config parameters in the command line
        return

    # If required, build the core_poi reference file
    if core_rebuild :
        print('Building Core Dataframe')

        # Use the frame builder class definition that corresponds to our mode
        if core_mode == 'Local' :
            core_poi_df = CoreBuilderL(files=core_files,\
                    path=core_path, columns=core_keep, compression=core_format).build()
        elif core_mode == 'Remote':
            core_poi_df = CoreBuilderR(files=core_files,\
                    path=core_path, columns=core_keep, compression=core_format).build()
        else:
            print('Unknown File Mode for Core POI')
            print("Mode must be 'Local' or 'Remote'")
            return

        # Read the places data (lat, lon) -> (state, county)
        if core_places is not None :
            print('Adding places information')
            places_df = DFBuilder(files=[core_places], path='', columns=['*']).build()

            print(places_df.head())
            print(core_poi_df.head())
            # Merge the data frames
            full_df = core_poi_df.merge(places_df, left_on=['safegraph_place_id'],\
                    right_on=['safegraph_place_id'], how='left')
        else:
            print('No places information available')

        # If we're adding the supplemental square footage do that
        if core_sqft :
            # Build the dataframe
            print('Adding square footage')
            sqft_df = DFBuilder(files=[core_sup_file], columns=['*'],\
                    compression='gzip').build()

            # Merge with the larger dataframe
            full_df = full_df.merge(sqft_df, left_on=['safegraph_place_id'],
                    right_on=['safegraph_place_id'], how='left')

        if core_save_pickle :
            try:
                print('Saving Pickle File {}'.format(core_pickle_name))
                pickle.dump(core_poi_df, open(core_pickle_name, 'wb'))
            except OSError :
                print('Unable to open file: {}'.format(core_pickle_name))
    else :
        # If the configuration didn't request a rebuild of the core poi info
        # then read that inforation from the specified pickle file
        try :
            print('Reading pickle file {}'.format(core_pickle_name))
            core_poi_df = pickle.load(open(core_pickle_name, 'rb'))
        except OSError:
            print('Unable to open file: {}'.format(core_pickle_name))
        except :
            print('Unexpected Error {}'.format(sys.exc_info()[0]))

    # If required, build the pattern file
    if pattern_rebuild and output_file != '':
        print('Building pattern dataframe')

        # Use the builder class that corresponds to our mode
        if pattern_mode == 'Local':
            pattern_df = PatternBuilderL(files=pattern_files, path=pattern_path,
                columns=pattern_keep, compression=pattern_format).build()
        elif pattern_mode == 'Remote':
            pattern_df = PatternBuilderR(files=pattern_files, path=pattern_path,
                columns=pattern_keep, compression=pattern_format).build()
        else:
            print('Unknown File Mode for Pattern Files')
            print("Mode must be 'Local' or 'Remote'")
            return


        print('Merging Data');
        if core_poi_df is not None and output_file != '':
            big_df  = core_poi_df.merge(pattern_df, left_on=['safegraph_place_id'],\
                    right_on=['safegraph_place_id'])

            try :
                big_df.to_csv(output_path+output_file)
            except OSError :
                print('Error: Unable to save to file {}'.format(output_path+output_file))
            except :
                print('Unexpected Error {}'.format(sys.exc_info()[0]))


        elif core_poi_df is None :
            print('Error: No Core POI file for rebuild')

    elif output_file == '' :
        print('Error: Pattern rebuild specified without output file')


def main():
    pattern_proc()

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

