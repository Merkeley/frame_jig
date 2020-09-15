'''

    File: frame_jig.py
        Class definitions for a general purpose data frame cleaner and builder.
    The steps for consolidating data into a single structure are similar for many
    data projects.  Pandas provides some powerful tools for merging data frames.
    The classes in this file attempt to put a wrapper around those dataframes so
    the cleaning/merging pipeline can be standardized for any project.

    Author: M Boals

    Date Created: 5/21/2020
    Last Update:

    Modification Log:


'''
from copy import copy
import glob

import pandas as pd

class DFBuilder:
    '''
        Class DFBuilder
            General purpose class definition for building a single dataframe.
        This class is designed to be the first step in a data processing pipline.
        The attributes and methods in this class are set up to:
            1. Configure your process
            2. Read your input files
            3. Clean the data
            4. Merge the cleaned data from multiple files
            5. Return a merged dataframe

        The class relies on the basic DataFrame definition from Pandas.

        Attributes:
            files - list of file names that are input for basic data
            path - path to the files above
            params - this is a place holder for keyword arguments that
                will be passed to the pandas.read_csv function.
            columns - list of column names in the data set that should be kept.
                Passing nothing or ['*'] will keep all the columns.

        Methods:

        These are attribute access methods.  Use them if you believe encapsulation is
        important. Each of these methods can be called without arguments to retreive
        the current settings.
            files - set the list of files to use as input, returns current files.
            path - set the path to the input files, returns current path.
            columns - set the list of column names to keep, returns current columns list.

        Operational Methods:
            clean - use this method to perform any standard data cleaning steps
                needed for your process.
            build - this method walks through building the dataframe.  The steps are:
                1. open the file (file_open)
                2. read the data
                3. filter the columns using the 'columns' attribute
                4. clean the data via df_clean
                5. concatenate the data with data from other files in the
                    'files' attribute list.

        Functions:
            file_open - pandas can operate with a file name or a file handle.  The
                file_open method should be used to open a file and return the handle
                to that input stream.  The handle can be for a local file or
                a remote file as needed.

    '''
    # Define class methods
    # Constructor
    def __init__(self, files=None, path=None, columns=None, axis=0, \
            suffixes=None, how='inner', on=None, left_on=None, right_on=None, \
            left_index=False, right_index=False, **kwargs):
        '''
            Method: __init__ - class constructor for DFBuilder

            Arguments:
                files - list of files to process in the building of the dataframe.
                    This list can contain wildcards that conform to the unix
                    wildcard characters.
                path - base path where to look for the files
                columns - columns to keep from the data files.  Accepts the wildcard
                    * to keep all the files.
                axis - axis corresponding to the direction the files should be
                    merged.  0 - files will be merged using the append method,
                    extending the dataframe along the index.  1 - files will be
                    extended using the merge command, columns will be added to the
                    dataframe.  If axis = 1 then additional arguments should be used
                    to specify how the dataframes will be extended.
                suffixes - list of strings indicating how the column names should be
                    modified for each of the dataframes.
                how - method for joining the dataframes ('left', 'right',
                    'inner', 'outer').  Default is inner.
                on - list of column names to use for the merge.
                left_on - list of column names in the left dataframe to use to merge
                right_on - list of column names in the right dataframe to use
                    when merging dataframes.
                left_index - use the left index when merging
                right_index - use the right index when merging
                **kwargs - additional keyword arguments to pass to the datafile parsing
                    method (Pandas read_csv)

            Description:
                constructor for an instance of DFBuilder
        '''
        # Save the values if there is something to save
        if files is not None:
            if isinstance(files[0]) == list:
                self._files = copy(files)
            else:
                self._files = [copy(files)]
        else:
            self._files = []

        if path is not None:
            self._path = copy(path)
        else:
            self._path = ''

        if kwargs:
            self._file_params = kwargs
        else:
            self._file_params = {}

        if columns is not None:
            self._keep_columns = copy(columns)
        else:
            self._keep_columns = []

        self._axis = axis
        self._suffixes = suffixes
        self._how = how
        self._on = on
        self._left_on = left_on
        self._right_on = right_on
        self._left_index = left_index
        self._right_index = right_index


    # These methods are simple access methods to encourage the use of encapsilation
    def path(self, path=None):
        '''
            Method: path

            Arguments:
                path - string containing the base path for data files.

            Description:
                Accessor method for the path attribute.
        '''
        if path is not None:
            self._path = copy(path)
        return self._path

    def files(self, files=None):
        '''
            Method: files

            Arguments:

            Description:
                Accessor method for the files attribute.
        '''
        if files is not None:
            if isinstance(files[0]) == list:
                self._files = copy(files)
            else:
                self._files = [copy(files)]
        return self._files

    def columns(self, columns=None):
        '''
            Method: columns

            Arguments:

            Description:
                Accessor method for the columns attribute.
        '''
        if columns is not None:
            self._keep_columns = copy(columns)
        return self._keep_columns

    # Define a place holder for this method.  Override for specific cases
    def clean(self, frame):
        '''
            Method: file_open

            Arguments:

            Description:
        '''
        # Only save the columns specified by keep_columns
        return frame[self._keep_columns]


    # Default file open method
    def file_open(self, file_name):
        '''
            Method: file_open

            Arguments:
                file_name - text string containing

            Description:
        '''
        try:
            # Try to open the file
            # Note that we are assuming this is opening as a text file
            file_p = open(file_name, 'r')
        except OSError:
            # Manage the possible errors
            print('Error opening file {}'.format(file_name))

        # Return the file pointer as a list
        yield file_p


    # Define the methods that actually do the work
    def build(self):
        '''
            Description:
                The build method uses the attributes that have been pre-configured
            to walk through the process of building the a dataframe from
            multiple input files.

            Return:
                consolidated, cleaned pandas.DataFrame

        '''
        # Build the dataframe from the specified parameters
        if self._files is None:
            raise ValueError('No data source specified')

        # Make a complete list of files.  Run through glob to take care of wildcards
        full_file_list = []
        for group in self._files:
            for block in group:
                full_file_list.extend(glob.glob(self._path+block))


        big_df = None

        # for each of the files
        for idx, file_name in enumerate(full_file_list):
            print(file_name, self._file_params)
            for file_p in self.file_open(file_name):
                temp_df = pd.read_csv(file_p, **self._file_params)

                # Reduce to the columns we want to keep
                if ((self._keep_columns is None) or
                    (self._keep_columns[0] == '*')):
                    self._keep_columns = temp_df.columns

                # Run the clean function
                temp_df = self.clean(temp_df)

                # If we're appending suffixes then create the list for this join
                if self._suffixes is not None:
                    tmp_suffixes = ["", self._suffixes[idx]]
                else:
                    tmp_suffixes = ["", ""]

                # Append to the dataframe with the collection of data
                if big_df is None:
                    big_df = temp_df.copy()
                    if self._suffixes is not None:
                        big_df.columns = [x + self._suffixes[0] for x in big_df.columns]
                elif self._axis == 0:
                    big_df = pd.concat([big_df, temp_df])
                elif self._axis == 1:
                    big_df = big_df.merge(temp_df, \
                            how=self._how, on=self._on, left_on=self._left_on, \
                            right_on=self._right_on, left_index=self._left_index,\
                            right_index=self._right_index, suffixes=tmp_suffixes)

        # Return the consolidated dataframe.
        return big_df
