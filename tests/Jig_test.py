import FrameJig as fj
import os
import pandas as pd

def build_test_df():
    # Create a list of available files
    my_files = ['*.csv.gz']

    # Decide which columns to keep
    core_keep_cols = ['location_name', \
                  'naics_code', 'latitude', 'longitude', \
                  'street_address', 'city', 'region', \
                  'postal_code']

    data_path = '/home/michael/Documents/Data/safegraph/core_poi/2020/07/'

    core_poi = fj.DFBuilder(files=my_files, \
            path=data_path, columns=core_keep_cols, \
            index_col=0, compression='gzip')

    core_poi_df = core_poi.build()
    if core_poi_df is not None :
        print(core_poi_df.head())
        print(core_poi_df.info())

def core_clean(frame):
    frame.dropna(inplace=True)
    return frame

def main() :
    build_test_df()

if __name__ == '__main__' :
    main()


