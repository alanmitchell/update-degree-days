#!/usr/local/bin/python3.6
"""Script that adds monthly heating degree day values to a pickled
Pandas DataFrame with the path 'data/degree_days.pkl' (compression = 'bz2').
It also saves the DataFrame as a CSV file at 'data/degree_days.csv'.  The
new degree-day information comes from the AHFC BMON site, https://bms.ahfc.us .

This script is typically run from a Cron job that schedules the script to
run on the first day of the month so that the prior month's degree days will
be available.  Dont' run the script late in the month or a partial month
may be prematurely added to the DataFrame because it satisfies the 
MIN_COVERAGE check described below.

This script assumes the pickled DataFrame already exists and has the
following format:

             month        hdd60        hdd65
 station                                     
 PAED    2018-02-01  1257.648675  1397.648675
 PAED    2018-03-01  1028.027773  1183.027773

 The index is the National Weather Service 4-letter station code.  The 
 'month' column is a first-of-the-month date identifying the month whose
 degree-days are shown.  'hdd60' and 'hdd65' are the heating degree-day 
 values: the first is base 60 degree F values and the second is base 65 
 deg F values.

 This script will acquire temperature data from the AHFC BMON site in order
 to calculate the degree-days for the most recent months not already
 present in the DataFrame.  All stations found in the index of the DataFrame
 will be updated.  The script assumes that the BMON sensor ID for a
 weather station's temperature data is the 4-letter station code with '_temp'
 appended, e.g. 'PAMR_temp'.

 The MIN_COVERAGE constant in the script controls the minimum amount of data
 coverage a month must have before being included.  Missing data is filled 
 in with the average value for the rest of the hours that do have data.

 -----------------------------------
 NOTES ON UTILIZING THE DATA

 To read this DataFrame back into a Python script, you can excecute the
 following if the DataFrame is available on a local drive:

    import pandas as pd
    df = pd.read_pickle('degree_days.pkl', compression='bz2')

If the file is located on a web server, you can read it with the following
code:

    import pandas as pd
    import requests
    from io import BytesIO
    b = requests.get('http://ahfc.webfactional.com/data/degree_days.pkl').content
    d = pd.read_pickle(BytesIO(b), compression='bz2')

Once you have a DataFrame, you can extract that portion of the DataFrame that
applies to one site by:

    df_one_site = df.loc['PAMR']

    or 

    df_one_site = df.query("station == 'PAMR'")
    (slower than above technique)

To extract one site with a subset of the months:

    df_one_site = df.query("station == 'PAMR' and month >= '2018-01-01'")


"""

from os.path import dirname, join, realpath
import sys
from datetime import datetime, timedelta
import pandas as pd
import requests

# Minimum fraction of the hours in a month that must have data in order
# to include the month.
MIN_COVERAGE = 0.7     

# path to this directory
APP_PATH = dirname(realpath(__file__))

# URL to the AHFC BMON site API
BMON_URL = 'https://bms.ahfc.us/api/v1/readings/{}/'

def dd_for_site(stn, start_date):
    """Returns a Pandas Dataframe of monthly heating degree-day values for
    'stn' (a NWS weather site code).  Degree days start in the month
    that 'start_date' (Python date/time object) 
    falls in and continue through the end of available
    data.  In the returned DataFrame, the index
    has a timestamp for each month returned, that being the first day
    of the month.  The columns of the DataFrame are "hdd65" and "hdd60"
    to designate base 65 F degree-days and base 60 F degree-days.
    Temperature data used to calculate degree-days comes from the AHFC
    BMON site.
    Missing hours are assumed to not deviate from the average of the 
    data present.  The column 'coverage' indicates the fraction of
    the months hours that actually have data.
    """
    # get beginning of month
    st_dt_1 = start_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    params = {
        'start_ts': st_dt_1.strftime('%Y-%m-%d'),
        'averaging': '1H'
    }
    sensor_id = '{}_temp'.format(stn)
    resp = requests.get(BMON_URL.format(sensor_id), params=params).json()
    
    if resp['status']=='success':
        df = pd.DataFrame(resp['data']['readings'], columns=['ts', 'temp'])
        df.set_index('ts', inplace=True)
        df.index = pd.to_datetime(df.index)

        # calculate the percentage of each month that has data
        dfc = df.resample('1M').count()
        dfc['total_hours'] = [i.day * 24 for i in dfc.index]    # index is last day of the month
        dfc['coverage'] = dfc.temp / dfc.total_hours

        # Now back to the main dataframe to calc degree-days
        df['hdd60'] = [(60.0 - x)/24.0 if x<60.0 else 0.0 for x in df.temp]
        df['hdd65'] = [(65.0 - x)/24.0 if x<65.0 else 0.0 for x in df.temp]
        df.drop(['temp'], axis=1, inplace=True)
        dfm = df.resample('1M').mean()
        dfm['coverage'] = dfc.coverage
        dfm['hdd60'] = dfm.hdd60 * dfc.total_hours
        dfm['hdd65'] = dfm.hdd65 * dfc.total_hours

        # Convert index timestamps to beginning of the month
        mos = [datetime(d.year, d.month, 1) for d in dfm.index]
        dfm.index = mos
        dfm.index.name = 'month'
        
    else:
        raise ValueError(str(resp['data']))
        
    return dfm

if __name__ == '__main__':

    df_exist = pd.read_pickle(join(APP_PATH, 'data/degree_days.pkl'), compression='bz2')

    # list of new DataFrames to add to the existing one
    new_dfs = []
    for stn in df_exist.index.unique():
        print('Processing {}: '.format(stn), end='')
        try:
            # get last month present for this station
            last_mo = df_exist.loc[stn].month.max()
            # get a date in the following month
            next_mo = last_mo + timedelta(days=32)  # could be a DST change in there; add 32 days to be safe

            # get degree days for missing months
            df_new = dd_for_site(stn, next_mo).query('coverage > @MIN_COVERAGE').copy()

            if len(df_new):
                # put this DataFrame in a form that can be concatenated to the existing one
                df_new.reset_index(inplace=True)
                df_new.index = [stn] * len(df_new)
                df_new.index.name = 'station'
                df_new.drop(columns=['coverage'], inplace=True)

                # add it to the list of new DataFrames to eventually add to the
                # degree-day DataFrame
                new_dfs.append(df_new)
                print('{} new months'.format(len(df_new)))

            else:
                print()

        except:
            print('{}: {}'.format(*sys.exc_info()[:2]))

    # Create a new DataFrame that combines the existing data with the new.
    df_final = pd.concat([df_exist] + new_dfs)
    # get it sorted by station and month
    df_final.reset_index(inplace=True)
    df_final.sort_values(['station', 'month'], inplace=True)
    df_final.set_index('station', inplace=True)

    # Save the DataFrame as a compressed pickle and a CSV file.
    df_final.to_pickle(join(APP_PATH, 'data/degree_days.pkl'), compression='bz2')
    df_final.to_csv(join(APP_PATH, 'data/degree_days.csv'))
