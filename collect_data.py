from io import BytesIO
from os import truncate
from urllib.request import urlopen
from zipfile import ZipFile
import pandas as pd
import numpy as np
import os
import sqlalchemy
from sqlalchemy import create_engine

def myengine():
    return create_engine(open("/workspace/pgres_url.txt", "r").read())

gb_all_url = 'https://www.philadelphiafed.org/-/media/frbp/assets/surveys-and-data/greenbook-data/gbweb/gbweb_all_column_format.zip?la=en&hash=22851EFA1EF12BDB30474720752BB409'
with urlopen(gb_all_url) as zipresp:
    with ZipFile(BytesIO(zipresp.read())) as zfile:
        zfile.extractall('/tmp/greenbooks')

def read_gb(fname):
    # Reads in the greenbook forecast data and reshapes to long format
    # Day is calculated weird to avoid setting column to scallar when there is no data in the excel file
    df = pd.DataFrame(pd.read_excel(fname, index_col = 'Date').stack()).reset_index()
    df.columns=['valuedate', 'var_fdate', 'value']
    df.loc[:, 'variable'] = df.loc[:, 'var_fdate'].apply(lambda x : x[0:-9] )
    df.loc[:, 'forecastdate'] = pd.to_datetime(df.loc[:, 'var_fdate'].apply(lambda x : x[-8:] ), format = '%Y%m%d')
    df.loc[:, 'year'] = np.floor(df.loc[:, 'valuedate'])
    df.loc[:, 'month'] = 3*np.round(10*(df.loc[:, 'valuedate'] - np.floor(df.loc[:, 'valuedate'])))
    df.loc[:, 'day'] = np.divide(np.floor(df.loc[:, 'valuedate']), np.floor(df.loc[:, 'valuedate']))
    df.loc[:, 'valuedate'] = pd.to_datetime(df.loc[:,['year', 'month', 'day']]) + pd.tseries.offsets.QuarterEnd()
    return df.loc[:, ['variable','forecastdate', 'valuedate', 'value']]


df = pd.concat([read_gb(fname) for fname in os.scandir('/tmp/greenbooks')], ignore_index=True)
df.to_sql("gb_forecasts", myengine(), if_exists='replace', index=False, method='multi', dtype={"forecastdate": sqlalchemy.Date(), "valuedate": sqlalchemy.Date()})


fname = 'https://www.philadelphiafed.org/-/media/frbp/assets/surveys-and-data/greenbook-data/greenbook_output_gap_dh_web.xlsx?la=en&hash=FFA675CD9C77F04E3F2BAA2D5657276D'
def read_gb_outgap(fname):
    df = pd.DataFrame(pd.read_excel(fname, index_col = 0).stack()).reset_index()
    df.columns=['valuedate', 'var_fdate', 'value']
    df.loc[:, 'variable'] = df.loc[:, 'var_fdate'].apply(lambda x : x[0:-7] )
    df.loc[:, 'forecastdate'] = pd.to_datetime(df.loc[:, 'var_fdate'].apply(lambda x : x[-6:] ), format = '%y%m%d')
    df.loc[:, 'year'] = pd.to_numeric(df.loc[:, 'valuedate'].apply(lambda x : x[0:4] ))
    df.loc[:, 'month'] = 3*pd.to_numeric(df.loc[:, 'valuedate'].apply(lambda x : x[5:7] ))
    df.loc[:, 'day'] = 1
    df.loc[:, 'valuedate'] = pd.to_datetime(df.loc[:,['year', 'month', 'day']]) + pd.tseries.offsets.QuarterEnd()
    return df.loc[:, ['variable','forecastdate', 'valuedate', 'value']]
read_gb_outgap(fname)
read_gb_outgap(fname).to_sql("gb_forecasts", myengine(), if_exists='append', index=False, method='multi', dtype={"forecastdate": sqlalchemy.Date(), "valuedate": sqlalchemy.Date()})


fname = 'https://www.federalreserve.gov/econresdata/notes/feds-notes/2016/files/ebp_csv.csv'
df = pd.read_csv(fname, parse_dates=['date'])
df.columns=['date', 'gz_spread', 'gz_premium', 'gz_default_prob']
df.set_index('date', inplace=True)
df = pd.DataFrame(df.stack()).reset_index()
df.columns=['date', 'variable', 'value']
df.to_sql("macro_data", myengine(), if_exists='replace', index=False, method='multi', dtype={"date": sqlalchemy.Date()})
