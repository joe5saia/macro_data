from io import BytesIO
from urllib.request import urlopen
from zipfile import ZipFile
import pandas as pd
import os
import numpy as np
import sqlalchemy
from sqlalchemy import create_engine
import datapungi_fed as dpf


def myengine():
    return create_engine(open("pgres_url.txt", "r").read())


gb_all_url = 'https://www.philadelphiafed.org/-/media/frbp/assets/surveys-and-data/greenbook-data/gbweb/gbweb_all_column_format.zip?la=en&hash=22851EFA1EF12BDB30474720752BB409'
with urlopen(gb_all_url) as zipresp:
    with ZipFile(BytesIO(zipresp.read())) as zfile:
        zfile.extractall('/tmp/greenbooks')


def read_gb(fname):
    # Reads in the greenbook forecast data and reshapes to long format
    # Day is calculated weird to avoid setting column to scallar when there is no data in the excel file
    df = pd.DataFrame(pd.read_excel(fname, index_col='Date').stack()).reset_index()
    df.columns = ['valuedate', 'var_fdate', 'value']
    df.loc[:, 'variable'] = df.loc[:, 'var_fdate'].apply(lambda x: x[0:-9])
    df.loc[:, 'forecastdate'] = pd.to_datetime(df.loc[:, 'var_fdate'].apply(lambda x: x[-8:]), format='%Y%m%d')
    df.loc[:, 'year'] = np.floor(df.loc[:, 'valuedate'])
    df.loc[:, 'month'] = 3 * np.round(10*(df.loc[:, 'valuedate'] - np.floor(df.loc[:, 'valuedate'])))
    df.loc[:, 'day'] = np.divide(np.floor(df.loc[:, 'valuedate']), np.floor(df.loc[:, 'valuedate']))
    df.loc[:, 'valuedate'] = pd.to_datetime(df.loc[:, ['year', 'month', 'day']]) + pd.tseries.offsets.QuarterEnd()
    return df.loc[:, ['variable', 'forecastdate', 'valuedate', 'value']]


df = pd.concat([read_gb(fname) for fname in os.scandir('/tmp/greenbooks')], ignore_index=True)
df.to_sql("gb_forecasts", myengine(), if_exists='replace', index=False, method='multi',
          dtype={"forecastdate": sqlalchemy.Date(), "valuedate": sqlalchemy.Date()})


def read_gb_outgap(fname):
    df = pd.DataFrame(pd.read_excel(fname, index_col=0).stack()).reset_index()
    df.columns = ['valuedate', 'var_fdate', 'value']
    df.loc[:, 'variable'] = df.loc[:, 'var_fdate'].apply(lambda x: x[0:-7])
    df.loc[:, 'forecastdate'] = pd.to_datetime(df.loc[:, 'var_fdate'].apply(lambda x: x[-6:]), format='%y%m%d')
    df.loc[:, 'year'] = pd.to_numeric(df.loc[:, 'valuedate'].apply(lambda x: x[0:4]))
    df.loc[:, 'month'] = 3*pd.to_numeric(df.loc[:, 'valuedate'].apply(lambda x: x[5:7]))
    df.loc[:, 'day'] = 1
    df.loc[:, 'valuedate'] = pd.to_datetime(df.loc[:, ['year', 'month', 'day']]) + pd.tseries.offsets.QuarterEnd()
    return df.loc[:, ['variable', 'forecastdate', 'valuedate', 'value']]


def read_ebp(fname):
    df = pd.read_csv(fname, parse_dates=['date'])
    df.columns = ['datem', 'gz_spread', 'gz_premium', 'gz_default_prob']
    df.set_index('datem', inplace=True)
    return df


def read_wrds_csv():
    """
    loops through all the files in wrdsdata and reads them in
    These files should be CSVs pulled from wrds. The date format should be monthly MMDDYY10 (e.g. 07/25/1984)
    """
    filepaths = [f'wrdsdata/{f}' for f in os.listdir("wrdsdata") if f.endswith('.csv')]
    df = pd.concat(map(lambda x: pd.read_csv(x, parse_dates=['caldt'], index_col='caldt'), filepaths), axis=1)
    df.index.rename('datem', inplace=True)
    df.index = df.index + pd.offsets.MonthBegin(-1)
    return df


def pull_fred_data(series):
    """
    Pull data from fred
    """
    fred = dpf.data("5240bbe3851ef2d1aaffd0877d6048dd")
    datas = [fred.series(s, frequency='m') for s in series]
    df = pd.concat(datas, axis=1)
    df.index.rename('datem', inplace=True)
    return df


def main():
    fname = 'https://www.philadelphiafed.org/-/media/frbp/assets/surveys-and-data/greenbook-data/greenbook_output_gap_dh_web.xlsx?la=en&hash=FFA675CD9C77F04E3F2BAA2D5657276D'
    read_gb_outgap(fname).to_sql("gb_forecasts", myengine(), if_exists='append', index=False,
                                 method='multi', dtype={"forecastdate": sqlalchemy.Date(), "valuedate": sqlalchemy.Date()})

    fseries = ['fedfunds', 'indpro', 'cpiaucsl', 'unrate', 'dgs2', 'dgs10', 'dgs1']
    df = pull_fred_data(fseries)
    df = df.join(read_wrds_csv(), how='outer')
    fname = 'https://www.federalreserve.gov/econresdata/notes/feds-notes/2016/files/ebp_csv.csv'
    df.join(read_ebp(fname), how='outer')
    dflong = pd.melt(df, ignore_index=False).dropna()
    print("Replacing macro_data SQL table!")
    dflong.to_sql('macro_data', myengine(), if_exists='replace', index=True,
                  method='multi', dtype={"datem": sqlalchemy.Date()})


if __name__ == "__main__":
    main()
