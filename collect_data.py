from io import BytesIO
from os import truncate
from urllib.request import urlopen
from zipfile import ZipFile
import pandas as pd
import numpy as np
import os
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.sql.sqltypes import Float

#############################################################
# variable: What is being forecasted
# forecastdate: date on which forecast is made
# valuedate: what date is being forecasted
# value: value of the forecast
# gb_date: date of the greenbook, corresponds to forecastdate
#############################################################


def myengine():
    return create_engine(open("/workspace/pgres_url.txt", "r").read())


# Code for reading in the greenbook series
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
    df = df.loc[:, ['variable', 'forecastdate', 'valuedate', 'value']]
    return df


def read_gbs():
    gb_all_url = 'https://www.philadelphiafed.org/-/media/frbp/assets/surveys-and-data/greenbook-data/gbweb/gbweb_all_column_format.zip?la=en&hash=22851EFA1EF12BDB30474720752BB409'
    with urlopen(gb_all_url) as zipresp:
        with ZipFile(BytesIO(zipresp.read())) as zfile:
            zfile.extractall('/tmp/greenbooks')
    df = pd.concat([read_gb(fname) for fname in os.scandir('/tmp/greenbooks')], ignore_index=True).set_index(['variable', 'forecastdate', 'valuedate'])
    #df.to_sql("gb_forecasts", myengine(), if_exists='replace', index=False, method='multi',
    #          dtype={"forecastdate": sqlalchemy.Date(), "valuedate": sqlalchemy.Date()})
    return df


# Code for rading in the output data

def read_gb_outgap():
    xgap_fname = 'https://www.philadelphiafed.org/-/media/frbp/assets/surveys-and-data/greenbook-data/greenbook_output_gap_dh_web.xlsx?la=en&hash=FFA675CD9C77F04E3F2BAA2D5657276D'
    df = pd.DataFrame(pd.read_excel(xgap_fname, index_col=0).stack()).reset_index()
    df.columns = ['valuedate', 'var_fdate', 'value']
    df.loc[:, 'variable'] = df.loc[:, 'var_fdate'].apply(lambda x: x[0:-7])
    df.loc[:, 'forecastdate'] = pd.to_datetime(df.loc[:, 'var_fdate'].apply(lambda x: x[-6:]), format='%y%m%d')
    df.loc[:, 'year'] = pd.to_numeric(df.loc[:, 'valuedate'].apply(lambda x: x[0:4]))
    df.loc[:, 'month'] = 3 * pd.to_numeric(df.loc[:, 'valuedate'].apply(lambda x: x[5:7]))
    df.loc[:, 'day'] = 1
    df.loc[:, 'valuedate'] = pd.to_datetime(df.loc[:, ['year', 'month', 'day']]) + pd.tseries.offsets.QuarterEnd()
    return df.loc[:, ['variable', 'forecastdate', 'valuedate', 'value']].set_index(['variable', 'forecastdate', 'valuedate'])



# Code for financial assumptions data
def finass_wide2long(df_wide, varname, gb_dates):
    df_wide = df_wide.join(gb_dates)
    df_wide.rename(columns={'t': 'valuedate'}, inplace=True)
    df_wide.set_index(['valuedate', 'forecastdate'], inplace=True)
    df_long = pd.DataFrame(df_wide.stack(), columns=['value'])
    df_long.index.set_names('reltime', level=-1, inplace=True)
    df_long.reset_index(level=-1, inplace=True)
    df_long.loc[df_long.reltime == 't.1', 'reltime'] = 't+0'
    df_long['relqtr'] = pd.to_numeric(df_long['reltime'].str[1:])
    df_long['valuedate'] = df_long.index.get_level_values('forecastdate') + pd.offsets.QuarterBegin()
    df_long['valuedate'] = df_long.apply(lambda x: x.valuedate + pd.DateOffset(months=3*x.relqtr), axis=1)
    df_long.drop(['reltime', 'relqtr'], axis=1, inplace=True)
    df_long['value'] = df_long['value'].astype(str).str.rstrip('+- ')
    df_long.query("value != ''", inplace=True)
    df_long['value'] = df_long['value'].astype(float)
    df_long['forecastdate'] = df_long.index.get_level_values('forecastdate')
    df_long['variable'] = varname
    df_long.reset_index(drop=True, inplace=True)
    df_long.set_index(['variable', 'forecastdate', 'valuedate'], inplace=True)
    return df_long


def read_gb_finass(gb_dates):
    fname = 'https://www.philadelphiafed.org/-/media/frbp/assets/surveys-and-data/greenbook-data/greenbook_financial_assumptions_interestrates_web.xls?la=en&hash=867BC5FACF9613C9FD6E31E838494213'
    xls = pd.ExcelFile(fname)

    def read(sheet, var):
        return finass_wide2long(pd.read_excel(xls, sheet_name=sheet, skiprows=2, index_col='FOMC Meeting', parse_dates=['t']), var, gb_dates)
    finass_data = []
    finass_data.append(read('Federal Funds Rate', 'fedfunds'))
    finass_data.append(read('3-Month', '3monthtreas'))
    finass_data.append(read('10-Year', '10yeartreas'))
    finass_data.append(read('30-Year', '30yeartreas'))
    finass_data.append(read('Mortgages (merged)', 'mortgagerate'))
    finass_data.append(read('Wilshire 5000', 'wilshire5000'))
    finass_data.append(read('Baa Corporate', 'Baayield'))
    finass_data.append(read('Corporate Bond', 'corpbond'))
    finass_data.append(read('Recent  Aaa Utility Bond', 'Aaautility'))
    return pd.concat(finass_data, ignore_index=False)


# read_gb_outgap(fname).to_sql("gb_forecasts", myengine(), if_exists='append', index=False,
#                             method='multi', dtype={"forecastdate": sqlalchemy.Date(), "valuedate": sqlalchemy.Date()})

# execess bond premium data
fname = 'https://www.federalreserve.gov/econresdata/notes/feds-notes/2016/files/ebp_csv.csv'
df = pd.read_csv(fname, parse_dates=['date'])
df.columns = ['date', 'gz_spread', 'gz_premium', 'gz_default_prob']
df.set_index('date', inplace=True)
df = pd.DataFrame(df.stack()).reset_index()
df.columns = ['date', 'variable', 'value']
df.to_sql("macro_data", myengine(), if_exists='replace', index=False,
          method='multi', dtype={"date": sqlalchemy.Date()})


def read_epm():
    fname = 'https://www.federalreserve.gov/econresdata/notes/feds-notes/2016/files/ebp_csv.csv'
    df = pd.read_csv(fname, parse_dates=['date'])
    df.columns = ['date', 'gz_spread', 'gz_premium', 'gz_default_prob']
    df.set_index('date', inplace=True)
    df = pd.DataFrame(df.stack()).reset_index()
    df.columns = ['date', 'variable', 'value']
    df.set_index(['variable', 'date'], inplace=True)
    return df


def main():
    # Read and upload the greenbook data
    gb_forecasts = read_gbs()
    gb_date_fname = 'https://www.philadelphiafed.org/-/media/frbp/assets/surveys-and-data/greenbook-data/greenbook_publication_dates_web.xlsx?la=en&hash=02104A132EEFCE6C867E4CA3AB023333'
    gb_dates = pd.read_excel(
        gb_date_fname, sheet_name=0, usecols=['FOMC Meeting', 'Greenbook Publication Date'],
        index_col='FOMC Meeting').rename(
        columns={'Greenbook Publication Date': 'forecastdate'})
    gb_xg_gap = read_gb_outgap()
    gb_finass = read_gb_finass(gb_dates)
    all_gb = pd.concat([gb_forecasts, gb_xg_gap, gb_finass]).sort_index()
    print("Greenbook data downloaded and read. Loading into SQL")
    all_gb.to_sql('gb_forecasts', myengine(), if_exists='replace', index=True, method='multi', dtype={"forecastdate": sqlalchemy.Date(), "valuedate": sqlalchemy.Date()})
    print("Greenbook data loaded!")
    # Read and upload the excess bond premium data
    epm = read_epm()
    print("Excess bond premium data downloaded and read. Loading into SQL")
    epm.to_sql('macro_forecasts', myengine(), if_exists='replace', index=True, method='multi', dtype={"date": sqlalchemy.Date()})
    print("Excess bond premium data loaded!")


if __name__ == "__main__":
    main()
