from prototypes import Base
from loader import Loader
from utilites import get_the_newest_fname
from pandas import read_csv
from pandas import date_range
from pandas import DataFrame
from datetime import datetime
from utilites import get_path
import os
import sys
from writer import Writer


class Reader(Base):
    """
    The main goal of this class to read quotes.
    Mode can be 'update' or date like 'dd-mm-yyyy'.
    In update mode metadata will be updated and the newest metadata file will be used
    to getting new quotes.
    If a date is specified, the metadata file like 'dd-mm-yyyy.csv'
    and quote files like '*_dd-mm-yyyy.csv' will be used
    """
    def __init__(self,
                 mode='update',
                 market_id=(),
                 market_name=(),
                 emitent_id=(),
                 emitent_code=(),
                 emitent_name=()):
        Base.__init__(self, __name__)

        self.mode = mode
        self.market_id = market_id
        self.market_name = market_name
        self.emitent_id = emitent_id
        self.emitent_code = emitent_code
        self.emitent_name = emitent_name
        self.tf_index, self.tf_symbol = self._get_timeframe()
        # TODO take out dirname to config
        self.quote_dir = 'quotes'

    def _get_timeframe(self):
        symbol = self.config['request']['period']
        index = self.config['request']['kinds_of_periods'][symbol]
        return index, symbol

    def _get_metadata_fname(self):
        path = Loader.path_to_metadata
        isfile = os.path.isfile(path + datetime.today().strftime("%d-%m-%Y") + '.csv')
        if self.mode == 'update' and isfile is not True:
            ldr = Loader()
            return get_the_newest_fname(path, pattern='*.csv')
        elif self.mode == 'update' and isfile:
            return get_the_newest_fname(path, pattern='*.csv')
        else:
            return Loader.path_to_metadata + self.mode + '.csv'

    def _get_metadata(self):
        fname = self._get_metadata_fname()
        try:
            df = read_csv(fname, sep=';')
            return df
        except Exception as e:
            self.logger.error('[%u] %s' % (os.getpid(), e))
            sys.exit(1)

    def _find_securities(self):
        df = self._get_metadata()
        res = df[
            (df.market_id.isin(self.market_id)) |
            (df.market_name.isin(self.market_name)) |
            (df.emitent_id.isin(self.emitent_id)) |
            (df.emitent_code.isin(self.emitent_code)) |
            (df.emitent_name.isin(self.emitent_name))].copy()

        res['emitent_code'] = res.emitent_code.apply(lambda x: x.replace("'", ""))
        res.index = range(res.shape[0])
        return res.iterrows()

    def _get_todate(self):
        if self.mode == 'update':
            to_date = str(datetime.today()).split()[0].split('-')
        else:
            to_date = self.mode.split('-')
            to_date[2], to_date[0] = to_date[0], to_date[2]
        return to_date

    @staticmethod
    def _make_fname(sec, tf, dir, to_date, mode='full_path'):
        directory = get_path(dir)
        to_date.reverse()
        fname = '_'.join((str(sec.market_id),
                          sec.market_name,
                          str(sec.emitent_id),
                          sec.emitent_code,
                          sec.emitent_name,
                          tf,
                          '-'.join(to_date))) + '.csv'
        if mode == 'full_path':
            fname = directory + fname
        elif mode == 'dir_only':
            fname = directory

        return fname

    def read(self, reference: dict, dfrom='2016-01-01', dto=None,
             price='CLOSE', volume=False, download_if_not_exists=True):

        df = self._make_initial_df(dfrom, dto)
        for _, sec in self._find_securities():
            fname = self._make_fname(sec,
                                     self.tf_symbol,
                                     self.quote_dir,
                                     self._get_todate())

            df = self.get_data_from_file_or_download_it(df,
                                                        download_if_not_exists,
                                                        fname,
                                                        price,
                                                        sec,
                                                        volume)

            df = self._dropnan(df, reference, sec)

        print(df.tail(20))

    def get_data_from_file_or_download_it(self, df, download_if_not_exists, fname, price, sec, volume):
        if os.path.isfile(fname):
            df = self._read_and_join_df(df, fname, price, sec, volume)
        else:
            self.logger.warn("[%u] %s doesn't exist" % (os.getpid(), fname))
            if download_if_not_exists:
                Writer(self.mode, emitent_id=[sec['emitent_id']]).save()
                df = self._read_and_join_df(df, fname, price, sec, volume)
        return df

    def _dropnan(self, df, reference, sec):
        reference_field_name = list(reference.keys())[0]
        reference_value = reference[reference_field_name]
        if sec[reference_field_name] == reference_value:
            try:
                df = df.dropna(subset=[sec['emitent_code']])
            except KeyError:
                self.logger.warn("[%u] Reference security paper %s:%s hasn't been found"
                                 % (os.getpid(), reference_field_name, reference_value))
                df = df.dropna()
        return df

    def _read_and_join_df(self, df, fname, price, sec, volume):
        df_temp = self._read_file(fname, price, sec, volume)
        df = df.join(df_temp)
        return df

    def _read_file(self, fname, price, sec, volume):
        self.logger.info("[%u] Start reading the %s: "
                         % (os.getpid(), fname))
        col_for_rename = {price: sec['emitent_code']}
        if volume:
            usecols = ['DATE', price, 'VOL']
            col_for_rename['VOL'] = sec['emitent_code'] + '_V'
        else:
            usecols = ['DATE', price]
        df_temp = read_csv(fname,
                           sep=';',
                           index_col='DATE',
                           parse_dates=True,
                           usecols=usecols,
                           na_values=['nan'])
        df_temp = df_temp.rename(columns=col_for_rename)
        return df_temp

    def _make_initial_df(self, dfrom, dto):
        if dto is None:
            dto = self._get_todate()
            dto.reverse()
            dto = '-'.join(dto)
        dates = date_range(dfrom, dto)
        return DataFrame(index=dates)
