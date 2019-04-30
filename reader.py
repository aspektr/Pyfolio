from pandas import read_csv
from pandas import date_range
from pandas import DataFrame
import os
from writer import Writer
from quotesio import QuotesIO


class Reader(QuotesIO):
    """
    The main goal of this class to read quotes.
    Mode can be 'update' or date like 'dd-mm-yyyy'.
    In update mode metadata will be updated and the newest metadata file will be used
    to getting new quotes.
    If a date is specified, the metadata file like 'dd-mm-yyyy.csv'
    and quote files like '*_dd-mm-yyyy.csv' will be used
    """

    def read(self, reference: dict, dfrom='2016-01-01', dto=None,
             price='CLOSE', volume=False, download_if_not_exists=True,
             normalize=True, daily_returns=True):

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

        if normalize:
            df = self._normalize_data(df)
        if daily_returns:
            df = self._compute_daily_returns(df)
        df = self._fill_missing_values(df)

        self.logger.info("[%u] Result dataset has size %d x %d" % (os.getpid(), df.shape[0], df.shape[1]))
        self.logger.info("[%u] First row:" % os.getpid())
        print(df.head(3))
        self.logger.info("[%u] Last row:" % os.getpid())
        print(df.tail(3))
        return df

    @staticmethod
    def _fill_missing_values(df_data):
        """Fill missing values in data frame, in place."""
        df_data.fillna(method='ffill', inplace=True)
        df_data.fillna(method='bfill', inplace=True)
        return df_data

    @staticmethod
    def _compute_daily_returns(df):
        """Compute and return the daily return values."""
        res = df.copy()
        res[1:] = (df[1:] / df[:-1].values) - 1
        res.ix[0, :] = 0
        return res

    @staticmethod
    def _normalize_data(df):
        df = df / df.ix[0, :]
        return df

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
