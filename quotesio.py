from prototypes import Base
from loader import Loader
from utilites import get_the_newest_fname
from utilites import get_path
from datetime import datetime
import os
import sys
from pandas import read_csv


class QuotesIO(Base):
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
            Loader()
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
            (df.emitent_id.isin(map(str, self.emitent_id))) |
            (df.emitent_code.isin(self.emitent_code)) |
            (df.emitent_name.isin(self.emitent_name))].copy()
        res['emitent_code'] = res.emitent_code.apply(lambda x: x.replace("'", ""))
        res.index = range(res.shape[0])
        self.logger.info("[%u] Found %s securities" % (os.getpid(), res.shape[0]))
        res.drop_duplicates(subset=['emitent_id'], keep='first', inplace=True)
        return res.iterrows()

    def _get_todate(self):
        if self.mode == 'update':
            to_date = str(datetime.today()).split()[0].split('-')
        else:
            to_date = self.mode.split('-')
            to_date[2], to_date[0] = to_date[0], to_date[2]
        return to_date

    @staticmethod
    def _make_fname(sec, tf, directory, to_date, mode='full_path'):
        directory = get_path(directory)
        to_date.reverse()
        sec.emitent_name = sec.emitent_name.replace('/', '_')
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
