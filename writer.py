import os
from pandas import read_csv
from loader import Loader
from datetime import datetime
from utilites import get_the_newest_fname
from utilites import create_folder_if_not_exists
from utilites import rotate_files
from utilites import get_path
import requests
import csv
from prototypes import Base


class Writer(Base):
    """
    The main goal of this class to get quotes and save it.
    Mode can be 'update' or date like 'dd-mm-yyyy'.
    In update mode metadata will be updated and the newest metadata file will be used.
    If date is specified metadata file like 'dd-mm-yyyy.csv' will be used
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

    def _get_metadata_fname(self):
        if self.mode == 'update':
            ldr = Loader()
            path = ldr.path_to_metadata
            return get_the_newest_fname(path, pattern='*.csv')
        else:
            return self.mode + '.csv'

    def _get_metadata(self):
        fname = self._get_metadata_fname()
        df = read_csv(fname, sep=';')
        return df

    def _find_securities(self):
        df = self._get_metadata()
        res = df[
            (df.market_id.isin(self.market_id)) |
            (df.market_name.isin(self.market_name)) |
            (df.emitent_id.isin(self.emitent_id)) |
            (df.emitent_code.isin(self.emitent_code)) |
            (df.emitent_name.isin(self.emitent_name))].copy()

        res['emitent_code'] = res.emitent_code.apply(lambda x: x.replace("'", ""))
        return res.iterrows()

    def _make_url(self, df_str):
        domen = 'http://export.finam.ru/'
        fname = 'payload.csv?'
        market = 'market=%s&' % df_str['market_id']
        em = 'em=%s&' % df_str['emitent_id']
        code = 'code=%s&' % df_str['emitent_code']
        apply = 'apply=0&'
        df = 'df=1&'
        mf = 'mf=0&'
        yf = 'yf=1990&'
        from_ = 'from=01.01.1990&'
        cur_date = str(datetime.today()).split()[0].split('-')
        dt = 'dt=%s&' % cur_date[2]
        mt = 'mt=%s&' % (int(cur_date[1].lstrip('0')) - 1)
        yt = 'yt=%s&' % cur_date[0]
        cur_date.reverse()
        to = 'to=%s&' % '-'.join(cur_date)
        p = 'p=%s&' % self.tf_index
        f = 'payload&'
        e = 'e=.csv&'
        cn = 'cn=%s&' % df_str['emitent_code']
        dtf = 'dtf=%s&' % self.config['request']['date_format']
        tmf = 'tmf=%s&' % self.config['request']['time_format']
        msor = 'MSOR=0&'
        mstime = 'mstime=on&'
        mstimever = 'mstimever=1&'
        sep = 'sep=%s&' % self.config['request']['sep_fields']
        sep2 = 'sep2=%s&' % self.config['request']['sep_digits']
        datf = 'datf=%s&' % self.config['request']['header']
        at = 'at=1'

        url = (domen
                   + fname
                   + market
                   + em
                   + code
                   + apply
                   + df
                   + mf
                   + yf
                   + from_
                   + dt
                   + mt
                   + yt
                   + to
                   + p
                   + f
                   + e
                   + cn
                   + dtf
                   + tmf
                   + msor
                   + mstime
                   + mstimever
                   + sep
                   + sep2
                   + datf
                   + at)
        return url

    def _get_timeframe(self):
        symbol = self.config['request']['period']
        index = self.config['request']['kinds_of_periods'][symbol]
        return index, symbol

    def save(self):
        create_folder_if_not_exists(dirname=self.quote_dir)
        for _, sec in self._find_securities():
            self.logger.info("[%u] Start saving the following: " % os.getpid())
            self.logger.info("[%u] %s" % (os.getpid(), sec))
            url = self._make_url(sec)
            self.logger.info("[%u] URL %s" % (os.getpid(), url))

            self._get_write_and_rotate(sec, url)

    def _get_write_and_rotate(self, sec, url):
        r = self._get_response(url)
        fname = self._make_fname(sec, self.tf_symbol, self.quote_dir)
        self._write_to_file(fname, r)
        self._rotate_files(sec)

    def _rotate_files(self, sec):
        path = self._make_fname(sec, self.tf_symbol, self.quote_dir, mode='dir_only')
        pattern = self._make_fname(sec, self.tf_symbol, self.quote_dir, mode='file_only')[:-14] + '*.csv'
        rotate_files(path, pattern)

    @staticmethod
    def _write_to_file(fname, r):
        with open(fname, 'w') as f:
            writer = csv.writer(f)
            reader = csv.reader(r.text.splitlines())

            for i, row in enumerate(reader):
                if i == 0:
                    row = [word.replace('<', '').replace('>', '') for word in row]
                writer.writerow(row)

    @staticmethod
    def _make_fname(sec, tf, dir, mode='full_path'):
        directory = get_path(dir)
        fname = '_'.join((str(sec.market_id),
                          sec.market_name,
                          str(sec.emitent_id),
                          sec.emitent_code,
                          sec.emitent_name,
                          tf,
                          datetime.today().strftime('%d-%m-%Y'))) + '.csv'
        if mode == 'full_path':
            fname = directory + fname
        elif mode == 'dir_only':
            fname = directory

        return fname

    def _get_response(self, url):
        r = requests.get(url,
                         headers=self.config['headers'],
                         allow_redirects=True)
        assert r.status_code == 200, "Response error - %s" % r.status_code
        return r




