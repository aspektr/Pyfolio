import os
from utilites import create_folder_if_not_exists
from utilites import rotate_files
import requests
import csv
from quotesio import QuotesIO


class Writer(QuotesIO):
    """
    The main goal of this class to get quotes and save it.
    Mode can be 'update' or date like 'dd-mm-yyyy'.
    In update mode metadata will be updated and the newest metadata file will be used.
    If date is specified metadata file like 'dd-mm-yyyy.csv' will be used
    """

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
        to_date = self._get_todate()
        dt = 'dt=%s&' % to_date[2]
        mt = 'mt=%s&' % (int(to_date[1].lstrip('0')) - 1)
        yt = 'yt=%s&' % to_date[0]
        to_date.reverse()
        to = 'to=%s&' % '-'.join(to_date)
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

    def save(self):
        create_folder_if_not_exists(dirname=self.quote_dir)
        for _, sec in self._find_securities():
            self.logger.info("[%u] Start downloading the following: " % os.getpid())
            self.logger.info("[%u] %s" % (os.getpid(), sec))
            url = self._make_url(sec)
            self.logger.info("[%u] URL %s" % (os.getpid(), url))

            self._get_write_and_rotate(sec, url)

    def _get_write_and_rotate(self, sec, url):
        r = self._get_response(url)
        fname = self._make_fname(sec, self.tf_symbol, self.quote_dir, self._get_todate())
        self._write_to_file(fname, r)
        if self.mode == 'update':
            self._rotate_files(sec)

    def _rotate_files(self, sec):
        path = self._make_fname(sec, self.tf_symbol, self.quote_dir, self._get_todate(), mode='dir_only')
        pattern = self._make_fname(sec,
                                   self.tf_symbol,
                                   self.quote_dir,
                                   self._get_todate(),
                                   mode='file_only')[:-14] + '*.csv'
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

    def _get_response(self, url):
        r = requests.get(url,
                         headers=self.config['headers'],
                         allow_redirects=True)
        assert r.status_code == 200, "Response error - %s" % r.status_code
        return r
