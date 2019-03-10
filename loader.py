import log
import requests
import json
import pandas as pd
from config import Config
import os


class Loader:
    def __init__(self):
        # Prepare logger
        log.setup()
        self.logger = log.logging.getLogger(__name__)

        # Read config
        self.config = Config(path="config/loader.yaml").load()
        self.logger.debug("[%u] Config is loaded " % os.getpid())
        self.logger.debug("[%u] Config is %s" %
                          (os.getpid(), self.config))

        self._markets_name = self._get_markets(key='id')
        self._data_from_cache = self._get_data_from_cache()
        self.emitent_ids = self._get_emitent_ids()
        self.emitent_names = self._get_emitent_names()
        self.emitent_codes = self._get_emitent_codes()
        self.emitent_markets_ids = self._get_emitent_markets()
        self.emitent_market_names = [self._markets_name[int(id)] for id in self.emitent_markets_ids]
        self.available_data = self._join_data()

    def _get_response(self, url):
        r = requests.get(url, headers=self.config['headers'])
        assert r.status_code == 200, "Response error - %s" % r.status_code
        self.logger.debug("Response is - %s" % r.text)
        return r.text

    def _get_markets(self, key):
        """
            Prepare dict of available markets
        :return: dict {market: id}
        """
        market_info_raw_data = self._get_response(self.config['url']['market_info'])
        market_info_substr = self._find_substring(text=market_info_raw_data,
                                                  start=self.config['str_template']['market_substring_start_from'],
                                                  stop=self.config['str_template']['market_substring_stop'])
        markets_in_json = self._turn_str_into_valid_json(market_info_substr)
        return self._to_dict(self._to_json(markets_in_json), key=key)

    def _find_substring(self, text, start, stop):
        """
            Extract substring from input text
        :param text: string for processing
        :param start: template which will be used to define start position
        :param stop: stop symbol
        :return: str
        """
        start_pos = text.find(start) + len(start)
        end_pos = text[start_pos:].find(stop)
        substring = text[start_pos:][:end_pos]
        self.logger.debug("Substring is = %s" % substring)
        return substring

    def _turn_str_into_valid_json(self, text):
        text = text.replace("value:", '"value":')\
                                                  .replace("title:", '"title":')\
                                                  .replace("'", '"')
        self.logger.debug("Valid json string is = %s" % text)
        return text

    def _to_json(self, text):
        """
            Turn substring into json
        :param text: str
        :return: json like {'value': 200, 'title': 'ММВБ-Top'}, {'value': 1, 'title': 'ММВБ Акции'},...
        """
        # transform substring to json
        json_data = json.loads(text)
        self.logger.debug("Transformed to JSON %s" % json_data)
        return json_data

    def _to_dict(self, json_data, key):
        """
            Turn substring into dict
        :param markets_substring:
        :return: dict like {'ММВБ-Top': 200, 'ММВБ Акции': 1,...
        """
        if key == 'id':
            # create dict value=>key title=>value
            dictionary = {json_doc['value']: json_doc['title'] for json_doc in json_data}
        elif key == 'name':
            # create dict title=>key value=>value
            dictionary = {json_doc['title']: json_doc['value'] for json_doc in json_data}
        else:
            self.logger.error("Unknown value of key parameter - %s" % key)
            dictionary = {}
        self.logger.debug("Transformed to dict %s" % dictionary)
        return dictionary

    def show(self, column):
        """
            Print pandas DataFrame of available data
        :param column: 'market_id', 'market_name', 'emitent_id', 'emitent_code' or 'emitent_name'
        :return: None
        """
        print(self.available_data[column].unique())

    def _get_data_from_cache(self):
        self.logger.info("[%u] Receiving data from cache..." % os.getpid())
        return self._get_response(self.config['url']['finam_cache'])

    def _get_emitent_ids(self):
        """
        :return: list of emitent ids
        """
        return self._get_emitent_info(type_info='emitent_ids')

    def _get_emitent_names(self):
        """
        :return: list of emitent names
        """
        return self._get_emitent_info(type_info='emitent_names')

    def _get_emitent_codes(self):
        """
        :return: list of emitent codes
        """
        return self._get_emitent_info(type_info='emitent_codes')

    def _get_emitent_markets(self):
        """
        :return: list of emitent markets
        """
        return self._get_emitent_info(type_info='emitent_markets')

    def _get_emitent_info(self, type_info):
        """
            Extract list of data from raw text
        :param type_info: str what kind info will be extract
        :return: list
        """
        text = self._data_from_cache
        start = self.config['str_template'][type_info]['start_from']
        stop = self.config['str_template'][type_info]['stop']
        split_symbol = self.config['str_template'][type_info]['split_symbol']
        emitent_substr = self._find_substring(text=text, start=start, stop=stop)
        emitent_list = emitent_substr.split(split_symbol)
        self.logger.debug('[%u] %s list is %s' %
                          (os.getpid(), type_info, emitent_list))
        self.logger.debug('[%u] First element %s list is %s' %
                          (os.getpid(), type_info, emitent_list[0]))
        return emitent_list

    def _join_data(self):
        self.logger.debug("market_id list has length: %d" % len(self.emitent_markets_ids))
        self.logger.debug("market_name list has length: %d" % len(self.emitent_market_names))
        self.logger.debug("emitent_id list has length: %d" % len(self.emitent_ids))
        self.logger.debug("emitent_code list has length: %d" % len(self.emitent_codes))
        self.logger.debug("emitent_name list has length: %d" % len(self.emitent_names))
        return pd.DataFrame({
            'market_id': self.emitent_markets_ids,
            'market_name': self.emitent_market_names,
            'emitent_id': self.emitent_ids,
            'emitent_code': self.emitent_codes,
            'emitent_name': self.emitent_names
        })
