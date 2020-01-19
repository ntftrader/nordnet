# import talib
import pandas as pd
# import numpy as np
# import re
import datetime
import simplejson as json
# import math
import requests
import time
import os

import inspect
from functools import wraps

EXCHANGES = [
    {'c': 'no', 'm': 'ose'},
    {'c': 'no', 'm': 'osloaxess'},
    {'c': 'no', 'm': 'merk'},
    {'c': 'dk', 'm': 'largecapcopenhagendkk'},
    {'c': 'dk', 'm': 'midcapcopenhagendkk'},
    {'c': 'dk', 'm': 'smallcapcopenhagendkk'},
    {'c': 'dk', 'm': 'firstnorthcsedkk'},
    {'c': 'dk', 'm': 'spdk'},
    {'c': 'dk', 'm': 'omxc25'},
    {'c': 'fi', 'm': 'largecaphelsinkieur'},
    {'c': 'fi', 'm': 'midcaphelsinkieur'},
    {'c': 'fi', 'm': 'smallcaphelsinkieur'},
    {'c': 'fi', 'm': 'firstnorthfinland'},
    {'c': 'fi', 'm': 'omxh25'},
    {'c': 'se', 'm': 'largecapstockholmsek'},
    {'c': 'se', 'm': 'midcapstockholmsek'},
    {'c': 'se', 'm': 'smallcapstockholmsek'},
    {'c': 'se', 'm': 'firstnorthsto'},
    {'c': 'se', 'm': 'firstnorthpremierstockholmsek'},
    {'c': 'se', 'm': 'ngm'},
    {'c': 'se', 'm': 'spse'},
    {'c': 'se', 'm': 'omxs30'},
    {'c': 'de', 'm': 'dedax'},
    {'c': 'de', 'm': 'dehdax'},
    {'c': 'de', 'm': 'demdax'},
    {'c': 'de', 'm': 'desdax'},
    {'c': 'de', 'm': 'detecdax'},
    {'c': 'us', 'm': 'nyse'},
    {'c': 'us', 'm': 'nq'},
    {'c': 'us', 'm': 'usdjia'},
    {'c': 'us', 'm': 'ussp100'},
    {'c': 'us', 'm': 'usnas100'},
    {'c': 'ca', 'm': 'catsxcomp'},
    {'c': 'ca', 'm': 'catsxgold'},
    {'c': 'ca', 'm': 'catsxmet'},
    {'c': 'ca', 'm': 'catsx60'},
]

INDICATORS = [
    {'c': 'NO', 'm': 'OSE', 'i': 'OBX'},
    {'c': 'SE', 'm': 'SSE', 'i': 'OMXSPI'},
    {'c': 'FI', 'm': 'HEX', 'i': 'OMXHPI'},
    {'c': 'DK', 'm': 'CSE', 'i': 'OMXC25'},
    {'c': 'DE', 'm': 'SIX', 'i': 'B-IDX-DAXI'},
    {'c': 'US', 'm': 'USX', 'i': 'DJX-CADOW'},
    {'c': 'UK', 'm': 'SIX', 'i': 'B-IDX-FTSE'},
    {'c': 'US', 'm': 'SIX', 'i': 'SIX-IDX-NCMP'},
    {'c': 'US', 'm': 'USX', 'i': 'DJX-FRDOW'},
    {'c': 'US', 'm': 'USX', 'i': 'DJX-HKDOWD'},
    {'c': 'US', 'm': 'USX', 'i': 'DJX-JPDOWD'},
    {'c': 'US', 'm': 'USX', 'i': 'DJX-DJSH'},
    {'c': 'US', 'm': 'SIX', 'i': 'RTS'},
    {'c': 'US', 'm': 'USX', 'i': 'DJX-B50IND'},
    {'c': 'US', 'm': 'USX', 'i': 'DJX-ZADOWD'},
    {'c': 'US', 'm': 'CME', 'i': 'ENQ100-1'}
]


def before(f):
    @wraps(f)
    def wrapper(self, *args, **kw):
        if hasattr(self, '_before') and inspect.ismethod(self._before):
            self._before()
        result = f(self, *args, **kw)
        return result

    return wrapper


class Nordnet:

    def __init__(self):
        self.BASE_HOST = 'www.nordnet.no'
        self.BASE_URL = 'https://{}'.format(self.BASE_HOST)
        self.COOKIE_FILE = 'nordnet_cookies.json'
        self.COOKIE_MAX_TIME = 600  # seconds

        self.HEADERS = {}
        self.HEADERS['client-id'] = 'NEXT'
        self.HEADERS['Accept'] = 'application/json'
        self.HEADERS['Host'] = self.BASE_HOST
        self.HEADERS['Origin'] = self.BASE_URL
        self.HEADERS['Referer'] = self.BASE_URL

        self.COOKIES = None
        self.NTAG = None

        self._mk_session()

    def _before(self):
        # if self._cookie_age() > self.COOKIE_MAX_TIME:
        self._mk_session(force=False)

    def _save_cookies(self):
        with open(self.COOKIE_FILE, 'w+') as outfile:
            json.dump({'cookies': self.COOKIES, 'ntag': self.NTAG}, outfile)

    def _open_cookies(self):

        with open(self.COOKIE_FILE, 'r') as infile:
            data = json.load(infile)
            self.COOKIES = data.get('cookies', {})
            self.NTAG = data.get('ntag', {})

    def _cookie_age(self):
        try:
            return time.time() - os.path.getmtime(self.COOKIE_FILE)
        except:
            return self.COOKIE_MAX_TIME + 10

    def _mk_session(self, force=True):
        try:
            if self._cookie_age() > self.COOKIE_MAX_TIME:

                r = requests.get('{}/market'.format(self.BASE_URL), headers=self.HEADERS)

                if r.status_code == 200:
                    self.COOKIES = requests.utils.dict_from_cookiejar(r.cookies)

                    html = r.text.split('<script>window.__initialState__=')[1].split(';</script>')[0]
                    params = json.loads(json.loads(html))

                    self.NTAG = params['meta']['ntag']
                    self._save_cookies()
            else:
                if force is True:
                    self._open_cookies()

            self.HEADERS['ntag'] = self.NTAG


        except Exception as e:
            raise Exception('Could not init session', e)

    def _login(self):
        resp = requests.get('{}/api/2/login'.format(self.BASE_URL), cookies=self.COOKIES, headers=self.HEADERS)

        if resp.status_code == 200:
            return True, resp.json()

        return False, None

    def _login_post(self):
        resp = requests.post('{}/api/2/login'.format(self.BASE_URL), json={'auth': ':', 'service': 'NEXTAPI'},
                             cookies=self.COOKIES, headers=self.HEADERS)

        if resp.status_code == 200:
            return True, resp.json()

        return False, None

    def _get_json(self, url, default_ret=[]):
        resp = requests.get(url=url,
                            cookies=self.COOKIES,
                            headers=self.HEADERS)
        return resp
        if resp.status_code == 200:
            return resp

        return default_ret

    @before
    def query(self, q) -> (bool, list):
        resp = requests.get('{}/api/2/instruments?query={}'.format(self.BASE_URL, q), cookies=self.COOKIES,
                            headers=self.HEADERS)
        if resp.status_code == 200:
            return True, resp.json()

        return False, []

    @before
    def search(self, q):
        status, query = self.query(q)
        instrument_id = None
        market_id = None
        tradables = []
        for i in query:
            if i['instrument_type'] == 'ESH' and i['instrument_group_type'] == 'EQ':
                # print('###', i['instrument_id'], i['name'], i['instrument_type'], i['instrument_group_type'])
                # print(i)
                tradables.append(i['tradables'])

                if q.upper() == i['symbol'] or q.upper() == i['isin_code']:
                    stock = i['symbol']
                    tradable_id = i['tradables'][0]['identifier']
                    market_id = i['tradables'][0]['market_id']
                    instrument_id = i['instrument_id']
                    break

        return status, instrument_id, market_id, tradables

    """
    for r in results:
        if r['mifid2_category'] == 0 and r['tradables'][0]['market_id'] in [15, 19]:
            # tradables[]market_id in [15, ]
            # instrument_type == 'ESH'
            print(r)
            iid = r['instrument_id']
    """

    @before
    def get_trades(self, instrument_id, max_trades=10000000000000) -> (bool, list):
        resp = requests.get('{}/api/2/instruments/{}/trades?count={}'.format(self.BASE_URL, instrument_id, max_trades),
                            cookies=self.COOKIES,
                            headers=self.HEADERS)

        if resp.status_code == 200:
            return True, resp.json()

        return False, []

    def get_trades_pd(self, instrument_id, vwap=True, turnover=False):
        status, trades = self.get_trades(instrument_id)

        if status is True:

            df = pd.DataFrame(trades[0]['trades'])
            df['tick_timestamp'] = pd.to_datetime(df.tick_timestamp, unit='ms')
            df['tick_timestamp'] = df['tick_timestamp'].dt.tz_localize('UTC').dt.tz_convert('Europe/Oslo')
            df['trade_timestamp'] = pd.to_datetime(df.trade_timestamp, unit='ms')
            df['trade_timestamp'] = df['trade_timestamp'].dt.tz_localize('UTC').dt.tz_convert('Europe/Oslo')
            df = df.reset_index(drop=False)
            df.set_index('tick_timestamp', inplace=True)

            df.sort_index(inplace=True, ascending=True)
            # df = df.iloc[::-1]
            if vwap is True:
                df['vwap'] = (df.volume * df.price).cumsum() / df.volume.cumsum()
            if turnover is True:
                df['turnover'] = (df.volume * df.price).cumsum()
            return df
        else:
            return pd.DataFrame()

    @before
    def get_prices(self, instrument_id) -> (bool, list):
        """"Returns bests and volumes, volume, vwap etc"""
        resp = requests.get('{}/api/2/instruments/price/{}'.format(self.BASE_URL, instrument_id),
                            cookies=self.COOKIES,
                            headers=self.HEADERS)

        if resp.status_code == 200:
            return True, resp.json()

        return False, []

    def get_prices_pd(self, instrument_id):
        status, prices = self.get_prices(instrument_id)

        if status is True:
            df = pd.DataFrame(prices)
            df['tick_timestamp'] = pd.to_datetime(df.tick_timestamp, unit='ms')
            df['tick_timestamp'] = df['tick_timestamp'].dt.tz_localize('UTC').dt.tz_convert('Europe/Oslo')
            df['trade_timestamp'] = pd.to_datetime(df.trade_timestamp, unit='ms')
            df['trade_timestamp'] = df['trade_timestamp'].dt.tz_localize('UTC').dt.tz_convert('Europe/Oslo')

            df = df.reset_index(drop=False)
            df.set_index('tick_timestamp', inplace=True)
            df = df.iloc[::-1]

            return df

        return pd.DataFrame()

    @before
    def get_trading_status(self, instrument_id) -> list:
        resp = requests.get('{}/api/2/instruments/trading_status/{}'.format(self.BASE_URL, instrument_id),
                            cookies=self.COOKIES,
                            headers=self.HEADERS)

        if resp.status_code == 200:
            return True, resp.json()

        return False, []

    @before
    def get_history(self, instrument_id, weeks=52):
        resp = requests.get(
            '{}/api/2/instruments/historical/prices/{}?fields=open,high,low,last,volume,turnover&weeks={}'.format(
                self.BASE_URL, instrument_id, weeks),
            cookies=self.COOKIES,
            headers=self.HEADERS)

        if resp.status_code == 200:
            return True, resp.json()

        return False, []

    def get_history_pd(self, instrument_id, weeks=52):
        status, history = self.get_history(instrument_id, weeks)

        if status is True:
            df = pd.DataFrame(history[0]['prices'])
            df.rename(columns={'last': 'close'}, inplace=True)
            df['time'] = pd.to_datetime(df.time, unit='ms')
            df['time'] = df['time'].dt.tz_localize('UTC').dt.tz_convert('Europe/Oslo')
            df = df.reset_index(drop=False)
            df.set_index(pd.DatetimeIndex(df['time']), inplace=True)
            # df = df.iloc[::-1]

            return df

        return pd.DataFrame()

    @before
    def get_historical_returns(self, instrument_id, local_currency=True, show_details=True):
        resp = requests.get(
            '{}/api/2/instruments/historical/returns/{}?local_currency=true&show_details=true'.format(self.BASE_URL,
                                                                                                      instrument_id),
            cookies=self.COOKIES,
            headers=self.HEADERS)

        if resp.status_code == 200:
            return True, resp.json()

        return False, []

    @before
    def get_instrument_stats(self, instrument_id):
        resp = requests.get('{}/api/2/instruments/statistics/{}'.format(self.BASE_URL, instrument_id),
                            cookies=self.COOKIES,
                            headers=self.HEADERS)

        if resp.status_code == 200:
            return True, resp.json()

        return False, []

    @before
    def get_instrument_financial(self, instrument_id):
        resp = requests.get('{}/api/2/company_information/financial/{}'.format(self.BASE_URL, instrument_id),
                            cookies=self.COOKIES,
                            headers=self.HEADERS)

        if resp.status_code == 200:
            return True, resp.json()

        return False, []

    @before
    def get_instrument_info(self, instrument_id):
        resp = requests.get('{0}/api/2/instruments/{1}'.format(self.BASE_URL, instrument_id),
                            cookies=self.COOKIES,
                            headers=self.HEADERS)

        if resp.status_code == 200:
            return True, resp.json()

        return False, []

    @before
    def get_instrument_tick_size(self, instrument_id):
        """
        Get tick size by price 12.50:
        [x['tick'] for x in t[0]['ticks'] if x['from_price']<12.5 and x['to_price']>12.50]
        :param instrument_id:
        :return:
        """

        status, info = self.get_instrument_info(instrument_id)

        if status is True:
            resp = requests.get('{}/api/2/tick_sizes/{}'.format(self.BASE_URL, info[0]['tradables'][0]['tick_size_id']),
                                cookies=self.COOKIES,
                                headers=self.HEADERS)

            if resp.status_code == 200:
                return True, resp.json()

        return False, []

    @before
    def get_instrument_leverages(self, instrument_id):
        resp = requests.get('{}/api/2/instruments/{}/leverages'.format(self.BASE_URL, instrument_id),
                            cookies=self.COOKIES,
                            headers=self.HEADERS)

        if resp.status_code == 200:
            return True, resp.json()

        return False, []

    @before
    def get_news_sources(self):
        resp = requests.get('{}/api/2/news_sources'.format(self.BASE_URL),
                            cookies=self.COOKIES,
                            headers=self.HEADERS)

        if resp.status_code == 200:
            return True, resp.json()

        return False, []

    @before
    def get_instrument_news(self, instrument_id, days=1):
        resp = requests.get('{}/api/2/news?instrument_id={}&days={}'.format(self.BASE_URL, instrument_id, days),
                            cookies=self.COOKIES,
                            headers=self.HEADERS)

        if resp.status_code == 200:
            return True, resp.json()

        return False, []

    def get_instrument_news_pd(self, instrument_id, days=1):
        status, news = self.get_instrument_info(instrument_id, days)

        if status is True:
            df = pd.DataFrame(news)
            df['timestamp'] = pd.to_datetime(df.timestamp, unit='ms')
            df['timestamp'] = df['timestamp'].dt.tz_localize('UTC').dt.tz_convert('Europe/Oslo')

            df = df.reset_index(drop=False)
            df.set_index('timestamp', inplace=True)
            df = df.iloc[::-1]

            return df

        return pd.DataFrame()

    @before
    def get_realtime_access(self):
        resp = requests.get('{}/api/2/realtime_access'.format(self.BASE_URL),
                            cookies=self.COOKIES,
                            headers=self.HEADERS)

        return resp.status_code

    @before
    def get_news(self, news_id):
        resp = requests.get('{}/api/2/news/{}'.format(self.BASE_URL, news_id),
                            cookies=self.COOKIES,
                            headers=self.HEADERS)

        if resp.status_code == 200:
            return True, resp.json()

        return False, []

    @before
    def get_all_news(self, max_number=25, page=0):
        # 'https://www.nordnet.no/api/2/news?source_id=9&news_lang=no,en&limit=20&offset=0'
        # source_id=9
        resp = requests.get(
            '{0}/api/2/news/?news_lang=no,en&limit={1}&offset={2}'.format(self.BASE_URL, max_number, page),
            cookies=self.COOKIES,
            headers=self.HEADERS)

        if resp.status_code == 200:
            return True, resp.json()

        return False, []

    @before
    def get_instrument_types(self):
        resp = self._get_json('{}/api/2/instruments/types'.format(self.BASE_URL))

        if resp.status_code == 200:
            return True, resp.json()

        return False, []

    def get_instrument_types_pd(self):
        status, types = self.get_instrument_types()

        if status is True:
            df = pd.DataFrame(types)

            df = df.reset_index(drop=False)
            df.set_index('instrument_type', inplace=True)
            df = df.iloc[::-1]

            return df

        return pd.DataFrame()

    @before
    def get_instrument_type(self, instrument_type):
        resp = requests.get('{}/api/2/instruments/types/{}'.format(self.BASE_URL, instrument_type),
                            cookies=self.COOKIES,
                            headers=self.HEADERS)
        if resp.status_code == 200:
            return True, resp.json()

        return False, []

    @before
    def get_sectors(self):
        resp = requests.get('{}/api/2/instruments/sectors'.format(self.BASE_URL),
                            cookies=self.COOKIES,
                            headers=self.HEADERS)
        if resp.status_code == 200:
            return True, resp.json()

        return False, []

    @before
    def get_sector(self, sector):
        resp = requests.get('{}/api/2/instruments/sectors/{}'.format(self.BASE_URL, sector),
                            cookies=self.COOKIES,
                            headers=self.HEADERS)
        if resp.status_code == 200:
            return True, resp.json()

        return False, []

    @before
    def get_lists(self):
        resp = requests.get('{}/api/2/lists'.format(self.BASE_URL),
                            cookies=self.COOKIES,
                            headers=self.HEADERS)
        if resp.status_code == 200:
            return True, resp.json()

        return False, []

    @before
    def get_list(self, list_id):
        resp = requests.get('{}/api/2/lists/{}'.format(self.BASE_URL, list_id),
                            cookies=self.COOKIES,
                            headers=self.HEADERS)
        if resp.status_code == 200:
            return True, resp.json()

        return False, []

    @before
    def get_tradables(self, identifier, market_id=15):
        resp = requests.get('{}/api/2/tradables/info/{}:{}'.format(self.BASE_URL, market_id, identifier),
                            cookies=self.COOKIES,
                            headers=self.HEADERS)
        if resp.status_code == 200:
            return True, resp.json()

        return False, []

    @before
    def get_tradables_intraday(self, identifier, market_id):
        resp = requests.get('{}/api/2/tradables/intraday/{}:{}'.format(self.BASE_URL, market_id, identifier),
                            cookies=self.COOKIES,
                            headers=self.HEADERS)
        if resp.status_code == 200:
            return True, resp.json()

        return False, []

    @before
    def get_tradables_trades(self, identifier, market_id=15, max_trades=10000000, weeks=5):
        resp = requests.get(
            '{}/api/2/tradables/trades/{}:{}?count={}&weeks={}'.format(self.BASE_URL, market_id, identifier,
                                                                       max_trades, weeks),
            cookies=self.COOKIES,
            headers=self.HEADERS)
        if resp.status_code == 200:
            return True, resp.json()

        return False, []

    def get_tradables_trades_pd(self, identifier, market_id, max_trades=10000000, weeks=5):
        status, trades = self.get_tradables_trades(market_id, identifier, max_trades=10000000, weeks=5)
        if status is True:
            df = pd.DataFrame(trades[0]['trades'])
            df['datetime'] = pd.to_datetime(df.trade_timestamp, unit='ms')
            df['datetime'] = df['datetime'].dt.tz_localize('UTC').dt.tz_convert('Europe/Oslo')
            df.set_index('datetime', inplace=True)

            return df

        return pd.DataFrame()

    @before
    def get_indicators(self):
        resp = requests.get('{}/api/2/indicators'.format(self.BASE_URL),
                            cookies=self.COOKIES,
                            headers=self.HEADERS)
        if resp.status_code == 200:
            return True, resp.json()

        return False, []

    def get_indicators_pd(self):
        status, indicators = self.get_indicators()
        if status is True:
            df = pd.DataFrame(indicators)
            df['open'] = pd.to_timedelta(df.open, unit='s')
            df['close'] = pd.to_timedelta(df.open, unit='s')
            df.set_index('identifier', inplace=True)

            return df

        return pd.DataFrame()

    @before
    def get_indicator_historical(self, indicator_id='OSE:OSEBX', weeks=10):
        resp = requests.get(
            '{}/api/2/indicators/historical/values/{}?weeks={}'.format(self.BASE_URL, indicator_id, weeks),
            cookies=self.COOKIES,
            headers=self.HEADERS)
        if resp.status_code == 200:
            return True, resp.json()

        return False, []

    @before
    def get_indicator_historical_pd(self, indicator_id='OSE:OSEBX', weeks=52):

        status, result = self.get_indicator_historical(indicator_id, weeks)
        if status is True:
            df = pd.DataFrame(result[0].get('prices', []))
            if df.shape[0] > 0:
                df.time = pd.to_datetime(df.time, unit='ms')
                df.set_index('time', inplace=True)
                return df

        return pd.DataFrame()

    @before
    def get_option_pairs(self, instrument_id):
        resp = requests.get(
            '{}/api/2/instruments/{}/option_pairs'.format(self.BASE_URL, instrument_id),
            cookies=self.COOKIES,
            headers=self.HEADERS)
        if resp.status_code == 200:
            return True, resp.json()

        return False, []

    @before
    def get_turnover_list(self, exchange_list='no:ose', max_instruments=10):
        """
        # Highest turnover
        # Instrument search - paging with limit=100, offset=0
        # sort_attribute turnover_volume turnover_normalized turnover name diff_pct yield_1w yield_1m yield_3m yield_ytd yield_1y yield_3y yield_5Y
        # pe ps pb eps dividend_yield dividend_per_share
        # no:ose no:merk no:osloaxess etc
        # exchange_list=no:ose|exchange_list=no:merk|exchange_list=no:osloaxess
        # apply_filters=exchange_country=DK NO FI SE DE US CA
        # entity_type MINIFUTURELIST
        :param exchange_list:
        :param max_instruments:
        :return:
        """
        resp = requests.get(
            '{0}/api/2/instrument_search/query/stocklist?limit={1}&sort_attribute=turnover_normalized&sort_order=desc&apply_filters=exchange_list={2}|diff_pct=[* TO 900]'.format(
                self.BASE_URL, max_instruments, exchange_list),
            cookies=self.COOKIES,
            headers=self.HEADERS)
        if resp.status_code == 200:
            return True, resp.json()

        return False, []

    @before
    def get_winner_list(self, exchange_list='no:ose', max_instruments=10):
        resp = requests.get(
            '{0}/api/2/instrument_search/query/stocklist?limit={1}&sort_attribute=diff_pct&sort_order=desc&apply_filters=exchange_list={2}|diff_pct=[* TO 900]'.format(
                self.BASE_URL, max_instruments, exchange_list),
            cookies=self.COOKIES,
            headers=self.HEADERS)
        if resp.status_code == 200:
            return True, resp.json()

        return False, []

    @before
    def get_looser_list(self, exchange_list='no:ose', max_instruments=10):
        resp = requests.get(
            '{0}/api/2/instrument_search/query/stocklist?limit={1}&sort_attribute=diff_pct&sort_order=asc&apply_filters=exchange_list={2}|diff_pct=[* TO 900]'.format(
                self.BASE_URL, max_instruments, exchange_list),
            cookies=self.COOKIES,
            headers=self.HEADERS)
        if resp.status_code == 200:
            return True, resp.json()

        return False, []

    @before
    def get_ranking_list(self, max_instruments=10, sort_order='desc', exchange_list='no:ose'):
        resp = requests.get(
            '{0}/api/2/instrument_search/query/stocklist?limit={1}&sort_attribute=diff_pct&sort_order={2}&apply_filters=exchange_list={3}|diff_pct=[* TO 900]'.format(
                self.BASE_URL, max_instruments, sort_order, exchange_list),
            cookies=self.COOKIES,
            headers=self.HEADERS)
        if resp.status_code == 200:
            return True, resp.json()

    @before
    def print_ranking_list(self, max_instruments=10, sort_order='desc', order=1):
        status, results = self.get_ranking_list(max_instruments=max_instruments, sort_order=sort_order)

        if status is True:
            for i in results.get('results', [])[::order]:
                print('{0: <20} {1:.2f}%'.format(  # i['instrument_info']['name'],
                    i['instrument_info']['symbol'],
                    i['price_info']['diff_pct']))

    @before
    def get_indicator_historical_sparks(self):
        resp = requests.get(
            '{}/api/2/indicators/historical/sparkpoints/'
            ''.format(
                self.BASE_URL),
            cookies=self.COOKIES,
            headers=self.HEADERS)
        if resp.status_code == 200:
            return True, resp.json()

        return False, []

    @before
    def get_indicator(self, indicator_id='OSEBX', src='OSE', from_date=datetime.datetime.now().strftime('%Y-%m-%d')):
       # print('{}/api/2/indicators/{}:{}?from={}'.format(self.BASE_URL, src, indicator_id, from_date))
        resp = requests.get('{}/api/2/indicators/{}:{}?from={}'.format(self.BASE_URL, src, indicator_id, from_date),
                            cookies=self.COOKIES,
                            headers=self.HEADERS)
        if resp.status_code == 200:
            return True, resp.json()

        return False, []

    @before
    def get_indicator_history(self, indicator_id='OSEBX', src='OSE',
                              from_date=datetime.datetime.now().strftime('%Y-%m-%d')):
        #print('{}/api/2/indicators/historical/values/{}:{}?from={}'.format(self.BASE_URL, src, indicator_id, from_date))
        resp = requests.get(
            '{}/api/2/indicators/historical/values/{}:{}?from={}'.format(self.BASE_URL, src, indicator_id, from_date),
            cookies=self.COOKIES,
            headers=self.HEADERS)
        if resp.status_code == 200:
            return True, resp.json()

        return False, []

    @before
    def get_commodity_index(self, entity_type='CURRENCY'):
        # Markets  INTEREST COMMODITY CURRENCY INDEX STOCKLIST
        resp = requests.get(
            '{0}/api/2/instrument_search/query/indicator?entity_type={1}&apply_filters=market_overview_group=NO_GLOBAL_MO'.format(
                self.BASE_URL, entity_type),
            cookies=self.COOKIES,
            headers=self.HEADERS)
        if resp.status_code == 200:
            return True, resp.json()

        return False, []

    @before
    def get_instrument_attributes(self, entity_type):
        # Markets  INTEREST COMMODITY CURRENCY INDEX STOCKLIST
        resp = requests.get(
            '{0}/api/2/instrument_search/attributes?entity_type={1}&expand=exchange_list&apply_filters=exchange_country=NO'.format(
                self.BASE_URL, entity_type),
            cookies=self.COOKIES,
            headers=self.HEADERS)
        if resp.status_code == 200:
            return True, resp.json()

        return False, []

    @before
    def get_turnover_norm_list(self, sort_order='desc', max_instruments=10):
        resp = requests.get(
            '{0}/api/2/instrument_search/query/stocklist?limit={1}&sort_attribute=turnover_normalized&sort_order={2}&apply_filters=exchange_country=NO|exchange_list=no:ose|diff_pct=[* TO 900]'.format(
                self.BASE_URL, max_instruments, sort_order),
            cookies=self.COOKIES,
            headers=self.HEADERS)
        if resp.status_code == 200:
            return True, resp.json()

        return False, []

    @before
    def get_shareville(self, instrument_id, max_comments=50):
        resp = requests.get(
            '{}/api/2/shareville/comments/instrument/{}?limit={}'.format(self.BASE_URL, instrument_id, max_comments),
            cookies=self.COOKIES,
            headers=self.HEADERS)

        if resp.status_code == 200:
            return True, resp.json()

        return False, []

    def get_shareville_pd(self, instrument_id, max_comments=50):
        status, comments = self.get_shareville(instrument_id, max_comments)

        if status is True:
            df = pd.DataFrame(comments)
            df['created'] = pd.to_datetime(df.created, unit='ms')
            df['created'] = df['created'].dt.tz_localize('UTC').dt.tz_convert('Europe/Oslo')

            df = df.reset_index(drop=False)
            df.set_index('created', inplace=True)
            df = df.iloc[::-1]

            return df

        return pd.DataFrame()

    @before
    def get_shareville_community(self, instrument_id):
        resp = requests.get('https://www.shareville.no/api/v1/marketflow/instruments/{}/rap'.format(instrument_id))
        if resp.status_code == 200:
            return True, resp.json()

        return False, {}

    def get_shareville_community_owners(self, instrument_id):

        status, r = self.get_shareville_community(instrument_id)

        return r.get('count', 0)

    @before
    def get_indexes(self) -> (bool, list):
        resp = requests.get(
            '{}/api/2/indicators/OSE:OSEBX,OSE:OBX,SSE:OMXSPI,CSE:OMXC25,HEX:OMXHPI,SIX:B-IDX-DAXI,SIX:SIX-IDX-N100,SIX:SIX-IDX-NCMP'.format(
                self.BASE_URL),
            cookies=self.COOKIES,
            headers=self.HEADERS)

        if resp.status_code == 200:
            return True, resp.json()

        return False, []

    def get_indexes_pd(self):
        status, indexes = self.get_indexes()

        if status is True:
            df = pd.DataFrame(indexes)

            df['open'] = pd.to_timedelta(df.open, unit='s')
            df['close'] = pd.to_timedelta(df.open, unit='s')

            df = df.reset_index(drop=False)
            df.set_index('identifier', inplace=True)
            df = df.iloc[::-1]

            return df

        return pd.DataFrame()

    def _get_stock_list(self, page, limit, filters):
        resp = requests.get('{}/api/2/instrument_search/query/stocklist?apply_filters={}&limit={}&offset={}'.format(self.BASE_URL, filters, limit, page),
                            cookies=self.COOKIES,
                            headers=self.HEADERS)
        if resp.status_code == 200:
            result = resp.json()
            return True, result.get('rows', 0), result.get('total_hits', 0), result.get('results')

        return False, 0, 0, []

    def _fix_price_info(self, p):

        p['ask'] = p.get('ask', {}).get('price', None)
        p['bid'] = p.get('bid', {}).get('price', None)

        p['open'] = p.get('open', {}).get('price', None)
        p['high'] = p.get('high', {}).get('price', None)
        p['low'] = p.get('low', {}).get('price', None)
        p['close'] = p.get('close', {}).get('price', None)
        p['last'] = p.get('last', {}).get('price', None)

        p['diff'] = p.get('diff', {}).get('diff', None)
        p['spread'] = p.get('spread', {}).get('price', None)

        return p

    @before
    def get_all_instruments(self, countries=['no', 'se']):
        limit = 100
        offset = 0
        free_text_search = '|'.join(['exchange_list={}:{}'.format(x['c'], x['m']) for x in EXCHANGES if x['c'] in countries])
        more_to_go = True
        results = []
        while more_to_go is True:

            status, rows, total, r = self._get_stock_list(page=offset, limit=limit, filters=free_text_search)
            if status is True:
                results += [{**x['instrument_info'], **x['status_info'],**x['market_info'],**self._fix_price_info(x['price_info']),**x['exchange_info'],**x['key_ratios_info'],**x['historical_returns_info']} for x in r]

            offset += limit

            if len(results) >= total-1 or limit>rows:
                more_to_go = False

        return True, results

    def get_all_instruments_pd(self, countries=['no', 'se']):
        status, instruments = self.get_all_instruments(countries=countries)

        if status is True:
            df = pd.DataFrame(instruments)
            df['tick_timestamp'] = pd.to_timedelta(df.tick_timestamp, unit='ms')
            return df

        return pd.DataFrame()

    def _get_indicator_list(self, page, limit):
        resp = requests.get(
            '{}/api/2/instrument_search/query/indicator?entity_type=INDEX&limit={}&offset={}'.format(self.BASE_URL,
                                                                                                    limit,
                                                                                                    page),
            cookies=self.COOKIES,
            headers=self.HEADERS)
        if resp.status_code == 200:
            result = resp.json()
            return True, result.get('rows', 0), result.get('total_hits', 0), result.get('results')

    @before
    def get_all_indicators(self, countries=['no', 'se']):

        limit = 100
        offset = 0
        more_to_go = True
        results = []
        while more_to_go is True:

            status, rows, total, r = self._get_indicator_list(page=offset, limit=limit)
            if status is True:
                results += [{**x['instrument_info'], **x['exchange_info'],
                             **self._fix_price_info(x['price_info']),
                             **x['historical_returns_info']} for x in r]

            offset += limit

            if len(results) >= total - 1 or limit > rows:
                more_to_go = False

        return True, results

    def get_all_indicators_pd(self, countries=['no', 'se']):
        status, indexes = self.get_all_indicators(countries=countries)

        if status is True:
            df = pd.DataFrame(indexes)
            df['tick_timestamp'] = pd.to_timedelta(df.tick_timestamp, unit='ms')
            return df

        return pd.DataFrame()