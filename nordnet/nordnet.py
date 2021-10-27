"""
    Nordnet
    =======

    A simple wrapper around nordnet.no's (not) public api

    Increase columns if using dataframes and cli
import pandas as pd
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

"""

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

from nordnet.settings import LOCAL_TZ, EXCHANGES, INDEXES, BASE_HOST, BASE_URL, COOKIE_FILE, COOKIE_MAX_TIME


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

        self.HEADERS = {}
        self.HEADERS['client-id'] = 'NEXT'
        self.HEADERS['Accept'] = 'application/json'
        self.HEADERS['Host'] = BASE_HOST
        self.HEADERS['Origin'] = BASE_URL
        self.HEADERS['Referer'] = BASE_URL

        self.COOKIES = None
        self.NTAG = None

        self._mk_session()

    def _before(self):
        # if self._cookie_age() > COOKIE_MAX_TIME:
        self._mk_session(force=False)

    def _save_cookies(self):
        with open(COOKIE_FILE, 'w+') as outfile:
            json.dump({'cookies': self.COOKIES, 'ntag': self.NTAG}, outfile)

    def _open_cookies(self):

        with open(COOKIE_FILE, 'r') as infile:
            data = json.load(infile)
            self.COOKIES = data.get('cookies', {})
            self.NTAG = data.get('ntag', {})

    def _cookie_age(self) -> int:
        try:
            print(COOKIE_FILE)
            return time.time() - os.path.getmtime(COOKIE_FILE)
        except:
            return COOKIE_MAX_TIME + 10

    def _mk_session(self, force=True):
        try:
            if self._cookie_age() > COOKIE_MAX_TIME:

                r = requests.get('{}/market'.format(BASE_URL), headers=self.HEADERS)

                if r.status_code == 200:
                    self.COOKIES = requests.utils.dict_from_cookiejar(r.cookies)

                    #html = r.text.split('<script>window.__initialState__=')[1].split(';</script>')[0]
                    html = r.text.split('>window.__initialState__=')[1].split(';</script>')[0]

                    #params = json.loads(json.loads(html))
                    start = "ntag\\\":\\\""
                    terminate = "\\\",\\\""
                    ntag_idx = html.find( start )
                    ntag_end = html.find(terminate)
                    print( ntag_idx )
                    print( ntag_idx + ntag_end )
                    print( html[ntag_idx+9: ntag_idx+36+9] )

                    self.NTAG = html[ntag_idx+9:ntag_idx+36+9]
                    #self.NTAG = params['meta']['ntag']
                    self._save_cookies()
            else:
                if force is True:
                    self._open_cookies()

            self.HEADERS['ntag'] = self.NTAG


        except Exception as e:
            raise Exception('Could not init session', e)

    def _login(self) -> (bool, dict):
        resp = requests.get('{}/api/2/login'.format(BASE_URL), cookies=self.COOKIES, headers=self.HEADERS)

        if resp.status_code == 200:
            return True, resp.json()

        return False, {}

    def _login_post(self) -> (bool, dict):
        resp = requests.post('{}/api/2/login'.format(BASE_URL), json={'auth': ':', 'service': 'NEXTAPI'},
                             cookies=self.COOKIES, headers=self.HEADERS)

        if resp.status_code == 200:
            return True, resp.json()

        return False, {}

    def _get_json(self, url, default_ret=[]) -> (bool, list):
        resp = requests.get(url=url,
                            cookies=self.COOKIES,
                            headers=self.HEADERS)
        return resp
        if resp.status_code == 200:
            return resp

        return default_ret

    def _GET(self, relative_url, raw_response=False) -> (bool, list):
        resp = requests.get('{}/api/2/{}'.format(BASE_URL, relative_url),
                            cookies=self.COOKIES,
                            headers=self.HEADERS)
        if resp.status_code == 200:
            if raw_response is True:
                return True, resp
            else:
                return True, resp.json()

        return False, []

    @before
    def main_query(self, q) -> (bool, list):
        return self._GET('main_search?query={}'.format(q))

    @before
    def main_search(self, q) -> (bool, int, int, list):

        status, result = self.main_query(q)

        instrument_id = None
        symbol = None
        instrument_id = None
        exchange_country = None
        results = []
        for r in result:
            if r['display_group_type'] == 'EQUITY':
                for i in r.get('results', []):
                    if i['instrument_type'] == 'ESH' and i['instrument_group_type'] == 'EQ':
                        results.append(
                            {
                                'symbol': i['display_symbol'],
                                'instrument_id': i['instrument_id'],
                                'exchange_country': i['exchange_country']
                            }
                        )

        return status, results  # instrument_id, symbol, exchange_country

    @before
    def query(self, q) -> (bool, list):
        return self._GET('instruments?query={}'.format(q))

    @before
    def search(self, q) -> (bool, int, int, list):

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
        return self._GET('instruments/{}/trades?count={}'.format(instrument_id, max_trades))

    def get_trades_pd(self, instrument_id, vwap=True, turnover=False) -> pd.DataFrame:
        status, trades = self.get_trades(instrument_id)

        if status is True:
            try:
                df = pd.DataFrame(trades[0]['trades'])
                df['tick_timestamp'] = pd.to_datetime(df.tick_timestamp, unit='ms')
                df['tick_timestamp'] = df['tick_timestamp'].dt.tz_localize('UTC').dt.tz_convert(LOCAL_TZ)
                df['trade_timestamp'] = pd.to_datetime(df.trade_timestamp, unit='ms')
                df['trade_timestamp'] = df['trade_timestamp'].dt.tz_localize('UTC').dt.tz_convert(LOCAL_TZ)
                df = df.reset_index(drop=False)
                df.set_index('tick_timestamp', inplace=True)

                df.sort_index(inplace=True, ascending=True)
                # df = df.iloc[::-1]
                if vwap is True:
                    df['vwap'] = (df.volume * df.price).cumsum() / df.volume.cumsum()
                if turnover is True:
                    df['turnover'] = (df.volume * df.price).cumsum()
                return df
            except:
                pass

        return pd.DataFrame()

    @before
    def get_prices(self, instrument_id) -> (bool, list):
        """"Returns bests and volumes, volume, vwap etc"""
        return self._GET('instruments/price/{}'.format(instrument_id))

    def get_prices_pd(self, instrument_id) -> pd.DataFrame:
        status, prices = self.get_prices(instrument_id)

        if status is True:
            df = pd.DataFrame(prices)
            df['tick_timestamp'] = pd.to_datetime(df.tick_timestamp, unit='ms')
            df['tick_timestamp'] = df['tick_timestamp'].dt.tz_localize('UTC').dt.tz_convert(LOCAL_TZ)
            df['trade_timestamp'] = pd.to_datetime(df.trade_timestamp, unit='ms')
            df['trade_timestamp'] = df['trade_timestamp'].dt.tz_localize('UTC').dt.tz_convert(LOCAL_TZ)

            df = df.reset_index(drop=False)
            df.set_index('tick_timestamp', inplace=True)
            df = df.iloc[::-1]

            return df

        return pd.DataFrame()

    @before
    def get_trading_status(self, instrument_id) -> (bool, list):
        return self._GET('instruments/trading_status/{}'.format(instrument_id))

    @before
    def get_history(self, instrument_id, weeks=52):
        return self._GET(
            'instruments/historical/prices/{}?fields=open,high,low,last,volume,turnover&weeks={}'.format(instrument_id,
                                                                                                         weeks))

    def get_history_pd(self, instrument_id, weeks=52) -> pd.DataFrame:
        status, history = self.get_history(instrument_id, weeks)

        if status is True:
            df = pd.DataFrame(history[0]['prices'])
            df.rename(columns={'last': 'close'}, inplace=True)
            df['time'] = pd.to_datetime(df.time, unit='ms')
            df['time'] = df['time'].dt.tz_localize('UTC').dt.tz_convert(LOCAL_TZ)
            df = df.reset_index(drop=False)
            df.set_index(pd.DatetimeIndex(df['time']), inplace=True)

            # Delete last if equal
            if df.tail(1).index.date == df.tail(2).head(1).index.date:
                df.drop(df.tail(1).index, inplace=True)
            # df = df.iloc[::-1]

            return df

        return pd.DataFrame()

    @before
    def get_historical_returns(self, instrument_id, local_currency=True, show_details=True) -> (bool, list):
        return self._GET(
            'instruments/historical/returns/{}?local_currency=true&show_details=true'.format(instrument_id))

    @before
    def get_instrument_stats(self, instrument_id) -> (bool, list):
        return self._GET('instruments/statistics/{}'.format(instrument_id))

    @before
    def get_instrument_financial(self, instrument_id) -> (bool, list):
        return self._GET('company_information/financial/{}'.format(instrument_id))

    @before
    def get_instrument_info(self, instrument_id) -> (bool, list):
        return self._GET('instruments/{}'.format(instrument_id))

    @before
    def get_instrument_tick_size(self, instrument_id) -> (bool, list):
        """
        Get tick size by price 12.50:
        [x['tick'] for x in t[0]['ticks'] if x['from_price']<12.5 and x['to_price']>12.50]
        :param instrument_id:
        :return:
        """

        status, info = self.get_instrument_info(instrument_id)

        if status is True:
            return self._GET('tick_sizes/{}'.format(info[0]['tradables'][0]['tick_size_id']))

        return False, []

    @before
    def get_instrument_leverages(self, instrument_id) -> (bool, list):
        return self._GET('instruments/{}/leverages'.format(instrument_id))

    @before
    def get_news_sources(self) -> (bool, list):
        return self._GET('news_sources')

    @before
    def get_instrument_news(self, instrument_id, days=1) -> (bool, list):
        return self._GET('news?instrument_id={}&days={}'.format(instrument_id, days))

    def get_instrument_news_pd(self, instrument_id, days=1) -> pd.DataFrame:
        status, news = self.get_instrument_info(instrument_id, days)

        if status is True:
            df = pd.DataFrame(news)
            df['timestamp'] = pd.to_datetime(df.timestamp, unit='ms')
            df['timestamp'] = df['timestamp'].dt.tz_localize('UTC').dt.tz_convert(LOCAL_TZ)

            df = df.reset_index(drop=False)
            df.set_index('timestamp', inplace=True)
            df = df.iloc[::-1]

            return df

        return pd.DataFrame()

    @before
    def get_realtime_access(self) -> (bool, list):
        return self._GET('realtime_access')

    @before
    def get_news(self, news_id) -> (bool, list):
        return self._GET('news/{}'.format(news_id))

    @before
    def get_all_news(self, max_number=25, page=0) -> (bool, list):
        # 'https://www.nordnet.no/api/2/news?source_id=9&news_lang=no,en&limit=20&offset=0'
        # source_id=9
        return self._GET('news/?news_lang=no,en&limit={0}&offset={1}'.format(max_number, page))

    def get_all_news_pd(self, max_number=25, page=0) -> pd.DataFrame:
        status, news = self.get_all_news(max_number=25, page=0)
        if status is True:
            df = pd.DataFrame(news)
            df['datetime'] = pd.to_datetime(df.timestamp, unit='ms')
            df['datetime'] = df['datetime'].dt.tz_localize('UTC').dt.tz_convert(LOCAL_TZ)
            df.set_index('datetime', inplace=True)

            return df

        return pd.DataFrame()

    @before
    def get_instrument_types(self) -> (bool, list):
        return self._GET('instruments/types')

    def get_instrument_types_pd(self) -> pd.DataFrame:
        status, types = self.get_instrument_types()

        if status is True:
            df = pd.DataFrame(types)

            df = df.reset_index(drop=False)
            df.set_index('instrument_type', inplace=True)
            df = df.iloc[::-1]

            return df

        return pd.DataFrame()

    @before
    def get_instrument_type(self, instrument_type) -> (bool, list):
        return self._GET('instruments/types/{}'.format(instrument_type))

    @before
    def get_sectors(self) -> (bool, list):
        return self._GET('instruments/sectors'.format())

    @before
    def get_sector(self, sector) -> (bool, list):
        return self._GET('instruments/sectors/{}'.format(sector))

    @before
    def get_lists(self) -> (bool, list):
        return self._GET('lists')

    @before
    def get_list(self, list_id) -> (bool, list):
        return self._GET('lists/{}'.format(list_id))

    @before
    def get_tradables(self, identifier, market_id=15) -> (bool, list):
        return self._GET('tradables/info/{}:{}'.format(market_id, identifier))

    @before
    def get_tradables_intraday(self, identifier, market_id) -> (bool, list):
        return self._GET('tradables/intraday/{}:{}'.format(market_id, identifier))

    @before
    def get_tradables_trades(self, identifier, market_id=15, max_trades=10000000, weeks=5) -> (bool, list):
        return self._GET('tradables/trades/{}:{}?count={}&weeks={}'.format(market_id, identifier, max_trades, weeks))

    def get_tradables_trades_pd(self, identifier, market_id, max_trades=10000000, weeks=5) -> pd.DataFrame:
        status, trades = self.get_tradables_trades(market_id, identifier, max_trades=10000000, weeks=5)
        if status is True:
            df = pd.DataFrame(trades[0]['trades'])
            df['datetime'] = pd.to_datetime(df.trade_timestamp, unit='ms')
            df['datetime'] = df['datetime'].dt.tz_localize('UTC').dt.tz_convert(LOCAL_TZ)
            df.set_index('datetime', inplace=True)

            return df

        return pd.DataFrame()

    @before
    def get_indicators(self) -> (bool, list):
        return self._GET('indicators')

    def get_indicators_pd(self) -> pd.DataFrame:
        status, indicators = self.get_indicators()
        if status is True:
            df = pd.DataFrame(indicators)
            df['open'] = pd.to_timedelta(df.open, unit='s')
            df['close'] = pd.to_timedelta(df.open, unit='s')
            df.set_index('identifier', inplace=True)

            return df

        return pd.DataFrame()

    @before
    def get_indicator_historical(self, indicator_id='OSE:OSEBX', weeks=10) -> (bool, list):
        return self._GET('indicators/historical/values/{}?weeks={}'.format(indicator_id, weeks))

    @before
    def get_indicator_historical_pd(self, indicator_id='OSE:OSEBX', weeks=52) -> pd.DataFrame:

        status, result = self.get_indicator_historical(indicator_id, weeks)
        if status is True:
            df = pd.DataFrame(result[0].get('prices', []))
            if df.shape[0] > 0:
                df.time = pd.to_datetime(df.time, unit='ms')
                df.set_index('time', inplace=True)
                return df

        return pd.DataFrame()

    @before
    def get_option_pairs(self, instrument_id) -> (bool, list):
        return self._GET('instruments/{}/option_pairs'.format(instrument_id))

    @before
    def get_turnover_list(self, exchange_list='no:ose', max_instruments=10) -> (bool, list):
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
        return self._GET(
            'instrument_search/query/stocklist?limit={0}&sort_attribute=turnover_normalized&sort_order=desc&apply_filters=exchange_list={1}|diff_pct=[* TO 900]'.format(
                max_instruments, exchange_list))

    @before
    def get_winner_list(self, exchange_list='no:ose', max_instruments=10) -> (bool, list):
        return self._GET(
            'instrument_search/query/stocklist?limit={0}&sort_attribute=diff_pct&sort_order=desc&apply_filters=exchange_list={1}|diff_pct=[* TO 900]'.format(
                max_instruments, exchange_list))

    @before
    def get_looser_list(self, exchange_list='no:ose', max_instruments=10) -> (bool, list):
        return self._GET(
            'instrument_search/query/stocklist?limit={0}&sort_attribute=diff_pct&sort_order=asc&apply_filters=exchange_list={1}|diff_pct=[* TO 900]'.format(
                max_instruments, exchange_list))

    @before
    def get_ranking_list(self, max_instruments=10, sort_order='desc', exchange_list='no:ose') -> (bool, list):
        return self._GET(
            'instrument_search/query/stocklist?limit={0}&sort_attribute=diff_pct&sort_order={1}&apply_filters=exchange_list={2}|diff_pct=[* TO 900]'.format(
                max_instruments, sort_order, exchange_list))

    @before
    def print_ranking_list(self, max_instruments=10, sort_order='desc', order=1) -> (bool, list):
        status, results = self.get_ranking_list(max_instruments=max_instruments, sort_order=sort_order)

        if status is True:
            for i in results.get('results', [])[::order]:
                print('{0: <20} {1:.2f}%'.format(  # i['instrument_info']['name'],
                    i['instrument_info']['symbol'],
                    i['price_info']['diff_pct']))

    @before
    def get_indicator_historical_sparks(self) -> (bool, list):
        return self._GET('indicators/historical/sparkpoints/')

    @before
    def get_indicator(self, indicator_id='OSEBX', src='OSE',
                      from_date=datetime.datetime.now().strftime('%Y-%m-%d')) -> (bool, list):
        # print('{}/api/2/indicators/{}:{}?from={}'.format(BASE_URL, src, indicator_id, from_date))
        return self._GET('indicators/{}:{}?from={}'.format(src, indicator_id, from_date))

    @before
    def get_indicator_history(self, indicator_id='OSEBX', src='OSE',
                              from_date=datetime.datetime.now().strftime('%Y-%m-%d')) -> (bool, list):
        # print('{}/api/2/indicators/historical/values/{}:{}?from={}'.format(BASE_URL, src, indicator_id, from_date))
        return self._GET('indicators/historical/values/{}:{}?from={}'.format(src, indicator_id, from_date))

    @before
    def get_commodity_index(self, entity_type='CURRENCY') -> (bool, list):
        # Markets  INTEREST COMMODITY CURRENCY INDEX STOCKLIST
        return self._GET(
            'instrument_search/query/indicator?entity_type={}&apply_filters=market_overview_group=NO_GLOBAL_MO'.format(
                entity_type))

    @before
    def get_instrument_attributes(self, entity_type) -> (bool, list):
        # Markets  INTEREST COMMODITY CURRENCY INDEX STOCKLIST
        return self._GET(
            'instrument_search/attributes?entity_type={1}&expand=exchange_list&apply_filters=exchange_country=NO'.format(
                entity_type))

    @before
    def get_turnover_norm_list(self, sort_order='desc', max_instruments=10) -> (bool, list):
        return self._GET(
            'instrument_search/query/stocklist?limit={0}&sort_attribute=turnover_normalized&sort_order={1}&apply_filters=exchange_country=NO|exchange_list=no:ose|diff_pct=[* TO 900]'.format(
                max_instruments, sort_order))

    @before
    def get_shareville(self, instrument_id, max_comments=50) -> (bool, list):
        return self._GET('shareville/comments/instrument/{}?limit={}'.format(instrument_id, max_comments))

    def get_shareville_pd(self, instrument_id, max_comments=50) -> pd.DataFrame:
        status, comments = self.get_shareville(instrument_id, max_comments)

        if status is True:
            df = pd.DataFrame(comments)
            df['created'] = pd.to_datetime(df.created, unit='ms')
            df['created'] = df['created'].dt.tz_localize('UTC').dt.tz_convert(LOCAL_TZ)

            df = df.reset_index(drop=False)
            df.set_index('created', inplace=True)
            df = df.iloc[::-1]

            return df

        return pd.DataFrame()

    @before
    def get_shareville_community(self, instrument_id) -> (bool, list):

        resp = requests.get('https://www.shareville.no/api/v1/marketflow/instruments/{}/rap'.format(instrument_id))
        if resp.status_code == 200:
            return True, resp.json()

        return False, {}

    def get_shareville_community_owners(self, instrument_id) -> (bool, list):

        status, r = self.get_shareville_community(instrument_id)

        return r.get('count', 0)

    def _fix_price_info(self, p) -> dict:
        """
        Unpacks the price_info dict
        :param p:
        :return:
        """

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
    def get_indicators(self, indicators) -> (bool, list):
        return self._GET('indicators/{}'.format(','.join(indicators)))

    def get_indicators_pd(self) -> pd.DataFrame:
        status, indicators = self.get_indicators()

        if status is True:
            df = pd.DataFrame(indicators)

            df['open'] = pd.to_timedelta(df.open, unit='s')
            df['close'] = pd.to_timedelta(df.open, unit='s')

            df = df.reset_index(drop=False)
            df.set_index('identifier', inplace=True)
            df = df.iloc[::-1]

            return df

        return pd.DataFrame()

    def _get_stock_list(self, page, limit, filters) -> (bool, list):
        status, result = self._GET(
            'instrument_search/query/stocklist?apply_filters={}&limit={}&offset={}'.format(filters, limit, page))

        return status, result.get('rows', 0), result.get('total_hits', 0), result.get('results')

    @before
    def get_all_instruments(self, countries=['no', 'se']) -> (bool, list):
        limit = 100
        offset = 0
        free_text_search = '|'.join(
            ['exchange_list={}:{}'.format(x['c'], x['m']) for x in EXCHANGES if x['c'] in countries])
        more_to_go = True
        results = []
        while more_to_go is True:

            status, rows, total, r = self._get_stock_list(page=offset, limit=limit, filters=free_text_search)
            if status is True:
                results += [{**x['instrument_info'], **x['status_info'], **x['market_info'],
                             **self._fix_price_info(x['price_info']), **x['exchange_info'], **x['key_ratios_info'],
                             **x['historical_returns_info']} for x in r]

            offset += limit

            if len(results) >= total - 1 or limit > rows:
                more_to_go = False

        return True, results

    def get_all_instruments_pd(self, countries=['no', 'se']) -> pd.DataFrame:
        status, instruments = self.get_all_instruments(countries=countries)

        if status is True:
            df = pd.DataFrame(instruments)
            df['tick_timestamp'] = pd.to_timedelta(df.tick_timestamp, unit='ms')
            return df

        return pd.DataFrame()

    # indicators
    def _get_indicator_list(self, page, limit) -> (bool, list):
        status, result = self._GET(
            'instrument_search/query/indicator?entity_type=INDEX&limit={}&offset={}'.format(limit, page))

        return status, result.get('rows', 0), result.get('total_hits', 0), result.get('results')

    @before
    def get_all_indicators(self, countries=['no', 'se']) -> (bool, list):

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

    def get_all_indicators_pd(self, countries=['no', 'se']) -> pd.DataFrame:
        status, indicators = self.get_all_indicators(countries=countries)

        if status is True:
            df = pd.DataFrame(indicators)
            df['tick_timestamp'] = pd.to_timedelta(df.tick_timestamp, unit='ms')
            return df

        return pd.DataFrame()

    # COMMODITIES
    def _get_commodity_list(self, page, limit) -> (bool, list):
        status, result = self._GET(
            'instrument_search/query/indicator?entity_type=COMMODITY&limit={}&offset={}'.format(limit, page))

        return status, result.get('rows', 0), result.get('total_hits', 0), result.get('results')

    @before
    def get_all_commodities(self, countries=['no', 'se']) -> (bool, list):

        limit = 100
        offset = 0
        more_to_go = True
        results = []
        while more_to_go is True:

            status, rows, total, r = self._get_commodity_list(page=offset, limit=limit)
            if status is True:
                results += [{**x['instrument_info'], **x['indicator_info'],
                             **x['exchange_info'],
                             **self._fix_price_info(x['price_info']),
                             **x['historical_returns_info']} for x in r]

            offset += limit

            if len(results) >= total - 1 or limit > rows:
                more_to_go = False

        return True, results

    def get_all_commodities_pd(self, countries=['no', 'se']) -> pd.DataFrame:
        status, commodities = self.get_all_commodities(countries=countries)

        if status is True:
            df = pd.DataFrame(commodities)
            df['tick_timestamp'] = pd.to_timedelta(df.tick_timestamp, unit='ms')
            return df

        return pd.DataFrame()

    # INTEREST
    def _get_interest_list(self, page, limit) -> (bool, list):
        status, result = self._GET(
            'instrument_search/query/indicator?entity_type=INTEREST&limit={}&offset={}'.format(limit, page))

        return status, result.get('rows', 0), result.get('total_hits', 0), result.get('results')

    @before
    def get_all_interest(self, countries=['no', 'se']) -> (bool, list):

        limit = 100
        offset = 0
        more_to_go = True
        results = []
        while more_to_go is True:

            status, rows, total, r = self._get_interest_list(page=offset, limit=limit)
            if status is True:
                results += [{**x['instrument_info'], **x['indicator_info'],
                             **x['exchange_info'],
                             **self._fix_price_info(x['price_info']),
                             **x['historical_returns_info']} for x in r]

            offset += limit

            if len(results) >= total - 1 or limit > rows:
                more_to_go = False

        return True, results

    def get_all_interest_pd(self, countries=['no', 'se']) -> pd.DataFrame:
        status, interest = self.get_all_interest(countries=countries)

        if status is True:
            df = pd.DataFrame(interest)
            df['tick_timestamp'] = pd.to_timedelta(df.tick_timestamp, unit='ms')
            return df

        return pd.DataFrame()

    # FOREX
    def _get_forex_list(self, page, limit) -> (bool, list):
        status, result = self._GET(
            'instrument_search/query/indicator?entity_type=CURRENCY&limit={}&offset={}'.format(limit, page))

        return status, result.get('rows', 0), result.get('total_hits', 0), result.get('results')

    @before
    def get_all_forex(self, countries=['no', 'se']) -> (bool, list):

        limit = 100
        offset = 0
        more_to_go = True
        results = []
        while more_to_go is True:

            status, rows, total, r = self._get_forex_list(page=offset, limit=limit)
            if status is True:
                results += [{**x['instrument_info'], **x['indicator_info'],
                             **x['exchange_info'],
                             **self._fix_price_info(x['price_info']),
                             **x['historical_returns_info']} for x in r]

            offset += limit

            if len(results) >= total - 1 or limit > rows:
                more_to_go = False

        return True, results

    def get_all_forex_pd(self, countries=['no', 'se']) -> pd.DataFrame:
        status, forex = self.get_all_forex(countries=countries)

        if status is True:
            df = pd.DataFrame(forex)
            df['tick_timestamp'] = pd.to_timedelta(df.tick_timestamp, unit='ms')
            return df

        return pd.DataFrame()
