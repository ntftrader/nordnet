# Nordnet

A python wrapper around Scandinavian broker [Nordnet](https://www.nordnet.no)'s external [REST api](https://api.test.nordnet.se/).

### Install
```
pip install git+https://github.com/ntftrader/nordnet.git
```

Or a specific version (tag):
```
pip install git+https://github.com/ntftrader/nordnet.git@1.0.0
```

Requirements are requests, pandas, numpy and simplejson.
### Simple
```
from nordnet import Nordnet
nn = Nordnet()
nn.main_search("DNB")
(True, [{'instrument_id': 16105640, 'symbol': 'DNB', 'name': 'DNB', 'exchange_country': 'NO'},
        {'instrument_id': 16121046, 'symbol': 'DNB', 'name': 'Dun & Bradstreet Corporation (The)', 'exchange_country': 'US'},
        {'instrument_id': 16117764, 'symbol': 'DNBF', 'name': 'DNB Financial Corp', 'exchange_country': 'US'}])

```

### Data structures
Methods calling the REST api will return a tuple `(status, results)` where status is a boolean and results a list.

The `status` boolean is `True` if the GET request returns http 200.

All methods with name ending with `*_pd` returns a pandas Dataframe.
```
df = nn.get_trades_pd(instrument_id=16117764, vwap=True, turnover=False)
df.tail()
```

