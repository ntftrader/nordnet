# Nordnet

A python wrapper around Scandinavian broker [Nordnet](https://www.nordnet.no)'s external [REST api](https://api.test.nordnet.se/).

Simple
```
from nordnet import Nordnet
nn = Nordnet()
nn.main_search("DNB")
(True, [{'symbol': 'DNB', 'instrument_id': 16105640, 'exchange_country': 'NO'},
             {'symbol': 'DNB', 'instrument_id': 16121046, 'exchange_country': 'US'},
             {'symbol': 'DNBF', 'instrument_id': 16117764, 'exchange_country': 'US'}])
```

### Data structures
Methods calling the REST api will return a tuple `(status, results)` where status is a boolean and results a list.

The `status` boolean is `True` if the GET request returns http 200.

All methods with name ending with `*_pd` returns a pandas Dataframe.
```
df = nn.get_trades_pd(instrument_id=16117764, vwap=True, turnover=False)
df.tail()
```

