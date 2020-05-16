
LOCAL_TZ = 'Europe/Oslo'
BASE_HOST = 'www.nordnet.no'
BASE_URL = 'https://{}'.format(BASE_HOST)
COOKIE_FILE = 'nordnet_cookies.json'
COOKIE_MAX_TIME = 600  # seconds


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

INDEXES = [
    {'c': 'NO', 'm': 'OSE', 'i': 'OBX'},
    {'c': 'SE', 'm': 'SSE', 'i': 'OMXSPI'},
    {'c': 'FI', 'm': 'HEX', 'i': 'OMXHPI'},
    {'c': 'DK', 'm': 'CSE', 'i': 'OMXC25'},
    {'c': 'DE', 'm': 'SIX', 'i': 'B-IDX-DAXI'},
    {'c': 'CA', 'm': 'USX', 'i': 'DJX-CADOW'},
    {'c': 'UK', 'm': 'SIX', 'i': 'B-IDX-FTSE'},
    {'c': 'FR', 'm': 'USX', 'i': 'DJX-FRDOW'},
    {'c': 'HK', 'm': 'USX', 'i': 'DJX-HKDOWD'},
    {'c': 'JP', 'm': 'USX', 'i': 'DJX-JPDOWD'},
    {'c': 'CH', 'm': 'USX', 'i': 'DJX-DJSH'},
    {'c': 'US', 'm': 'SIX', 'i': 'RTS'},
    {'c': 'IN', 'm': 'USX', 'i': 'DJX-B50IND'},
    {'c': 'ZA', 'm': 'USX', 'i': 'DJX-ZADOWD'},
    {'c': 'US', 'm': 'CME', 'i': 'ENQ100-1'},
    {'c': 'US', 'm': 'SIX', 'i': 'SIX-IDX-NCMP'}
]
