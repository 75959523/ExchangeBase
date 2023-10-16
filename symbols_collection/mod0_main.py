import os

from symbols_collection.mod10_bybit import bybit
from symbols_collection.mod11_xt import xt
from symbols_collection.mod12_hitbtc import hitbtc
from symbols_collection.mod13_bit_mart import bit_mart
from symbols_collection.mod14_bigone import bigone
from symbols_collection.mod15_jubi import jubi
from symbols_collection.mod16_la_token import la_token
from symbols_collection.mod17_coinex import coinex
from symbols_collection.mod18_gate_io import gate_io
from symbols_collection.mod19_coin_w import coin_w
from symbols_collection.mod1_okx import okx
from symbols_collection.mod20_bi_ka import bi_ka
from symbols_collection.mod21_hot_coin import hot_coin
from symbols_collection.mod22_digi_finex import digi_finex
from symbols_collection.mod23_l_bank import l_bank
from symbols_collection.mod24_bing_x import bing_x
from symbols_collection.mod25_probit import probit
from symbols_collection.mod26_kraken import kraken
from symbols_collection.mod27_ku_coin import ku_coin
from symbols_collection.mod28_poloniex import poloniex
from symbols_collection.mod2_binance import binance
from symbols_collection.mod3_huobi import huobi
from symbols_collection.mod4_bit_get import bit_get
from symbols_collection.mod5_bitfinex import bitfinex
from symbols_collection.mod6_mexc import mexc
from symbols_collection.mod7_bit_venus import bit_venus
from symbols_collection.mod8_deep_coin import deep_coin
from symbols_collection.mod9_ascend_ex import ascend_ex

okx()
binance()
huobi()
bit_get()
bitfinex()
mexc()
bit_venus()
deep_coin()
ascend_ex()
bybit()
xt()
hitbtc()
bit_mart()
bigone()
jubi()
la_token()
coinex()
gate_io()
coin_w()
bi_ka()
hot_coin()
digi_finex()
l_bank()
bing_x()
probit()
kraken()
ku_coin()
poloniex()

current_directory = os.getcwd()
all_files = os.listdir(current_directory)
for file_name in all_files:
    if file_name.endswith('.txt'):
        os.remove(os.path.join(current_directory, file_name))

