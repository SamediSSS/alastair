import os
import re
import time
import asyncio
import logging

import aiohttp
import numpy as np
import pandas as pd
import requests as req
import matplotlib.pyplot as plt

from datetime import datetime
from aiogram import Bot, Dispatcher, types
from aiogram.types import Message, CallbackQuery
from aiogram.filters import Command
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder
from aiogram.types import FSInputFile




pd.set_option('display.float_format', '{:.6f}'.format)

def usdt(x): return bool(re.search(r'USDT', x))

def extract_name(pair_name):
    # Заменяем все вариации "DSTU" на пустую строку
    cleaned_name = re.sub(r'(?i)(^USDT[-_]?|[-_]USDT$|[-_]USDT|USDT[-_]?)', '', pair_name)
    # Удаляем лишние символы и пробелы
    cleaned_name = cleaned_name.strip()
    return cleaned_name

# Асинхронные запросы
async def fetch(session, url, params=None, headers=None):
    async with session.get(url, params=params, headers=headers) as response:
        return await response.json(content_type=None)

async def get_data():
    ctime = int(time.time()*1000)
    async with aiohttp.ClientSession() as session:
        urls = {
            "binance": 'https://fapi.binance.com/fapi/v1/ticker/bookTicker',                                              #0
            "binance_f": 'https://fapi.binance.com/fapi/v1/premiumIndex',                                                 #1
            "mexc": 'https://contract.mexc.com/api/v1/contract/ticker',                                                   #2
            "mexc_f": 'https://contract.mexc.com/api/v1/contract/funding_rate',                                           #3
            "gate": 'https://fx-api.gateio.ws/api/v4/futures/usdt/tickers',                                               #4
            "gate_f": 'https://fx-api.gateio.ws/api/v4/futures/usdt/contracts',                                           #5
            "bybit": 'https://api.bybit.com/v5/market/tickers?category=linear',                                           #6
            "bingx": f'https://open-api.bingx.com/openApi/swap/v1/ticker/price?timestamp={int(time.time())}',             #7
            "bingx_f": f'https://open-api.bingx.com/openApi/swap/v2/quote/premiumIndex?timestamp={int(time.time())}',     #8
            "kucoin": 'https://api-futures.kucoin.com/api/v1/allTickers',                                                 #9
            "kucoin_f": 'https://api-futures.kucoin.com/api/v1/contracts/active',                                         #10
            "lbank": 'https://lbkperp.lbank.com//cfd/openApi/v1/pub/marketData?productGroup=SwapU',                       #11
            "htx":'https://api.hbdm.com/linear-swap-ex/market/bbo',                                                       #12
            "htx_f":'https://api.hbdm.com/linear-swap-api/v1/swap_batch_funding_rate',                                    #13
            "bitmart":'https://api-cloud-v2.bitmart.com/contract/public/details',                                         #14
            "XT":'https://fapi.xt.com/future/market/v1/public/cg/contracts',                                              #15
            "coinex":'https://api.coinex.com/v2/futures/ticker',                                                          #16
            "coinex_f":'https://api.coinex.com/v2/futures/funding-rate',                                                  #17
            "binance_s":'https://api1.binance.com/api/v3/ticker/bookTicker',                                              #18
            "mexc_s":'https://api.mexc.com/api/v3/ticker/bookTicker',                                                     #19
            "gate_s":'https://api.gateio.ws/api/v4/spot/tickers',                                                         #20
            "bybit_s":'https://api.bybit.com/v5/market/tickers?category=spot',                                            #21
            "bingx_s":f'https://open-api.bingx.com/openApi/spot/v1/ticker/24hr?timestamp={int(time.time())}',             #22
            "kucoin_s":'https://api.kucoin.com/api/v1/market/allTickers',                                                 #23
            "lbank_s":'https://api.lbkex.com/v2/supplement/ticker/price.do',                                              #24
            "htx_s":'https://api.huobi.pro/market/tickers',                                                               #25
            "bitmart_s":'https://api-cloud.bitmart.com/spot/quotation/v3/tickers',                                        #26
            "xt_s":'https://sapi.xt.com/v4/public/ticker',                                                                #27
            "coinex_s":'https://api.coinex.com/v1/market/ticker/all',                                                     #28
            "mexc_i2": 'https://contract.mexc.com/api/v1/contract/detail'
        }

        tasks = {key: fetch(session, url) for key, url in urls.items()}
        results = await asyncio.gather(*tasks.values())

        # Binance
        fut1 = pd.DataFrame(results[0]).astype({'bidPrice': 'float', 'askPrice': 'float'})[['symbol', 'bidPrice', 'askPrice']]
        fut1_f = pd.DataFrame(results[1]).astype({'lastFundingRate':'float', 'nextFundingTime':'float'})[['symbol', 'lastFundingRate', 'nextFundingTime']]
        fut1_f['t_s'] = fut1_f['nextFundingTime'] - ctime
        fut1_f['t_l'] = fut1_f['t_s']
        fut1 = pd.merge(fut1, fut1_f[['symbol', 'lastFundingRate', 't_s', 't_l']], on = 'symbol')   
        fut1.rename(columns = {'symbol':'coin', 'lastFundingRate':'f_s'}, inplace=True)
        fut1 = fut1[fut1['coin'].str.endswith('USDT')].reset_index(drop=True)
        fut1['coin'] = fut1['coin'].str.removesuffix('USDT')   
        fut1[['short', 'long']] = 'Binance'
        fut1['f_l'] = fut1['f_s']


        # Mexc
        fut_i2 = pd.DataFrame(results[29]['data'])
        fut_i2.rename(columns={"baseCoinName": "coin"}, inplace=True)

        fut2 = pd.DataFrame(results[2]['data']).astype({'ask1': 'float', 'bid1': 'float'})[['symbol', 'ask1', 'bid1']]
        fut2_f = pd.DataFrame(results[3]['data']).astype({'fundingRate':'float', 'nextSettleTime':'float'})[['symbol', 'fundingRate', 'nextSettleTime']]
        fut2_f['t_s'] = fut2_f['nextSettleTime'] - ctime 
        fut2_f['t_l'] = fut2_f['t_s']
        fut2 = pd.merge(fut2, fut2_f[['symbol', 'fundingRate', 't_s', 't_l']], on = 'symbol')
        fut2.rename(columns={'bid1': 'bidPrice', 'ask1':'askPrice', 'fundingRate':'f_s'}, inplace=True)
        fut2 = pd.merge(fut_i2[['symbol', 'coin']], fut2[['symbol', 'bidPrice', 'askPrice', 'f_s', 't_s', 't_l']], on='symbol')[['coin', 'askPrice', 'bidPrice', 'f_s', 't_s', 't_l']]
        fut2[['short', 'long']] = 'Mexc'
        fut2['f_l'] = fut2['f_s']

        # Gate
        fut3 = pd.DataFrame(results[4]).astype({'highest_bid': 'float', 'lowest_ask': 'float'})[['contract', 'lowest_ask', 'highest_bid']]
        fut3_f = pd.DataFrame(results[5]).astype({'funding_rate':'float', 'funding_next_apply':'float'})[['name', 'funding_rate', 'funding_next_apply']]
        fut3.rename(columns={'contract':'coin', 'highest_bid': 'bidPrice', 'lowest_ask':'askPrice'}, inplace=True)
        fut3_f.rename(columns={'name':'coin', 'funding_rate': 'f_s'}, inplace=True)
        fut3_f['t_s'] = fut3_f['funding_next_apply']*1000 - ctime 
        fut3_f['t_l'] = fut3_f['t_s']
        fut3 = pd.merge(fut3, fut3_f[['coin', 'f_s', 't_s', 't_l']], on = 'coin')
        fut3 = fut3[fut3['coin'].str.endswith('USDT')].reset_index(drop=True)
        fut3['coin'] = fut3['coin'].str.removesuffix('_USDT')
        fut3[['short', 'long']] = 'Gate'
        fut3['f_l'] = fut3['f_s']

        # Bybit

        fut4 = pd.DataFrame(results[6]['result']['list'])
        fut4 = fut4.drop(fut4.index[fut4['fundingRate']==''])
        fut4 = fut4.drop(fut4.index[fut4['ask1Price']==''])
        fut4 = fut4.drop(fut4.index[fut4['bid1Price']==''])
        fut4 = fut4.astype({'bid1Price': 'float', 'ask1Price': 'float', 'fundingRate':'float', 'nextFundingTime':'float'})
        fut4['t_s'] = fut4['nextFundingTime'] - ctime
        fut4['t_l'] = fut4['t_s']
        fut4 = fut4[['symbol', 'ask1Price', 'bid1Price', 'fundingRate', 't_s', 't_l']]
        fut4.rename(columns={'symbol':'coin', 'bid1Price': 'bidPrice', 'ask1Price':'askPrice', 'fundingRate':'f_s'}, inplace=True)
        fut4 = fut4[fut4['coin'].str.endswith('USDT')].reset_index(drop=True)
        fut4['coin'] = fut4['coin'].str.removesuffix('USDT')
        fut4[['short', 'long']] = 'Bybit'
        fut4['f_l'] = fut4['f_s']

        # Bingx
        fut5 = pd.DataFrame(results[7]['data']).astype({'price': 'float'})[['symbol', 'price']]
        fut5_f = pd.DataFrame(results[8]['data']).astype({'lastFundingRate':'float', 'nextFundingTime':'float'})
        fut5_f['t_s'] = fut5_f['nextFundingTime'] - ctime
        fut5_f['t_l'] = fut5_f['t_s']
        fut5 = pd.merge(fut5, fut5_f[['symbol', 'lastFundingRate', 't_s', 't_l']], on = 'symbol')
        fut5.rename(columns={'price': 'bidPrice', 'lastFundingRate':'f_s'}, inplace=True)
        fut5['askPrice'] = fut5['bidPrice']
        fut5['f_l'] = fut5['f_s']
        fut5[['coin', 'ccy']] = fut5['symbol'].str.split('-', n=1, expand=True)
        fut5 = fut5[fut5['ccy'] == 'USDT'].reset_index(drop=True)
        fut5[['short', 'long']] = 'Bingx'


        #Kucoin
        fut6 = pd.DataFrame(results[9]['data']).astype({'bestBidPrice': 'float', 'bestAskPrice': 'float'})[['symbol', 'bestBidPrice', 'bestAskPrice']]
        fut6.rename(columns={'bestBidPrice': 'bidPrice', 'bestAskPrice':'askPrice'}, inplace=True)
        fut6_f = pd.DataFrame(results[10]['data']).astype({'fundingFeeRate':'float', 'nextFundingRateTime':'float'})[['symbol', 'baseCurrency', 'fundingFeeRate', 'nextFundingRateTime']]
        fut6_f.rename(columns={'baseCurrency': 'coin', 'fundingFeeRate':'f_s', 'nextFundingRateTime':'t_s'}, inplace=True)
        fut6 = pd.merge(fut6, fut6_f, on = 'symbol')
        fut6 = fut6[fut6['symbol'].str.endswith('USDTM')][['coin', 'bidPrice', 'askPrice', 'f_s', 't_s']]
        fut6[['short', 'long']] = 'Kucoin'
        fut6['f_l'] = fut6['f_s']
        fut6['t_l'] = fut6['t_s']

        #LBank
        fut7 = pd.DataFrame(results[11]['data']).astype({'lastPrice':'float', 'nextFeeTime':'float', 'fundingRate':'float', 'volume':'float'})
        fut7 = fut7[fut7['volume']>1][['symbol', 'lastPrice', 'nextFeeTime', 'fundingRate']]
        fut7.rename(columns = {'symbol':'coin', 'lastPrice': 'bidPrice', 'fundingRate':'f_s', 'nextFeeTime':'t_s'}, inplace=True )
        fut7['coin'] = fut7['coin'].str.removesuffix('USDT')
        fut7['t_s'] = fut7['t_s'] - ctime
        fut7['askPrice'] = fut7['bidPrice']
        fut7['f_l'] = fut7['f_s']
        fut7['t_l'] = fut7['t_s']
        fut7[['short', 'long']] = 'LBank'

        #HTX
        fut8 = pd.DataFrame(results[12]['ticks'])[['contract_code', 'ask', 'bid']]
        fut8_f = pd.DataFrame(results[13]['data']).astype({'funding_rate':'float', 'funding_time':'float'})[['symbol', 'contract_code', 'funding_rate', 'funding_time']]
        fut8[['askPrice', 'askSize']] = fut8['ask'].apply(pd.Series)
        fut8[['bidPrice', 'bidSize']] = fut8['bid'].apply(pd.Series)
        fut8 = pd.merge(fut8, fut8_f, on = 'contract_code')[['symbol', 'askPrice', 'bidPrice', 'funding_rate', 'funding_time']]
        fut8.rename(columns={'symbol': 'coin', 'funding_rate':'f_s', 'funding_time':'t_s'}, inplace=True)
        fut8['t_s'] = fut8['t_s'] - ctime
        fut8['f_l'] = fut8['f_s']
        fut8['t_l'] = fut8['t_s']
        fut8[['short', 'long']] = 'HTX'

        #Bitmart
        fut9 = pd.DataFrame(results[14]['data']['symbols']).astype({'last_price':'float', 'expected_funding_rate':'float', 'funding_time':'float'})
        fut9 = fut9[fut9['quote_currency']=='USDT'][['base_currency', 'last_price', 'expected_funding_rate', 'funding_time']]
        fut9.rename(columns={'base_currency': 'coin', 'last_price':'bidPrice', 'expected_funding_rate':'f_s', 'funding_time':'t_s'}, inplace=True)
        fut9['t_s'] = fut9['t_s'] - ctime
        fut9['askPrice'] = fut9['bidPrice']
        fut9['f_l'] = fut9['f_s']
        fut9['t_l'] = fut9['t_s']
        fut9[['short', 'long']] = 'Bitmart'

        #XT
        fut10 = pd.DataFrame(results[15]).astype({'last_price':'float', 'funding_rate':'float', 'next_funding_rate_timestamp':'float'})
        fut10 = fut10[['base_currency', 'last_price', 'funding_rate', 'next_funding_rate_timestamp']]
        fut10.rename(columns={'base_currency': 'coin', 'last_price':'bidPrice', 'funding_rate':'f_s', 'next_funding_rate_timestamp':'t_s'}, inplace=True)
        fut10['t_s'] = fut10['t_s'] - ctime
        fut10['askPrice'] = fut10['bidPrice']
        fut10['f_l'] = fut10['f_s']
        fut10['t_l'] = fut10['t_s']
        fut10[['short', 'long']] = 'XT'

        #CoinEx
        fut11 = pd.DataFrame(results[16]['data']).astype({'last':'float'})[['market', 'last']]
        fut11_f = pd.DataFrame(results[17]['data']).astype({'latest_funding_rate':'float', 'next_funding_time':'float'})[['market', 'latest_funding_rate', 'next_funding_time']]
        fut11 = pd.merge(fut11, fut11_f, on = 'market')
        fut11['market'] = fut11['market'].str.removesuffix('USDT')
        fut11.rename(columns={'market': 'coin', 'last':'bidPrice', 'latest_funding_rate':'f_s', 'next_funding_time':'t_s'}, inplace=True)
        fut11['t_s'] = fut11['t_s'] - ctime
        fut11['askPrice'] = fut11['bidPrice']
        fut11['f_l'] = fut11['f_s']
        fut11['t_l'] = fut11['t_s']
        fut11[['short', 'long']] = 'CoinEx'

        #Binance spot
        spot1 = pd.DataFrame(results[18]).astype({'askPrice':'float'})
        spot1.rename(columns={"symbol": "coin"}, inplace=True)
        spot1 = spot1[spot1['coin'].str.endswith('USDT')].drop(columns = ['bidPrice', 'bidQty', 'askQty'])
        spot1['coin'] = spot1['coin'].str.removesuffix('USDT') 
        spot1['f_l'] = 0
        spot1['t_l'] = 30000000
        spot1['long'] = 'Binance_s'

        #Mexc spot
        spot2 = pd.DataFrame(results[19]).astype({'askPrice':'float'})
        spot2.rename(columns={"symbol": "coin"}, inplace=True)
        spot2 = spot2[spot2['coin'].str.endswith('USDT')].drop(columns = ['bidPrice', 'bidQty', 'askQty'])
        spot2['coin'] = spot2['coin'].str.removesuffix('USDT') 
        spot2['f_l'] = 0
        spot2['t_l'] = 30000000
        spot2['long'] = 'Mexc_s'

        #Gate spot
        spot3 = pd.DataFrame(results[20])
        spot3 = spot3.drop(spot3.index[spot3['lowest_ask']=='']).reset_index().astype({'lowest_ask':'float'})
        spot3.rename(columns={"currency_pair": "coin", 'lowest_ask':'askPrice'}, inplace=True)
        spot3 = spot3[spot3['coin'].str.endswith('USDT')][['coin', 'askPrice']]
        spot3['coin'] = spot3['coin'].str.removesuffix('_USDT') 
        spot3['f_l'] = 0
        spot3['t_l'] = 30000000
        spot3['long'] = 'Gate_s'

        #Bybit spot
        spot4 = pd.DataFrame(results[21]['result']['list'])
        spot4.rename(columns={"symbol": "coin", 'ask1Price':'askPrice'}, inplace=True)
        spot4 = spot4[spot4['coin'].str.endswith('USDT')][['coin', 'askPrice']].astype({'askPrice':'float'})
        spot4['coin'] = spot4['coin'].str.removesuffix('USDT') 
        spot4['f_l'] = 0
        spot4['t_l'] = 30000000
        spot4['long'] = 'Bybit_s'

        #Bingx spot
        spot5 = pd.DataFrame(results[22]['data'])
        spot5.rename(columns={"symbol": "coin", 'ask1Price':'askPrice'}, inplace=True)
        spot5 = spot5[spot5['coin'].str.endswith('USDT')][['coin', 'askPrice']].astype({'askPrice':'float'})
        spot5['coin'] = spot5['coin'].str.removesuffix('-USDT') 
        spot5['f_l'] = 0
        spot5['t_l'] = 30000000
        spot5['long'] = 'Bingx_s'

        #Kucoin spot
        spot6 = pd.DataFrame(results[23]['data']['ticker'])
        spot6.rename(columns={"symbol": "coin", 'sell':'askPrice'}, inplace=True)
        spot6 = spot6[spot6['coin'].str.endswith('USDT')][['coin', 'askPrice']].astype({'askPrice':'float'})
        spot6['coin'] = spot6['coin'].str.removesuffix('-USDT') 
        spot6['f_l'] = 0
        spot6['t_l'] = 30000000
        spot6['long'] = 'Kucoin_s'

        #LBank spot
        spot7 = pd.DataFrame(results[24]['data']).astype({'price':'float'})
        spot7.rename(columns = {'symbol':'coin', 'price':'askPrice'}, inplace = True)
        spot7['coin'] = spot7['coin'].str.upper().str.removesuffix('_USDT') 
        spot7['f_l'] = 0
        spot7['t_l'] = 30000000
        spot7['long'] = 'LBank_s'

        #HTX spot
        spot8 = pd.DataFrame(results[25]['data'])
        spot8.rename(columns={"symbol": "coin", 'ask':'askPrice'}, inplace=True)
        spot8 = spot8[spot8['coin'].str.endswith('usdt')][['coin', 'askPrice']].astype({'askPrice':'float'})
        spot8['coin'] = spot8['coin'].str.upper().str.removesuffix('USDT') 
        spot8['f_l'] = 0
        spot8['t_l'] = 30000000
        spot8['long'] = 'HTX_s'

        #Bitmart spot
        cols = ['coin', 'last', 'v_24h', 'qv_24h', 'open_24', 'high_24', 'low_24', \
                    'fluc', 'bidPrice', 'bid_sz', 'askPrice', 'ask_sz', 'ts']
        spot9 = pd.DataFrame(results[26]['data'], columns = cols)
        spot9 = spot9[spot9['coin'].str.endswith('USDT')][['coin', 'askPrice']].astype({'askPrice':'float'})
        spot9['coin'] = spot9['coin'].str.removesuffix('_USDT') 
        spot9['f_l'] = 0
        spot9['t_l'] = 30000000
        spot9['long'] = 'Bitmart_s'

        #XT spot
        spot10 = pd.DataFrame(results[27]['result']).astype({'ap':'float'})
        spot10.rename(columns = {'s':'coin', 'ap':'askPrice'}, inplace = True)
        spot10['coin'] = spot10['coin'].str.upper().str.removesuffix('_USDT')
        spot10 = spot10[['coin', 'askPrice']]
        spot10['f_l'] = 0
        spot10['t_l'] = 30000000
        spot10['long'] = 'XT_s'

        #CoinEx spot
        spot11 = pd.DataFrame(results[28]['data']['ticker']).T.reset_index()
        spot11.rename(columns = {'index':'coin', 'sell':'askPrice'}, inplace = True)
        spot11['coin'] = spot11['coin'].str.upper().str.removesuffix('USDT')
        spot11 = spot11[['coin', 'askPrice']].astype({'askPrice':'float'})
        spot11['f_l'] = 0
        spot11['t_l'] = 30000000
        spot11['long'] = 'CoinEx_s'


        futures = [fut1, fut2, fut3, fut4, fut5, fut6, fut7, fut8, fut9, fut10, fut11]
        spot = [spot1, spot2, spot3, spot4, spot5, spot6, spot7, spot8, spot9, spot10, spot11]


        spreads = pd.DataFrame(columns=['coin', 'bidPrice', 'askPrice', 'f_s', 'f_l', 't_s', 't_l', 'short', 'long', 's'])

        spreads_list = [
            futures[i][['coin', 'bidPrice', 'short', 'f_s', 't_s']]
            .merge(futures[j][['coin', 'askPrice', 'long', 'f_l', 't_l']], on='coin')[
                ['coin', 'bidPrice', 'askPrice', 'f_s', 'f_l', 't_s', 't_l', 'short', 'long']
            ]
            for i in range(len(futures))
            for j in range(len(futures)) if i != j
        ]
        spreads_f = pd.concat(spreads_list, ignore_index=True)
        spreads_f['f'] = True

        spreads_list = [
            futures[i][['coin', 'bidPrice', 'short', 'f_s', 't_s']]
            .merge(spot[j], on='coin')[
                ['coin', 'bidPrice', 'askPrice', 'f_s', 'f_l', 't_s', 't_l', 'short', 'long']
            ]
            for i in range(len(futures))
            for j in range(len(spot))
        ]
        spreads_s = pd.concat(spreads_list, ignore_index=True)
        spreads_s['f'] = False

        spreads = pd.concat([spreads_f, spreads_s], ignore_index=True)

        spreads['t_s'] = spreads['t_s']/60000
        spreads['t_l'] = spreads['t_l']/60000
        spreads['f_s'] = spreads['f_s']*100
        spreads['f_l'] = spreads['f_l']*100
        spreads['d'] = spreads['t_s'] - spreads['t_l']
        spreads['p_t'] = spreads['t_s']
        spreads['p_t'] = np.where(spreads['d'] > 10, spreads['t_l'], spreads['p_t'])
        spreads['f_l'] = np.where(spreads['d'] < -10, 0, spreads['f_l'])
        spreads['f_s'] = np.where(spreads['d'] > 10, 0, spreads['f_s'])

        spreads['c_spread'] = (((spreads['bidPrice']/spreads['askPrice'])-1)*100)
        spreads['f_spread'] = spreads['f_s'] - spreads['f_l']
        spreads['cf_spread'] = spreads['f_spread'] + spreads['c_spread']
        spreads.drop(['d'], axis=1, inplace = True)

        return spreads


##########################################


TOKEN = "7940678057:AAHt4xo8nDnqxstlB5qkk2F0J2KBmBXrFSk"
CHAT_ID = [432007724, 7342270415] 
bot = Bot(token=TOKEN)
dp = Dispatcher()

# Храним список наблюдаемых значений
user_watch = {}
user_filters = {}
course_watch = {}
data = pd.DataFrame()

# Реальные значения для выбора
values = ['Binance', 'Binance_s', 'Bingx', 'Bingx_s', 'Bitmart', 'Bitmart_s', 'Bybit', 'Bybit_s',
          'CoinEx', 'CoinEx_s', 'Gate', 'Gate_s', 'HTX', 'HTX_s', 'Kucoin', 'Kucoin_s',
          'LBank', 'LBank_s', 'Mexc', 'Mexc_s', 'XT', 'XT_s']

async def check_and_send():
    global data, user_watch
    while True:
        try:
            data = await get_data()

            for id in CHAT_ID:
                if id in course_watch and len(course_watch[id]) == 3 and course_watch[id][2] == True:
                    filtered_df = data[
                                    (data['c_spread'] > course_watch[id][0]) & 
                                    (data['c_spread'] < course_watch[id][1]) & 
                                    (data['f'] | course_watch[id][2])]
                    if not filtered_df.empty:
                        course_watch[id][2] = False
                        await bot.send_message(id, f"⚠️Найдены пары с курсовым спредом в диапазоне {course_watch[id][0]} - {course_watch[id][1]})\n"
                                                   f"Отслеживание остановлено")
                if id in user_watch and len(user_watch[id]) == 3:
                    coin, short, long = user_watch.get(id, [])
                    result = data[(data['coin'] == coin) & (data['short'] == short) & (data['long'] == long)]
                    row = result.iloc[0]
                    if (not result.empty) and (row['c_spread']<0):
                        response = (f"📊 Данные для {coin}:\n"
                                    f"🔹Short: {short} 🔹Long: {long}\n"
                                    f"💰Bid Price: {row['bidPrice']:.5f} 💵Ask Price: {row['askPrice']:.5f}\n"
                                    f"📉f_short: {row['f_s']:.5f}\t 📈f_long: {row['f_l']:.5f}\t Расчет: {row['p_t']:.5f}\n"
                                    f"☣️Cпреды: Course: {row['c_spread']:.5f}\t Fund: {row['f_spread']:.5f}\t cf: {row['cf_spread']:.5f}\n"
                                    f"⚠️Спред упал, отслеживание сброшено.")
                        del user_watch[id]
                        await bot.send_message(id, response)
            print('Обновление!')
            await asyncio.sleep(0.5)
        except KeyboardInterrupt:
            break
        except Exception as e: print(f"Ошибка: {e}, продолжаем работу...")
        


async def send_tracked_data(message: Message):
    """Отправляет пользователю данные по отслеживаемым параметрам"""
    watch = user_watch.get(message.chat.id, [])

    if len(watch) == 3:
        coin, short, long = watch
        result = data[(data['coin'] == coin) & (data['short'] == short) & (data['long'] == long)]

        if not result.empty:
            row = result.iloc[0]
            response = (f"📊 Данные для {coin}:\n"
                        f"🔹Short: {short} 🔹Long: {long}\n"
                        f"💰Bid Price: {row['bidPrice']:.5f} 💵Ask Price: {row['askPrice']:.5f}\n"
                        f"📉f_short: {row['f_s']:.5f}\t 📈f_long: {row['f_l']:.5f}\t Расчет: {row['p_t']:.5f}\n"
                        f"☣️Cпреды: Course: {row['c_spread']:.5f}\t Fund: {row['f_spread']:.5f}\t C+F: {row['cf_spread']:.5f}\n"
                       )
        else:
            response = "❌ Данных по выбранным параметрам нет."
    else:
        response = "❌ Неверные данные в списке отслеживания."

    await message.answer(response)        

async def df_to_image(df, chat_id):
    """Сохраняет датафрейм в изображение и возвращает путь к файлу"""
    img_path = f"table_{chat_id}.png"

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.axis("tight")
    ax.axis("off")

    # Добавляем заголовки к таблице
    table_data = [df.columns.tolist()] + df.values.tolist()
    ax.table(cellText=table_data, colLabels=None, cellLoc="center", loc="center")

    plt.savefig(img_path, dpi=300, bbox_inches="tight")
    plt.close(fig)

    return img_path    


#######################        

def get_values_keyboard():
    """Клавиатура с вариантами выбора"""
    keyboard = InlineKeyboardBuilder()
    for val in values:
        keyboard.button(text=val, callback_data=val)
    keyboard.adjust(2)  # Два столбца
    return keyboard.as_markup()

def get_sort_keyboard():
    """Клавиатура для сортировки"""
    keyboard = InlineKeyboardBuilder()
    for col in ['c_spread', 'f_spread', 'cf_spread']:
        keyboard.button(text=f"Сортировать по {col}", callback_data=f"sort_{col}")
    keyboard.adjust(1)
    return keyboard.as_markup()
    
def get_spot_keyboard():
    """Клавиатура для спота"""
    keyboard = InlineKeyboardBuilder()
    keyboard.button(text=f"Да", callback_data='True')
    keyboard.button(text=f"Нет", callback_data='False')
    keyboard.adjust(1)
    return keyboard.as_markup()

@dp.message(Command("start"))
async def send_welcome(message: Message):
    """Стартовое сообщение с кнопками"""
    keyboard = ReplyKeyboardBuilder()
    keyboard.button(text="Отслеживание")
    keyboard.button(text="Сброс отслеживания")
    keyboard.button(text="Запрос результатов")
    keyboard.button(text="Вкл/выкл отслеживание курсовых спредов")
    keyboard.button(text="Сброс отслеживания курсовых спредов")
    keyboard.adjust(1)  # Один столбец

    await message.answer("Выберите действие:", reply_markup=keyboard.as_markup(resize_keyboard=True))

############################################
@dp.message(lambda message: message.text == "Вкл/выкл отслеживание курсовых спредов")
async def check_or_start_tracking(message: Message):
    """Проверяем, есть ли активное отслеживание курсов или начинаем новое"""
    if message.chat.id in course_watch and len(course_watch[message.chat.id]) == 3 and course_watch[message.chat.id][2] == False:
        course_watch[message.chat.id][2] = True
        response = (f"Отслеживание спредов в диапазоне {course_watch[message.chat.id][0]} - {course_watch[message.chat.id][1]} включено!")
        await message.answer(response)
    elif message.chat.id in course_watch and len(course_watch[message.chat.id]) == 3 and course_watch[message.chat.id][2] == True:
        course_watch[message.chat.id][2] = False
        response = (f"Отслеживание спредов выключено!")
        await message.answer(response)
    else:
        await message.answer("Введите минимальный спред:")
        course_watch[message.chat.id] = []

@dp.message(lambda message: message.chat.id in course_watch and len(course_watch[message.chat.id]) == 0)
async def handle_min_spread(message: Message):
    course_watch[message.chat.id].append(float(message.text))
    await message.answer("Введите максимальный спред:")

@dp.message(lambda message: message.chat.id in course_watch and len(course_watch[message.chat.id]) == 1)
async def handle_coin_input(message: Message):
    course_watch[message.chat.id].append(float(message.text))
    course_watch[message.chat.id].append(True)
    await message.answer("Отслеживание спредов включено!")

@dp.message(lambda message: message.text == "Сброс отслеживания курсовых спредов")
async def reset_tracking(message: Message):
    if message.chat.id in course_watch:
        del course_watch[message.chat.id]
        await message.answer("🔄 Отслеживание успешно сброшено!")
    else:
        await message.answer("⚠️ У вас нет активного отслеживания.")
        
############################################

@dp.message(lambda message: message.text == "Отслеживание")
async def check_or_start_tracking(message: Message):
    """Проверяем, есть ли активное отслеживание или начинаем новое"""
    if message.chat.id in user_watch and len(user_watch[message.chat.id]) == 3:
        await send_tracked_data(message)
    else:
        await message.answer("Введите название монеты:")
        user_watch[message.chat.id] = []


@dp.message(lambda message: message.text == "Сброс отслеживания")
async def reset_tracking(message: Message):
    """Сброс отслеживания"""
    if message.chat.id in user_watch:
        del user_watch[message.chat.id]
        await message.answer("🔄 Отслеживание успешно сброшено!")
    else:
        await message.answer("⚠️ У вас нет активного отслеживания.")


@dp.message(lambda message: message.chat.id in user_watch and len(user_watch[message.chat.id]) == 0)
async def handle_coin_input(message: Message):
    """Запрашиваем первый параметр после ввода монеты"""
    user_watch[message.chat.id].append(message.text)
    await message.answer("Выберите шорт биржу:", reply_markup=get_values_keyboard())


@dp.callback_query(lambda call: call.message.chat.id in user_watch and len(user_watch[call.message.chat.id]) == 1)
async def handle_first_value(call: CallbackQuery):
    """Обрабатываем первый выбор из инлайн-кнопок"""
    user_watch[call.message.chat.id].append(call.data)
    await call.message.edit_text("Выберите лонг биржу:", reply_markup=get_values_keyboard())


@dp.callback_query(lambda call: call.message.chat.id in user_watch and len(user_watch[call.message.chat.id]) == 2)
async def handle_second_value(call: CallbackQuery):
    """Обрабатываем второй выбор из инлайн-кнопок"""
    global watch_flag
    user_watch[call.message.chat.id].append(call.data)
    await call.message.edit_text(f"✅ Отслеживание запущено для: {user_watch[call.message.chat.id]}")
    
@dp.message(lambda message: message.text == "Запрос результатов")
async def request_filters(message: Message):
    """Начинаем запрос фильтров"""
    user_filters[message.chat.id] = {}
    await message.answer("Введите минимальное значение для c_spread:")

@dp.message(lambda message: message.chat.id in user_filters and 'c_spread' not in user_filters[message.chat.id])
async def get_c_spread(message: Message):
    """Получаем минимальное значение c_spread"""
    user_filters[message.chat.id]['c_spread'] = float(message.text)
    await message.answer("Введите максимальное значение для c_spread:")
    

@dp.message(lambda message: message.chat.id in user_filters and 'c_spread_m' not in user_filters[message.chat.id])
async def get_c_spread_m(message: Message):
    """Получаем максимальное значение для c_spread"""
    user_filters[message.chat.id]['c_spread_m'] = float(message.text)
    await message.answer("Введите минимальное значение для f_spread:")


@dp.message(lambda message: message.chat.id in user_filters and 'f_spread' not in user_filters[message.chat.id])
async def get_f_spread(message: Message):
    """Получаем минимальное значение f_spread"""
    user_filters[message.chat.id]['f_spread'] = float(message.text)
    await message.answer("Введите минимальное значение для cf_spread:")


@dp.message(lambda message: message.chat.id in user_filters and 'сf_spread' not in user_filters[message.chat.id])
async def get_cf_spread(message: Message):
    """Получаем минимальное значение cf_spread"""
    user_filters[message.chat.id]['cf_spread'] = float(message.text)
    await message.answer("Включать спот позиции?", reply_markup = get_spot_keyboard())


@dp.callback_query(lambda call: call.message.chat.id in user_filters and 'spot' not in user_filters[call.message.chat.id])
async def get_spot(call: CallbackQuery):
    """Получаем включение спот позиций и предлагаем сортировку"""
    user_filters[call.message.chat.id]['spot'] = call.data
    await call.message.edit_text("Выберите столбец для сортировки:", reply_markup=get_sort_keyboard())
    
@dp.callback_query(lambda call: call.data.startswith("sort_"))
async def sort_and_send_results(call: CallbackQuery):
    """Фильтруем и сортируем данные, отправляем пользователю"""
    sort_col = call.data.split("_", 1)[1]
    chat_id = call.message.chat.id

    if sort_col not in ['c_spread', 'f_spread', 'cf_spread']:
        await call.message.answer(f"⚠ Ошибка: неверный параметр сортировки ({sort_col}).")
        return

    filters = user_filters.pop(chat_id, {})

    filters['spot'] = filters['spot'].lower() == 'true'
    
    filtered_df = data[
        (data['c_spread'] > filters['c_spread']) &
        (data['c_spread'] < filters['c_spread_m']) &
        (data['f_spread'] > filters['f_spread']) &
        (data['cf_spread'] > filters['cf_spread']) &
        (data['f'] | filters['spot'])
    ]

    if filtered_df.empty:
        await call.message.answer("❌ Нет данных по заданным фильтрам.")
        return
    if sort_col == 'c_spread':
        sorted_df = filtered_df[filtered_df['c_spread']<200].sort_values(by=sort_col, ascending=False).head(25)
    else:    
        sorted_df = filtered_df.sort_values(by=sort_col, ascending=False).head(25)

    # --- Генерация изображения ---
    img_path = await df_to_image(sorted_df, chat_id)

    # --- Отправляем изображение ---
    photo = FSInputFile(img_path)
    await bot.send_photo(chat_id, photo, caption="📊 Ваши результаты")

    # Удаляем картинку после отправки
    os.remove(img_path)

#######################        

async def main():
    asyncio.create_task(check_and_send())  # Запускаем проверку в фоне
    await dp.start_polling(bot)


#keep_alive()
asyncio.run(main())
