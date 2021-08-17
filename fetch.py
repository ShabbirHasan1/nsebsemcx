import requests, json, os, sys
import pandas as pd
from numpy import random
from time import sleep
from datetime import datetime as dt
import datetime as dtt
# =========================================================================================================================== #
class nsefetch:
    __NSE = "https://www.nseindia.com"
    __request_headers = {
                        'Host':'www.nseindia.com',
                        'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:82.0) Gecko/20100101 Firefox/82.0',
                        'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                        'Accept-Language':'en-US,en;q=0.5',
                        'Accept-Encoding':'gzip, deflate, br',
                        'DNT':'1',
                        'Connection':'keep-alive',
                        'Upgrade-Insecure-Requests':'1',
                        'Pragma':'no-cache',
                        'Cache-Control':'no-cache',
                        }

    # Fetch Proxies from http://www.freeproxylists.net/?c=IN&s=u
    __proxies = {
                'http': 'http://116.202.232.246:80',
                'https': 'http://116.202.228.164:3128',
                'https': 'http://116.202.228.162:3128',
                'https': 'http://210.212.128.246:8080',
                'https': 'http://125.99.58.110:3128',
                'https': 'http://182.48.240.2:80',
                'https': 'http://1.186.34.45:80',
                'https': 'http://123.108.201.18:84',
                'https': 'http://1.186.83.4:80',
                'https': 'http://175.101.12.10:84',
                'https': 'http://175.101.14.34:83',
                'https': 'http://27.116.51.119:8080',
                'https': 'http://115.241.225.42:80',
                'https': 'http://103.246.225.34:80',
                'https': 'http://182.71.146.148:8080',
                'https': 'http://117.239.240.202:53281'
                }

    __back_off = (random.uniform(58.853, 59.999), random.uniform(33.979, 57.791), random.uniform(29.123, 33.357), random.uniform(11.111, 27.737), 
                    random.uniform(4.051, 10.011), random.uniform(2.011, 3.047), random.uniform(1.571, 1.991), random.uniform(1.143, 1.147), 
                    random.uniform(0.731, 0.979), random.uniform(0.541, 0.701), random.uniform(0.229, 0.363))

    __today_folder_name = dt.today().date().strftime("%d_%b_%Y")
    __today_folder_path = f'./{dt.today().date().strftime("%d_%b_%Y")}/'
    __graphs_folder_path = f'./{dt.today().date().strftime("%d_%b_%Y")}/NIFTY/GraphsData/'

    def __init__(self, proxy=None):
        self.__s = requests.Session()
        if proxy:
            self.__s.proxies.update(self.__proxies)
        self.__check_or_create_expiry_directories()
        self.__fetch_nse()

    def __check_or_create_expiry_directories(self, GraphsData=False):
        ticker_list = ('NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MISC')
        if not (os.path.exists(self.__today_folder_path) and os.path.isdir(self.__today_folder_path)):
            os.system(f'mkdir -p ./{self.__today_folder_name}')
            for ticker in ticker_list:
                os.system(f'mkdir -p ./{self.__today_folder_name}/{ticker}/GraphsData')
                os.system(f'mkdir -p ./{self.__today_folder_name}/{ticker}/Options_Intraday_Snapshots')
                os.system(f'mkdir -p ./{self.__today_folder_name}/{ticker}/Futures_Intraday_Snapshots')
            print("NBN_Expiries Directory And Subdirectories Has Been Created Successfully")
        elif GraphsData and (not (os.path.exists(self.__graphs_folder_path) and os.path.isdir(self.__graphs_folder_path))):
            for ticker in ticker_list:
                os.system(f'mkdir -p ./{self.__today_folder_name}/{ticker}/GraphsData')
            print("NBN_Expiries GraphsData Directory And Subdirectories Has Been Created Successfully")
        else:
            print("NBN_Expiries Directory Exists")

    def __save_symbol_file(self, symbol, url, Options_Intraday_Snapshots=None, Futures_Intraday_Snapshots=None, GraphsData=None):
        if Options_Intraday_Snapshots:
            if symbol[0:4] == 'NIFT':
                file_path = f'./{self.__today_folder_name}/NIFTY/Options_Intraday_Snapshots/{symbol}_{dt.now().strftime("%Y-%b-%d_%H:%M:%S")}.json'
            elif symbol[0:4] == 'BANK':
                file_path = f'./{self.__today_folder_name}/BANKNIFTY/Options_Intraday_Snapshots/{symbol}_{dt.now().strftime("%Y-%b-%d_%H:%M:%S")}.json'
            elif symbol[0:4] == 'FINN':
                file_path = f'./{self.__today_folder_name}/FINNIFTY/Options_Intraday_Snapshots/{symbol}_{dt.now().strftime("%Y-%b-%d_%H:%M:%S")}.json'
            else:
                ile_path = f'./{self.__today_folder_name}/MISC/Options_Intraday_Snapshots/{symbol}_{dt.now().strftime("%Y-%b-%d_%H:%M:%S")}.json'
        if Futures_Intraday_Snapshots:
            if symbol[0:4] == 'NIFT':
                file_path = f'./{self.__today_folder_name}/NIFTY/Futures_Intraday_Snapshots/{symbol}_{dt.now().strftime("%Y-%b-%d_%H:%M:%S")}.json'
            elif symbol[0:4] == 'BANK':
                file_path = f'./{self.__today_folder_name}/BANKNIFTY/Futures_Intraday_Snapshots/{symbol}_{dt.now().strftime("%Y-%b-%d_%H:%M:%S")}.json'
            elif symbol[0:4] == 'FINN':
                file_path = f'./{self.__today_folder_name}/FINNIFTY/Futures_Intraday_Snapshots/{symbol}_{dt.now().strftime("%Y-%b-%d_%H:%M:%S")}.json'
            else:
                file_path = f'./{self.__today_folder_name}/MISC/Futures_Intraday_Snapshots/{symbol}_{dt.now().strftime("%Y-%b-%d_%H:%M:%S")}.json'
        if GraphsData:
            if symbol[0:4] == 'NIFT':
                file_path = f'./{self.__today_folder_name}/NIFTY/GraphsData/{symbol}_{dt.now().strftime("%Y-%b-%d_%H:%M:%S")}.json'
            elif symbol[0:4] == 'BANK':
                file_path = f'./{self.__today_folder_name}/BANKNIFTY/GraphsData/{symbol}_{dt.now().strftime("%Y-%b-%d_%H:%M:%S")}.json'
            elif symbol[0:4] == 'FINN':
                file_path = f'./{self.__today_folder_name}/FINNIFTY/GraphsData/{symbol}_{dt.now().strftime("%Y-%b-%d_%H:%M:%S")}.json'
            else:
                file_path = f'./{self.__today_folder_name}/MISC/GraphsData/{symbol}_{dt.now().strftime("%Y-%b-%d_%H:%M:%S")}.json'
        with open(file_path, 'w') as outfile:
            json.dump(self.fetch(url), outfile, indent=4)

    def __fetch_nse(self):
        no_of_retries: int = 11
        for _ in range(no_of_retries):
            try:
                self.__s.cookies.clear()
                output = self.__s.get(self.__NSE, headers=self.__request_headers, timeout=15)
                if output.status_code != 200:
                    output.raise_for_status()
                else:
                    return None
            except requests.exceptions.Timeout:
                # Maybe set up for a retry, or continue in a retry loop
                no_of_retries -= 1
                print(f'>!>!> Retries Left > {no_of_retries} <!<!<')
                sleep(self.__back_off[no_of_retries])
                continue
            except requests.exceptions.TooManyRedirects:
                # Tell the user their URL was bad and try a different one
                no_of_retries -= 1
                print(f'>!>!> Retries Left > {no_of_retries} <!<!<')
                sleep(self.__back_off[no_of_retries])
                continue
            except requests.exceptions.RequestException as e:
                # catastrophic error. bail.
                no_of_retries -= 1
                print(f'>!>!> Retries Left > {no_of_retries} <!<!<')
                sleep(self.__back_off[no_of_retries])
                continue
            except requests.exceptions.HTTPError as err:
                # raise SystemExit(e)
                no_of_retries -= 1
                print(f'>!>!> Retries Left > {no_of_retries} <!<!<')
                sleep(self.__back_off[no_of_retries])
                continue

    def fetch(self, url: str):
        no_of_retries: int = 11
        for _ in range(no_of_retries):
            try:
                print(url)
                output = self.__s.get(url, headers=self.__request_headers, timeout=30)
                if output.status_code != 200:
                    output.raise_for_status()
                else:
                    return output.json()
            except requests.exceptions.Timeout:
                # Maybe set up for a retry, or continue in a retry loop
                no_of_retries -= 1
                print(f'>!>!> Retries Left > {no_of_retries} <!<!<')
                sleep(self.__back_off[no_of_retries])
                self.__fetch_nse()
                continue
            except requests.exceptions.TooManyRedirects:
                # Tell the user their URL was bad and try a different one
                no_of_retries -= 1
                print(f'>!>!> Retries Left > {no_of_retries} <!<!<')
                sleep(self.__back_off[no_of_retries])
                self.__fetch_nse()
                continue
            except requests.exceptions.RequestException as e:
                # catastrophic error. bail.
                no_of_retries -= 1
                print(f'>!>!> Retries Left > {no_of_retries} <!<!<')
                sleep(self.__back_off[no_of_retries])
                self.__fetch_nse()
                continue
            except requests.exceptions.HTTPError as err:
                # raise SystemExit(e)
                no_of_retries -= 1
                # print(f'>!>!> Retries Left > {no_of_retries} <!<!<')
                sleep(self.__back_off[no_of_retries])
                self.__fetch_nse()
                continue

    def fetch_nse_options_json(self, symbols=None, symbols_option=None):
        if (not symbols and not symbols_option):
            symbols = ['NIFTY', 'BANKNIFTY', 'FINNIFTY']
            symbols_option = {'NIFTY': 'nse50_opt', 'BANKNIFTY': 'nifty_bank_opt', 'FINNIFTY': 'finnifty_opt'}
        symbol_option_chain_urls = {f'{symbol}_Option_Chain':f'https://www.nseindia.com/api/option-chain-indices?symbol={symbol}' for symbol in symbols}
        symbol_options_urls = {f'{symbol}_Options_Data':f'https://www.nseindia.com/api/liveEquity-derivatives?index={ticker}' for symbol, ticker in symbols_option.items()}
        for symbol, symbol_url in symbol_option_chain_urls.items():
            self.__save_symbol_file(symbol, symbol_url, Options_Intraday_Snapshots=True)
        for symbol, symbol_url in symbol_options_urls.items():
            self.__save_symbol_file(symbol.upper(), symbol_url, Options_Intraday_Snapshots=True)

    def fetch_nse_futures_json(self, symbols=None, symbols_future=None):
        if not symbols:
            symbols = ['NIFTY', 'BANKNIFTY', 'FINNIFTY']
            symbols_future = {'NIFTY': 'nse50_fut', 'BANKNIFTY': 'nifty_bank_fut', 'FINNIFTY': 'finnifty_fut'}
        symbol_futures_urls = {f'{symbol}_Futures_Data':f'https://www.nseindia.com/api/liveEquity-derivatives?index={ticker}' for symbol, ticker in symbols_future.items()}
        for symbol, symbol_url in symbol_futures_urls.items():
            self.__save_symbol_file(symbol.upper(), symbol_url, Futures_Intraday_Snapshots=True)

    def fetch_nse_options_graphs_json(self, symbols=None):
        if not symbols:
            symbols = ['NIFTY', 'BANKNIFTY', 'FINNIFTY']
        urls = {symbol:f'https://www.nseindia.com/api/option-chain-indices?symbol={symbol}' for symbol in symbols}
        # print(urls)
        symbol_json_data = { symbol:self.fetch(url) for symbol, url in urls.items() }
        symbol_strike_list_current_expiry = { symbol:[[[f'{item["CE"]["identifier"][6:-3]}', f'https://www.nseindia.com/api/chart-databyindex?index={item["CE"]["identifier"]}'], [f'{item["PE"]["identifier"][6:-3]}', f'https://www.nseindia.com/api/chart-databyindex?index={item["PE"]["identifier"]}']] for item in json_data['filtered']['data'] if ("CE" in item and "PE" in item)] for symbol, json_data in symbol_json_data.items() }

        symbol_strike_list_current_expiry = { symbol:{item[0]:item[1] for items in strike_n_url for item in items} for symbol, strike_n_url in symbol_strike_list_current_expiry.items() }
        # pprint(symbol_strike_list_current_expiry)
        for strike_list_urls in symbol_strike_list_current_expiry.values():
            for strike, strike_url in strike_list_urls.items():
                # print(f'{strike} >>> {strike_url}')
                self.__save_symbol_file(strike.upper(), strike_url, GraphsData=True)

    def fetch_nse_all_options_graphs_json(self, symbols=None, returns=None, pprints=None):
        if not symbols:
            symbols = ['NIFTY', 'BANKNIFTY', 'FINNIFTY']
        urls = {symbol:f'https://www.nseindia.com/api/option-chain-indices?symbol={symbol}' for symbol in symbols}
        # print(urls)
        symbol_json_data = { symbol:self.fetch(url) for symbol, url in urls.items() }
        symbol_strike_list_all_expiry = { symbol:[[[f'{item["CE"]["identifier"][6:-3]}', f'https://www.nseindia.com/api/chart-databyindex?index={item["CE"]["identifier"]}'], [f'{item["PE"]["identifier"][6:-3]}', f'https://www.nseindia.com/api/chart-databyindex?index={item["PE"]["identifier"]}']] for item in json_data['records']['data'] if ("CE" in item and "PE" in item)] for symbol, json_data in symbol_json_data.items() }

        symbol_strike_list_all_expiry = { symbol:{item[0]:item[1] for items in strike_n_url for item in items} for symbol, strike_n_url in symbol_strike_list_all_expiry.items() }
        if pprints:
            pprint(symbol_strike_list_all_expiry)
        if returns:
            return symbol_strike_list_all_expiry
        else:
            for strike_list_urls in symbol_strike_list_all_expiry.values():
                for strike, strike_url in strike_list_urls.items():
                    # print(f'{strike} >>> {strike_url}')
                    self.__save_symbol_file(strike.upper(), strike_url, GraphsData=True)

    def fetch_nbf_index_future_json(self):
        index_urls_to_fetch = {
                                'NIFTY50_VS_INDIAVIX': "https://www.nseindia.com/api/getNifty50VsindiaVix",
                                'NIFTY_INDEX_PreMarket': "https://www.nseindia.com/api/chart-databyindex?index=NIFTY%2050&indices=true&preopen=true",
                                'NIFTY_INDEX': "https://www.nseindia.com/api/chart-databyindex?index=NIFTY%2050&indices=true",
                                'NIFTY_NEXT50_INDEX_PreMarket': "https://www.nseindia.com/api/chart-databyindex?index=NIFTY%20NEXT%2050&indices=true&preopen=true",
                                'NIFTY_NEXT50_INDEX': "https://www.nseindia.com/api/chart-databyindex?index=NIFTY%20NEXT%2050&indices=true",
                                'NIFTY_MIDCAP50_INDEX_PreMarket': "https://www.nseindia.com/api/chart-databyindex?index=NIFTY%20MIDCAP%2050&indices=true&preopen=true",
                                'NIFTY_MIDCAP50_INDEX': "https://www.nseindia.com/api/chart-databyindex?index=NIFTY%20MIDCAP%2050&indices=true",
                                'BANKNIFTY_INDEX_PreMarket': "https://www.nseindia.com/api/chart-databyindex?index=NIFTY%20BANK&indices=true&preopen=true",
                                'BANKNIFTY_INDEX': "https://www.nseindia.com/api/chart-databyindex?index=NIFTY%20BANK&indices=true",
                                'FINNIFTY_INDEX_PreMarket': "https://www.nseindia.com/api/chart-databyindex?index=NIFTY%20FINANCIAL%20SERVICES&indices=true&preopen=true",
                                'FINNIFTY_INDEX': "https://www.nseindia.com/api/chart-databyindex?index=NIFTY%20FINANCIAL%20SERVICES&indices=true"
                              }
        symbols = {'NIFTY': 'nse50_fut', 'BANKNIFTY': 'nifty_bank_fut', 'FINNIFTY': 'finnifty_fut'}
        symbol_futures_urls = {symbol:f'https://www.nseindia.com/api/liveEquity-derivatives?index={ticker}' for symbol, ticker in symbols.items()}
        symbol_json_data = {symbol:self.fetch(url) for symbol, url in symbol_futures_urls.items()}
        symbol_futureidx_urls = {f"{item['identifier'][6:-6]}FUT":f"https://www.nseindia.com/api/chart-databyindex?index={item['identifier']}" for symbol, json_data in symbol_json_data.items() for item in json_data['data'] if item['underlying'] == symbol}
        symbol_indexes_futures_urls = index_urls_to_fetch | symbol_futureidx_urls
        for symbol, future_url in symbol_indexes_futures_urls.items():
            self.__save_symbol_file(symbol.upper(), future_url, GraphsData=True)

    def run_daily(self):
        # while (dt.combine(dt.now(), dtt.time(3, 44, 0)) < dt.now() < dt.combine(dt.now(), dtt.time(18, 29, 59))):
        if (dt.combine(dt.now(), dtt.time(3, 44, 0)) < dt.now() < dt.combine(dt.now(), dtt.time(10, 15, 0))):
            self.fetch_nse_options_json()
            self.fetch_nse_futures_json()
        elif (dt.combine(dt.now(), dtt.time(10, 15, 30)) < dt.now() < dt.combine(dt.now(), dtt.time(18, 29, 59))):
            self.__check_or_create_expiry_directories(GraphsData=True)
            self.fetch_nbf_index_future_json()
            self.fetch_nse_options_graphs_json()
            self.fetch_nse_all_options_graphs_json()
        else:
            sys.exit()
            # sleep(random.uniform(35.5, 45.5))

    def custom_command_if_i_need_anything_to_do_cutom(self):
        index_urls_to_fetch = {
                                'NIFTY50_VS_INDIAVIX': "https://www.nseindia.com/api/getNifty50VsindiaVix",
                                'NIFTY_INDEX_PreMarket': "https://www.nseindia.com/api/chart-databyindex?index=NIFTY%2050&indices=true&preopen=true",
                                'NIFTY_INDEX': "https://www.nseindia.com/api/chart-databyindex?index=NIFTY%2050&indices=true",
                                'NIFTY_NEXT50_INDEX_PreMarket': "https://www.nseindia.com/api/chart-databyindex?index=NIFTY%20NEXT%2050&indices=true&preopen=true",
                                'NIFTY_NEXT50_INDEX': "https://www.nseindia.com/api/chart-databyindex?index=NIFTY%20NEXT%2050&indices=true",
                                'NIFTY_MIDCAP50_INDEX_PreMarket': "https://www.nseindia.com/api/chart-databyindex?index=NIFTY%20MIDCAP%2050&indices=true&preopen=true",
                                'NIFTY_MIDCAP50_INDEX': "https://www.nseindia.com/api/chart-databyindex?index=NIFTY%20MIDCAP%2050&indices=true",
                                'BANKNIFTY_INDEX_PreMarket': "https://www.nseindia.com/api/chart-databyindex?index=NIFTY%20BANK&indices=true&preopen=true",
                                'BANKNIFTY_INDEX': "https://www.nseindia.com/api/chart-databyindex?index=NIFTY%20BANK&indices=true",
                                'FINNIFTY_INDEX_PreMarket': "https://www.nseindia.com/api/chart-databyindex?index=NIFTY%20FINANCIAL%20SERVICES&indices=true&preopen=true",
                                'FINNIFTY_INDEX': "https://www.nseindia.com/api/chart-databyindex?index=NIFTY%20FINANCIAL%20SERVICES&indices=true"
                              }
        for symbols, urls in index_urls_to_fetch.items():
            self.__save_symbol_file(symbols.upper(), urls, GraphsData=True)
# =========================================================================================================================== #

if __name__ == '__main__':
    nse = nsefetch()
    nse.run_daily()
    # nse.custom_command_if_i_need_anything_to_do_cutom()
    # nse.fetch_nse_options_json()
    # nse.fetch_nse_futures_json()
    # nse.fetch_nbf_index_future_json()
    # nse.fetch_nse_options_graphs_json()

# =========================================================================================================================== #
