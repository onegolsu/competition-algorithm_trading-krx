import datetime as dt

import pandas as pd

import kquant as kq


class SYMBOL_LOADER:
    """
    SYMBOL_LOADER : 거래가능한 주식 symbol을 필터-추출하는 클래스
    """

    @staticmethod
    def load_symbols_df() -> pd.DataFrame:
        """
        한국거래소 종목 목록 데이터프레임을 호출하는 메서드

        :return : 한국거래소 종목 목록 데이터프레임
        :rtype : pd.DataFrame
        """
        symbols_df = kq.symbol_stock()
        return symbols_df

    class SYMBOL_FILTER:
        """
        SYMBOl_FILTER : symbols를 filtering 하는 클래스
        """

        @staticmethod
        def filter__market(symbols_df: pd.DataFrame) -> pd.DataFrame:
            """
            market에 대한 필터링을 진행하는 메서드

            :param pd.DataFrame : symbols_df : 한국거래소 종목 목록 데이터프레임
            :return: MARKET이 [코스닥, 유가증권]에 속하는 데이터프레임
            :rtype: pd.DataFrame
            """
            filtered_symbols_df = symbols_df[
                (symbols_df["MARKET"].isin(["코스닥", "유가증권"]))
            ].copy()
            return filtered_symbols_df

        @staticmethod
        def filter__admin_issue(symbols_df: pd.DataFrame) -> pd.DataFrame:
            """
            ADMIN_ISSUE에 대한 필터링을 진행하는 메서드

            :param pd.DataFrame : symbols_df : 한국거래소 종목 목록 데이터프레임
            :return: ADMIN_ISSUE가 0인 데이터프레임
            :rtype: pd.DataFrame
            """
            filtered_symbols_df = symbols_df[(symbols_df["ADMIN_ISSUE"] == 0)].copy()
            return filtered_symbols_df

        @staticmethod
        def filter_sec_type(symbols_df: pd.DataFrame) -> pd.DataFrame:
            """
            SEC_TYPE에 대한 필터링을 진행하는 메서드

            :param pd.DataFrame : symbols_df : 한국거래소 종목 목록 데이터프레임
            :return: SEC_TYPE이 [ST, EF, EN]에 속하는 데이터프레임
            :rtype: pd.DataFrame
            """
            filtered_symbols_df = symbols_df[
                symbols_df["SEC_TYPE"].isin(["ST", "EF", "EN"])
            ].copy()
            return filtered_symbols_df

    def filter_symbols_df(self, symbols_df: pd.DataFrame) -> pd.DataFrame:
        """
        symbol_df 에 대한 필터링을 진행하는 메서드

        :param pd.DataFrame : symbols_df : 한국거래소 종목 목록 데이터프레임
        :return: SYMBOL_FILTER의 메서드를 거친 데이터프레임
        :rtype: pd.DataFrame
        """
        symbol_filter = self.SYMBOL_FILTER()
        filtered_symbols_df = symbol_filter.filter__market(symbols_df)
        filtered_symbols_df = symbol_filter.filter__admin_issue(filtered_symbols_df)
        filtered_symbols_df = symbol_filter.filter_sec_type(filtered_symbols_df)
        return filtered_symbols_df

    @staticmethod
    def get_symbols(symbols_df: pd.DataFrame) -> list:
        """
        symbols_df의 symbol을 중복을 제거하여 추출하는 메서드

        :param pd.DataFrame : symbols_df : 한국거래소 종목 목록 데이터프레임
        :return: symbols
        :rtype: list
        """
        symbols = sorted(set(symbols_df["SYMBOL"]))
        return symbols

    # SYMBOL_LOADER PIPELINE
    def __call__(self) -> list:
        """
        SYMBOL_LOADER의 파이프라인을 제공하는 메서드

        :return: 필터를 거친 symbols
        :rtype: list
        """
        symbols_df = self.load_symbols_df()
        filtered_symbols_df = self.filter_symbols_df(symbols_df)
        symbols = self.get_symbols(filtered_symbols_df)
        return symbols


class FUNDAMENTAL_LOADER:
    """
    FUNDAMENTAL_LOADER : fundamental_analysis를 위한 정보를 추출하는 클래스
    """

    def __init__(self, symbol: str, date: dt.date) -> None:
        """
        FUNDAMENTAL_LOADER의 생성자

        :param str symbol: stock의 symbol 입니다.
        :param datetime.date date: 현재 날짜 입니다.

        :attr : pd.DataFrmae daily_stock_df : 현재 날짜 기준 가장 최근 stock데이터 입니다.
        """
        self.symbol = symbol
        self.date = date
        self.daily_stock_df = kq.daily_stock(
            symbol,
            start_date=date - dt.timedelta(days=7),
            end_date=date,
        )

    def load_recent_close(self) -> float:
        """
        가장 최근 종가를 추출합니다.
        :return: 종가
        :rtype: float
        """
        daily_stock_df = self.daily_stock_df
        _close = daily_stock_df.sort_values("DATE").tail(1)["CLOSE"].values[0]
        return _close

    def load_recent_marketcap(self) -> float:
        """
        가장 최근 시가총액을 추출합니다.

        :return: 시가총액
        :rtype: float
        """
        daily_stock_df = self.daily_stock_df
        _marketcap = daily_stock_df.sort_values("DATE").tail(1)["MARKETCAP"].values[0]
        return float(_marketcap)

    def load_recent_netprofit(self) -> float:
        """
        공시자료 중 가장 최근 당기순이익을 추출합니다.

        :return: 당기순이익
        :rtype: float
        """
        netprofit_df = kq.account_history(
            symbol=self.symbol, account_code="122700", period="q"
        )
        netprofit_df.sort_values("YEARMONTH", inplace=True)
        _netprofit = netprofit_df.tail(1)["VALUE"].values[0] * 1000
        return float(_netprofit)

    def load_recent_assets(self) -> float:
        """
        공시자료 중 가장 최근 총 자산를 추출합니다.

        :return: 총 자산
        :rtype: float
        """
        assets_df = kq.account_history(
            symbol=self.symbol, account_code="111000", period="q"
        )
        assets_df.sort_values("YEARMONTH", inplace=True)
        _assets = assets_df.tail(1)["VALUE"].values[0] * 1000
        return float(_assets)

    def load_recent_current_assets(self) -> float:
        """
        공시자료 중 가장 최근 유동 자산를 추출합니다.

        :return: 유동 자산
        :rtype: float
        """
        current_assets_df = kq.account_history(
            symbol=self.symbol, account_code="111100", period="q"
        )
        current_assets_df.sort_values("YEARMONTH", inplace=True)
        _current_assets = current_assets_df.tail(1)["VALUE"].values[0] * 1000
        return float(_current_assets)

    def load_recent_liabilities(self) -> float:
        """
        공시자료 중 가장 최근 총 부채를 추출합니다.

        :return: 총 부채
        :rtype: float
        """
        liabilities_df = kq.account_history(
            symbol=self.symbol, account_code="113000", period="q"
        )
        liabilities_df.sort_values("YEARMONTH", inplace=True)
        _liabilities = liabilities_df.tail(1)["VALUE"].values[0] * 1000
        return float(_liabilities)

    def load_recent_equity(self) -> float:
        """
        공시자료 중 가장 최근 총 자본(총 자산 - 총 부채)를 추출합니다.

        :return: 총 자본(총 자산 - 총 부채)
        :rtype: float
        """
        equity_df = kq.account_history(
            symbol=self.symbol, account_code="115000", period="q"
        )
        equity_df.sort_values("YEARMONTH", inplace=True)
        _equity = equity_df.tail(1)["VALUE"].values[0] * 1000
        return float(_equity)

    def load_recent_EBITDA(self) -> float:
        """
        공시자료 중 가장 최근 EBITDA를 추출합니다.

        :return: EBITDA
        :rtype: float
        """
        ebitda_df = kq.account_history(
            symbol=self.symbol, account_code="123000", period="q"
        )
        ebitda_df.sort_values("YEARMONTH", inplace=True)
        _ebitda = ebitda_df.tail(1)["VALUE"].values[0] * 1000
        return float(_ebitda)

    def __call__(self) -> dict:
        """
        fundmanetal analysis를 위해 필요한 데이터를 가져와서 dictionary를 반환한다.

        :return: fundamental analysis를 위한 데이터 dictionary
        :rtype: dict
        """
        _close = self.load_recent_close()
        _marketcap = self.load_recent_marketcap()
        _netprofit = self.load_recent_netprofit()
        _assets = self.load_recent_assets()
        _equity = self.load_recent_equity()
        return {
            "SYMBOL": self.symbol,
            "CLOSE": _close,
            "MARKETCAP": _marketcap,
            "NETPROFIT": _netprofit,
            "ASSETS": _assets,
            "EQUITY": _equity,
        }
