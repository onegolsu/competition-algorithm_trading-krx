import json
import datetime

import pandas as pd
from sklearn.preprocessing import MinMaxScaler

from ..loader.api_loader import FUNDAMENTAL_LOADER


class PBR_PROCESSOR:
    """
    PBR_PROCESSOR : PBR에 대한 정보를 SCORE로 정제하여 반환하는 클래스
    """

    def __init__(self, fundamental_df: pd.DataFrame) -> None:
        self.fundamental_df = fundamental_df

    @staticmethod
    def append_pbr(fundamental_df: pd.DataFrame) -> pd.DataFrame:
        """
        PBR을 추가하는 메서드

        :param pd.DataFrame fundamental_df: fundamental 데이터를 가지고 있는 데이터프레임
        :return: PBR가 추가된 fundamental_df
        :rtype: pd.DataFrame
        """
        fundamental_df["PBR"] = fundamental_df["MARKETCAP"] / (fundamental_df["EQUITY"])
        return fundamental_df

    @staticmethod
    def filter_negative_pbr(fundamental_df: pd.DataFrame) -> pd.DataFrame:
        """
        PBR이 음수인 데이터를 제거하는 메서드

        :param pd.DataFrame fundamental_df: fundamental 데이터를 가지고 있는 데이터프레임
        :return: PBR이 음수가 아닌 fundamental_df
        :rtype: pd.DataFrame
        """
        fundamental_df = fundamental_df[fundamental_df["PBR"] > 0]
        return fundamental_df

    @staticmethod
    def append_score(fundamental_df: pd.DataFrame) -> pd.DataFrame:
        """
        PBR을 기준으로, 낮은 PER일 수록 큰 SCORE를 주는 메서드

        :param pd.DataFrame fundamental_df: fundamental 데이터를 가지고 있는 데이터프레임
        :return: SCORE가 추가된 fundamental_df
        :rtype: pd.DataFrame
        """
        fundamental_df["PBR_SCORE"] = (
            fundamental_df["PBR"].sum() / fundamental_df["PBR"]
        )
        return fundamental_df

    def __call__(self) -> pd.DataFrame:
        """
        PBR기반의 score 추가한 데이터프레임을 반환하는 메서드

        :return: SCORE가 추가된 fundamental_df
        :rtype: pd.DataFrame
        """
        fundamental_df = self.fundamental_df

        fundamental_df = self.append_pbr(fundamental_df)
        fundamental_df = self.filter_negative_pbr(fundamental_df)
        fundamental_df = self.append_score(fundamental_df)

        score_df = fundamental_df.loc[:, ["SYMBOL", "PBR_SCORE"]]
        return score_df


class PER_PROCESSOR:
    """
    PER_PROCESSOR : PER에 대한 정보를 SCORE로 정제하여 반환하는 클래스
    """

    def __init__(self, fundamental_df: pd.DataFrame) -> None:
        """
        PER_PROCESSOR의 생성자

        :param pd.DataFrame fundamental_df: fundamental 데이터를 가지고 있는 데이터프레임
        """
        self.fundamental_df = fundamental_df

    @staticmethod
    def append_per(fundamental_df: pd.DataFrame) -> pd.DataFrame:
        """
        PER을 추가하는 메서드

        :param pd.DataFrame fundamental_df: fundamental 데이터를 가지고 있는 데이터프레임
        :return: PER가 추가된 fundamental_df
        :rtype: pd.DataFrame
        """
        fundamental_df["PER"] = fundamental_df["MARKETCAP"] / (
            fundamental_df["NETPROFIT"]
        )
        return fundamental_df

    @staticmethod
    def filter_negative_per(fundamental_df: pd.DataFrame) -> pd.DataFrame:
        """
        PER이 음수인 데이터를 제거하는 메서드

        :param pd.DataFrame fundamental_df: fundamental 데이터를 가지고 있는 데이터프레임
        :return: PER이 음수가 아닌 fundamental_df
        :rtype: pd.DataFrame
        """
        fundamental_df = fundamental_df[fundamental_df["PER"] > 0]
        return fundamental_df

    @staticmethod
    def append_score(fundamental_df: pd.DataFrame) -> pd.DataFrame:
        """
        PER을 기준으로, 낮은 PER일 수록 큰 SCORE를 주는 메서드

        :param pd.DataFrame fundamental_df: fundamental 데이터를 가지고 있는 데이터프레임
        :return: SCORE가 추가된 fundamental_df
        :rtype: pd.DataFrame
        """
        fundamental_df["PER_SCORE"] = (
            fundamental_df["PER"].sum() / fundamental_df["PER"]
        )
        return fundamental_df

    def __call__(self) -> pd.DataFrame:
        """
        PER기반의 score 추가한 데이터프레임을 반환하는 메서드

        :return: SCORE가 추가된 fundamental_df
        :rtype: pd.DataFrame
        """
        fundamental_df = self.fundamental_df

        fundamental_df = self.append_per(fundamental_df)
        fundamental_df = self.filter_negative_per(fundamental_df)
        fundamental_df = self.append_score(fundamental_df)

        score_df = fundamental_df.loc[:, ["SYMBOL", "PER_SCORE"]]
        return score_df


class SCORE_PROCESSOR:
    """
    SCORE_PROCESSOR : PBR / PER을 종합하여 SCORE 데이터를 제공하는 클래스
    """

    def __init__(
        self,
        symbols: list,
        date: datetime.date,
        CFG: dict = {"pbr_ratio": 1, "per_ratio": 0.3},
    ) -> None:
        """
        SCORE_PROCESSOR의 생성자

        :param list symbols: score 확인할 symbols
        :param datetime.date date: 매매일 날짜
        :param dict  CFG: score_processor 파라미터
        """
        self.symbols = symbols
        self.date = date
        self.CFG = CFG

    @staticmethod
    def load_fundamental_df(symbols: list, date: datetime.date) -> pd.DataFrame:
        """
        기본적 분석을 위한 fundamental_df를 load하는 메서드

        :param list symbols: score 확인할 symbols
        :param datetime.date date: 매매일 날짜
        :return: 기본적 분석을 위한 데이터
        :rtype: pd.DataFrame
        """
        fundamental_data_list = list()
        for symbol in symbols:
            try:
                _fundamental_loader = FUNDAMENTAL_LOADER(symbol, date)
                _fundamental_data = _fundamental_loader()
                fundamental_data_list.append(_fundamental_data)
            except:
                pass
        fundamental_df = pd.DataFrame(fundamental_data_list)
        return fundamental_df

    @staticmethod
    def get_symbol_close_dict(fundamental_df: pd.DataFrame) -> dict:
        """
        종목:종가 사전을 추출하는 메서드

        :param pd.DataFrame fundamental_df: 기본적 분석 관련 데이터
        :return: {종목:종가}의 dictionary
        :rtype: dict
        """
        symbol_close_dict = fundamental_df.set_index("SYMBOL")["CLOSE"].to_dict()
        return symbol_close_dict

    @staticmethod
    def get_pbr_score_df(fundamental_df: pd.DataFrame) -> pd.DataFrame:
        """
        pbr 스코어를 제공하는 메서드

        :param pd.DataFrame fundamental_df: 기본적 분석 관련 데이터
        :return: pbr 점수가 있는 데이터
        :rtype: pd.DataFrame
        """
        pbr_processor = PBR_PROCESSOR(fundamental_df)
        pbr_score_df = pbr_processor()
        return pbr_score_df

    @staticmethod
    def get_per_score_df(fundamental_df: pd.DataFrame) -> pd.DataFrame:
        """
        per 스코어를 제공하는 메서드

        :param pd.DataFrame fundamental_df: 기본적 분석 관련 데이터
        :return: per 점수가 있는 데이터
        :rtype: pd.DataFrame
        """
        per_processor = PER_PROCESSOR(fundamental_df)
        per_score_df = per_processor()
        return per_score_df

    @staticmethod
    def format_score_df(
        symbol_close_dict: dict,
        pbr_score_df: pd.DataFrame,
        per_score_df: pd.DataFrame,
        CFG: dict,
    ) -> pd.DataFrame:
        """
        score_df를 반환하는 메서드

        :param dict symbol_close_dict: 종목의 종가를 가진 딕셔너리
        :param pd.DataFrame pbr_score_df: pbr 점수 관련 데이터
        :param pd.DataFrame per_score_df: per 점수 관련 데이터
        :param dict CFG: pbr, per의 weight 파라미터를 위한 딕셔너리

        :return: 총합(pbr,per) 데이터
        :rtype: pd.DataFrame
        """
        score_df = pbr_score_df.merge(
            per_score_df.loc[:, ["SYMBOL", "PER_SCORE"]], on="SYMBOL"
        )

        mms = MinMaxScaler()
        score_df.iloc[:, 1:] = mms.fit_transform(score_df.iloc[:, 1:])

        score_df["SCORE"] = (
            score_df["PBR_SCORE"] * CFG["pbr_ratio"]
            + score_df["PER_SCORE"] * CFG["per_ratio"]
        )
        score_df["CLOSE"] = score_df["SYMBOL"].map(symbol_close_dict)
        return score_df

    def __call__(self) -> pd.DataFrame:
        """
        SCORE_PROCESSOR의 파이프라인을 제공하는 메서드

        :return: 총합(pbr,per) 데이터
        :rtype: pd.DataFrame
        """
        symbols = self.symbols
        date = self.date
        CFG = self.CFG

        fundamental_df = self.load_fundamental_df(symbols, date)
        symbol_close_dict = self.get_symbol_close_dict(fundamental_df)

        pbr_score_df = self.get_pbr_score_df(fundamental_df)
        per_score_df = self.get_per_score_df(fundamental_df)

        score_df = self.format_score_df(
            symbol_close_dict, pbr_score_df, per_score_df, CFG
        )
        return score_df
