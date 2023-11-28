import os
import json
import pandas as pd

from .data.symbol_sector_dict import get_symbol_sector_dict

class SYMBOL_SECTOR_PROCESSOR:
    """
    SYMBOL_SECTOR_PROCESSOR : symbol의 sector_code를 찾아내는 클래스
    """

    def __init__(
        self,
        symbols: list,
        CFG: dict = {
            "sector_symbol_n": 30,
            "sample_n": 15,
        },
    ) -> None:
        """
        SYMBOL_SECTOR_PROCESSOR의 생성자

        :param list symbols: sector_code를 찾을 symbol들
        :param dict CFG: sector_code를 찾을 symbol들
        """
        self.symbols = symbols
        self.CFG = CFG

    @staticmethod
    def load_symbol_sector_dict() -> dict:
        """
        symbol:sector_code 딕셔너리를 읽어오는 메서드

        :return: symbol-sector_code 딕셔너리
        :rtype: dict
        """
        symbol_sector_dict = get_symbol_sector_dict()
        return symbol_sector_dict

    @staticmethod
    def format_symbol_df(symbols: list) -> pd.DataFrame:
        """
        symbol_df를 생성하는 메서드

        :param list symbols: sector_code를 찾을 symbol들
        :return: symbol의 데이터프레임
        :rtype: pd.DataFrame
        """
        symbol_df = pd.DataFrame(symbols, columns=["SYMBOL"])
        return symbol_df

    @staticmethod
    def append_sector(
        symbol_df: pd.DataFrame, symbol_sector_dict: dict
    ) -> pd.DataFrame:
        """
        symbol_df에 sector를 추가하는 메서드

        :param pd.DataFrame symbol_df: symbol의 데이터프레임
        :param dict symbol_sector_dict: symbol:sector의 딕셔너리
        :return: sector가 추가된 symbol_df
        :rtype: pd.DataFrame
        """
        symbol_df["SECTOR"] = symbol_df["SYMBOL"].map(symbol_sector_dict)
        return symbol_df

    @staticmethod
    def get_filtered_sectors(symbol_df: pd.DataFrame, n: int) -> list:
        """
        symbol_df를 필터링 하여 sector를 제공하는 메서드

        :param pd.DataFrame symbol_df: symbol의 데이터프레임
        :param int n: n개 이하의 symbol이 있는 sector 제거
        :return: 필터링 된 symbol_df의 sector
        :rtype: list
        """
        symbol_groupby = symbol_df.groupby("SECTOR")
        filtered_sectors = symbol_groupby.count()["SYMBOL"][
            symbol_groupby.count()["SYMBOL"] > n
        ].index
        return filtered_sectors

    @staticmethod
    def get_filtered_symbol_df(
        symbol_df: pd.DataFrame, filtered_sectors: list
    ) -> pd.DataFrame:
        """
        symbol_df를 필터링 하는 메서드

        :param pd.DataFrame symbol_df: symbol의 데이터프레임
        :param list filtered_sectors: filtered된 sector들
        :return: 필터링 된 symbol_df
        :rtype: pd.DataFrame
        """
        filtered_symbol_df = symbol_df[symbol_df["SECTOR"].isin(filtered_sectors)]
        return filtered_symbol_df

    @staticmethod
    def get_sampled_symbol_df(filtered_symbol_df: pd.DataFrame, n: int) -> pd.DataFrame:
        """
        각 sector별로 n개를 sampling하는 메서드

        :param pd.DataFrame filtered_symbol_df: filter된 symbol_df
        :param int n: 샘플링 갯수
        :return: 샘플링 된 symbol_df
        :rtype: pd.DataFrame
        """
        sampled_symbol_df = filtered_symbol_df.groupby("SECTOR").sample(n)
        return sampled_symbol_df

    def __call__(self) -> pd.DataFrame:
        """
        SYMBOL_SECTOR_PROCESSOR의 파이프라인을 제공하는 메서드

        :return: 샘플링 된 symbol_df
        :rtype: pd.DataFrame
        """
        symbols = self.symbols
        CFG = self.CFG

        symbol_sector_dict = self.load_symbol_sector_dict()

        symbol_df = self.format_symbol_df(symbols=symbols)
        symbol_df = self.append_sector(
            symbol_df=symbol_df, symbol_sector_dict=symbol_sector_dict
        )

        filtered_sectors = self.get_filtered_sectors(
            symbol_df=symbol_df, n=CFG["sector_symbol_n"]
        )
        filtered_symbol_df = self.get_filtered_symbol_df(
            symbol_df=symbol_df, filtered_sectors=filtered_sectors
        )
        sampled_symbol_df = self.get_sampled_symbol_df(
            filtered_symbol_df=filtered_symbol_df, n=CFG["sample_n"]
        )
        return sampled_symbol_df
