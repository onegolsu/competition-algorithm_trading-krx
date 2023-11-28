import json

import pandas as pd


class STATUS_LOADER:
    """
    STATUS_LOADER : 상태 정보 추출 클래스
    """

    def __init__(self, dict_df_result: dict, dict_df_position: dict) -> None:
        """
        STATUS_LOADER의 생성자

        :param dict dict_df_result: dict_df_result 입니다.
        :param dict dict_df_position: dict_df_position 입니다.
        :return: None
        :rtype: None
        """
        self.dict_df_result = dict_df_result
        self.dict_df_position = dict_df_position

    def get_current_cash(self) -> float:
        """
        현재 보유 cash를 반환하는 메서드

        :return : 현재 보유 cash
        :rtype: float
        """
        _dict_df_result = self.dict_df_result
        try:
            _df_result_total = _dict_df_result["TOTAL"]
            _current_cash = (
                _df_result_total.sort_values("DATE").tail(1)["CASH"].values[0]
            )
            return _current_cash
        except:
            return 1_000_000_000.0

    def get_status_df(self) -> pd.DataFrame:
        """
        현재 보유 position 관련 정보를 반환하는 메서드

        :return: 보유한 symbol, 갯수, 구매가격, 현재 가격을 데이터프레임 형태로 가져옵니다.
        :rtype: pd.DataFrame
        """
        current_symbol_list = list()
        _dict_df_result = self.dict_df_result
        _dict_df_position = self.dict_df_position

        _total_symbols = sorted(_dict_df_position.keys())

        for _symbol in _total_symbols:
            try:
                _symbol_result_df = _dict_df_result[_symbol]
                _symbol_position_df = _dict_df_position[_symbol]

                _current_price = (
                    _symbol_result_df.sort_values("DATE").tail(1)["PRICE"].values[0]
                )
                _trade_price = _symbol_position_df["TRADE_PRICE"].values[0]
                _current_qty = _symbol_position_df["QTY"].values[0]

                current_symbol_list.append(
                    {
                        "SYMBOL": _symbol,
                        "CURRENT_QTY": _current_qty,
                        "CURRENT_PRICE": _current_price,
                        "TRADE_PRICE": _trade_price,
                    }
                )
            except:
                pass
        return pd.DataFrame(
            current_symbol_list,
            columns=["SYMBOL", "CURRENT_QTY", "CURRENT_PRICE", "TRADE_PRICE"],
        )
