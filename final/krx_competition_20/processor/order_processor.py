import numpy as np
import pandas as pd


class BUYING_ORDER_PROCESSOR:
    """
    BUYING_ORDER_PROCESSOR : 매수주문을 생성하는 클래스
    """

    def __init__(
        self,
        score_df: pd.DataFrame,
        invest_money: float,
        status_df: pd.DataFrame,
        n: int,
    ) -> None:
        """
        BUYING_ORDER_PROCESSOR의 생성자

        :param pd.DataFrame score_df: symbol,score,close를 가진 데이터프레임
        :param float invest_money: 당일 활용 투자 금액
        :param pd.DataFrame status_df: 현재 position과 관련된 정보를 가진 데이터프레임
        :param int n: 당일 투자종목의 갯수
        """
        self.score_df = score_df
        self.invest_money = invest_money
        self.status_df = status_df
        self.n = n

    @staticmethod
    def filter_positioned_symbol(
        score_df: pd.DataFrame, positioned_symbol: list
    ) -> pd.DataFrame:
        """
        이미 position이 있는 종목을 필터링 하는 메소드

        :param pd.DataFrame score_df: symbol,score,close를 가진 데이터프레임
        :param list positioned_symbol: 현재 이미 position이 있는 symbol의 리스트
        """
        filtered_score_df = score_df[~(score_df["SYMBOL"].isin(positioned_symbol))]
        return filtered_score_df

    @staticmethod
    def get_filtered_score_df(score_df: pd.DataFrame, n: int) -> pd.DataFrame:
        """
        score_df의 상위 n개의 row를 추출하는 메서드

        :param pd.DataFrame score_df: symbol,score,close를 가진 데이터프레임
        :param int n: 당일 투자종목의 갯수
        """
        high_limit = np.percentile(score_df["SCORE"], 95)
        low_limit = np.percentile(score_df["SCORE"], 85)
        if n:
            filtered_score_df = score_df[
                (score_df["SCORE"] < high_limit) & (score_df["SCORE"] > low_limit)
            ].nlargest(n, "SCORE")
        else:
            filtered_score_df = score_df[
                (score_df["SCORE"] < high_limit) & (score_df["SCORE"] > low_limit)
            ]
        return filtered_score_df

    @staticmethod
    def append_price_invest(
        high_score_df: pd.DataFrame, invest_money: float
    ) -> pd.DataFrame:
        """
        당일 활용 투자 금액을 score 기준으로 분배한 column을 생성하는 메서드

        :param pd.DataFrame high_score_df: score_df의 score 상위 데이터프레임
        :param float invest_money: 당일 활용 투자 금액
        """
        high_score_df["PRICE_INVEST"] = (
            high_score_df["SCORE"] / high_score_df["SCORE"].sum()
        ) * invest_money
        return high_score_df

    @staticmethod
    def append_cnt_invest(high_score_df: pd.DataFrame) -> pd.DataFrame:
        """
        당일 활용 투자 금액 최근 종가로 나누어 주문 갯수를 column으로 생성하는 메서드

        :param pd.DataFrame high_score_df: score_df의 score 상위 데이터프레임
        """
        high_score_df["CNT_INVEST"] = (
            high_score_df["PRICE_INVEST"] // high_score_df["CLOSE"]
        )
        return high_score_df

    @staticmethod
    def get_order_from_df(df: pd.DataFrame) -> list:
        """
        데이터 프레임에서 signiture에 맞게 주문 list를 추출하는 메서드

        :param pd.DataFrame df: [SYMBOL,CNT_INVEST]를 가진 데이터프레임
        """
        orders = list(
            df.set_index("SYMBOL")["CNT_INVEST"].astype(int).to_dict().items()
        )
        return orders

    def __call__(self) -> list:
        """
        BUYING_ORDER_PROCESSOR의 pipeline을 진행하는 메서드

        """
        score_df = self.score_df
        invest_money = self.invest_money
        status_df = self.status_df
        n = self.n

        positioned_symbol = sorted(set(status_df["SYMBOL"]))
        filtered_positioned_df = self.filter_positioned_symbol(
            score_df, positioned_symbol
        )

        filtered_score_df = self.get_filtered_score_df(filtered_positioned_df, n)
        filtered_score_df = self.append_price_invest(filtered_score_df, invest_money)
        filtered_score_df = self.append_cnt_invest(filtered_score_df)
        buying_order = self.get_order_from_df(filtered_score_df)
        return buying_order


class SELLING_ORDER_PROCESSOR:
    """
    SELLING_ORDER_PROCESSOR : 매도주문을 추출하는 클래스
    """

    def __init__(
        self,
        status_df: pd.DataFrame,
        CFG: dict = {
            "upper_limit": 9,
            "lower_limit": -3,
        },
    ) -> None:
        """
        SELLING_ORDER_PROCESSOR의 생성자

        :param pd.DataFrame status_df: 현재 position과 관련된 정보를 가진 데이터프레임
        :param dict CFG: 매도로직을 위한 한계선 dictionary
        """
        self.status_df = status_df
        self.CFG = CFG

    @staticmethod
    def append_profit_loss(status_df: pd.DataFrame) -> pd.DataFrame:
        def calc_profit_loss(trade_price, current_price):
            profit_loss = ((current_price - trade_price) / trade_price) * 100
            return profit_loss

        status_df["PROFIT_LOSS"] = status_df.apply(
            lambda x: calc_profit_loss(x.TRADE_PRICE, x.CURRENT_PRICE), axis=1
        )
        return status_df

    @staticmethod
    def get_filter_status_df(status_df: pd.DataFrame, CFG: dict) -> pd.DataFrame:
        filter_status_df = status_df[
            (status_df["PROFIT_LOSS"] > CFG["upper_limit"])
            | (status_df["PROFIT_LOSS"] < CFG["lower_limit"])
        ]
        return filter_status_df

    @staticmethod
    def get_order_from_df(df: pd.DataFrame) -> list:
        """
        데이터 프레임에서 signiture에 맞게 주문 list를 추출하는 메서드

        :param pd.DataFrame df: [SYMBOL,CURRENT_QTY]를 가진 데이터프레임
        """
        orders = list(
            df.set_index("SYMBOL")["CURRENT_QTY"]
            .apply(lambda x: x * -1)
            .astype(int)
            .to_dict()
            .items()
        )
        return orders

    def __call__(self) -> list:
        """
        SELLING_ORDER_PROCESSOR의 pipeline을 진행하는 메서드
        """
        status_df = self.status_df
        CFG = self.CFG

        status_df = self.append_profit_loss(status_df)
        filter_status_df = self.get_filter_status_df(status_df, CFG)
        selling_orders = self.get_order_from_df(filter_status_df)

        return selling_orders


def merge_order(buying_orders: list, selling_orders: list) -> list[tuple[str, int]]:
    total_order = buying_orders + selling_orders
    total_order_df = pd.DataFrame(total_order, columns=["SYMBOL", "ORDER"])
    symbols_and_orders = list(
        total_order_df.groupby("SYMBOL").sum().squeeze().astype(int).to_dict().items()
    )
    return symbols_and_orders
