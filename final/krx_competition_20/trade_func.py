# TRADE_FUNC
import random
import logging
import datetime as dt

import pandas as pd
import kquant as kq
from sklearn.preprocessing import MinMaxScaler

from .loader.static_loader import STATUS_LOADER
from .loader.api_loader import SYMBOL_LOADER, FUNDAMENTAL_LOADER

from .processor.sector_processor import SYMBOL_SECTOR_PROCESSOR
from .processor.model_processor import PBR_PROCESSOR, PER_PROCESSOR
from .processor.model_processor import SCORE_PROCESSOR

from .processor.order_processor import BUYING_ORDER_PROCESSOR, SELLING_ORDER_PROCESSOR
from .processor.order_processor import merge_order


def trade_func(
    date: dt.date,
    dict_df_result: dict[str, pd.DataFrame],
    dict_df_position: dict[str, pd.DataFrame],
    logger: logging.Logger,
) -> list[tuple[str, int]]:
    """
    CFG : trade_fun의 파라미터 조정
    """
    CFG = {
        "cash_percentage": 0.75,  # 1일 투자 금액 (보유 현금 * 0.75)
        "buying_order_n": None,  # 1일 구매 stock 종류수
    }
    """
    STATUS_LOADER
    """
    status_loader = STATUS_LOADER(dict_df_result, dict_df_position)

    current_cash = status_loader.get_current_cash()
    invest_money = current_cash * CFG["cash_percentage"]

    status_df = status_loader.get_status_df()
    """
    SYMBOL_LOADER
    """
    symbol_loader = SYMBOL_LOADER()
    total_symbols = symbol_loader()

    """
    SYMBOL_SECTOR_PROCESSOR
    """
    symbol_sector_processor = SYMBOL_SECTOR_PROCESSOR(
        total_symbols,
        {
            "sector_symbol_n": 25,
            "sample_n": 20,
        },
    )
    sampled_symbol_df = symbol_sector_processor()

    """
    SCORE_PROCESSOR
    """
    sectors = sorted(set(sampled_symbol_df["SECTOR"]))

    score_df_list = list()
    for sector in sectors:
        _sector_symbol_df = sampled_symbol_df[sampled_symbol_df["SECTOR"] == sector]
        _symbols = sorted(set(_sector_symbol_df["SYMBOL"]))

        score_processor = SCORE_PROCESSOR(_symbols, date)
        _score_df = score_processor()
        score_df_list.append(_score_df)

    score_df = pd.concat(score_df_list)
    """
    BUYING_ORDER_PROCESSOR
    """
    buying_order_processor = BUYING_ORDER_PROCESSOR(
        score_df, invest_money, status_df, CFG["buying_order_n"]
    )
    buying_orders = buying_order_processor()

    selling_order_processor = SELLING_ORDER_PROCESSOR(
        status_df, {"upper_limit": 8, "lower_limit": -3}
    )
    selling_orders = selling_order_processor()

    symbols_and_orders = merge_order(buying_orders, selling_orders)
    return symbols_and_orders
