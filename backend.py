from datetime import datetime
import json
import os

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse

import akshare as ak
import tushare as ts

app = FastAPI(title="AkShare Limit Up API")

def _load_config():
    try:
        with open("config.json", "r", encoding="utf-8") as file:
            return json.load(file)
    except FileNotFoundError:
        return {}
    except json.JSONDecodeError:
        return {}


CONFIG = _load_config()
TS_TOKEN = os.getenv("TUSHARE_TOKEN", "") or CONFIG.get("tushare_token", "")
if TS_TOKEN:
    ts.set_token(TS_TOKEN)
    ts_pro = ts.pro_api()
else:
    ts_pro = None

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def home():
    return FileResponse("index.html")


@app.get("/health")
def health():
    return {"ok": True}


@app.get("/api/limit-up")
def get_limit_up(
    date: str = Query("", description="交易日期，格式 YYYYMMDD，可为空"),
):
    if date:
        data = ak.stock_zt_pool_em(date=date)
    else:
        data = ak.stock_zt_pool_em()

    if data.empty:
        return {"data": []}

    result = []
    for _, row in data.iterrows():
        result.append(
            {
                "code": row.get("代码"),
                "name": row.get("名称"),
                "price": row.get("最新价"),
                "change": row.get("涨跌幅"),
                "height": row.get("连板数"),
                "turnover": row.get("成交额"),
            }
        )

    return {"data": result}


def _parse_time(value):
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    try:
        return datetime.strptime(str(value), "%H:%M:%S")
    except ValueError:
        try:
            return datetime.strptime(str(value), "%H:%M")
        except ValueError:
            return None


def _safe_float(value):
    try:
        return float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return None


def _safe_int(value):
    try:
        return int(float(str(value).replace(",", "")))
    except (TypeError, ValueError):
        return None


def _is_first_board(row, first_board_only: bool):
    if not first_board_only:
        return True
    height = _safe_int(row.get("连板数"))
    return height == 1


def _normalize_ts_code(code: str):
    if not code:
        return None
    text = str(code)
    if "." in text:
        return text
    if text.startswith("6"):
        return f"{text}.SH"
    return f"{text}.SZ"


def _get_stock_basic_map():
    # 获取股票基础信息映射：名称与上市状态
    if ts_pro is None:
        return {}
    data = ts_pro.stock_basic(fields="ts_code,name,list_status")
    if data is None or data.empty:
        return {}
    return data.set_index("ts_code")[["name", "list_status"]].to_dict(orient="index")


def _get_circ_mv_map():
    # 获取最新交易日流通市值（单位：万元）
    if ts_pro is None:
        return {}
    trade_dates = _get_latest_trade_dates(limit=1)
    if not trade_dates:
        return {}
    data = ts_pro.daily_basic(trade_date=trade_dates[-1], fields="ts_code,circ_mv")
    if data is None or data.empty:
        return {}
    return data.set_index("ts_code")["circ_mv"].to_dict()


def _is_excluded_code(code: str, name: str, basic_map: dict, circ_mv_map: dict):
    # 排除指定代码段、ST/退市、流通市值不在 10~200 亿范围的股票
    if not code:
        return True
    pure_code = str(code).replace(".SH", "").replace(".SZ", "")
    if pure_code.startswith(("1", "9", "300", "301", "688")):
        return True
    if name and "ST" in str(name).upper():
        return True
    ts_code = _normalize_ts_code(pure_code)
    basic = basic_map.get(ts_code) if ts_code else None
    if basic:
        if "ST" in str(basic.get("name", "")).upper():
            return True
        if basic.get("list_status") == "D":
            return True
    if ts_code and circ_mv_map:
        circ_mv = circ_mv_map.get(ts_code)
        if circ_mv is None or circ_mv < 1000 or circ_mv > 20000:
            return True
    return False


def _match_board_quality(row, seal_time_limit: str, require_no_zha: bool):
    seal_time = _parse_time(
        row.get("首次封板时间")
        or row.get("首次封板时间 ")
        or row.get("首次封板时间(时间)")
        or row.get("首次封板时间(时分)")
        or row.get("首次封板时间(时:分)")
        or row.get("首次封板时间(时:分:秒)")
        or row.get("首次封板时间")
    )
    if seal_time:
        limit_dt = _parse_time(seal_time_limit)
        if limit_dt and seal_time > limit_dt:
            return False
    zha_ban = _safe_int(row.get("炸板次数"))
    if require_no_zha:
        if zha_ban is None:
            return False
        return zha_ban == 0
    if zha_ban is not None and zha_ban > 0:
        re_seal = _safe_float(
            row.get("回封耗时")
            or row.get("回封时间")
            or row.get("回封耗时(分钟)")
            or row.get("回封时间(分钟)")
            or row.get("回封耗时(分)")
            or row.get("回封时间(分)")
        )
        if re_seal is None or re_seal > 10:
            return False
    return True


def _match_turnover(row, min_value: float, max_value: float):
    turnover = _safe_float(row.get("换手率"))
    if turnover is None:
        return False
    return min_value <= turnover <= max_value


def _match_volume_ratio(row, min_ratio: float):
    ratio = _safe_float(row.get("量比"))
    if ratio is None:
        return False
    return ratio >= min_ratio


def _select_monitor_candidates(
    data,
    first_board_only: bool,
    seal_time_limit: str,
    require_no_zha: bool,
    turnover_min: float,
    turnover_max: float,
    volume_ratio_min: float,
    basic_map: dict,
    circ_mv_map: dict,
):
    # 根据首板/封板/换手/量能等条件筛选，并叠加排除规则
    result = []
    for _, row in data.iterrows():
        code = row.get("代码")
        name = row.get("名称")
        if _is_excluded_code(code, name, basic_map, circ_mv_map):
            continue
        if not _is_first_board(row, first_board_only):
            continue
        if not _match_board_quality(row, seal_time_limit, require_no_zha):
            continue
        if not _match_turnover(row, turnover_min, turnover_max):
            continue
        if not _match_volume_ratio(row, volume_ratio_min):
            continue
        result.append(
            {
                "code": code,
                "name": name,
                "price": row.get("最新价"),
                "change": row.get("涨跌幅"),
                "height": row.get("连板数"),
                "turnover_rate": row.get("换手率"),
                "volume_ratio": row.get("量比"),
                "first_seal_time": row.get("首次封板时间"),
                "zha_ban": row.get("炸板次数"),
            }
        )
    return result


def _get_latest_trade_dates(limit: int = 6):
    if ts_pro is None:
        return []
    today = datetime.now().strftime("%Y%m%d")
    data = ts_pro.trade_cal(start_date=str(int(today) - 20), end_date=today)
    if data is None or data.empty:
        return []
    open_dates = data[data["is_open"] == 1]["cal_date"].tolist()
    return open_dates[-limit:]


def _get_prev_trade_date():
    dates = _get_latest_trade_dates(limit=2)
    if len(dates) >= 2:
        return dates[-2]
    return None


def _get_auction_snapshot():
    if ts_pro is None:
        return None
    try:
        return ts_pro.stk_auction_o(time="09:25:00")
    except Exception:
        return None


def _build_auction_metrics():
    auction_df = _get_auction_snapshot()
    if auction_df is None or auction_df.empty:
        return None

    prev_date = _get_prev_trade_date()
    if not prev_date:
        return None

    ts_codes = auction_df["ts_code"].dropna().unique().tolist()
    if not ts_codes:
        return None

    prev_close_df = ts_pro.daily(trade_date=prev_date, ts_code=",".join(ts_codes))
    if prev_close_df is None or prev_close_df.empty:
        return None
    prev_close_df = prev_close_df[["ts_code", "close", "vol"]].rename(
        columns={"close": "prev_close", "vol": "prev_vol"}
    )

    basic_df = ts_pro.daily_basic(trade_date=prev_date, fields="ts_code,float_share")
    if basic_df is None or basic_df.empty:
        basic_df = None

    trade_dates = _get_latest_trade_dates(limit=6)
    avg_vol_df = None
    if len(trade_dates) >= 6:
        avg_vol_df = ts_pro.daily(start_date=trade_dates[0], end_date=prev_date)
        if avg_vol_df is not None and not avg_vol_df.empty:
            avg_vol_df = (
                avg_vol_df.groupby("ts_code")["vol"].mean().reset_index().rename(columns={"vol": "avg_vol"})
            )
        else:
            avg_vol_df = None

    name_df = ts_pro.stock_basic(fields="ts_code,name")

    auction_df = auction_df.merge(prev_close_df, on="ts_code", how="left")
    if basic_df is not None:
        auction_df = auction_df.merge(basic_df, on="ts_code", how="left")
    if avg_vol_df is not None:
        auction_df = auction_df.merge(avg_vol_df, on="ts_code", how="left")
    auction_df = auction_df.merge(name_df, on="ts_code", how="left")

    # 竞价监控也应用排除规则与流通市值过滤
    basic_map = _get_stock_basic_map()
    circ_mv_map = _get_circ_mv_map()
    auction_df = auction_df[~auction_df.apply(
        lambda row: _is_excluded_code(row.get("ts_code"), row.get("name"), basic_map, circ_mv_map),
        axis=1,
    )]

    auction_df["竞价涨幅"] = (auction_df["price"] - auction_df["prev_close"]) / auction_df["prev_close"] * 100
    auction_df["竞价量比"] = auction_df["vol"] / auction_df["avg_vol"]
    auction_df["竞价换手率"] = (auction_df["vol"] * 100) / (auction_df["float_share"] * 10000) * 100
    auction_df["竞量占比"] = (auction_df["vol"] / auction_df["prev_vol"]) * 100

    return auction_df


@app.get("/api/stock-info")
def get_stock_info(
    symbol: str = Query(..., description="股票代码，如 000001"),
):
    data = ak.stock_individual_info_em(symbol=symbol)
    if data.empty:
        return {"data": []}
    return {"data": data.to_dict(orient="records")}


@app.get("/api/stock-info-fields")
def get_stock_info_fields(
    symbol: str = Query("000001", description="股票代码，用于获取字段列表"),
):
    data = ak.stock_individual_info_em(symbol=symbol)
    if data.empty:
        return {"data": []}
    fields = list(data.columns)
    return {"data": fields}


@app.get("/api/monitor")
def get_monitor(
    first_board_only: bool = Query(True, description="是否仅首板（连板数=1）"),
    seal_time_limit: str = Query("10:00", description="封板时间上限，格式 HH:MM"),
    require_no_zha: bool = Query(False, description="是否必须无炸板"),
    turnover_min: float = Query(5.0, description="换手率下限"),
    turnover_max: float = Query(20.0, description="换手率上限"),
    volume_ratio_min: float = Query(1.5, description="量比下限"),
):
    data = ak.stock_zt_pool_em()
    if data.empty:
        return {"data": []}
    basic_map = _get_stock_basic_map()
    circ_mv_map = _get_circ_mv_map()
    candidates = _select_monitor_candidates(
        data,
        first_board_only=first_board_only,
        seal_time_limit=seal_time_limit,
        require_no_zha=require_no_zha,
        turnover_min=turnover_min,
        turnover_max=turnover_max,
        volume_ratio_min=volume_ratio_min,
        basic_map=basic_map,
        circ_mv_map=circ_mv_map,
    )
    return {"data": candidates}


@app.get("/api/auction-monitor")
def get_auction_monitor(
    pct_min: float = Query(3.0, description="竞价涨幅下限"),
    pct_max: float = Query(7.0, description="竞价涨幅上限"),
    volume_ratio_min: float = Query(5.0, description="竞价量比下限"),
    turnover_min: float = Query(0.8, description="竞价换手率下限"),
    volume_share_min: float = Query(8.0, description="竞量占比下限"),
):
    if ts_pro is None:
        return {"data": [], "error": "Tushare token 未配置"}

    auction_df = _build_auction_metrics()
    if auction_df is None or auction_df.empty:
        return {"data": []}

    filtered = auction_df[
        (auction_df["竞价涨幅"] >= pct_min)
        & (auction_df["竞价涨幅"] <= pct_max)
        & (auction_df["竞价量比"] >= volume_ratio_min)
        & (auction_df["竞价换手率"] >= turnover_min)
        & (auction_df["竞量占比"] >= volume_share_min)
    ]

    result = []
    for _, row in filtered.iterrows():
        result.append(
            {
                "code": row.get("ts_code"),
                "name": row.get("name"),
                "auction_price": row.get("price"),
                "auction_pct": row.get("竞价涨幅"),
                "auction_volume_ratio": row.get("竞价量比"),
                "auction_turnover": row.get("竞价换手率"),
                "auction_volume_share": row.get("竞量占比"),
            }
        )

    return {"data": result}
