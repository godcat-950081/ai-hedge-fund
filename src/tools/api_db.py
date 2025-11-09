import pandas as pd
from src.engine.database import db

def stock_hold_management_detail_em(symbol: str = "601139") -> pd.DataFrame:
    """
    获取股票的股东户详情数据
    {
    "_id": {
        "$oid": "683d67ca5e2460ebc4db3e1b"
    },
    "日期": "2025-05-30",
    "代码": "002264",
    "名称": "新华都",
    "变动人": "倪国涛",
    "变动股数": -10562300,
    "成交均价": 6.2209,
    "变动金额": -65707012.07,
    "变动原因": "集中竞价,大宗交易",
    "变动比例": 1.4673,
    "变动后持股数": 71728488,
    "持股种类": "A股",
    "董监高人员姓名": "倪国涛",
    "职务": "董事,董事长,总经理",
    "变动人与董监高的关系": "本人",
    "开始时持有": 82290788,
    "结束后持有": 71728488
    }
    """

    collection_name = "stock_hold_management_detail_em"
    collection = db[collection_name]
    query = {"代码": symbol}
    projection = {
        "_id": 0,
        "日期": 1,
        "代码": 1,
        "名称": 1,
        "变动人": 1,
        "变动股数": 1,
        "成交均价": 1,
        "变动金额": 1,
        "变动原因": 1,
        "变动比例": 1,
        "变动后持股数": 1,
        "持股种类": 1,
        "董监高人员姓名": 1,
        "职务": 1,
        "变动人与董监高的关系": 1,
        "开始时持有": 1,
        "结束后持有": 1
    }
    cursor = collection.find(query, projection)
    data = list(cursor)
    if not data:
        print(f"未找到股票代码 {symbol} 的股东户数详情数据。")
        return pd.DataFrame()
    df = pd.DataFrame(data)
    df["日期"] = pd.to_datetime(df["日期"], errors="coerce").dt.date
    df["变动股数"] = pd.to_numeric(df["变动股数"], errors="coerce")
    df["成交均价"] = pd.to_numeric(df["成交均价"], errors="coerce")
    df["变动金额"] = pd.to_numeric(df["变动金额"], errors="coerce")
    df["变动比例"] = pd.to_numeric(df["变动比例"], errors="coerce")
    df["变动后持股数"] = pd.to_numeric(df["变动后持股数"], errors="coerce")
    df["开始时持有"] = pd.to_numeric(df["开始时持有"], errors="coerce")
    df["结束后持有"] = pd.to_numeric(df["结束后持有"], errors="coerce")
    return df

if __name__ == "__main__":
    # 测试获取股东户数详情数据
    symbol = "601139"  # 示例股票代码
    df = stock_hold_management_detail_em(symbol)
    if not df.empty:
        print(f"股票代码 {symbol} 的股东户数详情数据:")
        print(df.head())
    else:
        print(f"未能获取股票代码 {symbol} 的股东户数详情数据。")

