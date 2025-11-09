import pandas as pd
import akshare as ak
from pymongo import MongoClient
from datetime import datetime, date

MONGO_URI = "mongodb://admin:Z!cxz098-lz@localhost:27017/"
DB_NAME = "ai-hedge-fund"
STATUS_COLLECTION = "update_status"
BATCH_SIZE = 1000

def get_last_update_date(db, table_name):
    status = db[STATUS_COLLECTION].find_one({"table": table_name})
    if status:
        return status.get("last_update")
    return None

def set_last_update_date(db, table_name, date_str):
    db[STATUS_COLLECTION].update_one(
        {"table": table_name},
        {"$set": {"last_update": date_str}},
        upsert=True
    )

def batch_insert(collection, records, batch_size=BATCH_SIZE):
    for i in range(0, len(records), batch_size):
        collection.insert_many(records[i:i+batch_size])

def update_table_with_func(db, collection_name, data_func):
    """
    通用数据获取与写入逻辑
    :param db: MongoDB数据库对象
    :param collection_name: 要写入的集合名
    :param data_func: 获取数据的函数,返回DataFrame
    """
    collection = db[collection_name]
    today_str = datetime.now().strftime("%Y-%m-%d")
    last_update = get_last_update_date(db, collection_name)

    if last_update == today_str:
        print(f"{collection_name} 已是最新，无需更新。")
        return

    print(f"开始从接口获取 {collection_name} 数据...")
    df = data_func()
    # 将所有datetime.date类型转为字符串
    df = df.applymap(lambda x: x.isoformat() if isinstance(x, (datetime, date)) else x)
    records = df.to_dict(orient="records")
    if not records:
        print(f"{collection_name} 未获取到数据，终止更新。")
        return

    print(f"清空 {collection_name} 原有数据...")
    collection.delete_many({})

    print(f"分批写入 {collection_name} 到MongoDB...")
    batch_insert(collection, records)

    set_last_update_date(db, collection_name, today_str)
    print(f"已写入{len(records)}条数据到 {collection_name}, 并更新最后更新时间为{today_str}")

def get_stock_hold_management_detail_em():
    return ak.stock_hold_management_detail_em()

def get_stock_zh_a_spot_em():
    return ak.stock_zh_a_spot_em()

def main():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]

    # 依次调用不同数据的更新
    update_table_with_func(db, "stock_hold_management_detail_em", get_stock_hold_management_detail_em)
    update_table_with_func(db, "stock_zh_a_spot_em", get_stock_zh_a_spot_em)
    # 可以继续添加其它数据表的更新调用

if __name__ == "__main__":
    main()