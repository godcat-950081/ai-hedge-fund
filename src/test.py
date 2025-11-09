import akshare as ak

from src.tools.api import get_financial_metrics, search_line_items, get_market_cap

# df = ak.stock_zh_a_spot_em()
# print(df)
# # Get information about 代码=601139
# for index, row in df.iterrows():
#     if row['代码'] == '601139':
#         series = row
#         break
# print(series)


# df = ak.stock_zh_a_daily(symbol="sh601139")
# print(df)
# print(df.columns.tolist())

# df = ak.stock_zh_a_hist(symbol="601139", start_date="20220101", end_date="20240501")
# print(df)

# df = ak.stock_financial_report_sina("601139", "利润表")
# print(df)

# df = ak.stock_financial_analysis_indicator(symbol="601139", start_year="2019")
# print(df)
# print all columns
# print(df.columns.tolist())
# ['日期', '摊薄每股收益(元)', '加权每股收益(元)', '每股收益_调整后(元)', '扣除非经常性损益后的每股收益(元)', '每股净资产_调整前(元)', '每股净资产_调整后(元)', '每股经营性现金流(元)', '每股资本公积金(元)', '每股未分配利润(元)', '调整后的每股净资产(元)', '总资产利润率(%)', '主营业务利润率(%)', '总资产净利润率(%)', '成本费用利润率(%)', '营业利润率(%)', '主营业务成本率(%)', '销售净利率(%)', '股本报酬率(%)', '净资产报酬率(%)', '资产报酬率(%)', '销售毛利率(%)', '三项费用比重', '非主营比重', '主营利润比重', '股息发放率(%)', '投资收益率(%)', '主营业务利润(元)', '净资产收益率(%)', '加权净资产收益率(%)', '扣除非经常性损益后的净利润(元)', '主营业务收入增长率(%)', '净利润增长率(%)', '净资产增长率(%)', '总资产增长率(%)', '应收账款周转率(次)', '应收账款周转天数(天)', '存货周转天数(天)', '存货周转率(次)', '固定资产周转率(次)', '总资产周转率(次)', '总资产周转天数(天)', '流动资产周转率(次)', '流动资产周转天数(天)', '股东权益周转率(次)', '流动比率', '速动比率', '现金比率(%)', '利息支付倍数', '长期债务与营运资金比率(%)', '股东权益比率(%)', '长期负债比率(%)', '股东权益与固定资产比率(%)', '负债与所有者权益比率(%)', '长期资产与长期资金比率(%)', '资本化比率(%)', '固定资产净值率(%)', '资本固定化比率(%)', '产权比率(%)', '清算价值比率(%)', '固定资产比重(%)', '资产负债率(%)', '总资产(元)', '经营现金净流量对销售收入比率(%)', '资产的经营现金流量回报率(%)', '经营现金净流量与净利润的比率(%)', '经营现金净流量对负债比率(%)', '现金流量比率(%)', '短期股票投资(元)', '短期债券投资(元)', '短期其它经营性投资(元)', '长期股票投资(元)', '长期债券投资(元)', '长期其它经营性投资(元)', '1年以内应收帐款(元)', '1-2年以内应收帐款(元)', '2-3年以内应收帐款(元)', '3年以内应收帐款(元)', '1年以内预付货款(元)', '1-2年以内预付货款(元)', '2-3年以内预付货款(元)', '3年以内预付货款(元)', '1年以内其它应收款(元)', '1-2年以内其它应收款(元)', '2-3年以内其它应收款(元)', '3年以内其它应收款(元)']


# poetry run python src/main.py --ticker 601139 --start-date 2024-01-01 --end-date 2025-05-23


# ----------------------------------------------------------------------------------------------------------
# Test get_financial_metrics method
def test_get_financial_metrics_single_run():
    """单独测试 get_financial_metrics 方法"""
    ticker = "601139"
    end_date = "2024-12-31"
    period = "ttm"
    limit = 5
    
    try:
        print(f"正在获取股票 {ticker} 的财务指标...")
        print(f"截止日期: {end_date}")
        print(f"期间: {period}")
        print(f"限制条数: {limit}")
        print("-" * 50)
        
        # 调用 get_financial_metrics 方法
        metrics = get_financial_metrics(ticker, end_date, period, limit)
        
        print(f"成功获取 {len(metrics)} 条财务指标记录")
        print("-" * 50)
        
        # 打印每条记录的关键信息
        for i, metric in enumerate(metrics, 1):
            print(f"记录 {i}:")
            print(f"  报告期间: {metric.report_period}")
            print(f"  每股收益: {metric.earnings_per_share}")
            print(f"  每股净资产: {metric.book_value_per_share}")
            print(f"  净资产收益率: {metric.return_on_equity}")
            print(f"  流动比率: {metric.current_ratio}")
            print(f"  资产负债率: {metric.debt_to_assets}")
            print(f"  销售毛利率: {metric.gross_margin}")
            print(f"  净利润增长率: {metric.earnings_growth}")
            print("-" * 30)
            
        # 测试第一条记录的详细信息
        if metrics:
            first_metric = metrics[0]
            print("第一条记录的详细信息:")
            print(f"  股票代码: {first_metric.ticker}")
            print(f"  货币: {first_metric.currency}")
            print(f"  市值: {first_metric.market_cap}")
            print(f"  市盈率: {first_metric.price_to_earnings_ratio}")
            print(f"  市净率: {first_metric.price_to_book_ratio}")
            print(f"  营业利润率: {first_metric.operating_margin}")
            print(f"  总资产净利润率: {first_metric.return_on_assets}")
            print(f"  速动比率: {first_metric.quick_ratio}")
            print(f"  现金比率: {first_metric.cash_ratio}")
            print(f"  利息支付倍数: {first_metric.interest_coverage}")
            print(f"  每股经营性现金流: {first_metric.free_cash_flow_per_share}")
            
    except Exception as e:
        print(f"测试失败: {e}")
        import traceback
        traceback.print_exc()


# Test search_line_items method
def test_search_line_items_single_run():
    """单独测试 search_line_items 方法"""
    ticker = "601139"
    end_date = "2024-12-31"
    period = "annual"
    limit = 10
    line_items = ["earnings_per_share", "revenue", "net_income", "book_value_per_share", 
                  "total_assets", "total_liabilities", "current_assets", "current_liabilities", 
                  "dividends_and_other_cash_distributions", "outstanding_shares"]
    
    try:
        print(f"正在获取股票 {ticker} 的财务项目...")
        print(f"截止日期: {end_date}")
        print(f"期间: {period}")
        print(f"限制条数: {limit}")
        print(f"请求的财务项目: {line_items}")
        print("-" * 80)
        
        # 调用 search_line_items 方法
        line_item_results = search_line_items(ticker, line_items, end_date, period, limit)
        
        print(f"成功获取 {len(line_item_results)} 条财务项目记录")
        print("-" * 80)
        
        # 打印每条记录的信息
        for i, item in enumerate(line_item_results, 1):
            print(f"记录 {i}:")
            print(f"  股票代码: {item.ticker}")
            print(f"  报告期间: {item.report_period}")
            print(f"  期间: {item.period}")
            print(f"  货币: {item.currency}")
            print(f"  每股收益: {item.earnings_per_share}")
            print(f"  营业收入: {item.revenue}")
            print(f"  净利润: {item.net_income}")
            print(f"  每股净资产: {item.book_value_per_share}")
            print(f"  总资产: {item.total_assets}")
            print(f"  总负债: {item.total_liabilities}")
            print(f"  流动资产: {item.current_assets}")
            print(f"  流动负债: {item.current_liabilities}")
            print(f"  分红和其他现金分配: {item.dividends_and_other_cash_distributions}")
            print(f"  流通股数: {item.outstanding_shares}")
            print("-" * 50)
            
        # 显示哪些字段有数据，哪些为空
        if line_item_results:
            first_item = line_item_results[0]
            print("第一条记录中各字段的数据状态:")
            for field_name in line_items:
                field_value = getattr(first_item, field_name, None)
                status = "有数据" if field_value is not None else "无数据"
                print(f"  {field_name}: {field_value} ({status})")
            
    except Exception as e:
        print(f"测试失败: {e}")
        import traceback
        traceback.print_exc()


# Test get_market_cap method
def test_get_market_cap_single_run():
    """单独测试 get_market_cap 方法"""
    ticker = "601139"
    end_date = "2024-12-31"
    
    try:
        print(f"正在获取股票 {ticker} 的市值...")
        print(f"截止日期: {end_date}")
        print("-" * 50)
        
        # 调用 get_market_cap 方法
        market_cap = get_market_cap(ticker, end_date)
        
        if market_cap is not None:
            print(f"成功获取市值: {market_cap:,.2f} 元")
            print(f"市值（亿元）: {market_cap / 100000000:.2f} 亿")
            print(f"市值（万元）: {market_cap / 10000:.2f} 万")
        else:
            print("未能获取到市值数据")
        
        print("-" * 50)
            
    except Exception as e:
        print(f"测试失败: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    # df = ak.stock_financial_report_sina("601139", "利润表")
    # print(df['基本每股收益'].head())
    # print(df['基本每股收益'].tail())  
    
    # 运行 get_financial_metrics 测试
    # test_get_financial_metrics_single_run()

    # 运行 search_line_items 测试
    # test_search_line_items_single_run()

    # df = ak.stock_financial_analysis_indicator(symbol="601139", start_year="2019")
    # print(df[['日期', '总资产(元)']].head())


    # 运行 get_market_cap 测试
    # test_get_market_cap_single_run()

    df = ak.stock_hold_management_detail_em()
    print(df)
    print(df.columns.tolist())

    # spot_df = ak.stock_zh_a_spot_em()
    # spot_row = spot_df[spot_df["代码"] == "601139"]
    # market_cap = spot_row.get("流通市值")  # 流通市值
    # print(f"流通市值: {market_cap}")
    # # 或者
    # market_cap = spot_row.get("总市值")     # 总市值
    # print(f"总市值: {market_cap}")

    # spot_df = ak.stock_zh_a_spot_em()
    # spot_row = spot_df[spot_df["代码"] == "601139"]
    # spot_row = spot_row.iloc[0]
    # # market_cap = spot_row.get("流通市值")
    # market_cap = spot_row.get("总市值")
    # print(f"总市值: {float(market_cap)}")