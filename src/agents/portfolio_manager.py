import json
from langchain_core.messages import HumanMessage
from langchain_core.prompts import ChatPromptTemplate

from src.graph.state import AgentState, show_agent_reasoning
from pydantic import BaseModel, Field
from typing_extensions import Literal
from src.utils.progress import progress
from src.utils.llm import call_llm


class PortfolioDecision(BaseModel):
    action: Literal["buy", "sell", "short", "cover", "hold"]
    quantity: int = Field(description="Number of shares to trade")
    confidence: float = Field(description="Confidence in the decision, between 0.0 and 100.0")
    reasoning: str = Field(description="Reasoning for the decision")


class PortfolioManagerOutput(BaseModel):
    decisions: dict[str, PortfolioDecision] = Field(description="Dictionary of ticker to trading decisions")


##### Portfolio Management Agent #####
def portfolio_management_agent(state: AgentState):
    """Makes final trading decisions and generates orders for multiple tickers"""

    # Get the portfolio and analyst signals
    portfolio = state["data"]["portfolio"]
    analyst_signals = state["data"]["analyst_signals"]
    tickers = state["data"]["tickers"]

    # Get position limits, current prices, and signals for every ticker
    position_limits = {}
    current_prices = {}
    max_shares = {}
    signals_by_ticker = {}
    for ticker in tickers:
        progress.update_status("portfolio_manager", ticker, "Processing analyst signals")

        # Get position limits and current prices for the ticker
        risk_data = analyst_signals.get("risk_management_agent", {}).get(ticker, {})
        position_limits[ticker] = risk_data.get("remaining_position_limit", 0)
        current_prices[ticker] = risk_data.get("current_price", 0)

        # Calculate maximum shares allowed based on position limit and price
        if current_prices[ticker] > 0:
            max_shares[ticker] = int(position_limits[ticker] / current_prices[ticker])
        else:
            max_shares[ticker] = 0

        # Get signals for the ticker
        ticker_signals = {}
        for agent, signals in analyst_signals.items():
            if agent != "risk_management_agent" and ticker in signals:
                ticker_signals[agent] = {"signal": signals[ticker]["signal"], "confidence": signals[ticker]["confidence"]}
        signals_by_ticker[ticker] = ticker_signals

    progress.update_status("portfolio_manager", None, "Generating trading decisions")

    # Generate the trading decision
    result = generate_trading_decision(
        tickers=tickers,
        signals_by_ticker=signals_by_ticker,
        current_prices=current_prices,
        max_shares=max_shares,
        portfolio=portfolio,
        model_name=state["metadata"]["model_name"],
        model_provider=state["metadata"]["model_provider"],
    )

    # Create the portfolio management message
    message = HumanMessage(
        content=json.dumps({ticker: decision.model_dump() for ticker, decision in result.decisions.items()}),
        name="portfolio_manager",
    )

    # Print the decision if the flag is set
    if state["metadata"]["show_reasoning"]:
        show_agent_reasoning({ticker: decision.model_dump() for ticker, decision in result.decisions.items()}, "Portfolio Manager")

    progress.update_status("portfolio_manager", None, "Done")

    return {
        "messages": state["messages"] + [message],
        "data": state["data"],
    }


def generate_trading_decision(
    tickers: list[str],
    signals_by_ticker: dict[str, dict],
    current_prices: dict[str, float],
    max_shares: dict[str, int],
    portfolio: dict[str, float],
    model_name: str,
    model_provider: str,
) -> PortfolioManagerOutput:
    """Attempts to get a decision from the LLM with retry logic"""
    # Create the prompt template

    # template = ChatPromptTemplate.from_messages(
    #     [
    #         (
    #             "system",
    #             """You are a portfolio manager making final trading decisions based on multiple tickers.

    #           Trading Rules:
    #           - For long positions:
    #             * Only buy if you have available cash
    #             * Only sell if you currently hold long shares of that ticker
    #             * Sell quantity must be ≤ current long position shares
    #             * Buy quantity must be ≤ max_shares for that ticker
              
    #           - For short positions:
    #             * Only short if you have available margin (position value × margin requirement)
    #             * Only cover if you currently have short shares of that ticker
    #             * Cover quantity must be ≤ current short position shares
    #             * Short quantity must respect margin requirements
              
    #           - The max_shares values are pre-calculated to respect position limits
    #           - Consider both long and short opportunities based on signals
    #           - Maintain appropriate risk management with both long and short exposure

    #           Available Actions:
    #           - "buy": Open or add to long position
    #           - "sell": Close or reduce long position
    #           - "short": Open or add to short position
    #           - "cover": Close or reduce short position
    #           - "hold": No action

    #           Inputs:
    #           - signals_by_ticker: dictionary of ticker → signals
    #           - max_shares: maximum shares allowed per ticker
    #           - portfolio_cash: current cash in portfolio
    #           - portfolio_positions: current positions (both long and short)
    #           - current_prices: current prices for each ticker
    #           - margin_requirement: current margin requirement for short positions (e.g., 0.5 means 50%)
    #           - total_margin_used: total margin currently in use
    #           """,
    #         ),
    #         (
    #             "human",
    #             """Based on the team's analysis, make your trading decisions for each ticker.

    #           Here are the signals by ticker:
    #           {signals_by_ticker}

    #           Current Prices:
    #           {current_prices}

    #           Maximum Shares Allowed For Purchases:
    #           {max_shares}

    #           Portfolio Cash: {portfolio_cash}
    #           Current Positions: {portfolio_positions}
    #           Current Margin Requirement: {margin_requirement}
    #           Total Margin Used: {total_margin_used}

    #           Output strictly in JSON with the following structure:
    #           {{
    #             "decisions": {{
    #               "TICKER1": {{
    #                 "action": "buy/sell/short/cover/hold",
    #                 "quantity": integer,
    #                 "confidence": float between 0 and 100,
    #                 "reasoning": "string"
    #               }},
    #               "TICKER2": {{
    #                 ...
    #               }},
    #               ...
    #             }}
    #           }}
    #           """,
    #         ),
    #     ]
    # )

    template = ChatPromptTemplate.from_messages(
        [
            (
                "system",
                """您是一名投资组合经理，基于多个股票代码做出最终交易决策。
                重要提示：请用中文回复，但交易动作保持英文。

                交易规则：
                - 多头仓位：
                  * 只有在有可用现金时才能买入
                  * 只有在当前持有该股票多头股份时才能卖出
                  * 卖出数量必须 ≤ 当前多头仓位股数
                  * 买入数量必须 ≤ 该股票的最大股数限制
                
                - 空头仓位：
                  * 只有在有可用保证金时才能做空（仓位价值 x 保证金要求）
                  * 只有在当前持有该股票空头股份时才能平仓
                  * 平仓数量必须 ≤ 当前空头仓位股数
                  * 做空数量必须符合保证金要求
                
                - max_shares 值已预先计算以符合仓位限制
                - 根据信号考虑多空双向机会
                - 在多空仓位中保持适当的风险管理

                可用交易动作（保持英文）：
                - "buy": 开仓或增加多头仓位
                - "sell": 平仓或减少多头仓位
                - "short": 开仓或增加空头仓位
                - "cover": 平仓或减少空头仓位
                - "hold": 不进行任何操作

                输入参数：
                - signals_by_ticker: 股票代码 → 信号的字典
                - max_shares: 每个股票允许的最大股数
                - portfolio_cash: 投资组合中的当前现金
                - portfolio_positions: 当前仓位（多头和空头）
                - current_prices: 每个股票的当前价格
                - margin_requirement: 空头仓位的当前保证金要求(例如0.5表示50%)
                - total_margin_used: 当前使用的总保证金
                """,
            ),
            (
                "human",
                """基于团队分析，请为每个股票代码做出交易决策。

                各股票信号：
                {signals_by_ticker}

                当前价格：
                {current_prices}

                购买允许的最大股数：
                {max_shares}

                投资组合现金：{portfolio_cash}
                当前仓位：{portfolio_positions}
                当前保证金要求：{margin_requirement}
                已使用总保证金：{total_margin_used}

                请严格按照以下JSON格式输出(reasoning用中文,action保持英文):
                {{
                  "decisions": {{
                    "TICKER1": {{
                      "action": "buy/sell/short/cover/hold",
                      "quantity": integer,
                      "confidence": float between 0 and 100,
                      "reasoning": "中文推理说明"
                    }},
                    "TICKER2": {{
                      ...
                    }},
                    ...
                  }}
                }}
                """,
            ),
        ]
    )

    # Generate the prompt
    prompt = template.invoke(
        {
            "signals_by_ticker": json.dumps(signals_by_ticker, indent=2),
            "current_prices": json.dumps(current_prices, indent=2),
            "max_shares": json.dumps(max_shares, indent=2),
            "portfolio_cash": f"{portfolio.get('cash', 0):.2f}",
            "portfolio_positions": json.dumps(portfolio.get("positions", {}), indent=2),
            "margin_requirement": f"{portfolio.get('margin_requirement', 0):.2f}",
            "total_margin_used": f"{portfolio.get('margin_used', 0):.2f}",
        }
    )

    # Create default factory for PortfolioManagerOutput
    def create_default_portfolio_output():
        return PortfolioManagerOutput(decisions={ticker: PortfolioDecision(action="hold", quantity=0, confidence=0.0, reasoning="Error in portfolio management, defaulting to hold") for ticker in tickers})

    return call_llm(prompt=prompt, model_name=model_name, model_provider=model_provider, pydantic_model=PortfolioManagerOutput, agent_name="portfolio_manager", default_factory=create_default_portfolio_output)
