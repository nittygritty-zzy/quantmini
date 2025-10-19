"""
Financial Ratios Calculator - Compute financial ratios from statements

Computes comprehensive financial ratios from balance sheets, income statements,
and cash flow statements since you don't have access to Polygon's ratios endpoint.

Categories:
- Profitability ratios
- Liquidity ratios
- Leverage ratios
- Efficiency ratios
- Valuation ratios
- Growth ratios
"""

import polars as pl
from typing import Dict, Optional, List
import logging

logger = logging.getLogger(__name__)


class FinancialRatiosCalculator:
    """
    Calculate financial ratios from financial statements

    All ratios are computed from balance sheets, income statements,
    and cash flow statements data.
    """

    def __init__(self):
        """Initialize ratios calculator"""
        logger.info("FinancialRatiosCalculator initialized")

    def calculate_all_ratios(
        self,
        balance_sheet: pl.DataFrame,
        income_statement: pl.DataFrame,
        cash_flow: Optional[pl.DataFrame] = None,
        market_data: Optional[pl.DataFrame] = None
    ) -> pl.DataFrame:
        """
        Calculate all financial ratios

        Args:
            balance_sheet: Balance sheet data
            income_statement: Income statement data
            cash_flow: Optional cash flow data
            market_data: Optional market data (price, shares) for valuation ratios

        Returns:
            DataFrame with computed ratios
        """
        logger.info("Calculating all financial ratios")

        # Merge statements on ticker and period
        merged = self._merge_statements(balance_sheet, income_statement, cash_flow)

        if len(merged) == 0:
            logger.warning("No data to calculate ratios")
            return pl.DataFrame()

        # Calculate each category of ratios
        ratios = merged.with_columns([
            # Profitability Ratios
            *self._calculate_profitability_ratios(),

            # Liquidity Ratios
            *self._calculate_liquidity_ratios(),

            # Leverage/Solvency Ratios
            *self._calculate_leverage_ratios(),

            # Efficiency Ratios
            *self._calculate_efficiency_ratios(),

            # Cash Flow Ratios
            *self._calculate_cash_flow_ratios(),
        ])

        # Add valuation ratios if market data provided
        if market_data is not None:
            ratios = self._add_valuation_ratios(ratios, market_data)

        logger.info(f"Calculated ratios for {len(ratios)} periods")

        return ratios

    def _ensure_columns_exist(self, df: pl.DataFrame, required_columns: List[str]) -> pl.DataFrame:
        """Add missing columns as null to avoid errors"""
        missing_cols = [col for col in required_columns if col not in df.columns]
        if missing_cols:
            logger.debug(f"Adding {len(missing_cols)} missing columns: {missing_cols[:5]}...")
            for col in missing_cols:
                df = df.with_columns(pl.lit(None).alias(col))
        return df

    def _merge_statements(
        self,
        balance_sheet: pl.DataFrame,
        income_statement: pl.DataFrame,
        cash_flow: Optional[pl.DataFrame]
    ) -> pl.DataFrame:
        """Merge financial statements on ticker and period"""

        # Join on ticker, fiscal_year, fiscal_period
        join_keys = ['ticker', 'fiscal_year', 'fiscal_period', 'timeframe']

        merged = balance_sheet.join(
            income_statement,
            on=join_keys,
            how='inner',
            suffix='_is'
        )

        if cash_flow is not None and len(cash_flow) > 0:
            merged = merged.join(
                cash_flow,
                on=join_keys,
                how='left',
                suffix='_cf'
            )

        # Ensure all required columns exist (add as null if missing)
        required = [
            # Balance sheet
            'assets', 'current_assets', 'noncurrent_assets', 'inventory',
            'accounts_receivable', 'fixed_assets',
            'liabilities', 'current_liabilities', 'noncurrent_liabilities',
            'long_term_debt', 'short_term_debt', 'accounts_payable',
            'equity',
            # Income statement
            'revenues', 'cost_of_revenue', 'gross_profit',
            'operating_income_loss', 'operating_expenses',
            'research_and_development', 'selling_general_and_administrative_expenses',
            'net_income_loss', 'diluted_earnings_per_share',
            'interest_expense', 'income_tax_expense_benefit',
            # Cash flow
            'net_cash_flow_from_operating_activities',
            'net_cash_flow_from_investing_activities',
            'net_cash_flow_from_financing_activities'
        ]

        merged = self._ensure_columns_exist(merged, required)

        return merged

    def _calculate_profitability_ratios(self) -> List[pl.Expr]:
        """Calculate profitability ratios"""
        return [
            # Gross Profit Margin = (Revenue - COGS) / Revenue
            (pl.when(pl.col('revenues').is_not_null() & (pl.col('revenues') != 0))
             .then((pl.col('revenues') - pl.col('cost_of_revenue')) / pl.col('revenues') * 100)
             .otherwise(None)
             .alias('gross_profit_margin')),

            # Operating Profit Margin = Operating Income / Revenue
            (pl.when(pl.col('revenues').is_not_null() & (pl.col('revenues') != 0))
             .then(pl.col('operating_income_loss') / pl.col('revenues') * 100)
             .otherwise(None)
             .alias('operating_profit_margin')),

            # Net Profit Margin = Net Income / Revenue
            (pl.when(pl.col('revenues').is_not_null() & (pl.col('revenues') != 0))
             .then(pl.col('net_income_loss') / pl.col('revenues') * 100)
             .otherwise(None)
             .alias('net_profit_margin')),

            # Return on Assets (ROA) = Net Income / Total Assets
            (pl.when(pl.col('assets').is_not_null() & (pl.col('assets') != 0))
             .then(pl.col('net_income_loss') / pl.col('assets') * 100)
             .otherwise(None)
             .alias('return_on_assets')),

            # Return on Equity (ROE) = Net Income / Total Equity
            (pl.when(pl.col('equity').is_not_null() & (pl.col('equity') != 0))
             .then(pl.col('net_income_loss') / pl.col('equity') * 100)
             .otherwise(None)
             .alias('return_on_equity')),

            # Return on Invested Capital (ROIC) = Net Income / (Debt + Equity)
            (pl.when((pl.col('equity') + pl.col('long_term_debt').fill_null(0)) != 0)
             .then(pl.col('net_income_loss') /
                   (pl.col('equity') + pl.col('long_term_debt').fill_null(0)) * 100)
             .otherwise(None)
             .alias('return_on_invested_capital')),
        ]

    def _calculate_liquidity_ratios(self) -> List[pl.Expr]:
        """Calculate liquidity ratios"""
        # Note: Polygon's balance sheet may not have cash_and_cash_equivalents
        # So we'll use current_assets as a proxy for cash ratio if needed
        return [
            # Current Ratio = Current Assets / Current Liabilities
            (pl.when(pl.col('current_liabilities').is_not_null() &
                    (pl.col('current_liabilities') != 0))
             .then(pl.col('current_assets') / pl.col('current_liabilities'))
             .otherwise(None)
             .alias('current_ratio')),

            # Quick Ratio = (Current Assets - Inventory) / Current Liabilities
            (pl.when(pl.col('current_liabilities').is_not_null() &
                    (pl.col('current_liabilities') != 0))
             .then((pl.col('current_assets') - pl.col('inventory').fill_null(0)) /
                   pl.col('current_liabilities'))
             .otherwise(None)
             .alias('quick_ratio')),

            # Working Capital = Current Assets - Current Liabilities
            ((pl.col('current_assets') - pl.col('current_liabilities'))
             .alias('working_capital')),
        ]

    def _calculate_leverage_ratios(self) -> List[pl.Expr]:
        """Calculate leverage/solvency ratios"""
        return [
            # Debt to Equity = Total Debt / Total Equity
            (pl.when(pl.col('equity').is_not_null() & (pl.col('equity') != 0))
             .then((pl.col('long_term_debt').fill_null(0) +
                    pl.col('short_term_debt').fill_null(0)) / pl.col('equity'))
             .otherwise(None)
             .alias('debt_to_equity')),

            # Debt to Assets = Total Debt / Total Assets
            (pl.when(pl.col('assets').is_not_null() & (pl.col('assets') != 0))
             .then((pl.col('long_term_debt').fill_null(0) +
                    pl.col('short_term_debt').fill_null(0)) / pl.col('assets'))
             .otherwise(None)
             .alias('debt_to_assets')),

            # Equity Multiplier = Total Assets / Total Equity
            (pl.when(pl.col('equity').is_not_null() & (pl.col('equity') != 0))
             .then(pl.col('assets') / pl.col('equity'))
             .otherwise(None)
             .alias('equity_multiplier')),

            # Interest Coverage = EBIT / Interest Expense
            (pl.when(pl.col('interest_expense').is_not_null() &
                    (pl.col('interest_expense') != 0))
             .then(pl.col('operating_income_loss') / pl.col('interest_expense'))
             .otherwise(None)
             .alias('interest_coverage_ratio')),
        ]

    def _calculate_efficiency_ratios(self) -> List[pl.Expr]:
        """Calculate efficiency/activity ratios"""
        return [
            # Asset Turnover = Revenue / Total Assets
            (pl.when(pl.col('assets').is_not_null() & (pl.col('assets') != 0))
             .then(pl.col('revenues') / pl.col('assets'))
             .otherwise(None)
             .alias('asset_turnover')),

            # Inventory Turnover = COGS / Average Inventory
            (pl.when(pl.col('inventory').is_not_null() & (pl.col('inventory') != 0))
             .then(pl.col('cost_of_revenue') / pl.col('inventory'))
             .otherwise(None)
             .alias('inventory_turnover')),

            # Receivables Turnover = Revenue / Accounts Receivable
            (pl.when(pl.col('accounts_receivable').is_not_null() &
                    (pl.col('accounts_receivable') != 0))
             .then(pl.col('revenues') / pl.col('accounts_receivable'))
             .otherwise(None)
             .alias('receivables_turnover')),

            # Days Sales Outstanding = 365 / Receivables Turnover
            (pl.when(pl.col('accounts_receivable').is_not_null() &
                    (pl.col('accounts_receivable') != 0) &
                    (pl.col('revenues') != 0))
             .then(365 / (pl.col('revenues') / pl.col('accounts_receivable')))
             .otherwise(None)
             .alias('days_sales_outstanding')),
        ]

    def _calculate_cash_flow_ratios(self) -> List[pl.Expr]:
        """Calculate cash flow ratios"""
        return [
            # Operating Cash Flow Ratio = Operating Cash Flow / Current Liabilities
            (pl.when(pl.col('net_cash_flow_from_operating_activities').is_not_null() &
                    pl.col('current_liabilities').is_not_null() &
                    (pl.col('current_liabilities') != 0))
             .then(pl.col('net_cash_flow_from_operating_activities') /
                   pl.col('current_liabilities'))
             .otherwise(None)
             .alias('operating_cash_flow_ratio')),

            # Free Cash Flow = Operating Cash Flow - Capital Expenditures
            # Note: CapEx is negative in cash flow statements
            (pl.when(pl.col('net_cash_flow_from_operating_activities').is_not_null())
             .then(pl.col('net_cash_flow_from_operating_activities') +
                   pl.col('net_cash_flow_from_investing_activities').fill_null(0))
             .otherwise(None)
             .alias('free_cash_flow')),

            # Cash Flow to Debt = Operating Cash Flow / Total Debt
            (pl.when((pl.col('long_term_debt').fill_null(0) +
                     pl.col('short_term_debt').fill_null(0)) != 0)
             .then(pl.col('net_cash_flow_from_operating_activities') /
                   (pl.col('long_term_debt').fill_null(0) +
                    pl.col('short_term_debt').fill_null(0)))
             .otherwise(None)
             .alias('cash_flow_to_debt')),
        ]

    def _add_valuation_ratios(
        self,
        ratios: pl.DataFrame,
        market_data: pl.DataFrame
    ) -> pl.DataFrame:
        """Add valuation ratios using market data"""

        # Join with market data (price, shares outstanding)
        ratios = ratios.join(
            market_data,
            on=['ticker', 'fiscal_year', 'fiscal_period'],
            how='left'
        )

        ratios = ratios.with_columns([
            # Market Cap = Price * Shares Outstanding
            (pl.col('price') * pl.col('shares_outstanding'))
            .alias('market_cap'),

            # P/E Ratio = Price / EPS
            (pl.when(pl.col('diluted_earnings_per_share').is_not_null() &
                    (pl.col('diluted_earnings_per_share') != 0))
             .then(pl.col('price') / pl.col('diluted_earnings_per_share'))
             .otherwise(None)
             .alias('pe_ratio')),

            # P/B Ratio = Market Cap / Book Value (Equity)
            (pl.when(pl.col('equity').is_not_null() & (pl.col('equity') != 0))
             .then((pl.col('price') * pl.col('shares_outstanding')) / pl.col('equity'))
             .otherwise(None)
             .alias('pb_ratio')),

            # P/S Ratio = Market Cap / Revenue
            (pl.when(pl.col('revenues').is_not_null() & (pl.col('revenues') != 0))
             .then((pl.col('price') * pl.col('shares_outstanding')) / pl.col('revenues'))
             .otherwise(None)
             .alias('ps_ratio')),

            # EV/EBITDA (simplified, without cash & debt adjustments)
            (pl.when(pl.col('operating_income_loss').is_not_null() &
                    (pl.col('operating_income_loss') != 0))
             .then((pl.col('price') * pl.col('shares_outstanding')) /
                   pl.col('operating_income_loss'))
             .otherwise(None)
             .alias('ev_to_ebitda_simple')),
        ])

        return ratios

    def calculate_growth_rates(
        self,
        statements: pl.DataFrame,
        periods: int = 4
    ) -> pl.DataFrame:
        """
        Calculate year-over-year or quarter-over-quarter growth rates

        Args:
            statements: Financial statements with time series data
            periods: Number of periods to look back (4 for YoY, 1 for QoQ)

        Returns:
            DataFrame with growth rates
        """
        logger.info(f"Calculating {periods}-period growth rates")

        # Sort by ticker and date
        statements = statements.sort(['ticker', 'fiscal_year', 'fiscal_period'])

        # Calculate growth rates for key metrics
        growth = statements.with_columns([
            # Revenue growth
            ((pl.col('revenues') - pl.col('revenues').shift(periods)) /
             pl.col('revenues').shift(periods) * 100)
            .over('ticker')
            .alias('revenue_growth'),

            # Net income growth
            ((pl.col('net_income_loss') - pl.col('net_income_loss').shift(periods)) /
             pl.col('net_income_loss').shift(periods).abs() * 100)
            .over('ticker')
            .alias('net_income_growth'),

            # Asset growth
            ((pl.col('assets') - pl.col('assets').shift(periods)) /
             pl.col('assets').shift(periods) * 100)
            .over('ticker')
            .alias('asset_growth'),

            # Equity growth
            ((pl.col('equity') - pl.col('equity').shift(periods)) /
             pl.col('equity').shift(periods) * 100)
            .over('ticker')
            .alias('equity_growth'),
        ])

        return growth


def main():
    """Example usage"""
    import sys

    print("✅ FinancialRatiosCalculator initialized\n")
    print("This calculator computes comprehensive financial ratios from statements:")
    print("\n📊 Profitability Ratios:")
    print("   - Gross Profit Margin")
    print("   - Operating Profit Margin")
    print("   - Net Profit Margin")
    print("   - ROA (Return on Assets)")
    print("   - ROE (Return on Equity)")
    print("   - ROIC (Return on Invested Capital)")

    print("\n💰 Liquidity Ratios:")
    print("   - Current Ratio")
    print("   - Quick Ratio")
    print("   - Cash Ratio")
    print("   - Working Capital")

    print("\n📈 Leverage Ratios:")
    print("   - Debt to Equity")
    print("   - Debt to Assets")
    print("   - Equity Multiplier")
    print("   - Interest Coverage")

    print("\n⚡ Efficiency Ratios:")
    print("   - Asset Turnover")
    print("   - Inventory Turnover")
    print("   - Receivables Turnover")
    print("   - Days Sales Outstanding")

    print("\n💵 Cash Flow Ratios:")
    print("   - Operating Cash Flow Ratio")
    print("   - Free Cash Flow")
    print("   - Cash Flow to Debt")

    print("\n🏷️  Valuation Ratios (if market data provided):")
    print("   - P/E Ratio")
    print("   - P/B Ratio")
    print("   - P/S Ratio")
    print("   - EV/EBITDA")

    print("\n📈 Growth Metrics:")
    print("   - Revenue Growth (YoY/QoQ)")
    print("   - Net Income Growth")
    print("   - Asset Growth")
    print("   - Equity Growth")


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
