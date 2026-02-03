"""
収益構造分析エンジン
"""

import pandas as pd
import numpy as np
from typing import Dict, List
import sys


class ProfitabilityAnalyzer:
    
    def __init__(self, processor=None):
        self.processor = processor
    
    def calculate_marginal_profit_metrics(self, sales: float, cogs: float, operating_expenses: float) -> Dict:
        try:
            if sales == 0:
                return self._empty_metrics()
            
            marginal_profit = sales - cogs
            marginal_profit_rate = marginal_profit / sales
            variable_cost_rate = cogs / sales
            operating_profit = marginal_profit - operating_expenses
            operating_profit_rate = operating_profit / sales
            breakeven_sales = operating_expenses / marginal_profit_rate if marginal_profit_rate > 0 else 0
            margin_of_safety = sales - breakeven_sales
            safety_rate = margin_of_safety / sales if sales > 0 else 0
            operating_leverage = marginal_profit / operating_profit if operating_profit > 0 else 0
            
            return {
                'sales': sales,
                'cogs': cogs,
                'marginal_profit': marginal_profit,
                'marginal_profit_rate': marginal_profit_rate,
                'variable_cost_rate': variable_cost_rate,
                'fixed_costs': operating_expenses,
                'operating_profit': operating_profit,
                'operating_profit_rate': operating_profit_rate,
                'breakeven_sales': breakeven_sales,
                'margin_of_safety': margin_of_safety,
                'safety_rate': safety_rate,
                'operating_leverage': operating_leverage,
                'is_profitable': sales >= breakeven_sales
            }
        except Exception as e:
            return self._empty_metrics()
    
    def analyze_monthly_profitability(self, pl_data: pd.DataFrame) -> pd.DataFrame:
        try:
            month_cols = [col for col in pl_data.columns if col not in ['項目名', 'item_name', '項目タイプ']]
            
            if not month_cols:
                return pd.DataFrame()
            
            results = []
            for month_col in month_cols:
                sales = self._get_pl_value(pl_data, '売上', month_col)
                if sales == 0:
                    sales = self._get_pl_value(pl_data, '売上高', month_col)
                
                cogs = self._get_pl_value(pl_data, '売上原価', month_col)
                opex = self._get_pl_value(pl_data, '販管費', month_col)
                if opex == 0:
                    opex = self._get_pl_value(pl_data, '販売費及び一般管理費', month_col)
                
                if sales == 0:
                    continue
                
                metrics = self.calculate_marginal_profit_metrics(sales, cogs, opex)
                metrics['month'] = month_col
                results.append(metrics)
            
            return pd.DataFrame(results)
        except Exception as e:
            return pd.DataFrame()
    
    def _get_pl_value(self, pl_data: pd.DataFrame, item_name: str, month_col: str) -> float:
        try:
            row = pl_data[pl_data['項目名'] == item_name]
            if row.empty and 'item_name' in pl_data.columns:
                row = pl_data[pl_data['item_name'] == item_name]
            if row.empty:
                row = pl_data[pl_data['項目名'].str.contains(item_name, na=False)]
            
            if not row.empty and month_col in row.columns:
                value = row[month_col].iloc[0]
                return float(value) if pd.notna(value) else 0
            return 0
        except:
            return 0
    
    def _empty_metrics(self) -> Dict:
        return {
            'sales': 0, 'cogs': 0, 'marginal_profit': 0, 'marginal_profit_rate': 0,
            'variable_cost_rate': 0, 'fixed_costs': 0, 'operating_profit': 0,
            'operating_profit_rate': 0, 'breakeven_sales': 0, 'margin_of_safety': 0,
            'safety_rate': 0, 'operating_leverage': 0, 'is_profitable': False
        }


def analyze_profitability_from_db(period_id: int, processor) -> Dict:
    try:
        conn = processor._get_connection()
        placeholder = '%s' if hasattr(processor, 'use_postgres') and processor.use_postgres else '?'
        query = f"SELECT item_name, month, value FROM actual_data WHERE fiscal_period_id = {placeholder} ORDER BY month, item_name"
        df = pd.read_sql_query(query, conn, params=(period_id,))
        conn.close()
        
        if df.empty:
            return {}
        
        pl_data = df.pivot(index='item_name', columns='month', values='value').reset_index()
        pl_data.columns.name = None
        pl_data = pl_data.rename(columns={'item_name': '項目名'})
        
        analyzer = ProfitabilityAnalyzer(processor)
        monthly_df = analyzer.analyze_monthly_profitability(pl_data)
        
        if monthly_df.empty:
            return {}
        
        avg_marginal_profit_rate = monthly_df['marginal_profit_rate'].mean()
        avg_safety_rate = monthly_df['safety_rate'].mean()
        
        trend = 'stable'
        if len(monthly_df) >= 3:
            recent_3 = monthly_df['marginal_profit_rate'].tail(3).values
            if recent_3[2] > recent_3[0] * 1.02:
                trend = 'improving'
            elif recent_3[2] < recent_3[0] * 0.98:
                trend = 'deteriorating'
        
        return {
            'monthly_data': monthly_df,
            'average_marginal_profit_rate': avg_marginal_profit_rate,
            'average_safety_rate': avg_safety_rate,
            'trend': trend
        }
    except Exception as e:
        sys.stderr.write(f"❌ エラー: {e}\n")
        import traceback
        traceback.print_exc(file=sys.stderr)
        return {}
