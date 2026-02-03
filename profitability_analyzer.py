"""
収益構造分析エンジン (Profitability Analyzer)
限界利益率、損益分岐点、安全余裕率などを計算
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
import sys


class ProfitabilityAnalyzer:
    """収益構造分析エンジン"""
    
    def __init__(self, processor=None):
        """
        初期化
        
        Args:
            processor: DataProcessorインスタンス（オプション）
        """
        self.processor = processor
    
    def calculate_marginal_profit_metrics(
        self, 
        sales: float, 
        cogs: float, 
        operating_expenses: float
    ) -> Dict:
        """
        限界利益率と関連指標を計算
        
        Args:
            sales: 売上高
            cogs: 売上原価（変動費）
            operating_expenses: 販管費（固定費）
            
        Returns:
            Dict: 収益構造指標
        """
        try:
            if sales == 0:
                return self._empty_metrics()
            
            # 限界利益
            marginal_profit = sales - cogs
            
            # 限界利益率
            marginal_profit_rate = marginal_profit / sales
            
            # 変動費率
            variable_cost_rate = cogs / sales
            
            # 営業利益
            operating_profit = marginal_profit - operating_expenses
            
            # 営業利益率
            operating_profit_rate = operating_profit / sales
            
            # 損益分岐点売上高
            breakeven_sales = operating_expenses / marginal_profit_rate if marginal_profit_rate > 0 else 0
            
            # 安全余裕額
            margin_of_safety = sales - breakeven_sales
            
            # 安全余裕率
            safety_rate = margin_of_safety / sales if sales > 0 else 0
            
            # 経営レバレッジ
            operating_leverage = marginal_profit / operating_profit if operating_profit > 0 else 0
            
            result = {
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
            
            return result
            
        except Exception as e:
            sys.stderr.write(f"❌ 限界利益率計算エラー: {e}\n")
            import traceback
            traceback.print_exc(file=sys.stderr)
            sys.stderr.flush()
            return self._empty_metrics()
    
    def analyze_monthly_profitability(self, pl_data: pd.DataFrame) -> pd.DataFrame:
        """
        月次収益構造を分析
        
        Args:
            pl_data: PLデータ（DataFrame）
            
        Returns:
            DataFrame: 月次分析結果
        """
        try:
            sys.stderr.write("📊 月次収益構造分析開始\n")
            sys.stderr.flush()
            
            # 月度列を取得
            month_cols = [col for col in pl_data.columns 
                         if col not in ['項目名', '項目タイプ'] and isinstance(col, str)]
            
            results = []
            
            for month_col in month_cols:
                # 売上
                sales = self._get_pl_value(pl_data, '売上', month_col)
                
                # 売上原価
                cogs = self._get_pl_value(pl_data, '売上原価', month_col)
                
                # 販管費
                opex = self._get_pl_value(pl_data, '販管費', month_col)
                
                # 限界利益率計算
                metrics = self.calculate_marginal_profit_metrics(sales, cogs, opex)
                metrics['month'] = month_col
                
                results.append(metrics)
            
            df = pd.DataFrame(results)
            
            sys.stderr.write(f"✅ 月次収益構造分析完了: {len(df)}ヶ月\n")
            sys.stderr.flush()
            
            return df
            
        except Exception as e:
            sys.stderr.write(f"❌ 月次分析エラー: {e}\n")
            import traceback
            traceback.print_exc(file=sys.stderr)
            sys.stderr.flush()
            return pd.DataFrame()
    
    def calculate_price_impact(
        self, 
        current_sales: float,
        current_cogs: float,
        price_change_rate: float,
        demand_elasticity: float = -0.5
    ) -> Dict:
        """
        価格変更の影響を試算
        
        Args:
            current_sales: 現在の売上
            current_cogs: 現在の売上原価
            price_change_rate: 価格変更率（0.1 = 10%値上げ）
            demand_elasticity: 需要の価格弾力性（デフォルト-0.5）
            
        Returns:
            Dict: 影響試算
        """
        try:
            # 数量変化率
            volume_change_rate = price_change_rate * demand_elasticity
            
            # 新しい売上
            new_sales = current_sales * (1 + price_change_rate) * (1 + volume_change_rate)
            
            # 新しい売上原価（数量に比例）
            new_cogs = current_cogs * (1 + volume_change_rate)
            
            # 限界利益の変化
            current_marginal_profit = current_sales - current_cogs
            new_marginal_profit = new_sales - new_cogs
            marginal_profit_change = new_marginal_profit - current_marginal_profit
            
            return {
                'price_change_rate': price_change_rate,
                'volume_change_rate': volume_change_rate,
                'new_sales': new_sales,
                'sales_change': new_sales - current_sales,
                'new_cogs': new_cogs,
                'new_marginal_profit': new_marginal_profit,
                'marginal_profit_change': marginal_profit_change,
                'marginal_profit_change_rate': marginal_profit_change / current_marginal_profit if current_marginal_profit > 0 else 0
            }
            
        except Exception as e:
            sys.stderr.write(f"❌ 価格影響試算エラー: {e}\n")
            sys.stderr.flush()
            return {}
    
    def suggest_target_sales(
        self,
        current_sales: float,
        marginal_profit_rate: float,
        fixed_costs: float,
        target_profit: float = 0
    ) -> Dict:
        """
        目標利益を達成するための必要売上高を計算
        
        Args:
            current_sales: 現在の売上
            marginal_profit_rate: 限界利益率
            fixed_costs: 固定費
            target_profit: 目標営業利益
            
        Returns:
            Dict: 必要売上高と追加売上
        """
        try:
            # 必要売上高 = (固定費 + 目標利益) ÷ 限界利益率
            required_sales = (fixed_costs + target_profit) / marginal_profit_rate if marginal_profit_rate > 0 else 0
            
            # 追加必要売上
            additional_sales = required_sales - current_sales
            
            # 達成率
            achievement_rate = current_sales / required_sales if required_sales > 0 else 0
            
            return {
                'target_profit': target_profit,
                'required_sales': required_sales,
                'current_sales': current_sales,
                'additional_sales': additional_sales,
                'achievement_rate': achievement_rate,
                'gap_percentage': (additional_sales / current_sales * 100) if current_sales > 0 else 0
            }
            
        except Exception as e:
            sys.stderr.write(f"❌ 目標売上計算エラー: {e}\n")
            sys.stderr.flush()
            return {}
    
    def compare_with_industry_average(
        self,
        marginal_profit_rate: float,
        industry: str = 'general'
    ) -> Dict:
        """
        業界平均との比較
        
        Args:
            marginal_profit_rate: 自社の限界利益率
            industry: 業種
            
        Returns:
            Dict: 比較結果
        """
        # 業界平均データ
        industry_averages = {
            'general': {'rate': 0.35, 'range': (0.30, 0.45)},
            'retail': {'rate': 0.30, 'range': (0.25, 0.40)},
            'manufacturing': {'rate': 0.40, 'range': (0.35, 0.50)},
            'service': {'rate': 0.50, 'range': (0.40, 0.60)},
            'it': {'rate': 0.60, 'range': (0.50, 0.70)}
        }
        
        avg_data = industry_averages.get(industry, industry_averages['general'])
        avg_rate = avg_data['rate']
        min_rate, max_rate = avg_data['range']
        
        # 偏差
        deviation = marginal_profit_rate - avg_rate
        deviation_pct = (deviation / avg_rate * 100) if avg_rate > 0 else 0
        
        # 評価
        if marginal_profit_rate >= max_rate:
            rating = 'excellent'
            message = '業界トップクラス'
        elif marginal_profit_rate >= avg_rate:
            rating = 'good'
            message = '業界平均以上'
        elif marginal_profit_rate >= min_rate:
            rating = 'average'
            message = '業界平均レベル'
        else:
            rating = 'below_average'
            message = '業界平均を下回る'
        
        return {
            'company_rate': marginal_profit_rate,
            'industry_average': avg_rate,
            'industry_range': (min_rate, max_rate),
            'deviation': deviation,
            'deviation_percentage': deviation_pct,
            'rating': rating,
            'message': message
        }
    
    def _get_pl_value(self, pl_data: pd.DataFrame, item_name: str, month_col: str) -> float:
        """PLデータから特定項目の値を取得"""
        try:
            row = pl_data[pl_data['項目名'] == item_name]
            if not row.empty and month_col in row.columns:
                value = row[month_col].iloc[0]
                return float(value) if pd.notna(value) else 0
            return 0
        except:
            return 0
    
    def _empty_metrics(self) -> Dict:
        """空の指標を返す"""
        return {
            'sales': 0,
            'cogs': 0,
            'marginal_profit': 0,
            'marginal_profit_rate': 0,
            'variable_cost_rate': 0,
            'fixed_costs': 0,
            'operating_profit': 0,
            'operating_profit_rate': 0,
            'breakeven_sales': 0,
            'margin_of_safety': 0,
            'safety_rate': 0,
            'operating_leverage': 0,
            'is_profitable': False
        }


def analyze_profitability_from_db(period_id: int, processor) -> Dict:
    """
    データベースから実績データを取得して収益構造を分析
    
    Args:
        period_id: 会計期間ID
        processor: DataProcessorインスタンス
        
    Returns:
        Dict: 分析結果
    """
    try:
        sys.stderr.write(f"🚀 収益構造分析開始: 期間ID={period_id}\n")
        sys.stderr.flush()
        
        # 実績データを取得
        conn = processor._get_connection()
        
        # プレースホルダーの選択
        placeholder = '%s' if hasattr(processor, 'use_postgres') and processor.use_postgres else '?'
        
        query = f"""
            SELECT item_name, month, value
            FROM actual_data
            WHERE fiscal_period_id = {placeholder}
            ORDER BY month, item_name
        """
        
        df = pd.read_sql_query(query, conn, params=(period_id,))
        conn.close()
        
        if df.empty:
            sys.stderr.write("⚠️ データなし\n")
            sys.stderr.flush()
            return {}
        
        # ピボット形式に変換
        pl_data = df.pivot(index='item_name', columns='month', values='value').reset_index()
        pl_data.columns.name = None
        pl_data = pl_data.rename(columns={'item_name': '項目名'})
        
        # 分析エンジン初期化
        analyzer = ProfitabilityAnalyzer(processor)
        
        # 月次分析
        monthly_df = analyzer.analyze_monthly_profitability(pl_data)
        
        # 平均値計算
        avg_marginal_profit_rate = monthly_df['marginal_profit_rate'].mean() if not monthly_df.empty else 0
        avg_safety_rate = monthly_df['safety_rate'].mean() if not monthly_df.empty else 0
        
        # トレンド判定
        trend = 'stable'
        if len(monthly_df) >= 3:
            recent_3 = monthly_df['marginal_profit_rate'].tail(3).values
            if recent_3[2] > recent_3[0] * 1.02:
                trend = 'improving'
            elif recent_3[2] < recent_3[0] * 0.98:
                trend = 'deteriorating'
        
        sys.stderr.write(f"✅ 収益構造分析完了\n")
        sys.stderr.flush()
        
        return {
            'monthly_data': monthly_df,
            'average_marginal_profit_rate': avg_marginal_profit_rate,
            'average_safety_rate': avg_safety_rate,
            'trend': trend
        }
        
    except Exception as e:
        sys.stderr.write(f"❌ 収益構造分析エラー: {e}\n")
        import traceback
        traceback.print_exc(file=sys.stderr)
        sys.stderr.flush()
        return {}
