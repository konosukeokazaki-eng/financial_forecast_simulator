"""
財務予測エンジン
過去実績から自動予測し、手動調整を可能にする
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import sys


class ForecastEngine:
    """財務予測エンジン"""
    
    def __init__(self, processor):
        """
        初期化
        
        Args:
            processor: DataProcessorインスタンス
        """
        self.processor = processor
    
    def forecast_sales_yoy(self, actuals: List[float], periods_ahead: int = 12) -> List[float]:
        """
        前年同月比法で売上予測
        
        Args:
            actuals: 過去実績データ（月次）
            periods_ahead: 予測期間（月数）
            
        Returns:
            List[float]: 予測値のリスト
        """
        try:
            sys.stderr.write(f"📊 前年同月比予測開始: {len(actuals)}ヶ月の実績データ\n")
            sys.stderr.flush()
            
            forecasts = []
            
            if len(actuals) < 12:
                # 1年未満の場合は移動平均
                sys.stderr.write("⚠️ 1年未満のデータ → 移動平均を使用\n")
                sys.stderr.flush()
                return self._moving_average_forecast(actuals, periods_ahead)
            
            # 過去12ヶ月の平均成長率を計算
            growth_rates = []
            for i in range(1, min(12, len(actuals))):
                if actuals[i-1] != 0:
                    rate = (actuals[i] / actuals[i-1]) - 1
                    growth_rates.append(rate)
            
            avg_growth_rate = sum(growth_rates) / len(growth_rates) if growth_rates else 0
            
            sys.stderr.write(f"   平均成長率: {avg_growth_rate*100:.2f}%\n")
            sys.stderr.flush()
            
            # 予測
            for i in range(periods_ahead):
                # 前年同月を取得
                year_ago_index = len(actuals) - 12 + (i % 12)
                
                if year_ago_index >= 0 and year_ago_index < len(actuals):
                    last_year_value = actuals[year_ago_index]
                    forecast = last_year_value * (1 + avg_growth_rate)
                else:
                    # データがない場合は最後の値をベースに
                    forecast = actuals[-1] * (1 + avg_growth_rate)
                
                forecasts.append(forecast)
            
            sys.stderr.write(f"✅ 予測完了: {len(forecasts)}ヶ月\n")
            sys.stderr.flush()
            
            return forecasts
            
        except Exception as e:
            sys.stderr.write(f"❌ 予測エラー: {e}\n")
            import traceback
            traceback.print_exc(file=sys.stderr)
            sys.stderr.flush()
            return [0] * periods_ahead
    
    def _moving_average_forecast(self, actuals: List[float], periods_ahead: int) -> List[float]:
        """
        移動平均法で予測
        
        Args:
            actuals: 過去実績データ
            periods_ahead: 予測期間
            
        Returns:
            List[float]: 予測値のリスト
        """
        n = min(3, len(actuals))
        avg = sum(actuals[-n:]) / n
        return [avg] * periods_ahead
    
    def forecast_with_cost_structure(
        self, 
        sales_forecast: List[float],
        variable_cost_rate: float = 0.60,
        fixed_costs: float = 20000000
    ) -> pd.DataFrame:
        """
        変動費・固定費構造での予測
        
        Args:
            sales_forecast: 売上予測
            variable_cost_rate: 変動費率（デフォルト60%）
            fixed_costs: 月間固定費
            
        Returns:
            DataFrame: 予測PL
        """
        try:
            sys.stderr.write("💰 費用構造予測開始\n")
            sys.stderr.flush()
            
            data = []
            for i, sales in enumerate(sales_forecast):
                # 変動費
                cogs = sales * variable_cost_rate
                
                # 粗利
                gross_profit = sales - cogs
                
                # 営業利益
                operating_profit = gross_profit - fixed_costs
                
                data.append({
                    '月': f'{i+1}月',
                    '売上': sales,
                    '売上原価': cogs,
                    '粗利': gross_profit,
                    '販管費': fixed_costs,
                    '営業利益': operating_profit,
                    '変動費率': variable_cost_rate,
                    '粗利率': (gross_profit / sales) if sales > 0 else 0
                })
            
            df = pd.DataFrame(data)
            
            sys.stderr.write(f"✅ 費用予測完了: {len(df)}ヶ月\n")
            sys.stderr.flush()
            
            return df
            
        except Exception as e:
            sys.stderr.write(f"❌ 費用予測エラー: {e}\n")
            import traceback
            traceback.print_exc(file=sys.stderr)
            sys.stderr.flush()
            return pd.DataFrame()
    
    def calculate_seasonal_index(self, monthly_data: List[float]) -> Dict[int, float]:
        """
        季節指数を計算
        
        Args:
            monthly_data: 過去24ヶ月以上の月次データ
            
        Returns:
            Dict[int, float]: 月ごとの季節指数（1-12月）
        """
        try:
            if len(monthly_data) < 24:
                sys.stderr.write("⚠️ 季節指数計算には24ヶ月以上のデータが必要\n")
                sys.stderr.flush()
                return {i: 1.0 for i in range(1, 13)}
            
            sys.stderr.write("📊 季節指数計算開始\n")
            sys.stderr.flush()
            
            # 月ごとにグループ化
            monthly_groups = {i: [] for i in range(1, 13)}
            
            for i, value in enumerate(monthly_data):
                month = (i % 12) + 1
                monthly_groups[month].append(value)
            
            # 各月の平均
            monthly_avg = {m: sum(v)/len(v) for m, v in monthly_groups.items() if len(v) > 0}
            
            # 全体平均
            overall_avg = sum(monthly_avg.values()) / len(monthly_avg)
            
            # 季節指数
            seasonal_index = {m: avg / overall_avg for m, avg in monthly_avg.items()}
            
            sys.stderr.write("✅ 季節指数計算完了\n")
            for month, index in seasonal_index.items():
                sys.stderr.write(f"   {month}月: {index:.3f}\n")
            sys.stderr.flush()
            
            return seasonal_index
            
        except Exception as e:
            sys.stderr.write(f"❌ 季節指数計算エラー: {e}\n")
            import traceback
            traceback.print_exc(file=sys.stderr)
            sys.stderr.flush()
            return {i: 1.0 for i in range(1, 13)}
    
    def forecast_with_seasonality(
        self,
        base_forecast: List[float],
        seasonal_index: Dict[int, float],
        start_month: int = 1
    ) -> List[float]:
        """
        季節性を考慮した予測
        
        Args:
            base_forecast: ベース予測値
            seasonal_index: 季節指数
            start_month: 開始月（1-12）
            
        Returns:
            List[float]: 季節調整後の予測値
        """
        adjusted_forecast = []
        
        for i, base in enumerate(base_forecast):
            month = ((start_month + i - 1) % 12) + 1
            adjusted = base * seasonal_index.get(month, 1.0)
            adjusted_forecast.append(adjusted)
        
        return adjusted_forecast
    
    def create_scenarios(
        self,
        base_forecast: List[float],
        optimistic_rate: float = 0.15,
        pessimistic_rate: float = 0.15
    ) -> Dict[str, List[float]]:
        """
        3シナリオ予測を生成
        
        Args:
            base_forecast: 標準予測
            optimistic_rate: 楽観シナリオの上乗せ率
            pessimistic_rate: 悲観シナリオの削減率
            
        Returns:
            Dict: 'standard', 'optimistic', 'pessimistic'の予測値
        """
        return {
            'standard': base_forecast,
            'optimistic': [v * (1 + optimistic_rate) for v in base_forecast],
            'pessimistic': [v * (1 - pessimistic_rate) for v in base_forecast]
        }


class ForecastAdjustment:
    """予測の手動調整を管理"""
    
    def __init__(self, base_forecast: float):
        """
        初期化
        
        Args:
            base_forecast: 自動予測の値
        """
        self.base = base_forecast
        self.adjustments = []
    
    def add_adjustment(self, name: str, amount: float, reason: str = ""):
        """
        調整項目を追加
        
        Args:
            name: 調整項目名
            amount: 調整額（プラスまたはマイナス）
            reason: 理由
        """
        self.adjustments.append({
            'name': name,
            'amount': amount,
            'reason': reason,
            'timestamp': datetime.now()
        })
    
    def remove_adjustment(self, index: int):
        """
        調整項目を削除
        
        Args:
            index: 調整項目のインデックス
        """
        if 0 <= index < len(self.adjustments):
            del self.adjustments[index]
    
    def get_final_forecast(self) -> float:
        """
        最終予測値を取得
        
        Returns:
            float: 調整後の予測値
        """
        total_adjustment = sum(a['amount'] for a in self.adjustments)
        return self.base + total_adjustment
    
    def get_breakdown(self) -> Dict:
        """
        内訳を取得
        
        Returns:
            Dict: 自動予測、調整項目、最終予測の内訳
        """
        return {
            'base_forecast': self.base,
            'adjustments': self.adjustments,
            'total_adjustment': sum(a['amount'] for a in self.adjustments),
            'final_forecast': self.get_final_forecast()
        }
    
    def to_dict(self) -> Dict:
        """辞書形式に変換"""
        return self.get_breakdown()


def auto_forecast_from_actuals(
    period_id: int,
    processor,
    forecast_months: int = 12
) -> pd.DataFrame:
    """
    実績データから自動予測を生成
    
    Args:
        period_id: 会計期間ID
        processor: DataProcessorインスタンス
        forecast_months: 予測月数
        
    Returns:
        DataFrame: 予測データ
    """
    try:
        sys.stderr.write(f"🚀 自動予測生成開始: 期間ID={period_id}\n")
        sys.stderr.flush()
        
        # 実績データを取得
        conn = processor._get_connection()
        query = """
            SELECT item_name, amount, month
            FROM actual_data
            WHERE fiscal_period_id = ?
            ORDER BY month
        """
        
        # SQLiteとPostgreSQLで異なるプレースホルダー
        if hasattr(processor, 'use_postgres') and processor.use_postgres:
            query = query.replace('?', '%s')
        
        df_actuals = pd.read_sql_query(query, conn, params=(period_id,))
        
        # カラム名を'value'にリネーム（後続処理との互換性）
        df_actuals = df_actuals.rename(columns={'amount': 'value'})
        
        if df_actuals.empty:
            sys.stderr.write("⚠️ 実績データなし\n")
            sys.stderr.flush()
            return pd.DataFrame()
        
        # 予測エンジン初期化
        engine = ForecastEngine(processor)
        
        # 項目ごとに予測
        forecast_data = []
        
        for item_name in df_actuals['item_name'].unique():
            item_actuals = df_actuals[df_actuals['item_name'] == item_name]
            actuals_list = item_actuals['value'].tolist()
            
            # 前年同月比予測
            forecasts = engine.forecast_sales_yoy(actuals_list, forecast_months)
            
            for i, forecast_value in enumerate(forecasts):
                forecast_data.append({
                    'item_name': item_name,
                    'month': i + 1,
                    'forecast_value': forecast_value,
                    'method': 'yoy'
                })
        
        result_df = pd.DataFrame(forecast_data)
        
        sys.stderr.write(f"✅ 自動予測完了: {len(result_df)}レコード\n")
        sys.stderr.flush()
        
        return result_df
        
    except Exception as e:
        sys.stderr.write(f"❌ 自動予測エラー: {e}\n")
        import traceback
        traceback.print_exc(file=sys.stderr)
        sys.stderr.flush()
        return pd.DataFrame()
