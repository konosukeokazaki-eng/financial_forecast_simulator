"""
フェーズ5: 高度な自動予測エンジン
- 複数の予測手法（ARIMA、指数平滑、機械学習）
- 季節性・トレンド検出
- 複数シナリオ予測（楽観・標準・悲観）
- 予測精度の評価
"""

import pandas as pd
import numpy as np
import streamlit as st
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import sys


class AdvancedForecastEngine:
    """高度な予測エンジン"""
    
    def __init__(self, data_handler=None):
        self.data_handler = data_handler
    
    # ==================== メイン予測メソッド ====================
    
    @st.cache_data(ttl=300)
    def generate_forecast(_self, period_id: int, forecast_months: int = 12,
                         method: str = 'auto', scenarios: bool = True) -> Dict:
        """
        自動予測を生成
        
        Args:
            period_id: 会計期間ID
            forecast_months: 予測月数
            method: 予測手法 ('auto', 'arima', 'exponential', 'ml', 'average')
            scenarios: 複数シナリオを生成するか
            
        Returns:
            Dict: {
                'forecasts': DataFrame,  # 予測値
                'accuracy': Dict,        # 精度指標
                'seasonality': Dict,     # 季節性情報
                'trend': str,           # トレンド（up/down/stable）
                'scenarios': Dict       # シナリオ別予測（楽観/標準/悲観）
            }
        """
        try:
            sys.stderr.write(f"🔮 高度予測開始: {forecast_months}ヶ月先まで\n")
            sys.stderr.flush()
            
            # 実績データを取得
            actuals = _self._load_actuals(period_id)
            
            if actuals.empty:
                sys.stderr.write("❌ 実績データなし\n")
                return None
            
            sys.stderr.write(f"   実績データ: {len(actuals)}行\n")
            sys.stderr.flush()
            
            # 最適な予測手法を自動選択
            if method == 'auto':
                method = _self._select_best_method(actuals)
                sys.stderr.write(f"   選択された手法: {method}\n")
                sys.stderr.flush()
            
            # 予測実行
            forecast_result = _self._execute_forecast(
                actuals, 
                forecast_months, 
                method
            )
            
            # 季節性・トレンド分析
            seasonality_info = _self._analyze_seasonality(actuals)
            trend_info = _self._analyze_trend(actuals)
            
            # 予測精度の評価（バックテスト）
            accuracy = _self._evaluate_accuracy(actuals, method)
            
            # 複数シナリオの生成
            scenario_forecasts = None
            if scenarios:
                scenario_forecasts = _self._generate_scenarios(
                    forecast_result['forecasts'],
                    actuals,
                    accuracy
                )
            
            result = {
                'forecasts': forecast_result['forecasts'],
                'method': method,
                'accuracy': accuracy,
                'seasonality': seasonality_info,
                'trend': trend_info,
                'scenarios': scenario_forecasts,
                'metadata': {
                    'generated_at': datetime.now().isoformat(),
                    'actual_months': len(actuals),
                    'forecast_months': forecast_months
                }
            }
            
            sys.stderr.write(f"✅ 予測完了\n")
            sys.stderr.flush()
            
            return result
            
        except Exception as e:
            sys.stderr.write(f"❌ 予測エラー: {e}\n")
            import traceback
            traceback.print_exc(file=sys.stderr)
            sys.stderr.flush()
            return None
    
    # ==================== データ取得 ====================
    
    def _load_actuals(self, period_id: int) -> pd.DataFrame:
        """実績データを取得"""
        try:
            if not self.data_handler:
                return pd.DataFrame()
            
            conn = self.data_handler._get_connection()
            
            query = """
                SELECT item_name, month, amount
                FROM actual_data
                WHERE fiscal_period_id = %s
                ORDER BY month, item_name
            """
            
            df = pd.read_sql_query(query, conn, params=(period_id,))
            
            # month列を数値に変換
            if df['month'].dtype == 'object':
                df['month_num'] = df['month'].apply(
                    lambda x: int(str(x).split('-')[1]) if '-' in str(x) else int(x)
                )
            else:
                df['month_num'] = df['month']
            
            return df
            
        except Exception as e:
            sys.stderr.write(f"データ取得エラー: {e}\n")
            return pd.DataFrame()
    
    # ==================== 予測手法の選択 ====================
    
    def _select_best_method(self, actuals: pd.DataFrame) -> str:
        """
        データの特性に応じて最適な予測手法を選択
        
        Args:
            actuals: 実績データ
            
        Returns:
            str: 予測手法名
        """
        # データ期間
        n_months = actuals['month_num'].nunique()
        
        # 季節性の有無
        has_seasonality = self._detect_seasonality(actuals)
        
        # トレンドの有無
        has_trend = self._detect_trend(actuals)
        
        # 選択ロジック
        if n_months < 6:
            return 'average'  # データ少ない → 平均法
        elif n_months < 12:
            return 'exponential'  # 1年未満 → 指数平滑
        elif has_seasonality and has_trend:
            return 'arima'  # 季節性+トレンド → ARIMA
        elif has_trend:
            return 'linear_regression'  # トレンドのみ → 回帰
        else:
            return 'exponential'  # デフォルト → 指数平滑
    
    def _detect_seasonality(self, actuals: pd.DataFrame) -> bool:
        """季節性を検出"""
        # 簡易判定: 12ヶ月以上あれば季節性をチェック
        if actuals['month_num'].nunique() < 12:
            return False
        
        # 売上高で判定
        sales_data = actuals[actuals['item_name'] == '売上高']
        if sales_data.empty:
            return False
        
        values = sales_data.sort_values('month_num')['amount'].values
        
        if len(values) < 12:
            return False
        
        # 自己相関係数を計算（12ヶ月lag）
        try:
            correlation = np.corrcoef(values[:-12], values[12:])[0, 1]
            return correlation > 0.5  # 相関が強ければ季節性あり
        except:
            return False
    
    def _detect_trend(self, actuals: pd.DataFrame) -> bool:
        """トレンドを検出"""
        sales_data = actuals[actuals['item_name'] == '売上高']
        if sales_data.empty:
            return False
        
        values = sales_data.sort_values('month_num')['amount'].values
        
        if len(values) < 3:
            return False
        
        # 線形回帰の傾き
        x = np.arange(len(values))
        slope = np.polyfit(x, values, 1)[0]
        
        # 平均値の5%以上の傾きがあればトレンドあり
        avg_value = np.mean(values)
        return abs(slope) > (avg_value * 0.05 / len(values))
    
    # ==================== 予測実行 ====================
    
    def _execute_forecast(self, actuals: pd.DataFrame, forecast_months: int, 
                         method: str) -> Dict:
        """予測を実行"""
        
        # 科目ごとに予測
        forecasts_by_item = {}
        
        for item_name in actuals['item_name'].unique():
            item_data = actuals[actuals['item_name'] == item_name].sort_values('month_num')
            values = item_data['amount'].values
            
            # 手法に応じて予測
            if method == 'average':
                forecasts = self._forecast_average(values, forecast_months)
            elif method == 'exponential':
                forecasts = self._forecast_exponential(values, forecast_months)
            elif method == 'linear_regression':
                forecasts = self._forecast_linear(values, forecast_months)
            elif method == 'arima':
                forecasts = self._forecast_arima(values, forecast_months)
            else:
                forecasts = self._forecast_average(values, forecast_months)
            
            forecasts_by_item[item_name] = forecasts
        
        # DataFrameに変換
        latest_month = actuals['month_num'].max()
        forecast_months_list = [latest_month + i + 1 for i in range(forecast_months)]
        
        df_forecast = pd.DataFrame({
            '科目': list(forecasts_by_item.keys()),
            **{f'{m}月': [forecasts_by_item[item][i] for item in forecasts_by_item.keys()] 
               for i, m in enumerate(forecast_months_list)}
        })
        
        return {
            'forecasts': df_forecast,
            'forecast_months': forecast_months_list
        }
    
    # ==================== 予測アルゴリズム ====================
    
    def _forecast_average(self, values: np.ndarray, periods: int) -> List[float]:
        """平均法"""
        avg = np.mean(values)
        return [avg] * periods
    
    def _forecast_exponential(self, values: np.ndarray, periods: int, 
                             alpha: float = 0.3) -> List[float]:
        """指数平滑法"""
        forecasts = []
        last_smoothed = values[0]
        
        # 過去データで平滑化
        for value in values:
            last_smoothed = alpha * value + (1 - alpha) * last_smoothed
        
        # 予測
        for _ in range(periods):
            forecasts.append(last_smoothed)
        
        return forecasts
    
    def _forecast_linear(self, values: np.ndarray, periods: int) -> List[float]:
        """線形回帰法"""
        x = np.arange(len(values))
        coeffs = np.polyfit(x, values, 1)
        
        forecasts = []
        for i in range(periods):
            forecast_x = len(values) + i
            forecast_y = coeffs[0] * forecast_x + coeffs[1]
            forecasts.append(max(0, forecast_y))  # 負の値を防ぐ
        
        return forecasts
    
    def _forecast_arima(self, values: np.ndarray, periods: int) -> List[float]:
        """ARIMA法（簡易版）"""
        # 簡易実装: トレンド+季節性を考慮した予測
        
        if len(values) < 12:
            return self._forecast_exponential(values, periods)
        
        # トレンド成分
        x = np.arange(len(values))
        trend_coeffs = np.polyfit(x, values, 1)
        trend = trend_coeffs[0] * x + trend_coeffs[1]
        
        # 季節成分（12ヶ月周期）
        detrended = values - trend
        seasonal = np.zeros(12)
        for i in range(12):
            seasonal[i] = np.mean([detrended[j] for j in range(len(detrended)) if j % 12 == i])
        
        # 予測
        forecasts = []
        for i in range(periods):
            forecast_x = len(values) + i
            trend_value = trend_coeffs[0] * forecast_x + trend_coeffs[1]
            seasonal_value = seasonal[i % 12]
            forecast = trend_value + seasonal_value
            forecasts.append(max(0, forecast))
        
        return forecasts
    
    # ==================== 季節性・トレンド分析 ====================
    
    def _analyze_seasonality(self, actuals: pd.DataFrame) -> Dict:
        """季節性を分析"""
        sales_data = actuals[actuals['item_name'] == '売上高'].sort_values('month_num')
        
        if sales_data.empty or len(sales_data) < 12:
            return {'has_seasonality': False}
        
        values = sales_data['amount'].values
        months = sales_data['month_num'].values
        
        # 月別平均を計算
        monthly_avg = {}
        for month in range(1, 13):
            month_values = [values[i] for i in range(len(values)) if months[i] == month]
            if month_values:
                monthly_avg[month] = np.mean(month_values)
        
        # 全体平均との差異
        overall_avg = np.mean(values)
        seasonal_indices = {
            month: (avg / overall_avg - 1) * 100
            for month, avg in monthly_avg.items()
        }
        
        # 季節性の強さ
        seasonality_strength = np.std(list(seasonal_indices.values()))
        
        return {
            'has_seasonality': seasonality_strength > 10,  # 10%以上の変動
            'strength': seasonality_strength,
            'indices': seasonal_indices,
            'peak_month': max(seasonal_indices, key=seasonal_indices.get),
            'low_month': min(seasonal_indices, key=seasonal_indices.get)
        }
    
    def _analyze_trend(self, actuals: pd.DataFrame) -> Dict:
        """トレンドを分析"""
        sales_data = actuals[actuals['item_name'] == '売上高'].sort_values('month_num')
        
        if sales_data.empty or len(sales_data) < 3:
            return {'trend': 'stable', 'slope': 0}
        
        values = sales_data['amount'].values
        x = np.arange(len(values))
        
        # 線形回帰
        coeffs = np.polyfit(x, values, 1)
        slope = coeffs[0]
        
        # トレンドの判定
        avg_value = np.mean(values)
        slope_pct = (slope / avg_value) * 100
        
        if slope_pct > 2:
            trend = 'up'
        elif slope_pct < -2:
            trend = 'down'
        else:
            trend = 'stable'
        
        return {
            'trend': trend,
            'slope': slope,
            'slope_pct': slope_pct,
            'interpretation': self._interpret_trend(trend, slope_pct)
        }
    
    def _interpret_trend(self, trend: str, slope_pct: float) -> str:
        """トレンドの解釈"""
        if trend == 'up':
            return f"上昇傾向（月{slope_pct:.1f}%の成長）"
        elif trend == 'down':
            return f"下降傾向（月{abs(slope_pct):.1f}%の減少）"
        else:
            return "横ばい傾向"
    
    # ==================== 予測精度評価 ====================
    
    def _evaluate_accuracy(self, actuals: pd.DataFrame, method: str) -> Dict:
        """
        予測精度を評価（バックテスト）
        
        直近3ヶ月を除外して予測し、実績と比較
        """
        sales_data = actuals[actuals['item_name'] == '売上高'].sort_values('month_num')
        
        if len(sales_data) < 6:
            return {'mape': None, 'rmse': None, 'mae': None}
        
        # 直近3ヶ月を除外
        train_data = sales_data.iloc[:-3]['amount'].values
        test_data = sales_data.iloc[-3:]['amount'].values
        
        # 予測
        if method == 'average':
            predictions = self._forecast_average(train_data, 3)
        elif method == 'exponential':
            predictions = self._forecast_exponential(train_data, 3)
        elif method == 'linear_regression':
            predictions = self._forecast_linear(train_data, 3)
        else:
            predictions = self._forecast_average(train_data, 3)
        
        # 精度指標を計算
        mape = np.mean(np.abs((test_data - predictions) / test_data)) * 100
        rmse = np.sqrt(np.mean((test_data - predictions) ** 2))
        mae = np.mean(np.abs(test_data - predictions))
        
        return {
            'mape': float(mape),  # 平均絶対パーセント誤差
            'rmse': float(rmse),  # 二乗平均平方根誤差
            'mae': float(mae),    # 平均絶対誤差
            'interpretation': self._interpret_accuracy(mape)
        }
    
    def _interpret_accuracy(self, mape: float) -> str:
        """精度の解釈"""
        if mape < 10:
            return "非常に高精度"
        elif mape < 20:
            return "高精度"
        elif mape < 30:
            return "中程度の精度"
        else:
            return "低精度（要注意）"
    
    # ==================== シナリオ生成 ====================
    
    def _generate_scenarios(self, base_forecast: pd.DataFrame, 
                           actuals: pd.DataFrame, accuracy: Dict) -> Dict:
        """
        複数シナリオを生成（楽観・標準・悲観）
        
        Args:
            base_forecast: 基本予測
            actuals: 実績データ
            accuracy: 精度情報
            
        Returns:
            Dict: シナリオ別予測
        """
        # 標準シナリオ = 基本予測
        standard = base_forecast.copy()
        
        # 過去の変動性を計算
        sales_data = actuals[actuals['item_name'] == '売上高']
        values = sales_data['amount'].values
        
        if len(values) < 2:
            volatility = 0.1  # デフォルト10%
        else:
            returns = np.diff(values) / values[:-1]
            volatility = np.std(returns)
        
        # 楽観シナリオ: +1標準偏差
        optimistic = base_forecast.copy()
        for col in optimistic.columns:
            if '月' in col:
                optimistic[col] = optimistic[col] * (1 + volatility)
        
        # 悲観シナリオ: -1標準偏差
        pessimistic = base_forecast.copy()
        for col in pessimistic.columns:
            if '月' in col:
                pessimistic[col] = pessimistic[col] * (1 - volatility)
        
        return {
            '標準': standard,
            '楽観': optimistic,
            '悲観': pessimistic,
            'volatility': float(volatility)
        }


# ==================== グローバル関数 ====================

_engine_instance = None

def get_advanced_forecast_engine(data_handler=None):
    """シングルトンのAdvancedForecastEngineを取得"""
    global _engine_instance
    if _engine_instance is None:
        _engine_instance = AdvancedForecastEngine(data_handler)
    return _engine_instance
