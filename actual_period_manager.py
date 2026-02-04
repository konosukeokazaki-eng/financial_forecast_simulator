"""
実績締月管理モジュール
actual_dataテーブルから最新の実績締月を取得し、各画面で使用
"""

import pandas as pd
from typing import Optional, Dict
import sys


class ActualPeriodManager:
    """実績締月管理クラス"""
    
    def __init__(self, processor):
        """
        初期化
        
        Args:
            processor: DataProcessorインスタンス
        """
        self.processor = processor
    
    def get_latest_actual_month(self, period_id: int) -> Optional[int]:
        """
        指定会計期間の最新実績締月を取得
        
        Args:
            period_id: 会計期間ID
            
        Returns:
            int: 最新締月（1-12）、データがなければNone
        """
        try:
            conn = self.processor._get_connection()
            
            # PostgreSQL用のクエリ
            query = """
                SELECT DISTINCT month
                FROM actual_data
                WHERE fiscal_period_id = %s
                ORDER BY month DESC
                LIMIT 1
            """
            
            df = pd.read_sql_query(query, conn, params=(period_id,))
            
            if df.empty:
                sys.stderr.write(f"⚠️ 実績データなし: period_id={period_id}\n")
                sys.stderr.flush()
                return None
            
            month_value = df.iloc[0]['month']
            
            # month列が文字列（'2025-03'形式）の場合、月番号を抽出
            if isinstance(month_value, str) and '-' in month_value:
                month_num = int(month_value.split('-')[1])
            else:
                month_num = int(month_value)
            
            sys.stderr.write(f"✅ 最新実績締月: {month_num}月 (period_id={period_id})\n")
            sys.stderr.flush()
            
            return month_num
            
        except Exception as e:
            sys.stderr.write(f"❌ 実績締月取得エラー: {e}\n")
            import traceback
            traceback.print_exc(file=sys.stderr)
            sys.stderr.flush()
            return None
    
    def get_actual_months_list(self, period_id: int) -> list:
        """
        実績データが存在する月のリストを取得
        
        Args:
            period_id: 会計期間ID
            
        Returns:
            list: 月番号のリスト（昇順）
        """
        try:
            conn = self.processor._get_connection()
            
            query = """
                SELECT DISTINCT month
                FROM actual_data
                WHERE fiscal_period_id = %s
                ORDER BY month ASC
            """
            
            df = pd.read_sql_query(query, conn, params=(period_id,))
            
            if df.empty:
                return []
            
            # month列を月番号に変換
            months = []
            for month_value in df['month']:
                if isinstance(month_value, str) and '-' in month_value:
                    month_num = int(month_value.split('-')[1])
                else:
                    month_num = int(month_value)
                months.append(month_num)
            
            return sorted(months)
            
        except Exception as e:
            sys.stderr.write(f"❌ 実績月リスト取得エラー: {e}\n")
            return []
    
    def get_actual_data_for_month(self, period_id: int, month: int) -> pd.DataFrame:
        """
        指定月の実績データを取得
        
        Args:
            period_id: 会計期間ID
            month: 月（1-12）
            
        Returns:
            DataFrame: 実績データ
        """
        try:
            conn = self.processor._get_connection()
            
            # month列の形式に応じてクエリを調整
            query = """
                SELECT item_name, month, amount
                FROM actual_data
                WHERE fiscal_period_id = %s
                ORDER BY month, item_name
            """
            
            df = pd.read_sql_query(query, conn, params=(period_id,))
            
            if df.empty:
                return pd.DataFrame()
            
            # month列を月番号に変換
            if df['month'].dtype == 'object' or isinstance(df['month'].iloc[0], str):
                df['month_num'] = df['month'].apply(
                    lambda x: int(str(x).split('-')[1]) if '-' in str(x) else int(x)
                )
            else:
                df['month_num'] = df['month']
            
            # 指定月でフィルタ
            df_month = df[df['month_num'] == month].copy()
            
            # カラム名を統一
            df_month = df_month.rename(columns={'amount': 'value'})
            
            return df_month[['item_name', 'value']]
            
        except Exception as e:
            sys.stderr.write(f"❌ 月次実績データ取得エラー: {e}\n")
            return pd.DataFrame()
    
    def get_cumulative_actual_data(self, period_id: int, up_to_month: int) -> Dict:
        """
        期首から指定月までの累計実績データを取得
        
        Args:
            period_id: 会計期間ID
            up_to_month: 累計終了月（1-12）
            
        Returns:
            Dict: 科目名 → 累計金額
        """
        try:
            conn = self.processor._get_connection()
            
            query = """
                SELECT item_name, month, amount
                FROM actual_data
                WHERE fiscal_period_id = %s
                ORDER BY month, item_name
            """
            
            df = pd.read_sql_query(query, conn, params=(period_id,))
            
            if df.empty:
                return {}
            
            # month列を月番号に変換
            if df['month'].dtype == 'object' or isinstance(df['month'].iloc[0], str):
                df['month_num'] = df['month'].apply(
                    lambda x: int(str(x).split('-')[1]) if '-' in str(x) else int(x)
                )
            else:
                df['month_num'] = df['month']
            
            # 指定月までのデータでフィルタ
            df_filtered = df[df['month_num'] <= up_to_month].copy()
            
            # 科目ごとに累計
            cumulative = df_filtered.groupby('item_name')['amount'].sum().to_dict()
            
            return cumulative
            
        except Exception as e:
            sys.stderr.write(f"❌ 累計実績データ取得エラー: {e}\n")
            return {}
    
    def is_actual_month(self, period_id: int, month: int) -> bool:
        """
        指定月に実績データが存在するか確認
        
        Args:
            period_id: 会計期間ID
            month: 月（1-12）
            
        Returns:
            bool: 実績データが存在すればTrue
        """
        actual_months = self.get_actual_months_list(period_id)
        return month in actual_months
    
    def get_actual_vs_forecast_split(self, period_id: int) -> Dict:
        """
        実績月と予測月の分割情報を取得
        
        Args:
            period_id: 会計期間ID
            
        Returns:
            Dict: {
                'latest_actual_month': int,
                'actual_months': [1,2,3,...],
                'forecast_months': [7,8,9,...],
                'has_actual': bool
            }
        """
        latest_actual = self.get_latest_actual_month(period_id)
        actual_months = self.get_actual_months_list(period_id)
        
        if latest_actual is None:
            return {
                'latest_actual_month': 0,
                'actual_months': [],
                'forecast_months': list(range(1, 13)),
                'has_actual': False
            }
        
        forecast_months = [m for m in range(1, 13) if m > latest_actual]
        
        return {
            'latest_actual_month': latest_actual,
            'actual_months': actual_months,
            'forecast_months': forecast_months,
            'has_actual': True
        }
    
    def format_month_display(self, month: int, is_actual: bool = True) -> str:
        """
        月の表示形式を生成
        
        Args:
            month: 月（1-12）
            is_actual: 実績月かどうか
            
        Returns:
            str: 表示用文字列（例: "3月(実績)" or "7月(予測)"）
        """
        suffix = "(実績)" if is_actual else "(予測)"
        return f"{month}月{suffix}"
