"""
統合データハンドラーモジュール
data_processor + actual_period_manager を統合し、キャッシュ最適化
"""

import pandas as pd
import streamlit as st
from typing import Optional, Dict, List
import os


class DataHandler:
    """統合データハンドラー（最適化版）"""
    
    def __init__(self, db_path=None):
        self.use_postgres = False
        self.conn_string = None
        self.db_path = db_path
        
        # PostgreSQL接続設定
        if hasattr(st, 'secrets') and 'database' in st.secrets:
            try:
                db_config = st.secrets['database']
                self.conn_string = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
                
                test_conn = self._test_postgres_connection()
                if test_conn:
                    self.use_postgres = True
            except:
                self.use_postgres = False
        
        # SQLiteの場合
        if not self.use_postgres:
            if db_path is None:
                base_dir = os.path.dirname(os.path.abspath(__file__))
                self.db_path = os.path.join(base_dir, "financial_data.db")
            else:
                self.db_path = db_path
        
        self._init_db()
    
    def _test_postgres_connection(self):
        """PostgreSQL接続テスト"""
        try:
            import psycopg2
            from urllib.parse import urlparse
            result = urlparse(self.conn_string)
            conn = psycopg2.connect(
                database=result.path[1:],
                user=result.username,
                password=result.password,
                host=result.hostname,
                port=result.port
            )
            conn.close()
            return True
        except:
            return False
    
    def _init_db(self):
        """データベース初期化（省略 - 既存のコードと同じ）"""
        pass
    
    @st.cache_resource
    def _get_connection(_self):
        """
        データベース接続を取得（キャッシュ付き）
        接続をリソースとしてキャッシュ
        """
        if _self.use_postgres:
            import psycopg2
            from urllib.parse import urlparse
            
            result = urlparse(_self.conn_string)
            return psycopg2.connect(
                database=result.path[1:],
                user=result.username,
                password=result.password,
                host=result.hostname,
                port=result.port
            )
        else:
            import sqlite3
            return sqlite3.connect(_self.db_path, check_same_thread=False)
    
    # ============ 実績締月管理（キャッシュ付き） ============
    
    @st.cache_data(ttl=300)  # 5分キャッシュ
    def get_latest_actual_month(_self, period_id: int) -> Optional[int]:
        """
        最新実績締月を取得（キャッシュ付き）
        
        Args:
            period_id: 会計期間ID
            
        Returns:
            int: 最新締月
        """
        try:
            conn = _self._get_connection()
            
            query = """
                SELECT DISTINCT month
                FROM actual_data
                WHERE fiscal_period_id = %s
                ORDER BY month DESC
                LIMIT 1
            """
            
            df = pd.read_sql_query(query, conn, params=(period_id,))
            
            if df.empty:
                return None
            
            month_value = df.iloc[0]['month']
            
            # 月番号を抽出
            if isinstance(month_value, str) and '-' in month_value:
                month_num = int(month_value.split('-')[1])
            else:
                month_num = int(month_value)
            
            return month_num
            
        except:
            return None
    
    @st.cache_data(ttl=300)
    def get_actual_months_list(_self, period_id: int) -> list:
        """実績月リストを取得（キャッシュ付き）"""
        try:
            conn = _self._get_connection()
            
            query = """
                SELECT DISTINCT month
                FROM actual_data
                WHERE fiscal_period_id = %s
                ORDER BY month ASC
            """
            
            df = pd.read_sql_query(query, conn, params=(period_id,))
            
            if df.empty:
                return []
            
            months = []
            for month_value in df['month']:
                if isinstance(month_value, str) and '-' in month_value:
                    month_num = int(month_value.split('-')[1])
                else:
                    month_num = int(month_value)
                months.append(month_num)
            
            return sorted(months)
            
        except:
            return []
    
    @st.cache_data(ttl=300)
    def get_actual_vs_forecast_split(_self, period_id: int) -> Dict:
        """実績/予測分割情報を取得（キャッシュ付き）"""
        latest_actual = _self.get_latest_actual_month(period_id)
        actual_months = _self.get_actual_months_list(period_id)
        
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
    
    @st.cache_data(ttl=300)
    def get_cumulative_actual_data(_self, period_id: int, up_to_month: int) -> Dict:
        """累計実績データを取得（キャッシュ付き）"""
        try:
            conn = _self._get_connection()
            
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
            
        except:
            return {}
    
    # ============ データ取得（キャッシュ付き） ============
    
    @st.cache_data(ttl=600)  # 10分キャッシュ
    def load_pl_data(_self, period_id: int) -> pd.DataFrame:
        """PLデータを取得（キャッシュ付き）"""
        try:
            conn = _self._get_connection()
            
            query = """
                SELECT item_name, month, amount
                FROM actual_data
                WHERE fiscal_period_id = %s
                ORDER BY month, item_name
            """
            
            df = pd.read_sql_query(query, conn, params=(period_id,))
            return df
            
        except:
            return pd.DataFrame()
    
    @st.cache_data(ttl=600)
    def load_bs_data(_self, period_id: int) -> pd.DataFrame:
        """BSデータを取得（キャッシュ付き）"""
        # 実装は既存と同じ
        return pd.DataFrame()
    
    # ============ デバッグ出力制御 ============
    
    @staticmethod
    def debug_log(message: str):
        """デバッグ出力（環境変数で制御）"""
        if os.getenv('DEBUG', 'false').lower() == 'true':
            import sys
            sys.stderr.write(message)
            sys.stderr.flush()


# ============ グローバル関数（後方互換性） ============

# 既存コードとの互換性のため、グローバル関数も提供
_handler_instance = None

def get_data_handler():
    """シングルトンのDataHandlerを取得"""
    global _handler_instance
    if _handler_instance is None:
        _handler_instance = DataHandler()
    return _handler_instance
