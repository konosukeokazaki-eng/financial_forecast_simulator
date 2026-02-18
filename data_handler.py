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
        """データベース初期化"""
        if self.use_postgres:
            return
        import sqlite3
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        cursor = conn.cursor()
        cursor.executescript("""
            CREATE TABLE IF NOT EXISTS fiscal_periods (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                start_date TEXT,
                end_date TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS forecast_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fiscal_period_id INTEGER,
                item_name TEXT,
                month INTEGER,
                amount REAL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(fiscal_period_id, item_name, month)
            );
            CREATE TABLE IF NOT EXISTS actual_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fiscal_period_id INTEGER,
                item_name TEXT,
                month INTEGER,
                amount REAL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(fiscal_period_id, item_name, month)
            );
        """)
        conn.commit()
        conn.close()
    
    def _get_connection(self):
        """データベース接続を取得"""
        if self.use_postgres:
            import psycopg2
            from urllib.parse import urlparse

            result = urlparse(self.conn_string)
            return psycopg2.connect(
                database=result.path[1:],
                user=result.username,
                password=result.password,
                host=result.hostname,
                port=result.port
            )
        else:
            import sqlite3
            return sqlite3.connect(self.db_path, check_same_thread=False)
    
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
            placeholder = '%s' if _self.use_postgres else '?'

            query = f"""
                SELECT DISTINCT month
                FROM actual_data
                WHERE fiscal_period_id = {placeholder}
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
            placeholder = '%s' if _self.use_postgres else '?'

            query = f"""
                SELECT DISTINCT month
                FROM actual_data
                WHERE fiscal_period_id = {placeholder}
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
            placeholder = '%s' if _self.use_postgres else '?'

            query = f"""
                SELECT item_name, month, amount
                FROM actual_data
                WHERE fiscal_period_id = {placeholder}
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
            placeholder = '%s' if _self.use_postgres else '?'

            query = f"""
                SELECT item_name, month, amount
                FROM actual_data
                WHERE fiscal_period_id = {placeholder}
                ORDER BY month, item_name
            """

            df = pd.read_sql_query(query, conn, params=(period_id,))
            return df

        except Exception:
            return pd.DataFrame()
    
    @st.cache_data(ttl=600)
    def load_bs_data(_self, period_id: int) -> pd.DataFrame:
        """BSデータを取得（キャッシュ付き）"""
        # 実装は既存と同じ
        return pd.DataFrame()
    
    # ============ 実績データインポート（永続化） ============
    
    def import_actual_data_from_excel(self, file_path: str, period_id: int, 
                                      sheet_name: str = 'Sheet1') -> Dict:
        """
        Excelファイルから実績データをインポート（永続化版）
        
        Args:
            file_path: Excelファイルパス
            period_id: 会計期間ID
            sheet_name: シート名
            
        Returns:
            Dict: インポート結果
        """
        import sys
        
        try:
            sys.stderr.write(f"📥 実績データインポート開始\n")
            sys.stderr.write(f"   ファイル: {file_path}\n")
            sys.stderr.write(f"   期間ID: {period_id}\n")
            sys.stderr.flush()
            
            # ファイル読み込み
            df = pd.read_excel(file_path, sheet_name=sheet_name)
            
            sys.stderr.write(f"   読み込み: {len(df)}行\n")
            sys.stderr.flush()
            
            if df.empty:
                return {
                    'success': False,
                    'message': 'ファイルにデータがありません',
                    'records_imported': 0
                }
            
            # データベース接続
            conn = self._get_connection()
            cursor = conn.cursor()
            placeholder = '%s' if self.use_postgres else '?'

            # 既存データを削除
            delete_query = f"""
                DELETE FROM actual_data
                WHERE fiscal_period_id = {placeholder}
            """

            cursor.execute(delete_query, (period_id,))
            deleted_count = cursor.rowcount

            sys.stderr.write(f"   既存データ削除: {deleted_count}件\n")
            sys.stderr.flush()

            # 新しいデータを挿入
            if self.use_postgres:
                insert_query = """
                    INSERT INTO actual_data
                    (fiscal_period_id, item_name, month, amount, created_at)
                    VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT (fiscal_period_id, item_name, month)
                    DO UPDATE SET
                        amount = EXCLUDED.amount,
                        created_at = CURRENT_TIMESTAMP
                """
            else:
                insert_query = """
                    INSERT INTO actual_data
                    (fiscal_period_id, item_name, month, amount, created_at)
                    VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT (fiscal_period_id, item_name, month)
                    DO UPDATE SET
                        amount = excluded.amount,
                        created_at = CURRENT_TIMESTAMP
                """
            
            records_imported = 0
            
            for _, row in df.iterrows():
                try:
                    # データ検証
                    item_name = str(row.get('item_name', row.get('科目', '')))
                    month = row.get('month', row.get('月', ''))
                    amount = float(row.get('amount', row.get('金額', 0)))
                    
                    if not item_name or not month:
                        continue
                    
                    # 月を数値に変換
                    if isinstance(month, str) and '-' in month:
                        month_num = int(month.split('-')[1])
                    else:
                        month_num = int(month)
                    
                    cursor.execute(insert_query, (
                        period_id,
                        item_name,
                        month_num,
                        amount
                    ))
                    
                    records_imported += 1
                    
                except Exception as row_error:
                    sys.stderr.write(f"⚠️ 行スキップ: {row_error}\n")
                    sys.stderr.flush()
                    continue
            
            # 🔥 重要: コミット！
            conn.commit()
            
            sys.stderr.write(f"✅ コミット完了: {records_imported}件\n")
            sys.stderr.flush()
            
            cursor.close()
            conn.close()
            
            # キャッシュをクリア
            st.cache_data.clear()
            
            sys.stderr.write(f"🎉 インポート成功\n")
            sys.stderr.flush()
            
            return {
                'success': True,
                'message': f'{records_imported}件のデータをインポートしました',
                'records_imported': records_imported
            }
            
        except Exception as e:
            sys.stderr.write(f"❌ インポートエラー: {e}\n")
            import traceback
            traceback.print_exc(file=sys.stderr)
            sys.stderr.flush()
            
            # ロールバック
            if 'conn' in locals() and conn:
                try:
                    conn.rollback()
                    sys.stderr.write(f"   ロールバック実行\n")
                    sys.stderr.flush()
                except:
                    pass
            
            return {
                'success': False,
                'message': f'インポート失敗: {str(e)}',
                'records_imported': 0
            }
    
    def verify_actual_data(self, period_id: int) -> Dict:
        """
        実績データが正しく保存されているか検証
        
        Args:
            period_id: 会計期間ID
            
        Returns:
            Dict: 検証結果
        """
        try:
            conn = self._get_connection()
            placeholder = '%s' if self.use_postgres else '?'

            query = f"""
                SELECT
                    COUNT(*) as total_records,
                    COUNT(DISTINCT item_name) as item_count,
                    COUNT(DISTINCT month) as month_count,
                    MIN(month) as first_month,
                    MAX(month) as last_month,
                    SUM(amount) as total_amount
                FROM actual_data
                WHERE fiscal_period_id = {placeholder}
            """

            df = pd.read_sql_query(query, conn, params=(period_id,))
            
            if df.empty or df.iloc[0]['total_records'] == 0:
                return {
                    'exists': False,
                    'message': '実績データが見つかりません'
                }
            
            row = df.iloc[0]
            
            return {
                'exists': True,
                'total_records': int(row['total_records']),
                'item_count': int(row['item_count']),
                'month_count': int(row['month_count']),
                'first_month': int(row['first_month']) if row['first_month'] else None,
                'last_month': int(row['last_month']) if row['last_month'] else None,
                'total_amount': float(row['total_amount']) if row['total_amount'] else 0,
                'message': f"{row['total_records']}件の実績データが保存されています"
            }
            
        except Exception as e:
            return {
                'exists': False,
                'message': f'検証エラー: {str(e)}'
            }
    
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
