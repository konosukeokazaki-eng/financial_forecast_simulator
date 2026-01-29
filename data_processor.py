import sqlite3
import pandas as pd
import numpy as np
import re
import os
from datetime import datetime, timedelta
import streamlit as st
import sys

class DataProcessor:
    def __init__(self, db_path=None):
        # データベース接続の設定
        self.use_postgres = False
        self.conn_string = None
        
        # Streamlit Secretsからデータベース設定を取得
        sys.stderr.write("=" * 80 + "\n")
        sys.stderr.write("🔍 データベース接続チェック開始...\n")
        sys.stderr.write(f"   hasattr(st, 'secrets'): {hasattr(st, 'secrets')}\n")
        sys.stderr.flush()
        
        # Streamlit Secretsの安全なチェック
        has_secrets = False
        try:
            # st.secrets自体へのアクセスで例外が発生する場合がある
            if hasattr(st, 'secrets') and len(st.secrets) > 0:
                has_secrets = True
        except:
            has_secrets = False

        if has_secrets and 'database' in st.secrets:
            try:
                db_config = st.secrets['database']
                sys.stderr.write(f"   host: {db_config.get('host', 'NOT SET')}\n")
                sys.stderr.write(f"   database: {db_config.get('database', 'NOT SET')}\n")
                sys.stderr.write(f"   user: {db_config.get('user', 'NOT SET')}\n")
                sys.stderr.write(f"   port: {db_config.get('port', 'NOT SET')}\n")
                sys.stderr.write(f"   password: {'SET' if db_config.get('password') else 'NOT SET'}\n")
                sys.stderr.flush()
                
                self.conn_string = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
                
                # 接続テスト
                sys.stderr.write("   接続テストを実行中...\n")
                sys.stderr.flush()
                test_conn = self._test_postgres_connection()
                if test_conn:
                    self.use_postgres = True
                    sys.stderr.write("✅ PostgreSQL接続成功 - Supabaseを使用します\n")
                    sys.stderr.write(f"   ホスト: {db_config['host']}\n")
                else:
                    sys.stderr.write("⚠️ PostgreSQL接続テスト失敗 - SQLiteにフォールバック\n")
                    self.use_postgres = False
                sys.stderr.flush()
            except Exception as e:
                sys.stderr.write(f"⚠️ PostgreSQL設定エラー、SQLiteにフォールバック: {e}\n")
                import traceback
                traceback.print_exc()
                sys.stderr.flush()
                self.use_postgres = False
        else:
            sys.stderr.write("ℹ️ Supabase設定なし、または不完全 - SQLiteを使用します\n")
            sys.stderr.flush()
        
        # SQLiteの場合
        if not self.use_postgres:
            if db_path is None:
                base_dir = os.path.dirname(os.path.abspath(__file__))
                self.db_path = os.path.join(base_dir, "financial_data.db")
            else:
                self.db_path = db_path
            sys.stderr.write(f"📁 SQLiteデータベース: {self.db_path}\n")
            sys.stderr.flush()
        
        sys.stderr.write("=" * 80 + "\n")
        sys.stderr.flush()
        
        self._init_db()
        
        # 標準的な勘定科目リスト (要件定義書の3.1に準拠)
        self.all_items = [
            # 売上関連
            "売上高",
            "売上原価",
            # 売上総利益
            "売上総損益金額",
            # 販売管理費 (人件費)
            "役員報酬", "給料手当", "賞与", "法定福利費", "福利厚生費",
            # 採用・外注
            "採用教育費", "外注費",
            # 販売費
            "荷造運賃", "広告宣伝費", "販売手数料", "販売促進費",
            # 一般管理費
            "交際費", "会議費", "旅費交通費", "通信費", "消耗品費", "修繕費",
            "事務用品費", "水道光熱費", "新聞図書費", "諸会費", "支払手数料",
            "車両費", "地代家賃", "賃借料", "保険料", "租税公課", "支払報酬料",
            "研究開発費", "研修費", "減価償却費", "貸倒損失(販)", "雑費", "少額交際費",
            # 販売管理費計
            "販売管理費計",
            # 営業損益
            "営業損益金額",
            # 営業外損益
            "営業外収益合計", "営業外費用合計",
            # 経常損益
            "経常損益金額",
            # 特別損益
            "特別利益合計", "特別損失合計",
            # 税引前当期純損益
            "税引前当期純損益金額",
            # 法人税等
            "法人税、住民税及び事業税",
            # 当期純損益
            "当期純損益金額"
        ]
        
        # 販売管理費項目リスト
        self.ga_items = [
            "役員報酬", "給料手当", "賞与", "法定福利費", "福利厚生費",
            "採用教育費", "外注費", "荷造運賃", "広告宣伝費", "交際費",
            "会議費", "旅費交通費", "通信費", "販売手数料", "販売促進費",
            "消耗品費", "修繕費", "事務用品費", "水道光熱費", "新聞図書費",
            "諸会費", "支払手数料", "車両費", "地代家賃", "賃借料",
            "保険料", "租税公課", "支払報酬料", "研究開発費", "研修費",
            "減価償却費", "貸倒損失(販)", "雑費", "少額交際費"
        ]
        
        # 計算項目リスト (ユーザーが編集できない項目)
        self.calculated_items = [
            "売上総損益金額", "販売管理費計", "営業損益金額",
            "経常損益金額", "税引前当期純損益金額", "当期純損益金額"
        ]
        
        # 弥生会計の項目名マッピング
        self.item_mapping = {
            "売上高": ["売上高", "売上金額", "売上高合計"],
            "売上原価": ["売上原価", "仕入高", "売上原価合計"],
            "役員報酬": ["役員報酬"],
            "給料手当": ["給料手当", "給与手当", "給料"],
            "賞与": ["賞与"],
            "法定福利費": ["法定福利費"],
            "福利厚生費": ["福利厚生費"],
            "採用教育費": ["採用教育費", "採用費", "教育費"],
            "外注費": ["外注費"],
            "荷造運賃": ["荷造運賃"],
            "広告宣伝費": ["広告宣伝費"],
            "販売手数料": ["販売手数料"],
            "販売促進費": ["販売促進費"],
            "交際費": ["交際費", "接待交際費"],
            "会議費": ["会議費"],
            "旅費交通費": ["旅費交通費"],
            "通信費": ["通信費"],
            "消耗品費": ["消耗品費"],
            "修繕費": ["修繕費"],
            "事務用品費": ["事務用品費"],
            "水道光熱費": ["水道光熱費"],
            "新聞図書費": ["新聞図書費"],
            "諸会費": ["諸会費"],
            "支払手数料": ["支払手数料"],
            "車両費": ["車両費"],
            "地代家賃": ["地代家賃", "家賃"],
            "賃借料": ["賃借料"],
            "保険料": ["保険料"],
            "租税公課": ["租税公課"],
            "支払報酬料": ["支払報酬料"],
            "研究開発費": ["研究開発費"],
            "研修費": ["研修費"],
            "減価償却費": ["減価償却費"],
            "貸倒損失(販)": ["貸倒損失", "貸倒損失(販)"],
            "雑費": ["雑費"],
            "少額交際費": ["少額交際費"],
            "営業外収益合計": ["営業外収益", "営業外収益合計"],
            "営業外費用合計": ["営業外費用", "営業外費用合計"],
            "特別利益合計": ["特別利益", "特別利益合計"],
            "特別損失合計": ["特別損失", "特別損失合計"],
            "法人税、住民税及び事業税": ["法人税", "法人税等", "法人税、住民税及び事業税"]
        }
    
    def _test_postgres_connection(self):
        """PostgreSQL接続をテスト"""
        try:
            import psycopg2
            from urllib.parse import urlparse
            
            result = urlparse(self.conn_string)
            sys.stderr.write(f"   接続パラメータ:\n")
            sys.stderr.write(f"     - database: {result.path[1:]}\n")
            sys.stderr.write(f"     - user: {result.username}\n")
            sys.stderr.write(f"     - host: {result.hostname}\n")
            sys.stderr.write(f"     - port: {result.port}\n")
            sys.stderr.flush()
            
            conn = psycopg2.connect(
                database=result.path[1:],
                user=result.username,
                password=result.password,
                host=result.hostname,
                port=result.port,
                connect_timeout=10
            )
            conn.close()
            return True
        except Exception as e:
            sys.stderr.write(f"   ❌ 接続テスト失敗: {type(e).__name__}: {e}\n")
            import traceback
            sys.stderr.write("   スタックトレース:\n")
            traceback.print_exc(file=sys.stderr)
            sys.stderr.flush()
            return False
    
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
            return sqlite3.connect(self.db_path)
    
    def _execute_query(self, query, params=None):
        """クエリを実行（PostgreSQLとSQLiteの互換性対応）"""
        if self.use_postgres:
            # PostgreSQL用にプレースホルダーを変換 (? → %s)
            query = query.replace('?', '%s')
        
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            # SQLiteの場合は ? をそのまま使い、PostgreSQLの場合は %s に変換済み
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            conn.commit()
            return cursor
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()

    def _get_cursor(self, conn):
        """カーソルを取得（PostgreSQLとSQLiteの互換性対応）"""
        return conn.cursor()

    def _format_query(self, query):
        """クエリのプレースホルダーを変換"""
        if self.use_postgres:
            return query.replace('?', '%s')
        return query

    def _init_db(self):
        """データベーステーブルの初期化 (要件定義書の2.3に準拠)"""
        if self.use_postgres:
            self._init_postgres_db()
        else:
            self._init_sqlite_db()
    
    def _init_sqlite_db(self):
        """SQLiteデータベースの初期化"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        # 2.3.1 会社マスタ
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS companies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_name ON companies(name)')
        
        # 2.3.2 会計期マスタ
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS fiscal_periods (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            comp_id INTEGER NOT NULL,
            period_num INTEGER NOT NULL,
            start_date TEXT NOT NULL,
            end_date TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (comp_id) REFERENCES companies (id),
            UNIQUE(comp_id, period_num),
            CHECK (start_date < end_date)
        )
        ''')
        
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_comp_period ON fiscal_periods(comp_id, period_num)')
        
        # 2.3.3 実績データ
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS actual_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fiscal_period_id INTEGER NOT NULL,
            item_name TEXT NOT NULL,
            month TEXT NOT NULL,
            amount REAL DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (fiscal_period_id) REFERENCES fiscal_periods (id),
            UNIQUE(fiscal_period_id, item_name, month)
        )
        ''')
        
        # 2.3.4 予測データ
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS forecast_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fiscal_period_id INTEGER NOT NULL,
            scenario TEXT NOT NULL,
            item_name TEXT NOT NULL,
            month TEXT NOT NULL,
            amount REAL DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (fiscal_period_id) REFERENCES fiscal_periods (id),
            UNIQUE(fiscal_period_id, scenario, item_name, month)
        )
        ''')
        
        # 2.3.5 補助科目データ
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS sub_accounts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fiscal_period_id INTEGER NOT NULL,
            scenario TEXT NOT NULL,
            parent_item TEXT NOT NULL,
            sub_account_name TEXT NOT NULL,
            month TEXT NOT NULL,
            amount REAL DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (fiscal_period_id) REFERENCES fiscal_periods (id),
            UNIQUE(fiscal_period_id, scenario, parent_item, sub_account_name, month)
        )
        ''')
        
        conn.commit()
        conn.close()

    def _init_postgres_db(self):
        """PostgreSQLデータベースの初期化"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        # 会社マスタ
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS companies (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        # 会計期マスタ
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS fiscal_periods (
            id SERIAL PRIMARY KEY,
            comp_id INTEGER NOT NULL REFERENCES companies(id),
            period_num INTEGER NOT NULL,
            start_date TEXT NOT NULL,
            end_date TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(comp_id, period_num)
        )
        ''')
        
        # 実績データ
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS actual_data (
            id SERIAL PRIMARY KEY,
            fiscal_period_id INTEGER NOT NULL REFERENCES fiscal_periods(id),
            item_name TEXT NOT NULL,
            month TEXT NOT NULL,
            amount DOUBLE PRECISION DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(fiscal_period_id, item_name, month)
        )
        ''')
        
        # 予測データ
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS forecast_data (
            id SERIAL PRIMARY KEY,
            fiscal_period_id INTEGER NOT NULL REFERENCES fiscal_periods(id),
            scenario TEXT NOT NULL,
            item_name TEXT NOT NULL,
            month TEXT NOT NULL,
            amount DOUBLE PRECISION DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(fiscal_period_id, scenario, item_name, month)
        )
        ''')
        
        # 補助科目データ
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS sub_accounts (
            id SERIAL PRIMARY KEY,
            fiscal_period_id INTEGER NOT NULL REFERENCES fiscal_periods(id),
            scenario TEXT NOT NULL,
            parent_item TEXT NOT NULL,
            sub_account_name TEXT NOT NULL,
            month TEXT NOT NULL,
            amount DOUBLE PRECISION DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(fiscal_period_id, scenario, parent_item, sub_account_name, month)
        )
        ''')
        
        conn.commit()
        conn.close()

    # --- 会社・会計期管理 ---
    
    def get_companies(self):
        conn = self._get_connection()
        df = pd.read_sql_query("SELECT * FROM companies ORDER BY name", conn)
        conn.close()
        return df
    
    def add_company(self, name):
        try:
            self._execute_query("INSERT INTO companies (name) VALUES (?)", (name,))
            return True, "会社を登録しました"
        except Exception as e:
            return False, str(e)
            
    def get_company_periods(self, comp_id):
        conn = self._get_connection()
        query = self._format_query("SELECT * FROM fiscal_periods WHERE comp_id = ? ORDER BY period_num DESC")
        # PostgreSQLとSQLiteの両方で動作するようにカラム名を小文字に統一
        df = pd.read_sql_query(query, conn, params=(comp_id,))
        conn.close()
        return df
        
    def add_fiscal_period(self, comp_id, period_num, start_date, end_date):
        try:
            self._execute_query(
                "INSERT INTO fiscal_periods (comp_id, period_num, start_date, end_date) VALUES (?, ?, ?, ?)",
                (comp_id, period_num, start_date, end_date)
            )
            return True, "会計期を登録しました"
        except Exception as e:
            return False, str(e)

    # --- データ取得・保存 ---
    
    def get_fiscal_months(self, fiscal_period_id):
        """会計期間内の月リスト(YYYY-MM)を取得"""
        conn = self._get_connection()
        cursor = conn.cursor()
        query = self._format_query("SELECT start_date, end_date FROM fiscal_periods WHERE id = ?")
        cursor.execute(query, (fiscal_period_id,))
        result = cursor.fetchone()
        conn.close()
        
        if not result:
            return []
            
        start_date = datetime.strptime(result[0], '%Y-%m-%d')
        end_date = datetime.strptime(result[1], '%Y-%m-%d')
        
        months = []
        curr = start_date
        while curr <= end_date:
            months.append(curr.strftime('%Y-%m'))
            # 次の月へ
            if curr.month == 12:
                curr = curr.replace(year=curr.year + 1, month=1)
            else:
                curr = curr.replace(month=curr.month + 1)
        
        return months[:12] # 最大12ヶ月

    def load_actual_data(self, fiscal_period_id):
        """実績データをロードしてピボット形式で返す"""
        # IDを確実に整数に変換
        try:
            if isinstance(fiscal_period_id, bytes):
                import struct
                fiscal_period_id = struct.unpack('<Q', fiscal_period_id.ljust(8, b'\x00'))[0]
            fiscal_period_id = int(fiscal_period_id)
        except:
            pass

        conn = self._get_connection()
        query = self._format_query("SELECT item_name, month, amount FROM actual_data WHERE fiscal_period_id = ?")
        df = pd.read_sql_query(query, conn, params=(fiscal_period_id,))
        conn.close()
        
        months = self.get_fiscal_months(fiscal_period_id)
        
        # 全項目と全月を網羅したベースDataFrameを作成
        result = pd.DataFrame({'項目名': self.all_items})
        for m in months:
            result[m] = 0.0
            
        if not df.empty:
            # ピボット
            pivot_df = df.pivot(index='item_name', columns='month', values='amount').reset_index()
            pivot_df = pivot_df.rename(columns={'item_name': '項目名'})
            
            # ベースにマージ
            # 既存の月列を削除してからマージ
            result_base = result[['項目名']]
            result = pd.merge(result_base, pivot_df, on='項目名', how='left').fillna(0)
            
            # 欠落している月列を補完
            for m in months:
                if m not in result.columns:
                    result[m] = 0.0
        
        # 月列の順序を整える
        cols = ['項目名'] + months
        return result[cols]

    def load_forecast_data(self, fiscal_period_id, scenario):
        """予測データをロードしてピボット形式で返す"""
        # IDを確実に整数に変換
        try:
            if isinstance(fiscal_period_id, bytes):
                import struct
                fiscal_period_id = struct.unpack('<Q', fiscal_period_id.ljust(8, b'\x00'))[0]
            fiscal_period_id = int(fiscal_period_id)
        except:
            pass

        conn = self._get_connection()
        query = self._format_query("SELECT item_name, month, amount FROM forecast_data WHERE fiscal_period_id = ? AND scenario = ?")
        df = pd.read_sql_query(query, conn, params=(fiscal_period_id, scenario))
        conn.close()
        
        months = self.get_fiscal_months(fiscal_period_id)
        
        # 全項目と全月を網羅したベースDataFrameを作成
        result = pd.DataFrame({'項目名': self.all_items})
        for m in months:
            result[m] = 0.0
            
        if not df.empty:
            # ピボット
            pivot_df = df.pivot(index='item_name', columns='month', values='amount').reset_index()
            pivot_df = pivot_df.rename(columns={'item_name': '項目名'})
            
            # ベースにマージ
            result_base = result[['項目名']]
            result = pd.merge(result_base, pivot_df, on='項目名', how='left').fillna(0)
            
            # 欠落している月列を補完
            for m in months:
                if m not in result.columns:
                    result[m] = 0.0
        
        # 月列の順序を整える
        cols = ['項目名'] + months
        return result[cols]

    def save_actual_item(self, fiscal_period_id, item_name, month_values):
        """特定の項目の実績値を保存"""
        # IDを確実に整数に変換
        try:
            if isinstance(fiscal_period_id, bytes):
                import struct
                fiscal_period_id = struct.unpack('<Q', fiscal_period_id.ljust(8, b'\x00'))[0]
            fiscal_period_id = int(fiscal_period_id)
        except:
            pass

        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            for month, amount in month_values.items():
                if self.use_postgres:
                    cursor.execute('''
                        INSERT INTO actual_data (fiscal_period_id, item_name, month, amount)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (fiscal_period_id, item_name, month)
                        DO UPDATE SET amount = EXCLUDED.amount
                    ''', (fiscal_period_id, item_name, month, amount))
                else:
                    cursor.execute('''
                        INSERT OR REPLACE INTO actual_data (fiscal_period_id, item_name, month, amount)
                        VALUES (?, ?, ?, ?)
                    ''', (fiscal_period_id, item_name, month, amount))
            
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"Error saving actual item: {e}")
            return False

    def save_forecast_item(self, fiscal_period_id, scenario, item_name, month_values):
        """特定の項目の予測値を保存"""
        # IDを確実に整数に変換
        try:
            if isinstance(fiscal_period_id, bytes):
                import struct
                fiscal_period_id = struct.unpack('<Q', fiscal_period_id.ljust(8, b'\x00'))[0]
            fiscal_period_id = int(fiscal_period_id)
        except:
            pass

        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            for month, amount in month_values.items():
                if self.use_postgres:
                    cursor.execute('''
                        INSERT INTO forecast_data (fiscal_period_id, scenario, item_name, month, amount)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (fiscal_period_id, scenario, item_name, month)
                        DO UPDATE SET amount = EXCLUDED.amount
                    ''', (fiscal_period_id, scenario, item_name, month, amount))
                else:
                    cursor.execute('''
                        INSERT OR REPLACE INTO forecast_data (fiscal_period_id, scenario, item_name, month, amount)
                        VALUES (?, ?, ?, ?, ?)
                    ''', (fiscal_period_id, scenario, item_name, month, amount))
            
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"Error saving forecast item: {e}")
            return False

    # --- 補助科目管理 ---
    
    def get_sub_accounts_for_parent(self, fiscal_period_id, scenario, parent_item):
        conn = self._get_connection()
        query = self._format_query("SELECT * FROM sub_accounts WHERE fiscal_period_id = ? AND scenario = ? AND parent_item = ?")
        df = pd.read_sql_query(query, conn, params=(fiscal_period_id, scenario, parent_item))
        conn.close()
        return df
        
    def save_sub_account(self, fiscal_period_id, scenario, parent_item, sub_name, month_values):
        # IDを確実に整数に変換
        try:
            if isinstance(fiscal_period_id, bytes):
                import struct
                fiscal_period_id = struct.unpack('<Q', fiscal_period_id.ljust(8, b'\x00'))[0]
            fiscal_period_id = int(fiscal_period_id)
        except:
            pass

        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            for month, amount in month_values.items():
                if self.use_postgres:
                    cursor.execute('''
                        INSERT INTO sub_accounts (fiscal_period_id, scenario, parent_item, sub_account_name, month, amount)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (fiscal_period_id, scenario, parent_item, sub_account_name, month)
                        DO UPDATE SET amount = EXCLUDED.amount
                    ''', (fiscal_period_id, scenario, parent_item, sub_name, month, amount))
                else:
                    cursor.execute('''
                        INSERT OR REPLACE INTO sub_accounts (fiscal_period_id, scenario, parent_item, sub_account_name, month, amount)
                        VALUES (?, ?, ?, ?, ?, ?)
                    ''', (fiscal_period_id, scenario, parent_item, sub_name, month, amount))
            
            # 親項目の合計値を更新
            self._update_parent_from_subs(fiscal_period_id, scenario, parent_item)
            
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"Error saving sub account: {e}")
            return False
            
    def delete_sub_account(self, fiscal_period_id, scenario, parent_item, sub_name):
        # IDを確実に整数に変換
        try:
            if isinstance(fiscal_period_id, bytes):
                import struct
                fiscal_period_id = struct.unpack('<Q', fiscal_period_id.ljust(8, b'\x00'))[0]
            fiscal_period_id = int(fiscal_period_id)
        except:
            pass

        try:
            query = self._format_query("DELETE FROM sub_accounts WHERE fiscal_period_id = ? AND scenario = ? AND parent_item = ? AND sub_account_name = ?")
            self._execute_query(
                query,
                (fiscal_period_id, scenario, parent_item, sub_name)
            )
            # 親項目の合計値を更新
            self._update_parent_from_subs(fiscal_period_id, scenario, parent_item)
            return True
        except Exception as e:
            print(f"Error deleting sub account: {e}")
            return False
            
    def _update_parent_from_subs(self, fiscal_period_id, scenario, parent_item):
        """補助科目の合計を親項目の予測値に反映"""
        conn = self._get_connection()
        query = self._format_query("SELECT month, SUM(amount) as total FROM sub_accounts WHERE fiscal_period_id = ? AND scenario = ? AND parent_item = ? GROUP BY month")
        df = pd.read_sql_query(query, conn, params=(fiscal_period_id, scenario, parent_item))
        conn.close()
        
        if not df.empty:
            month_values = dict(zip(df['month'], df['total']))
            self.save_forecast_item(fiscal_period_id, scenario, parent_item, month_values)

    # --- PL計算 ---
    
    def calculate_pl(self, actuals_df, forecasts_df, split_index, months):
        """
        損益計算書を計算 (要件定義書の3.2に準拠)
        
        計算ロジック:
        - 売上総損益金額 = 売上高 - 売上原価
        - 販売管理費計 = 33項目の合計
        - 営業損益金額 = 売上総損益金額 - 販売管理費計
        - 経常損益金額 = 営業損益金額 + 営業外収益合計 - 営業外費用合計
        - 税引前当期純損益金額 = 経常損益金額 + 特別利益合計 - 特別損失合計
        - 当期純損益金額 = 税引前当期純損益金額 - 法人税、住民税及び事業税
        """
        df = pd.DataFrame({'項目名': self.all_items})
        
        # 実績と予測の結合
        for i, m in enumerate(months):
            if i < split_index:
                # 実績
                if m in actuals_df.columns:
                    df[m] = df['項目名'].map(actuals_df.set_index('項目名')[m])
                else:
                    df[m] = 0.0
            else:
                # 予測
                if m in forecasts_df.columns:
                    df[m] = df['項目名'].map(forecasts_df.set_index('項目名')[m])
                else:
                    df[m] = 0.0
        
        df = df.fillna(0)
        
        # 計算項目の算出
        def get_val(item_name):
            row = df[df['項目名'] == item_name]
            if not row.empty:
                return row[months].iloc[0]
            return pd.Series(0.0, index=months)

        # 売上総損益金額 = 売上高 - 売上原価
        sales = get_val("売上高")
        cogs = get_val("売上原価")
        gp = sales - cogs
        df.loc[df['項目名'] == "売上総損益金額", months] = gp.values
        
        # 販売管理費計 = 33項目の合計
        ga_total = pd.Series(0.0, index=months)
        for item in self.ga_items:
            ga_total += get_val(item)
        df.loc[df['項目名'] == "販売管理費計", months] = ga_total.values
        
        # 営業損益金額 = 売上総損益金額 - 販売管理費計
        op = gp - ga_total
        df.loc[df['項目名'] == "営業損益金額", months] = op.values
        
        # 経常損益金額 = 営業損益金額 + 営業外収益合計 - 営業外費用合計
        non_op_inc = get_val("営業外収益合計")
        non_op_exp = get_val("営業外費用合計")
        ord_p = op + non_op_inc - non_op_exp
        df.loc[df['項目名'] == "経常損益金額", months] = ord_p.values
        
        # 税引前当期純損益金額 = 経常損益金額 + 特別利益合計 - 特別損失合計
        sp_inc = get_val("特別利益合計")
        sp_exp = get_val("特別損失合計")
        pre_tax = ord_p + sp_inc - sp_exp
        df.loc[df['項目名'] == "税引前当期純損益金額", months] = pre_tax.values
        
        # 当期純損益金額 = 税引前当期純損益金額 - 法人税、住民税及び事業税
        tax = get_val("法人税、住民税及び事業税")
        net_p = pre_tax - tax
        df.loc[df['項目名'] == "当期純損益金額", months] = net_p.values
        
        # 合計列の追加
        df['実績合計'] = df[months[:split_index]].sum(axis=1)
        df['予測合計'] = df[months[split_index:]].sum(axis=1)
        df['合計'] = df['実績合計'] + df['予測合計']
        
        # タイプ（要約/詳細）の付与
        summary_items = ["売上高", "売上総損益金額", "販売管理費計", "営業損益金額", "経常損益金額", "当期純損益金額"]
        df['タイプ'] = df['項目名'].apply(lambda x: '要約' if x in summary_items else '詳細')
        
        return df

    # --- データインポート ---
    
    def import_yayoi_excel(self, file_path, fiscal_period_id, preview_only=False):
        """
        弥生会計Excelからデータをインポート
        preview_only=True の場合はプレビュー用のDataFrameを返す
        """
        try:
            # IDを確実に整数に変換
            try:
                if isinstance(fiscal_period_id, bytes):
                    # SQLiteで稀に発生するバイナリIDの対応
                    import struct
                    fiscal_period_id = struct.unpack('<Q', fiscal_period_id.ljust(8, b'\x00'))[0]
                fiscal_period_id = int(fiscal_period_id)
            except:
                pass

            # 会計期の情報を取得
            conn = self._get_connection()
            cursor = conn.cursor()
            query = self._format_query("SELECT start_date, end_date FROM fiscal_periods WHERE id = ?")
            cursor.execute(query, (fiscal_period_id,))
            result = cursor.fetchone()
            conn.close()
            
            if not result:
                return pd.DataFrame(), "会計期が見つかりません"
            
            start_date_str, end_date_str = result
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
            end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
            
            # 会計年度の開始月を取得
            fiscal_start_month = start_date.month
            fiscal_start_year = start_date.year
            
            xls = pd.ExcelFile(file_path)
            imported_data = {item: {} for item in self.all_items}
            
            for sheet_name in xls.sheet_names:
                df = pd.read_excel(xls, sheet_name=sheet_name, header=None)
                
                month_cols = {}
                
                # 月の列を特定
                for r in range(min(20, len(df))):
                    for c in range(len(df.columns)):
                        val = str(df.iloc[r, c])
                        # 月のパターンを検出 (例: "8月度", "9月度")
                        match = re.search(r'(\d{1,2})月', val)
                        if match:
                            month_num = int(match.group(1))
                            
                            # 会計年度に基づいて年を決定
                            # 開始月以降は当年、開始月より前は翌年
                            if month_num >= fiscal_start_month:
                                year = fiscal_start_year
                            else:
                                year = fiscal_start_year + 1
                            
                            month_str = f"{year}-{month_num:02d}"
                            month_cols[month_str] = c
                
                if not month_cols:
                    continue
                
                # 項目の行を特定して数値を抽出
                for r in range(len(df)):
                    item_val = ""
                    for c in range(min(3, len(df.columns))):
                        v = str(df.iloc[r, c]).strip()
                        if v and v != "nan":
                            item_val = v
                            break
                    
                    if not item_val:
                        continue
                    
                    target_item = None
                    for std_name, aliases in self.item_mapping.items():
                        if any(alias in item_val for alias in aliases):
                            target_item = std_name
                            break
                    
                    if not target_item and item_val in self.all_items:
                        target_item = item_val
                    
                    if target_item:
                        for m, col_idx in month_cols.items():
                            raw_val = df.iloc[r, col_idx]
                            try:
                                if isinstance(raw_val, str):
                                    clean_val = raw_val.replace(',', '').replace('¥', '').replace('円', '').strip()
                                    if clean_val.startswith('△') or clean_val.startswith('▲'):
                                        val = -float(clean_val[1:])
                                    elif clean_val.startswith('(') and clean_val.endswith(')'):
                                        val = -float(clean_val[1:-1])
                                    else:
                                        val = float(clean_val)
                                else:
                                    val = float(raw_val)
                                
                                if not np.isnan(val):
                                    imported_data[target_item][m] = val
                            except:
                                pass
            
            # DataFrameに変換
            imported_df = pd.DataFrame.from_dict(imported_data, orient='index').reset_index().rename(columns={'index': '項目名'})
            
            # 月列を取得してソート
            month_cols = [c for c in imported_df.columns if c != '項目名']
            if month_cols:
                # YYYY-MM形式の月をソート
                try:
                    month_cols_sorted = sorted(month_cols, key=lambda x: pd.to_datetime(x + '-01'))
                    imported_df = imported_df[['項目名'] + month_cols_sorted]
                except:
                    pass  # ソート失敗時はそのまま
            
            # 項目名でソート
            imported_df['項目名'] = pd.Categorical(imported_df['項目名'], categories=self.all_items, ordered=True)
            imported_df = imported_df.sort_values('項目名').reset_index(drop=True)
            
            return imported_df, "データ抽出に成功しました"
            
        except Exception as e:
            return pd.DataFrame(), str(e)

    def save_extracted_data(self, fiscal_period_id, imported_df):
        """抽出されたDataFrameをデータベースに保存"""
        # IDを確実に整数に変換
        try:
            if isinstance(fiscal_period_id, bytes):
                import struct
                fiscal_period_id = struct.unpack('<Q', fiscal_period_id.ljust(8, b'\x00'))[0]
            fiscal_period_id = int(fiscal_period_id)
        except:
            pass

        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # 既存のデータを削除
            query = self._format_query("DELETE FROM actual_data WHERE fiscal_period_id = ?")
            cursor.execute(query, (fiscal_period_id,))
            
            months = [c for c in imported_df.columns if c != '項目名']
            
            # バルクインサート用のデータを準備
            insert_data = []
            for _, row in imported_df.iterrows():
                for m in months:
                    val = row[m]
                    if val != 0 and not pd.isna(val):
                        insert_data.append((fiscal_period_id, row['項目名'], m, float(val)))
            
            # 一括挿入
            if insert_data:
                if self.use_postgres:
                    cursor.executemany(
                        "INSERT INTO actual_data (fiscal_period_id, item_name, month, amount) VALUES (%s, %s, %s, %s)",
                        insert_data
                    )
                else:
                    cursor.executemany(
                        "INSERT INTO actual_data (fiscal_period_id, item_name, month, amount) VALUES (?, ?, ?, ?)",
                        insert_data
                    )
            
            conn.commit()
            return True, "インポートが完了しました"
        except Exception as e:
            if conn:
                conn.rollback()
            return False, str(e)
        finally:
            if conn:
                conn.close()
