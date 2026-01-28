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
        
        if hasattr(st, 'secrets'):
            sys.stderr.write(f"   'database' in st.secrets: {'database' in st.secrets}\n")
            if 'database' in st.secrets:
                sys.stderr.write(f"   st.secrets['database'] keys: {list(st.secrets['database'].keys())}\n")
            sys.stderr.flush()
        
        if hasattr(st, 'secrets') and 'database' in st.secrets:
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
            sys.stderr.write("ℹ️ Supabase設定なし - SQLiteを使用します\n")
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
            conn = psycopg2.connect(
                database=result.path[1:],
                user=result.username,
                password=result.password,
                host=result.hostname,
                port=result.port
            )
            conn.close()
            return True
        except Exception as e:
            print(f"   接続テスト失敗: {e}")
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
            amount REAL NOT NULL DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (fiscal_period_id) REFERENCES fiscal_periods (id),
            UNIQUE(fiscal_period_id, item_name, month)
        )
        ''')
        
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_period_item ON actual_data(fiscal_period_id, item_name)')
        
        # 2.3.4 予測データ
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS forecast_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fiscal_period_id INTEGER NOT NULL,
            scenario TEXT NOT NULL,
            item_name TEXT NOT NULL,
            month TEXT NOT NULL,
            amount REAL NOT NULL DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (fiscal_period_id) REFERENCES fiscal_periods (id),
            UNIQUE(fiscal_period_id, scenario, item_name, month),
            CHECK (scenario IN ('現実', '楽観', '悲観'))
        )
        ''')
        
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_period_scenario ON forecast_data(fiscal_period_id, scenario)')
        
        # 2.3.5 補助科目
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS sub_accounts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fiscal_period_id INTEGER NOT NULL,
            scenario TEXT NOT NULL,
            parent_item TEXT NOT NULL,
            sub_account_name TEXT NOT NULL,
            month TEXT NOT NULL,
            amount REAL NOT NULL DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (fiscal_period_id) REFERENCES fiscal_periods(id),
            UNIQUE(fiscal_period_id, scenario, parent_item, sub_account_name, month)
        )
        ''')
        
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_period_parent ON sub_accounts(fiscal_period_id, parent_item)')
        
        # 2.3.6 勘定科目属性
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS item_attributes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fiscal_period_id INTEGER NOT NULL,
            item_name TEXT NOT NULL,
            is_variable INTEGER DEFAULT 0,
            variable_rate REAL DEFAULT 0.0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (fiscal_period_id) REFERENCES fiscal_periods(id),
            UNIQUE(fiscal_period_id, item_name),
            CHECK (is_variable IN (0, 1)),
            CHECK (variable_rate >= 0 AND variable_rate <= 1)
        )
        ''')
        
        conn.commit()
        conn.close()
    
    def _init_postgres_db(self):
        """PostgreSQLデータベースの初期化"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        # 2.3.1 会社マスタ
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS companies (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_name ON companies(name)')
        
        # 2.3.2 会計期マスタ
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS fiscal_periods (
            id SERIAL PRIMARY KEY,
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
            id SERIAL PRIMARY KEY,
            fiscal_period_id INTEGER NOT NULL,
            item_name TEXT NOT NULL,
            month TEXT NOT NULL,
            amount REAL NOT NULL DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (fiscal_period_id) REFERENCES fiscal_periods (id),
            UNIQUE(fiscal_period_id, item_name, month)
        )
        ''')
        
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_period_item ON actual_data(fiscal_period_id, item_name)')
        
        # 2.3.4 予測データ
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS forecast_data (
            id SERIAL PRIMARY KEY,
            fiscal_period_id INTEGER NOT NULL,
            scenario TEXT NOT NULL,
            item_name TEXT NOT NULL,
            month TEXT NOT NULL,
            amount REAL NOT NULL DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (fiscal_period_id) REFERENCES fiscal_periods (id),
            UNIQUE(fiscal_period_id, scenario, item_name, month),
            CHECK (scenario IN ('現実', '楽観', '悲観'))
        )
        ''')
        
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_period_scenario ON forecast_data(fiscal_period_id, scenario)')
        
        # 2.3.5 補助科目
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS sub_accounts (
            id SERIAL PRIMARY KEY,
            fiscal_period_id INTEGER NOT NULL,
            scenario TEXT NOT NULL,
            parent_item TEXT NOT NULL,
            sub_account_name TEXT NOT NULL,
            month TEXT NOT NULL,
            amount REAL NOT NULL DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (fiscal_period_id) REFERENCES fiscal_periods(id),
            UNIQUE(fiscal_period_id, scenario, parent_item, sub_account_name, month)
        )
        ''')
        
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_period_parent ON sub_accounts(fiscal_period_id, parent_item)')
        
        # 2.3.6 勘定科目属性
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS item_attributes (
            id SERIAL PRIMARY KEY,
            fiscal_period_id INTEGER NOT NULL,
            item_name TEXT NOT NULL,
            is_variable INTEGER DEFAULT 0,
            variable_rate REAL DEFAULT 0.0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (fiscal_period_id) REFERENCES fiscal_periods(id),
            UNIQUE(fiscal_period_id, item_name),
            CHECK (is_variable IN (0, 1)),
            CHECK (variable_rate >= 0 AND variable_rate <= 1)
        )
        ''')
        
        conn.commit()
        conn.close()
    
    def _read_sql_query(self, query, params=None):
        """SQLクエリを実行してDataFrameを返す（PostgreSQLとSQLiteの互換性対応）"""
        if self.use_postgres:
            # PostgreSQL用にプレースホルダーを変換 (? → %s)
            query = query.replace('?', '%s')
        
        conn = self._get_connection()
        try:
            if params:
                df = pd.read_sql_query(query, conn, params=params)
            else:
                df = pd.read_sql_query(query, conn)
            return df
        finally:
            conn.close()
    
    def _sort_months(self, df, fiscal_period_id):
        """会計期の開始月を考慮して月をソート"""
        try:
            # 会計期情報を取得
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(
                "SELECT start_date, end_date FROM fiscal_periods WHERE id = ?",
                (fiscal_period_id,)
            )
            result = cursor.fetchone()
            conn.close()
            
            if not result:
                return df
            
            start_date_str, end_date_str = result
            
            # YYYY-MM形式の月をdatetimeに変換してソート
            if 'month' in df.columns:
                df['_month_dt'] = pd.to_datetime(df['month'] + '-01')
                df = df.sort_values('_month_dt').drop(columns=['_month_dt'])
            
            return df
        except Exception as e:
            print(f"Error sorting months: {e}")
            return df

    def get_companies(self):
        """会社一覧を取得"""
        return self._read_sql_query("SELECT * FROM companies ORDER BY name")

    def add_company(self, company_name):
        """会社を追加"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("INSERT INTO companies (name) VALUES (?)", (company_name,))
            conn.commit()
            conn.close()
            return True
        except:
            return False

    def get_company_periods(self, comp_id):
        """指定会社の会計期一覧を取得"""
        return self._read_sql_query(
            "SELECT * FROM fiscal_periods WHERE comp_id = ? ORDER BY period_num DESC",
            params=(comp_id,)
        )

    def add_fiscal_period(self, comp_id, period_num, start_date, end_date):
        """会計期を追加"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO fiscal_periods (comp_id, period_num, start_date, end_date) VALUES (?, ?, ?, ?)",
                (comp_id, period_num, start_date, end_date)
            )
            conn.commit()
            conn.close()
            return True
        except:
            return False

    def get_period_info(self, period_id):
        """会計期情報を取得"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM fiscal_periods WHERE id = ?", (period_id,))
        row = cursor.fetchone()
        conn.close()
        if row:
            return {
                "id": row[0],
                "comp_id": row[1],
                "period_num": row[2],
                "start_date": row[3],
                "end_date": row[4]
            }
        return None

    def get_company_id_from_period_id(self, fiscal_period_id):
        """会計期IDから会社IDを取得"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT comp_id FROM fiscal_periods WHERE id = ?", (fiscal_period_id,))
        result = cursor.fetchone()
        conn.close()
        return result[0] if result else None

    def get_fiscal_months(self, comp_id, fiscal_period_id):
        """会計期の月リストを取得"""
        period = self.get_period_info(fiscal_period_id)
        if not period:
            return []
        
        start = datetime.strptime(period['start_date'], '%Y-%m-%d')
        end = datetime.strptime(period['end_date'], '%Y-%m-%d')
        
        months = []
        curr = start
        while curr <= end:
            months.append(curr.strftime('%Y-%m'))
            if curr.month == 12:
                curr = curr.replace(year=curr.year + 1, month=1)
            else:
                curr = curr.replace(month=curr.month + 1)
        return months

    def get_split_index(self, comp_id, current_month, fiscal_period_id):
        """実績と予測の境界インデックスを取得"""
        months = self.get_fiscal_months(comp_id, fiscal_period_id)
        try:
            return months.index(current_month) + 1
        except:
            return 0

    def load_actual_data(self, fiscal_period_id):
        """実績データを読み込み"""
        df = self._read_sql_query(
            "SELECT item_name as 項目名, month, amount FROM actual_data WHERE fiscal_period_id = ?",
            params=(fiscal_period_id,)
        )
        
        if df.empty:
            return pd.DataFrame({'項目名': self.all_items}).fillna(0)
        
        df = df.drop_duplicates(subset=['項目名', 'month'], keep='last')
        
        # 月を正しくソート（会計期順）
        df = self._sort_months(df, fiscal_period_id)
        
        pivot_df = df.pivot(index='項目名', columns='month', values='amount').reset_index()
        
        all_items_df = pd.DataFrame({'項目名': self.all_items})
        pivot_df = pd.merge(all_items_df, pivot_df, on='項目名', how='left').fillna(0)
        return pivot_df

    def load_forecast_data(self, fiscal_period_id, scenario):
        """予測データを読み込み"""
        df = self._read_sql_query(
            "SELECT item_name as 項目名, month, amount FROM forecast_data WHERE fiscal_period_id = ? AND scenario = ?",
            params=(fiscal_period_id, scenario)
        )
        
        if df.empty:
            return pd.DataFrame({'項目名': self.all_items}).fillna(0)
        
        df = df.drop_duplicates(subset=['項目名', 'month'], keep='last')
        
        # 月を正しくソート（会計期順）
        df = self._sort_months(df, fiscal_period_id)
        
        pivot_df = df.pivot(index='項目名', columns='month', values='amount').reset_index()
        
        all_items_df = pd.DataFrame({'項目名': self.all_items})
        pivot_df = pd.merge(all_items_df, pivot_df, on='項目名', how='left').fillna(0)
        return pivot_df

    def save_actual_item(self, fiscal_period_id, item_name, values_dict):
        """実績データを保存"""
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            for month, amount in values_dict.items():
                if self.use_postgres:
                    # PostgreSQL用のUPSERT
                    cursor.execute(
                        """
                        INSERT INTO actual_data (fiscal_period_id, item_name, month, amount) 
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (fiscal_period_id, item_name, month) 
                        DO UPDATE SET amount = EXCLUDED.amount
                        """,
                        (fiscal_period_id, item_name, month, float(amount))
                    )
                else:
                    # SQLite用のUPSERT
                    cursor.execute(
                        "INSERT OR REPLACE INTO actual_data (fiscal_period_id, item_name, month, amount) VALUES (?, ?, ?, ?)",
                        (fiscal_period_id, item_name, month, float(amount))
                    )
            
            conn.commit()
            return True
        except Exception as e:
            sys.stderr.write(f"Error saving actual data: {e}\n")
            sys.stderr.flush()
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                conn.close()

    def save_forecast_item(self, fiscal_period_id, scenario, item_name, values_dict):
        """予測データを保存"""
        conn = None
        try:
            sys.stderr.write(f"💾 予測データ保存開始: {item_name}, シナリオ: {scenario}\n")
            sys.stderr.write(f"   use_postgres: {self.use_postgres}\n")
            sys.stderr.write(f"   データ件数: {len(values_dict)}\n")
            sys.stderr.flush()
            
            conn = self._get_connection()
            cursor = conn.cursor()
            
            saved_count = 0
            for month, amount in values_dict.items():
                if self.use_postgres:
                    # PostgreSQL用のUPSERT
                    cursor.execute(
                        """
                        INSERT INTO forecast_data (fiscal_period_id, scenario, item_name, month, amount) 
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (fiscal_period_id, scenario, item_name, month) 
                        DO UPDATE SET amount = EXCLUDED.amount
                        """,
                        (fiscal_period_id, scenario, item_name, month, float(amount))
                    )
                else:
                    # SQLite用のUPSERT（updated_atなし）
                    cursor.execute(
                        "INSERT OR REPLACE INTO forecast_data (fiscal_period_id, scenario, item_name, month, amount) VALUES (?, ?, ?, ?, ?)",
                        (fiscal_period_id, scenario, item_name, month, float(amount))
                    )
                saved_count += 1
            
            conn.commit()
            sys.stderr.write(f"✅ 保存成功: {saved_count}件のデータを保存しました\n")
            sys.stderr.flush()
            return True
        except Exception as e:
            sys.stderr.write(f"❌ Error saving forecast data: {e}\n")
            import traceback
            traceback.print_exc()
            sys.stderr.flush()
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                conn.close()

    def load_sub_accounts(self, fiscal_period_id, scenario):
        """補助科目データを読み込み"""
        return self._read_sql_query(
            "SELECT * FROM sub_accounts WHERE fiscal_period_id = ? AND scenario = ?",
            params=(fiscal_period_id, scenario)
        )

    def get_sub_accounts_for_parent(self, fiscal_period_id, scenario, parent_item):
        """特定親項目の補助科目を取得"""
        return self._read_sql_query(
            "SELECT * FROM sub_accounts WHERE fiscal_period_id = ? AND scenario = ? AND parent_item = ?",
            params=(fiscal_period_id, scenario, parent_item)
        )

    def save_sub_account(self, fiscal_period_id, scenario, parent_item, sub_account_name, values_dict):
        """補助科目を保存"""
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            for month, amount in values_dict.items():
                if self.use_postgres:
                    # PostgreSQL用のUPSERT
                    cursor.execute(
                        """
                        INSERT INTO sub_accounts (fiscal_period_id, scenario, parent_item, sub_account_name, month, amount) 
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (fiscal_period_id, scenario, parent_item, sub_account_name, month) 
                        DO UPDATE SET amount = EXCLUDED.amount
                        """,
                        (fiscal_period_id, scenario, parent_item, sub_account_name, month, float(amount))
                    )
                else:
                    # SQLite用のUPSERT
                    cursor.execute(
                        "INSERT OR REPLACE INTO sub_accounts (fiscal_period_id, scenario, parent_item, sub_account_name, month, amount) VALUES (?, ?, ?, ?, ?, ?)",
                        (fiscal_period_id, scenario, parent_item, sub_account_name, month, float(amount))
                    )
            
            conn.commit()
            return True
        except Exception as e:
            sys.stderr.write(f"Error saving sub account: {e}\n")
            sys.stderr.flush()
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                conn.close()

    def delete_sub_account(self, fiscal_period_id, scenario, parent_item, sub_account_name):
        """補助科目を削除"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(
                "DELETE FROM sub_accounts WHERE fiscal_period_id = ? AND scenario = ? AND parent_item = ? AND sub_account_name = ?",
                (fiscal_period_id, scenario, parent_item, sub_account_name)
            )
            conn.commit()
            conn.close()
            return True
        except:
            return False

    def calculate_growth_forecast(self, actuals_df, item_name, split_index, months):
        """成長率ベースの予測計算 (要件定義書の5.5.2に準拠)"""
        forecast_values = {}
        
        actual_months = months[:split_index]
        forecast_months = months[split_index:]
        
        if len(actual_months) < 2:
            # 実績が2ヶ月未満の場合は前月踏襲
            if len(actual_months) == 1:
                last_value = actuals_df[actuals_df['項目名'] == item_name][actual_months[0]].iloc[0]
            else:
                last_value = 0
            
            for m in forecast_months:
                forecast_values[m] = last_value
            
            return forecast_values
        
        # 前月比成長率の平均を計算
        item_row = actuals_df[actuals_df['項目名'] == item_name]
        actual_values = [item_row[m].iloc[0] for m in actual_months]
        
        growth_rates = []
        for i in range(1, len(actual_values)):
            if actual_values[i-1] != 0:
                rate = (actual_values[i] - actual_values[i-1]) / abs(actual_values[i-1])
                growth_rates.append(rate)
        
        if len(growth_rates) == 0:
            avg_growth_rate = 0
        else:
            # 異常値を除外 (±100%以上の変動は除外)
            filtered_rates = [r for r in growth_rates if abs(r) < 1.0]
            if len(filtered_rates) > 0:
                avg_growth_rate = np.mean(filtered_rates)
            else:
                avg_growth_rate = 0
        
        # 予測値の生成
        last_actual_value = actual_values[-1]
        current_forecast_value = last_actual_value
        
        for m in forecast_months:
            if last_actual_value != 0:
                current_forecast_value *= (1 + avg_growth_rate)
            else:
                current_forecast_value += avg_growth_rate
                
            forecast_values[m] = current_forecast_value
            
        return forecast_values

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

    def import_yayoi_excel(self, file_path, fiscal_period_id, preview_only=False):
        """
        弥生会計Excelからデータをインポート
        preview_only=True の場合はプレビュー用のDataFrameを返す
        """
        try:
            # 会計期の情報を取得
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(
                "SELECT start_date, end_date FROM fiscal_periods WHERE id = ?",
                (fiscal_period_id,)
            )
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
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # 既存のデータを削除
            cursor.execute("DELETE FROM actual_data WHERE fiscal_period_id = ?", (fiscal_period_id,))
            
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
