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
        
        # 補助科目が設定できる親項目（売上と原価を追加）
        self.parent_items_with_sub_accounts = [
            "売上高",
            "売上原価",
            "外注費",
            "広告宣伝費",
            "旅費交通費",
            "地代家賃"
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
            amount REAL NOT NULL DEFAULT 0,
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
            amount REAL NOT NULL DEFAULT 0,
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
            amount DOUBLE PRECISION NOT NULL DEFAULT 0,
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
            amount DOUBLE PRECISION NOT NULL DEFAULT 0,
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
            amount DOUBLE PRECISION NOT NULL DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(fiscal_period_id, scenario, parent_item, sub_account_name, month)
        )
        ''')
        
        conn.commit()
        conn.close()

    def _read_sql_query(self, query, params=None):
        """SQLクエリを実行してDataFrameを返す（PostgreSQLとSQLiteの互換性対応）"""
        if self.use_postgres:
            # PostgreSQL用にプレースホルダーを変換 (? → %s)
            query = query.replace('?', '%s')
        
        # IDの型変換（SQLiteでバイナリになる問題への対策）
        if params:
            new_params = []
            for p in params:
                if isinstance(p, bytes):
                    try:
                        new_params.append(int.from_bytes(p, 'little'))
                    except:
                        new_params.append(p)
                else:
                    new_params.append(p)
            params = tuple(new_params)

        conn = self._get_connection()
        try:
            # PostgreSQLの場合はSQLAlchemyエンジンを使用（警告回避）
            if self.use_postgres:
                from sqlalchemy import create_engine
                engine = create_engine(self.conn_string)
                df = pd.read_sql_query(query, engine, params=params)
                engine.dispose()
            else:
                df = pd.read_sql_query(query, conn, params=params)
            
            # SQLiteでIDがバイナリ形式で返ってくる場合の対策
            for col in df.columns:
                if col.endswith('_id') or col == 'id':
                    df[col] = df[col].apply(lambda x: int.from_bytes(x, 'little') if isinstance(x, bytes) else x)
            
            return df
        finally:
            if not self.use_postgres:
                conn.close()

    def _sort_months(self, df, fiscal_period_id):
        """月を会計期間の順序でソート"""
        try:
            period = self.get_period_info(fiscal_period_id)
            if not period:
                return df
            
            start_date = datetime.strptime(period['start_date'], '%Y-%m-%d')
            end_date = datetime.strptime(period['end_date'], '%Y-%m-%d')
            
            # 会計期間の月リストを作成
            months_order = []
            curr = start_date
            while curr <= end_date:
                months_order.append(curr.strftime('%Y-%m'))
                if curr.month == 12:
                    curr = curr.replace(year=curr.year + 1, month=1)
                else:
                    curr = curr.replace(month=curr.month + 1)
            
            # ソート用のマッピング
            order_map = {m: i for i, m in enumerate(months_order)}
            
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
            sys.stderr.write(f"💾 add_company() 開始: '{company_name}'\n")
            sys.stderr.write(f"   use_postgres: {self.use_postgres}\n")
            sys.stderr.flush()
            
            conn = self._get_connection()
            cursor = conn.cursor()
            
            if self.use_postgres:
                sys.stderr.write("   PostgreSQLモードでINSERT実行\n")
                cursor.execute("INSERT INTO companies (name) VALUES (%s)", (company_name,))
            else:
                sys.stderr.write("   SQLiteモードでINSERT実行\n")
                cursor.execute("INSERT INTO companies (name) VALUES (?)", (company_name,))
            
            conn.commit()
            sys.stderr.write("   コミット成功\n")
            sys.stderr.flush()
            conn.close()
            
            sys.stderr.write("✅ add_company() 成功\n")
            sys.stderr.flush()
            return True
        except Exception as e:
            sys.stderr.write(f"❌ add_company() 失敗: {e}\n")
            import traceback
            traceback.print_exc(file=sys.stderr)
            sys.stderr.flush()
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
            # IDの型変換
            if isinstance(comp_id, bytes):
                comp_id = int.from_bytes(comp_id, 'little')

            conn = self._get_connection()
            cursor = conn.cursor()
            
            if self.use_postgres:
                cursor.execute(
                    "INSERT INTO fiscal_periods (comp_id, period_num, start_date, end_date) VALUES (%s, %s, %s, %s)",
                    (comp_id, period_num, start_date, end_date)
                )
            else:
                cursor.execute(
                    "INSERT INTO fiscal_periods (comp_id, period_num, start_date, end_date) VALUES (?, ?, ?, ?)",
                    (comp_id, period_num, start_date, end_date)
                )
            
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            sys.stderr.write(f"❌ add_fiscal_period() 失敗: {e}\n")
            sys.stderr.flush()
            return False

    def get_period_info(self, period_id):
        """会計期情報を取得"""
        # IDの型変換
        if isinstance(period_id, bytes):
            period_id = int.from_bytes(period_id, 'little')

        conn = self._get_connection()
        cursor = conn.cursor()
        
        if self.use_postgres:
            cursor.execute("SELECT * FROM fiscal_periods WHERE id = %s", (period_id,))
        else:
            cursor.execute("SELECT * FROM fiscal_periods WHERE id = ?", (period_id,))
        
        row = cursor.fetchone()
        conn.close()
        if row:
            # SQLiteでIDがバイナリ形式で返ってくる場合の対策
            row_id = row[0]
            if isinstance(row_id, bytes):
                row_id = int.from_bytes(row_id, 'little')
            row_comp_id = row[1]
            if isinstance(row_comp_id, bytes):
                row_comp_id = int.from_bytes(row_comp_id, 'little')

            return {
                "id": row_id,
                "comp_id": row_comp_id,
                "period_num": row[2],
                "start_date": row[3],
                "end_date": row[4]
            }
        return None

    def get_company_id_from_period_id(self, fiscal_period_id):
        """会計期IDから会社IDを取得"""
        # IDの型変換
        if isinstance(fiscal_period_id, bytes):
            fiscal_period_id = int.from_bytes(fiscal_period_id, 'little')

        conn = self._get_connection()
        cursor = conn.cursor()
        
        if self.use_postgres:
            cursor.execute("SELECT comp_id FROM fiscal_periods WHERE id = %s", (fiscal_period_id,))
        else:
            cursor.execute("SELECT comp_id FROM fiscal_periods WHERE id = ?", (fiscal_period_id,))
        
        result = cursor.fetchone()
        conn.close()
        if result:
            res = result[0]
            return int.from_bytes(res, 'little') if isinstance(res, bytes) else res
        return None

    def get_fiscal_months(self, comp_id_or_period_id, fiscal_period_id=None):
        """会計期の月リストを取得 (引数が1つの場合はperiod_idとして扱う)"""
        # 引数が1つの場合、または2つ目がNoneの場合、最初の引数をperiod_idとして扱う
        target_period_id = fiscal_period_id if fiscal_period_id is not None else comp_id_or_period_id
        
        # IDの型変換
        if isinstance(target_period_id, bytes):
            target_period_id = int.from_bytes(target_period_id, 'little')
            
        period = self.get_period_info(target_period_id)
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
        months = self.get_fiscal_months(fiscal_period_id)
        try:
            return months.index(current_month) + 1
        except:
            return 0

    def load_actual_data(self, fiscal_period_id):
        """実績データを読み込み"""
        # IDの型変換
        if isinstance(fiscal_period_id, bytes):
            fiscal_period_id = int.from_bytes(fiscal_period_id, 'little')

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
        # IDの型変換
        if isinstance(fiscal_period_id, bytes):
            fiscal_period_id = int.from_bytes(fiscal_period_id, 'little')

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
        # IDの型変換
        if isinstance(fiscal_period_id, bytes):
            fiscal_period_id = int.from_bytes(fiscal_period_id, 'little')

        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # バッチ処理用のデータを準備
            batch_data = [
                (fiscal_period_id, item_name, month, float(amount))
                for month, amount in values_dict.items()
            ]
            
            if self.use_postgres:
                # PostgreSQL用のUPSERT（バッチ）
                from psycopg2.extras import execute_values
                execute_values(
                    cursor,
                    """
                    INSERT INTO actual_data (fiscal_period_id, item_name, month, amount) 
                    VALUES %s
                    ON CONFLICT (fiscal_period_id, item_name, month) 
                    DO UPDATE SET amount = EXCLUDED.amount
                    """,
                    batch_data
                )
            else:
                # SQLite用のUPSERT（バッチ）
                cursor.executemany(
                    "INSERT OR REPLACE INTO actual_data (fiscal_period_id, item_name, month, amount) VALUES (?, ?, ?, ?)",
                    batch_data
                )
            
            conn.commit()
            return True, "実績データを保存しました"
        except Exception as e:
            sys.stderr.write(f"Error saving actual data: {e}\n")
            sys.stderr.flush()
            if conn:
                conn.rollback()
            return False, str(e)
        finally:
            if conn:
                conn.close()

    def save_forecast_item(self, fiscal_period_id, scenario, item_name, values_dict):
        """予測データを保存"""
        # IDの型変換
        if isinstance(fiscal_period_id, bytes):
            fiscal_period_id = int.from_bytes(fiscal_period_id, 'little')

        conn = None
        try:
            sys.stderr.write(f"💾 予測データ保存開始: {item_name}, シナリオ: {scenario}\n")
            sys.stderr.write(f"   use_postgres: {self.use_postgres}\n")
            sys.stderr.write(f"   データ件数: {len(values_dict)}\n")
            sys.stderr.flush()
            
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # バッチ処理用のデータを準備
            batch_data = [
                (fiscal_period_id, scenario, item_name, month, float(amount))
                for month, amount in values_dict.items()
            ]
            
            if self.use_postgres:
                # PostgreSQL用のUPSERT（バッチ）
                from psycopg2.extras import execute_values
                execute_values(
                    cursor,
                    """
                    INSERT INTO forecast_data (fiscal_period_id, scenario, item_name, month, amount) 
                    VALUES %s
                    ON CONFLICT (fiscal_period_id, scenario, item_name, month) 
                    DO UPDATE SET amount = EXCLUDED.amount
                    """,
                    batch_data
                )
            else:
                # SQLite用のUPSERT（バッチ）
                cursor.executemany(
                    "INSERT OR REPLACE INTO forecast_data (fiscal_period_id, scenario, item_name, month, amount) VALUES (?, ?, ?, ?, ?)",
                    batch_data
                )
            
            conn.commit()
            sys.stderr.write(f"✅ 保存成功: {len(batch_data)}件のデータを保存しました\n")
            sys.stderr.flush()
            return True, f"{len(batch_data)}件の予測データを保存しました"
        except Exception as e:
            sys.stderr.write(f"❌ Error saving forecast data: {e}\n")
            import traceback
            traceback.print_exc()
            sys.stderr.flush()
            if conn:
                conn.rollback()
            return False, str(e)
        finally:
            if conn:
                conn.close()

    def load_sub_accounts(self, fiscal_period_id, scenario):
        """補助科目データを読み込み"""
        # IDの型変換
        if isinstance(fiscal_period_id, bytes):
            fiscal_period_id = int.from_bytes(fiscal_period_id, 'little')

        if self.use_postgres:
            query = "SELECT * FROM sub_accounts WHERE fiscal_period_id = %s AND scenario = %s"
        else:
            query = "SELECT * FROM sub_accounts WHERE fiscal_period_id = ? AND scenario = ?"
        
        return self._read_sql_query(query, params=(fiscal_period_id, scenario))

    def get_sub_accounts_for_parent(self, fiscal_period_id, scenario, parent_item):
        """親項目に紐づく補助科目を取得"""
        # IDの型変換
        if isinstance(fiscal_period_id, bytes):
            fiscal_period_id = int.from_bytes(fiscal_period_id, 'little')

        if self.use_postgres:
            query = "SELECT * FROM sub_accounts WHERE fiscal_period_id = %s AND scenario = %s AND parent_item = %s"
        else:
            query = "SELECT * FROM sub_accounts WHERE fiscal_period_id = ? AND scenario = ? AND parent_item = ?"
        
        return self._read_sql_query(query, params=(fiscal_period_id, scenario, parent_item))

    def save_sub_account(self, fiscal_period_id, scenario, parent_item, sub_account_name, values_dict):
        """補助科目データを保存"""
        # IDの型変換
        if isinstance(fiscal_period_id, bytes):
            fiscal_period_id = int.from_bytes(fiscal_period_id, 'little')

        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # バッチ処理用のデータを準備
            batch_data = [
                (fiscal_period_id, scenario, parent_item, sub_account_name, month, float(amount))
                for month, amount in values_dict.items()
            ]
            
            if self.use_postgres:
                # PostgreSQL用のUPSERT（バッチ）
                from psycopg2.extras import execute_values
                execute_values(
                    cursor,
                    """
                    INSERT INTO sub_accounts (fiscal_period_id, scenario, parent_item, sub_account_name, month, amount) 
                    VALUES %s
                    ON CONFLICT (fiscal_period_id, scenario, parent_item, sub_account_name, month) 
                    DO UPDATE SET amount = EXCLUDED.amount
                    """,
                    batch_data
                )
            else:
                # SQLite用のUPSERT（バッチ）
                cursor.executemany(
                    "INSERT OR REPLACE INTO sub_accounts (fiscal_period_id, scenario, parent_item, sub_account_name, month, amount) VALUES (?, ?, ?, ?, ?, ?)",
                    batch_data
                )
            
            conn.commit()
            return True, "補助科目を保存しました"
        except Exception as e:
            if conn:
                conn.rollback()
            return False, str(e)
        finally:
            if conn:
                conn.close()

    def delete_sub_account(self, fiscal_period_id, scenario, parent_item, sub_account_name):
        """補助科目を削除"""
        # IDの型変換
        if isinstance(fiscal_period_id, bytes):
            fiscal_period_id = int.from_bytes(fiscal_period_id, 'little')

        try:
            if self.use_postgres:
                self._execute_query(
                    "DELETE FROM sub_accounts WHERE fiscal_period_id = %s AND scenario = %s AND parent_item = %s AND sub_account_name = %s",
                    (fiscal_period_id, scenario, parent_item, sub_account_name)
                )
            else:
                self._execute_query(
                    "DELETE FROM sub_accounts WHERE fiscal_period_id = ? AND scenario = ? AND parent_item = ? AND sub_account_name = ?",
                    (fiscal_period_id, scenario, parent_item, sub_account_name)
                )
            return True, "補助科目を削除しました"
        except Exception as e:
            return False, str(e)

    def calculate_pl(self, actuals_df, forecasts_df, split_idx, months):
        """損益計算書(PL)を計算"""
        pl_df = pd.DataFrame(columns=['項目名'] + months + ['合計'])
        
        # 実績と予測を結合
        combined_data = []
        for item in self.all_items:
            row_data = {'項目名': item}
            actual_row = actuals_df[actuals_df['項目名'] == item]
            forecast_row = forecasts_df[forecasts_df['項目名'] == item]
            
            total = 0
            for i, month in enumerate(months):
                if i < split_idx:
                    val = actual_row[month].iloc[0] if not actual_row.empty and month in actual_row.columns else 0
                else:
                    val = forecast_row[month].iloc[0] if not forecast_row.empty and month in forecast_row.columns else 0
                row_data[month] = val
                total += val
            
            row_data['合計'] = total
            combined_data.append(row_data)
        
        pl_df = pd.DataFrame(combined_data)
        
        # 計算項目の算出
        def get_row_values(item_name):
            row = pl_df[pl_df['項目名'] == item_name]
            if row.empty:
                return np.zeros(len(months) + 1)
            return row[months + ['合計']].values[0]

        # 1. 売上総利益 = 売上高 - 売上原価
        sales = get_row_values("売上高")
        cogs = get_row_values("売上原価")
        gp = sales - cogs
        pl_df.loc[pl_df['項目名'] == "売上総損益金額", months + ['合計']] = gp
        
        # 2. 販売管理費計
        ga_total = np.zeros(len(months) + 1)
        for item in self.ga_items:
            ga_total += get_row_values(item)
        pl_df.loc[pl_df['項目名'] == "販売管理費計", months + ['合計']] = ga_total
        
        # 3. 営業利益 = 売上総利益 - 販売管理費計
        op = gp - ga_total
        pl_df.loc[pl_df['項目名'] == "営業損益金額", months + ['合計']] = op
        
        # 4. 経常利益 = 営業利益 + 営業外収益 - 営業外費用
        non_op_inc = get_row_values("営業外収益合計")
        non_op_exp = get_row_values("営業外費用合計")
        ord_profit = op + non_op_inc - non_op_exp
        pl_df.loc[pl_df['項目名'] == "経常損益金額", months + ['合計']] = ord_profit
        
        # 5. 税引前当期純利益 = 経常利益 + 特別利益 - 特別損失
        sp_inc = get_row_values("特別利益合計")
        sp_exp = get_row_values("特別損失合計")
        pre_tax_profit = ord_profit + sp_inc - sp_exp
        pl_df.loc[pl_df['項目名'] == "税引前当期純損益金額", months + ['合計']] = pre_tax_profit
        
        # 6. 当期純利益 = 税引前当期純利益 - 法人税等
        taxes = get_row_values("法人税、住民税及び事業税")
        net_profit = pre_tax_profit - taxes
        pl_df.loc[pl_df['項目名'] == "当期純損益金額", months + ['合計']] = net_profit
        
        # 表示用のタイプ分け
        pl_df['タイプ'] = '詳細'
        pl_df.loc[pl_df['項目名'].isin(self.calculated_items), 'タイプ'] = '要約'
        pl_df.loc[pl_df['項目名'] == "売上高", 'タイプ'] = '要約'
        pl_df.loc[pl_df['項目名'] == "売上原価", 'タイプ'] = '要約'
        
        return pl_df

    def register_company(self, name):
        """会社を登録（重複チェック付き）"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # 重複チェック
            if self.use_postgres:
                cursor.execute("SELECT id FROM companies WHERE name = %s", (name,))
            else:
                cursor.execute("SELECT id FROM companies WHERE name = ?", (name,))
            
            if cursor.fetchone():
                conn.close()
                return False, f"会社名 '{name}' は既に登録されています"
            
            if self.use_postgres:
                cursor.execute("INSERT INTO companies (name) VALUES (%s)", (name,))
            else:
                cursor.execute("INSERT INTO companies (name) VALUES (?)", (name,))
            
            conn.commit()
            conn.close()
            return True, f"会社 '{name}' を登録しました"
        except Exception as e:
            return False, str(e)

    def register_fiscal_period(self, comp_id, period_num, start_date, end_date):
        """会計期を登録（重複チェック付き）"""
        try:
            # IDの型変換
            if isinstance(comp_id, bytes):
                comp_id = int.from_bytes(comp_id, 'little')

            conn = self._get_connection()
            cursor = conn.cursor()
            
            # 重複チェック
            if self.use_postgres:
                cursor.execute("SELECT id FROM fiscal_periods WHERE comp_id = %s AND period_num = %s", (comp_id, period_num))
            else:
                cursor.execute("SELECT id FROM fiscal_periods WHERE comp_id = ? AND period_num = ?", (comp_id, period_num))
            
            if cursor.fetchone():
                conn.close()
                return False, f"第{period_num}期は既に登録されています"
            
            if self.use_postgres:
                cursor.execute(
                    "INSERT INTO fiscal_periods (comp_id, period_num, start_date, end_date) VALUES (%s, %s, %s, %s)",
                    (comp_id, period_num, start_date, end_date)
                )
            else:
                cursor.execute(
                    "INSERT INTO fiscal_periods (comp_id, period_num, start_date, end_date) VALUES (?, ?, ?, ?)",
                    (comp_id, period_num, start_date, end_date)
                )
            
            conn.commit()
            conn.close()
            return True, f"第{period_num}期を登録しました"
        except Exception as e:
            return False, str(e)

    def import_yayoi_excel(self, file_path, fiscal_period_id, preview_only=True):
        """弥生会計のExcelからデータを抽出"""
        try:
            # IDの型変換
            if isinstance(fiscal_period_id, bytes):
                fiscal_period_id = int.from_bytes(fiscal_period_id, 'little')

            # 会計期間の情報を取得
            conn = self._get_connection()
            cursor = conn.cursor()
            if self.use_postgres:
                cursor.execute("SELECT start_date, end_date FROM fiscal_periods WHERE id = %s", (fiscal_period_id,))
            else:
                cursor.execute("SELECT start_date, end_date FROM fiscal_periods WHERE id = ?", (fiscal_period_id,))
            result = cursor.fetchone()
            conn.close()
            
            if not result:
                return pd.DataFrame(), "会計期間情報が見つかりません"
            
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
                            
                            # 会計期間内の月のみを対象とする
                            month_dt = datetime.strptime(month_str + "-01", '%Y-%m-%d')
                            if start_date <= month_dt <= end_date:
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
        # IDの型変換
        if isinstance(fiscal_period_id, bytes):
            fiscal_period_id = int.from_bytes(fiscal_period_id, 'little')

        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # 既存のデータを削除
            if self.use_postgres:
                cursor.execute("DELETE FROM actual_data WHERE fiscal_period_id = %s", (fiscal_period_id,))
            else:
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

    def create_forecast_template(self, fiscal_period_id, scenario="現実"):
        """予測データ入力用のExcelテンプレートを作成"""
        # 会計期間情報を取得
        period_info = self.get_period_info(fiscal_period_id)
        if not period_info:
            return None
        
        # 月リストを取得
        comp_id = period_info['comp_id']
        months = self.get_fiscal_months(comp_id, fiscal_period_id)
        
        # テンプレートDataFrameを作成
        template_df = pd.DataFrame({
            '項目名': self.all_items
        })
        
        # 各月の列を追加（初期値0）
        for month in months:
            template_df[month] = 0
        
        return template_df
    
    def save_forecast_from_excel(self, fiscal_period_id, scenario, imported_df):
        """ExcelからインポートされたDataFrameを予測データとして保存"""
        conn = None
        try:
            sys.stderr.write(f"💾 予測データ一括保存開始: シナリオ={scenario}\n")
            sys.stderr.write(f"   項目数: {len(imported_df)}\n")
            sys.stderr.flush()
            
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # 月のカラムを取得
            months = [col for col in imported_df.columns if col != '項目名']
            sys.stderr.write(f"   月数: {len(months)}\n")
            sys.stderr.flush()
            
            # バッチ処理用のデータを準備
            batch_data = []
            for _, row in imported_df.iterrows():
                item_name = row['項目名']
                for month in months:
                    amount = row[month]
                    if pd.notna(amount) and amount != 0:
                        batch_data.append((
                            fiscal_period_id,
                            scenario,
                            item_name,
                            month,
                            float(amount)
                        ))
            
            sys.stderr.write(f"   保存データ件数: {len(batch_data)}\n")
            sys.stderr.flush()
            
            if batch_data:
                if self.use_postgres:
                    # PostgreSQL用のUPSERT（バッチ）
                    from psycopg2.extras import execute_values
                    execute_values(
                        cursor,
                        """
                        INSERT INTO forecast_data (fiscal_period_id, scenario, item_name, month, amount) 
                        VALUES %s
                        ON CONFLICT (fiscal_period_id, scenario, item_name, month) 
                        DO UPDATE SET amount = EXCLUDED.amount
                        """,
                        batch_data
                    )
                else:
                    # SQLite用のUPSERT（バッチ）
                    cursor.executemany(
                        "INSERT OR REPLACE INTO forecast_data (fiscal_period_id, scenario, item_name, month, amount) VALUES (?, ?, ?, ?, ?)",
                        batch_data
                    )
            
            conn.commit()
            sys.stderr.write(f"✅ 予測データ一括保存成功: {len(batch_data)}件\n")
            sys.stderr.flush()
            return True, f"{len(batch_data)}件の予測データをインポートしました"
        
        except Exception as e:
            sys.stderr.write(f"❌ 予測データインポートエラー: {e}\n")
            import traceback
            traceback.print_exc(file=sys.stderr)
            sys.stderr.flush()
            if conn:
                conn.rollback()
            return False, str(e)
        
        finally:
            if conn:
                conn.close()

    def delete_sub_account_all_periods(self, comp_id, scenario, parent_item, sub_account_name):
        """特定の補助科目を全期から削除"""
        conn = None
        try:
            sys.stderr.write(f"🗑️ 全期削除開始: {parent_item} -> {sub_account_name}\n")
            sys.stderr.flush()
            
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # この会社のすべての期を取得
            periods = self.get_company_periods(comp_id)
            
            deleted_count = 0
            for _, period in periods.iterrows():
                period_id = period['id']
                
                if self.use_postgres:
                    cursor.execute(
                        """
                        DELETE FROM sub_accounts 
                        WHERE fiscal_period_id = %s 
                        AND scenario = %s 
                        AND parent_item = %s 
                        AND sub_account_name = %s
                        """,
                        (period_id, scenario, parent_item, sub_account_name)
                    )
                else:
                    cursor.execute(
                        """
                        DELETE FROM sub_accounts 
                        WHERE fiscal_period_id = ? 
                        AND scenario = ? 
                        AND parent_item = ? 
                        AND sub_account_name = ?
                        """,
                        (period_id, scenario, parent_item, sub_account_name)
                    )
                
                deleted_count += cursor.rowcount
            
            conn.commit()
            sys.stderr.write(f"✅ 全期削除成功: {deleted_count}件削除\n")
            sys.stderr.flush()
            return True, f"{len(periods)}期から削除しました（{deleted_count}件）"
        
        except Exception as e:
            sys.stderr.write(f"❌ 全期削除エラー: {e}\n")
            sys.stderr.flush()
            if conn:
                conn.rollback()
            return False, str(e)
        
        finally:
            if conn:
                conn.close()
    
    def copy_sub_account_to_all_periods(self, comp_id, source_period_id, scenario, parent_item, sub_account_name):
        """補助科目を他の全期にコピー"""
        conn = None
        try:
            sys.stderr.write(f"📋 全期コピー開始: {parent_item} -> {sub_account_name}\n")
            sys.stderr.flush()
            
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # ソース期のデータを取得
            if self.use_postgres:
                cursor.execute(
                    """
                    SELECT month, amount 
                    FROM sub_accounts 
                    WHERE fiscal_period_id = %s 
                    AND scenario = %s 
                    AND parent_item = %s 
                    AND sub_account_name = %s
                    """,
                    (source_period_id, scenario, parent_item, sub_account_name)
                )
            else:
                cursor.execute(
                    """
                    SELECT month, amount 
                    FROM sub_accounts 
                    WHERE fiscal_period_id = ? 
                    AND scenario = ? 
                    AND parent_item = ? 
                    AND sub_account_name = ?
                    """,
                    (source_period_id, scenario, parent_item, sub_account_name)
                )
            
            source_data = cursor.fetchall()
            
            if not source_data:
                return False, "コピー元のデータが見つかりません"
            
            # この会社のすべての期を取得
            periods = self.get_company_periods(comp_id)
            
            copied_count = 0
            for _, period in periods.iterrows():
                period_id = period['id']
                
                if period_id == source_period_id:
                    continue  # ソース期はスキップ
                
                # 各月のデータを挿入
                for month, amount in source_data:
                    if self.use_postgres:
                        cursor.execute(
                            """
                            INSERT INTO sub_accounts (fiscal_period_id, scenario, parent_item, sub_account_name, month, amount) 
                            VALUES (%s, %s, %s, %s, %s, %s)
                            ON CONFLICT (fiscal_period_id, scenario, parent_item, sub_account_name, month) 
                            DO UPDATE SET amount = EXCLUDED.amount
                            """,
                            (period_id, scenario, parent_item, sub_account_name, month, amount)
                        )
                    else:
                        cursor.execute(
                            "INSERT OR REPLACE INTO sub_accounts (fiscal_period_id, scenario, parent_item, sub_account_name, month, amount) VALUES (?, ?, ?, ?, ?, ?)",
                            (period_id, scenario, parent_item, sub_account_name, month, amount)
                        )
                    copied_count += 1
            
            conn.commit()
            sys.stderr.write(f"✅ 全期コピー成功: {copied_count}件追加\n")
            sys.stderr.flush()
            return True, f"{len(periods)-1}期にコピーしました（{copied_count}件）"
        
        except Exception as e:
            sys.stderr.write(f"❌ 全期コピーエラー: {e}\n")
            sys.stderr.flush()
            if conn:
                conn.rollback()
            return False, str(e)
        
        finally:
            if conn:
                conn.close()

    def calculate_bs_data(self, fiscal_period_id):
        """貸借対照表データを計算（簡易版）"""
        try:
            # 実際のBSデータが必要だが、簡易的にPLから推計
            # 本来はBS専用のデータ入力が必要
            
            # ダミーデータを返す（実装例）
            bs_data = {
                '流動資産': {
                    '現金及び預金': 50000000,
                    '売掛金': 30000000,
                    '棚卸資産': 20000000,
                },
                '固定資産': {
                    '有形固定資産': 80000000,
                    '無形固定資産': 10000000,
                },
                '流動負債': {
                    '買掛金': 25000000,
                    '短期借入金': 30000000,
                },
                '固定負債': {
                    '長期借入金': 50000000,
                },
                '純資産': {
                    '資本金': 30000000,
                    '利益剰余金': 55000000,
                }
            }
            
            return bs_data
        except Exception as e:
            sys.stderr.write(f"❌ BS計算エラー: {e}\n")
            return {}
    
    def calculate_cf_data(self, fiscal_period_id):
        """キャッシュフロー計算書データを計算"""
        try:
            # PLデータから簡易的にCFを計算
            actuals = self.load_actual_data(fiscal_period_id)
            forecasts = self.load_forecast_data(fiscal_period_id, "現実")
            
            # 営業CFの計算（簡易版）
            operating_cf = {}
            
            # 売上高から売掛金増減を考慮（簡易的に90%を現金化と仮定）
            if not forecasts.empty:
                sales_row = forecasts[forecasts['項目名'] == '売上高']
                if not sales_row.empty:
                    months = [col for col in sales_row.columns if col not in ['項目名']]
                    for month in months:
                        sales = sales_row[month].iloc[0] if month in sales_row.columns else 0
                        operating_cf[month] = sales * 0.9  # 簡易的に90%を現金化
            
            # 投資CF（簡易版）
            investing_cf = {}
            for month in operating_cf.keys():
                investing_cf[month] = -5000000  # 月次固定の設備投資
            
            # 財務CF（簡易版）
            financing_cf = {}
            for month in operating_cf.keys():
                financing_cf[month] = -2000000  # 月次借入金返済
            
            return {
                '営業CF': operating_cf,
                '投資CF': investing_cf,
                '財務CF': financing_cf
            }
        except Exception as e:
            sys.stderr.write(f"❌ CF計算エラー: {e}\n")
            return {}
    
    def calculate_financial_ratios(self, fiscal_period_id):
        """経営指標を計算"""
        try:
            # PLデータを取得
            actuals = self.load_actual_data(fiscal_period_id)
            
            if actuals.empty:
                return {}
            
            months = [col for col in actuals.columns if col not in ['項目名']]
            
            # 売上高
            sales = 0
            sales_row = actuals[actuals['項目名'] == '売上高']
            if not sales_row.empty:
                for month in months:
                    if month in sales_row.columns:
                        val = sales_row[month].iloc[0]
                        if pd.notna(val):
                            sales += float(val)
            
            # 売上原価
            cogs = 0
            cogs_row = actuals[actuals['項目名'] == '売上原価']
            if not cogs_row.empty:
                for month in months:
                    if month in cogs_row.columns:
                        val = cogs_row[month].iloc[0]
                        if pd.notna(val):
                            cogs += float(val)
            
            # 営業利益
            operating_profit = 0
            op_row = actuals[actuals['項目名'] == '営業損益金額']
            if not op_row.empty:
                for month in months:
                    if month in op_row.columns:
                        val = op_row[month].iloc[0]
                        if pd.notna(val):
                            operating_profit += float(val)
            
            # 当期純利益
            net_profit = 0
            net_row = actuals[actuals['項目名'] == '当期純損益金額']
            if not net_row.empty:
                for month in months:
                    if month in net_row.columns:
                        val = net_row[month].iloc[0]
                        if pd.notna(val):
                            net_profit += float(val)
            
            # BSデータ（簡易版）
            total_assets = 190000000  # ダミー
            total_equity = 85000000   # ダミー
            current_assets = 100000000  # ダミー
            current_liabilities = 55000000  # ダミー
            
            # 経営指標を計算
            ratios = {
                '売上高': sales,
                '売上総利益': sales - cogs,
                '営業利益': operating_profit,
                '当期純利益': net_profit,
                '売上総利益率': ((sales - cogs) / sales * 100) if sales > 0 else 0,
                '営業利益率': (operating_profit / sales * 100) if sales > 0 else 0,
                '当期純利益率': (net_profit / sales * 100) if sales > 0 else 0,
                'ROA': (net_profit / total_assets * 100) if total_assets > 0 else 0,
                'ROE': (net_profit / total_equity * 100) if total_equity > 0 else 0,
                '流動比率': (current_assets / current_liabilities * 100) if current_liabilities > 0 else 0,
                '自己資本比率': (total_equity / total_assets * 100) if total_assets > 0 else 0,
            }
            
            return ratios
        except Exception as e:
            sys.stderr.write(f"❌ 経営指標計算エラー: {e}\n")
            return {}
    
    def calculate_breakeven_analysis(self, fiscal_period_id):
        """損益分岐点分析"""
        try:
            forecasts = self.load_forecast_data(fiscal_period_id, "現実")
            
            if forecasts.empty:
                return {}
            
            months = [col for col in forecasts.columns if col not in ['項目名']]
            
            # 売上高
            sales = 0
            sales_row = forecasts[forecasts['項目名'] == '売上高']
            if not sales_row.empty:
                for month in months:
                    if month in sales_row.columns:
                        val = sales_row[month].iloc[0]
                        if pd.notna(val):
                            sales += float(val)
            
            # 変動費（売上原価と仮定）
            variable_costs = 0
            vc_row = forecasts[forecasts['項目名'] == '売上原価']
            if not vc_row.empty:
                for month in months:
                    if month in vc_row.columns:
                        val = vc_row[month].iloc[0]
                        if pd.notna(val):
                            variable_costs += float(val)
            
            # 固定費（販管費の合計と仮定）
            fixed_costs = 0
            for item in self.ga_items:
                item_row = forecasts[forecasts['項目名'] == item]
                if not item_row.empty:
                    for month in months:
                        if month in item_row.columns:
                            val = item_row[month].iloc[0]
                            if pd.notna(val):
                                fixed_costs += float(val)
            
            # 限界利益率
            contribution_margin_ratio = ((sales - variable_costs) / sales) if sales > 0 else 0
            
            # 損益分岐点売上高
            breakeven_sales = (fixed_costs / contribution_margin_ratio) if contribution_margin_ratio > 0 else 0
            
            # 安全余裕率
            safety_margin_ratio = ((sales - breakeven_sales) / sales * 100) if sales > 0 else 0
            
            return {
                '売上高': sales,
                '変動費': variable_costs,
                '固定費': fixed_costs,
                '限界利益': sales - variable_costs,
                '限界利益率': contribution_margin_ratio * 100,
                '損益分岐点売上高': breakeven_sales,
                '安全余裕率': safety_margin_ratio,
                '損益分岐点比率': (breakeven_sales / sales * 100) if sales > 0 else 0
            }
        except Exception as e:
            sys.stderr.write(f"❌ 損益分岐点分析エラー: {e}\n")
            return {}

    def calculate_balance_sheet(self, fiscal_period_id):
        """貸借対照表を計算"""
        # 簡易BS（将来的に拡張可能）
        bs_items = {
            "資産の部": {
                "流動資産": ["現金及び預金", "売掛金", "棚卸資産", "その他流動資産"],
                "固定資産": ["有形固定資産", "無形固定資産", "投資その他の資産"]
            },
            "負債の部": {
                "流動負債": ["買掛金", "短期借入金", "未払金", "その他流動負債"],
                "固定負債": ["長期借入金", "その他固定負債"]
            },
            "純資産の部": {
                "株主資本": ["資本金", "利益剰余金"]
            }
        }
        return bs_items
    
    def calculate_cash_flow(self, fiscal_period_id):
        """キャッシュフロー計算書を計算（間接法）"""
        # 実績データを取得
        actuals = self.load_actual_data(fiscal_period_id)
        
        # 営業CF、投資CF、財務CFの計算
        cf_data = {
            "営業活動によるキャッシュフロー": {},
            "投資活動によるキャッシュフロー": {},
            "財務活動によるキャッシュフロー": {}
        }
        
        # 営業CFの計算（簡易版：税引前利益をベース）
        net_income_row = actuals[actuals['項目名'] == '税引前当期純損益金額']
        if not net_income_row.empty:
            months = [col for col in actuals.columns if col not in ['項目名']]
            for month in months:
                if month in net_income_row.columns:
                    cf_data["営業活動によるキャッシュフロー"][month] = net_income_row[month].iloc[0]
        
        return cf_data
    
    def calculate_financial_indicators(self, fiscal_period_id):
        """経営指標を計算"""
        actuals = self.load_actual_data(fiscal_period_id)
        months = [col for col in actuals.columns if col not in ['項目名']]
        
        # 必要な項目を取得
        sales = actuals[actuals['項目名'] == '売上高']
        cogs = actuals[actuals['項目名'] == '売上原価']
        operating_profit = actuals[actuals['項目名'] == '営業損益金額']
        ordinary_profit = actuals[actuals['項目名'] == '経常損益金額']
        net_profit = actuals[actuals['項目名'] == '当期純損益金額']
        
        indicators = {}
        
        for month in months:
            month_indicators = {}
            
            # 売上高
            sales_val = float(sales[month].iloc[0]) if not sales.empty and month in sales.columns else 0
            
            # 売上原価
            cogs_val = float(cogs[month].iloc[0]) if not cogs.empty and month in cogs.columns else 0
            
            # 営業利益
            op_val = float(operating_profit[month].iloc[0]) if not operating_profit.empty and month in operating_profit.columns else 0
            
            # 経常利益
            ord_val = float(ordinary_profit[month].iloc[0]) if not ordinary_profit.empty and month in ordinary_profit.columns else 0
            
            # 当期純利益
            net_val = float(net_profit[month].iloc[0]) if not net_profit.empty and month in net_profit.columns else 0
            
            # 粗利率
            month_indicators['粗利率'] = ((sales_val - cogs_val) / sales_val * 100) if sales_val != 0 else 0
            
            # 営業利益率
            month_indicators['営業利益率'] = (op_val / sales_val * 100) if sales_val != 0 else 0
            
            # 経常利益率
            month_indicators['経常利益率'] = (ord_val / sales_val * 100) if sales_val != 0 else 0
            
            # 当期純利益率
            month_indicators['当期純利益率'] = (net_val / sales_val * 100) if sales_val != 0 else 0
            
            indicators[month] = month_indicators
        
        return indicators
