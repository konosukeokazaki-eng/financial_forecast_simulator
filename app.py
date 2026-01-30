import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import sqlite3
import os
import tempfile
from data_processor import DataProcessor
from datetime import datetime

# ページ設定
st.set_page_config(
    page_title="財務予測シミュレーター",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# カスタムCSS - ビジネスライクなデザイン
st.markdown("""
<style>
    /* メインコンテナ */
    .main {
        padding: 0rem 1rem;
        background-color: #f5f7fa;
    }
    
    /* タイトル */
    h1 {
        color: #1a1a2e;
        font-weight: 700;
        margin-bottom: 0.5rem;
        font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
        border-left: 4px solid #2e7d32;
        padding-left: 1rem;
    }
    
    h2 {
        color: #2c3e50;
        font-weight: 600;
        border-bottom: 2px solid #2e7d32;
        padding-bottom: 0.5rem;
        margin-top: 2rem;
        font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
    }
    
    h3 {
        color: #34495e;
        font-weight: 600;
        font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
    }
    
    /* サマリーカード - 洗練されたビジネススタイル */
    .summary-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1.5rem;
        border-radius: 8px;
        color: white;
        margin-bottom: 1rem;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        border-left: 4px solid #5a67d8;
    }
    
    .summary-card-blue {
        background: linear-gradient(135deg, #1e3c72 0%, #2a5298 100%);
        padding: 1.5rem;
        border-radius: 8px;
        color: white;
        box-shadow: 0 4px 12px rgba(30, 60, 114, 0.3);
        border-left: 4px solid #4a90e2;
    }
    
    .summary-card-green {
        background: linear-gradient(135deg, #2e7d32 0%, #43a047 100%);
        padding: 1.5rem;
        border-radius: 8px;
        color: white;
        box-shadow: 0 4px 12px rgba(46, 125, 50, 0.3);
        border-left: 4px solid #66bb6a;
    }
    
    .summary-card-orange {
        background: linear-gradient(135deg, #e65100 0%, #f57c00 100%);
        padding: 1.5rem;
        border-radius: 8px;
        color: white;
        box-shadow: 0 4px 12px rgba(230, 81, 0, 0.3);
        border-left: 4px solid #ff9800;
    }
    
    .summary-card-purple {
        background: linear-gradient(135deg, #6a1b9a 0%, #8e24aa 100%);
        padding: 1.5rem;
        border-radius: 8px;
        color: white;
        box-shadow: 0 4px 12px rgba(106, 27, 154, 0.3);
        border-left: 4px solid #ab47bc;
    }
    
    .summary-card-teal {
        background: linear-gradient(135deg, #00695c 0%, #00897b 100%);
        padding: 1.5rem;
        border-radius: 8px;
        color: white;
        box-shadow: 0 4px 12px rgba(0, 105, 92, 0.3);
        border-left: 4px solid #26a69a;
    }
    
    .card-title {
        font-size: 0.9rem;
        font-weight: 500;
        margin-bottom: 0.5rem;
        opacity: 0.95;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    
    .card-value {
        font-size: 2rem;
        font-weight: 700;
        margin-bottom: 0.3rem;
        font-family: 'Courier New', monospace;
    }
    
    .card-subtitle {
        font-size: 0.85rem;
        opacity: 0.9;
        font-weight: 400;
    }
    
    /* インフォボックス - プロフェッショナル */
    .info-box {
        background-color: #e3f2fd;
        border-left: 4px solid #1976d2;
        padding: 1rem 1.5rem;
        border-radius: 4px;
        margin-bottom: 1.5rem;
        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.05);
    }
    
    .warning-box {
        background-color: #fff3e0;
        border-left: 4px solid #f57c00;
        padding: 1rem 1.5rem;
        border-radius: 4px;
        margin-bottom: 1.5rem;
        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.05);
    }
    
    .success-box {
        background-color: #e8f5e9;
        border-left: 4px solid #43a047;
        padding: 1rem 1.5rem;
        border-radius: 4px;
        margin-bottom: 1.5rem;
        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.05);
    }
    
    /* KPI カード */
    .kpi-card {
        background: white;
        padding: 1.5rem;
        border-radius: 8px;
        border: 1px solid #e0e0e0;
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.08);
        margin-bottom: 1rem;
    }
    
    .kpi-label {
        font-size: 0.85rem;
        color: #757575;
        font-weight: 500;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        margin-bottom: 0.5rem;
    }
    
    .kpi-value {
        font-size: 2.2rem;
        color: #1a1a2e;
        font-weight: 700;
        font-family: 'Courier New', monospace;
    }
    
    .kpi-change {
        font-size: 0.9rem;
        margin-top: 0.3rem;
    }
    
    .kpi-positive {
        color: #2e7d32;
        font-weight: 600;
    }
    
    .kpi-negative {
        color: #c62828;
        font-weight: 600;
    }
    
    /* テーブルスタイル */
    .dataframe {
        border: 1px solid #e0e0e0 !important;
        border-radius: 4px;
        overflow: hidden;
    }
    
    /* ボタンスタイル */
    .stButton > button {
        border-radius: 4px;
        font-weight: 600;
        letter-spacing: 0.5px;
        text-transform: uppercase;
        font-size: 0.85rem;
        transition: all 0.3s ease;
    }
    
    .stButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 8px rgba(0, 0, 0, 0.15);
    }
    
    /* サイドバー */
    [data-testid="stSidebar"] {
        background-color: #1a1a2e;
    }
    
    [data-testid="stSidebar"] .stMarkdown {
        color: #ffffff;
    }
    
    /* タブスタイル */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        background-color: white;
        padding: 0.5rem;
        border-radius: 8px;
        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.05);
    }
    
    .stTabs [data-baseweb="tab"] {
        font-weight: 600;
        color: #424242;
        border-radius: 4px;
    }
    
    .stTabs [aria-selected="true"] {
        background-color: #2e7d32;
        color: white;
    }
</style>
""", unsafe_allow_html=True)

# 初期化
if 'processor' not in st.session_state:
    st.session_state.processor = DataProcessor()
processor = st.session_state.processor

# キャッシュ付きデータ読み込み関数（高速化）
@st.cache_data(ttl=60)  # 60秒間キャッシュ
def load_actual_data_cached(period_id, _processor):
    """実績データをキャッシュ付きで読み込み"""
    return _processor.load_actual_data(period_id)

@st.cache_data(ttl=60)
def load_forecast_data_cached(period_id, scenario, _processor):
    """予測データをキャッシュ付きで読み込み"""
    return _processor.load_forecast_data(period_id, scenario)

@st.cache_data(ttl=60)
def load_sub_accounts_cached(period_id, scenario, _processor):
    """補助科目データをキャッシュ付きで読み込み"""
    return _processor.load_sub_accounts(period_id, scenario)

@st.cache_data(ttl=300)  # 5分間キャッシュ（変更頻度が低い）
def get_companies_cached(_processor):
    """会社一覧をキャッシュ付きで取得"""
    return _processor.get_companies()

@st.cache_data(ttl=300)
def get_company_periods_cached(comp_id, _processor):
    """会計期間一覧をキャッシュ付きで取得"""
    return _processor.get_company_periods(comp_id)

@st.cache_data(ttl=300)
def get_fiscal_months_cached(comp_id, period_id, _processor):
    """会計月一覧をキャッシュ付きで取得"""
    return _processor.get_fiscal_months(comp_id, period_id)

# ヘルパー関数: 安全なint変換
def safe_int(value):
    """NaN/None対応の安全なint変換"""
    try:
        if pd.isna(value) or value is None:
            return 0
        return int(float(value))
    except (ValueError, TypeError):
        return 0

# サイドバー
st.sidebar.markdown("""
<div style='text-align: center; padding: 1rem 0;'>
    <h1 style='color: #1f77b4; margin: 0; font-size: 1.8rem;'>📊</h1>
    <h2 style='color: #2c3e50; margin: 0.5rem 0 0 0; font-size: 1.3rem;'>財務予測<br>シミュレーター</h2>
</div>
""", unsafe_allow_html=True)

st.sidebar.markdown("---")

# ユーザー情報とログアウト
st.sidebar.markdown(f"**👤 {st.session_state.username}**")
if st.sidebar.button("ログアウト", type="secondary"):
    st.session_state.authenticated = False
    st.session_state.username = ""
    st.rerun()

st.sidebar.markdown("---")

# データベース接続状態の表示
if processor.use_postgres:
    st.sidebar.success("🌐 Supabase接続中")
else:
    st.sidebar.warning("💾 SQLite使用中")
    st.sidebar.caption("⚠️ データは一時的です")

st.sidebar.markdown("---")

# 会社選択
companies = get_companies_cached(processor)
if companies.empty:
    st.sidebar.info("🏢 会社を登録してください")
    st.sidebar.markdown("👉 システム設定から会社を追加")
    # 強制的にシステム設定ページに
    st.session_state.page = "システム設定"
    selected_comp_name = ""
    selected_comp_id = None
    
    # メニューを表示（システム設定のみ使用可能）
    st.sidebar.markdown("---")
    st.sidebar.markdown("### 📋 メニュー")
    st.sidebar.markdown("⚙️ システム設定")
    
else:
    comp_names = companies['name'].tolist()
    
    # 前回の選択を保存
    prev_comp_id = st.session_state.get('selected_comp_id', None)
    
    selected_comp_name = st.sidebar.selectbox(
        "🏢 会社を選択",
        comp_names,
        key="comp_select"
    )
    selected_comp_id = int(companies[companies['name'] == selected_comp_name]['id'].iloc[0])
    
    # 会社が変更された場合、データをリフレッシュ
    if prev_comp_id != selected_comp_id:
        # session_stateをクリア（データ再読み込み用）
        for key in ['actuals_df', 'forecasts_df', 'imported_df', 'show_import_button']:
            if key in st.session_state:
                del st.session_state[key]
    
    st.session_state.selected_comp_id = selected_comp_id
    st.session_state.selected_comp_name = selected_comp_name

    # 期選択
    periods = get_company_periods_cached(selected_comp_id, processor)
    if periods.empty:
        st.sidebar.info("📅 会計期間を登録してください")
        st.sidebar.markdown("👉 システム設定から期を追加")
        selected_period_num = 0
        selected_period_id = None
    else:
        # 前回の選択を保存
        prev_period_id = st.session_state.get('selected_period_id', None)
        
        period_options = [
            f"第{row['period_num']}期 ({row['start_date']} 〜 {row['end_date']})"
            for _, row in periods.iterrows()
        ]
        selected_period_str = st.sidebar.selectbox(
            "📅 期を選択",
            period_options,
            key="period_select"
        )
        selected_period_num = int(selected_period_str.split('第')[1].split('期')[0])
        periods.columns = [c.lower() for c in periods.columns]
        
        period_match = periods[periods['period_num'] == selected_period_num]
        if not period_match.empty:
            if 'id' in period_match.columns:
                selected_period_id = int(period_match['id'].iloc[0])
            else:
                selected_period_id = int(period_match.iloc[0, 0])
            
            # 期が変更された場合、データをリフレッシュ
            if prev_period_id != selected_period_id:
                # session_stateをクリア（データ再読み込み用）
                for key in ['actuals_df', 'forecasts_df', 'imported_df', 'show_import_button']:
                    if key in st.session_state:
                        del st.session_state[key]
                
            st.session_state.selected_period_id = selected_period_id
            st.session_state.selected_period_num = selected_period_num
            st.session_state.start_date = period_match['start_date'].iloc[0]
            st.session_state.end_date = period_match['end_date'].iloc[0]
        else:
            st.error("選択された期が見つかりません")
            selected_period_id = None

    # 予測シナリオ
    st.sidebar.markdown("### 🎯 予測シナリオ")
    st.session_state.scenario = st.sidebar.radio(
        "シナリオを選択",
        ["現実", "楽観", "悲観"],
        horizontal=True,
        label_visibility="collapsed"
    )
    
    # シナリオ設定
    if 'scenario_rates' not in st.session_state:
        st.session_state.scenario_rates = {
            "現実": 0.0,
            "楽観": 0.1,
            "悲観": -0.1
        }
    
    # 表示設定
    st.sidebar.markdown("### ⚙️ 表示設定")
    st.session_state.display_mode = st.sidebar.radio(
        "表示モード",
        ["要約", "詳細"],
        horizontal=True
    )
    
    # 月次リスト取得
    if selected_period_id:
        months = get_fiscal_months_cached(selected_comp_id, selected_period_id, processor)
        
        # 実績締月の選択
        if 'current_month' not in st.session_state or st.session_state.current_month not in months:
            st.session_state.current_month = months[0]
            
        st.session_state.current_month = st.sidebar.selectbox(
            "実績締月を選択",
            months,
            index=months.index(st.session_state.current_month) if st.session_state.current_month in months else 0
        )

    # メニュー
    st.sidebar.markdown("---")
    st.sidebar.markdown("### 📋 メニュー")
    
    menu_options = [
        "着地予測ダッシュボード",
        "損益計算書 (PL)",
        "キャッシュフロー計算書 (CF)",
        "経営指標ダッシュボード",
        "損益分岐点分析",
        "予測 VS 実績比較",
        "期間比較分析",
        "実績データ入力",
        "予測データ入力",
        "データインポート",
        "シナリオ一括設定",
        "システム設定"
    ]
    
    st.session_state.page = st.sidebar.radio(
        "ページ移動",
        menu_options,
        label_visibility="collapsed"
    )

# --------------------------------------------------------------------------------
# ヘルパー関数
# --------------------------------------------------------------------------------
def format_currency(val):
    """通貨フォーマット"""
    if pd.isna(val):
        return "¥0"
    return f"¥{safe_int(val):,}"

def format_percent(val):
    """パーセントフォーマット"""
    if pd.isna(val):
        return "0.0%"
    return f"{val:.1f}%"

# --------------------------------------------------------------------------------
# メインコンテンツ
# --------------------------------------------------------------------------------

# システム設定ページ（会社未登録時でも表示）
if st.session_state.page == "システム設定":
    st.title("⚙️ システム設定")
    
    tab1, tab2, tab3 = st.tabs(["🏢 会社設定", "📅 会計期間設定", "🔍 データベース診断"])
    
    with tab1:
        st.subheader("会社情報の管理")
        
        # 新規会社登録
        with st.form("company_form"):
            new_company_name = st.text_input("新規会社名", placeholder="株式会社サンプル")
            if st.form_submit_button("➕ 会社を登録", type="primary"):
                if new_company_name:
                    success, msg = processor.register_company(new_company_name)
                    if success:
                        st.success(msg)
                        st.rerun()
                    else:
                        st.error(msg)
                else:
                    st.error("会社名を入力してください")
        
        st.markdown("---")
        
        # 登録済み会社一覧
        st.subheader("📋 登録済み会社")
        if not companies.empty:
            st.dataframe(companies, width=600)
        else:
            st.info("登録されている会社がありません")
            
    with tab2:
        st.subheader("会計期間の管理")
        
        if companies.empty:
            st.warning("先に会社を登録してください")
        else:
            comp_id_for_period = st.selectbox(
                "対象会社を選択",
                companies['id'].tolist(),
                format_func=lambda x: companies[companies['id'] == x]['name'].iloc[0]
            )
            
            with st.form("period_form"):
                col1, col2 = st.columns(2)
                with col1:
                    period_num = st.number_input("期数 (第n期)", min_value=1, value=1)
                with col2:
                    start_date = st.date_input("開始日")
                    end_date = st.date_input("終了日")
                
                if st.form_submit_button("➕ 期を追加", type="primary"):
                    if start_date and end_date:
                        if start_date < end_date:
                            success, msg = processor.register_fiscal_period(comp_id_for_period, period_num, str(start_date), str(end_date))
                            if success:
                                st.success(msg)
                                st.rerun()
                            else:
                                st.error(msg)
                        else:
                            st.error("❌ 終了日は開始日より後である必要があります")
                    else:
                        st.error("❌ すべてのフィールドを入力してください")
            
            st.markdown("---")
            
            # 登録済み期間一覧
            st.subheader("📋 登録済み会計期間")
            
            if 'selected_comp_id' in st.session_state and st.session_state.selected_comp_id:
                periods_list = processor.get_company_periods(st.session_state.selected_comp_id)
                if not periods_list.empty:
                    st.dataframe(periods_list, width=800)
                else:
                    st.info("登録されている会計期間がありません")
            else:
                st.info("会社を選択すると、その会社の期間が表示されます")
    
    with tab3:
        st.subheader("🔍 データベース診断")
        
        # 接続状態
        st.markdown("### 📡 接続状態")
        if processor.use_postgres:
            st.success("✅ **PostgreSQL (Supabase) 接続中**")
            st.markdown("""
            <div class="success-box">
                <strong>データは永続的に保存されます</strong><br>
                • アプリ再起動後もデータが残ります<br>
                • 複数デバイスから同じデータにアクセス可能<br>
                • データは安全にクラウドに保存されています
            </div>
            """, unsafe_allow_html=True)
            
            # Supabase設定情報
            if hasattr(st, 'secrets') and 'database' in st.secrets:
                st.markdown("### ⚙️ Supabase設定")
                config_info = {
                    "項目": ["ホスト", "データベース", "ユーザー", "ポート"],
                    "値": [
                        st.secrets['database']['host'],
                        st.secrets['database']['database'],
                        st.secrets['database']['user'],
                        str(st.secrets['database']['port'])
                    ]
                }
                st.table(pd.DataFrame(config_info))
        else:
            st.warning("⚠️ **SQLite ローカルデータベース使用中**")
            st.markdown("""
            <div class="warning-box">
                <strong>データは一時的です</strong><br>
                • Streamlit Cloudではアプリ再起動時にデータが消えます<br>
                • ローカル環境では問題なく動作します<br>
                • 永続化するにはSupabaseの設定が必要です
            </div>
            """, unsafe_allow_html=True)
        
        st.markdown("---")
        
        # データ統計
        st.markdown("### 📊 データ統計")
        
        companies_stat = processor.get_companies()
        total_companies = len(companies_stat)
        
        st.metric("登録会社数", f"{total_companies}社")
        
        if total_companies > 0 and 'selected_comp_id' in st.session_state and st.session_state.selected_comp_id:
            periods_stat = processor.get_company_periods(st.session_state.selected_comp_id)
            st.metric("会計期間数", f"{len(periods_stat)}期")
        
        # 接続テスト
        st.markdown("---")
        st.markdown("### 🧪 接続テスト")
        
        if st.button("🔄 データベース接続をテスト", type="primary"):
            with st.spinner("接続テスト中..."):
                try:
                    # 簡単なクエリで接続テスト
                    test_result = processor.get_companies()
                    st.success(f"✅ 接続成功！会社データを{len(test_result)}件取得しました")
                except Exception as e:
                    st.error(f"❌ 接続失敗: {str(e)}")

# データの読み込み（期が選択されている場合のみ）
if 'selected_period_id' in st.session_state and st.session_state.selected_period_id is not None:
        # キャッシュされたデータを使用
        if 'actuals_df' not in st.session_state:
            st.session_state.actuals_df = load_actual_data_cached(st.session_state.selected_period_id, processor)
        if 'forecasts_df' not in st.session_state:
            st.session_state.forecasts_df = load_forecast_data_cached(st.session_state.selected_period_id, "現実", processor)
            
        actuals_df = st.session_state.actuals_df.copy()
        forecasts_df = st.session_state.forecasts_df.copy()
        
        # シナリオ調整
        if st.session_state.scenario != "現実":
            rate = st.session_state.scenario_rates[st.session_state.scenario]
            split_idx = months.index(st.session_state.current_month) + 1 if st.session_state.current_month in months else 0
            forecast_months = months[split_idx:]
            # DataFrameに存在する月のみを使用
            available_forecast_months = [m for m in forecast_months if m in forecasts_df.columns]
            
            for item in processor.all_items:
                if item == "売上高":
                    forecasts_df.loc[forecasts_df['項目名'] == item, available_forecast_months] *= (1 + rate)
                elif item == "売上原価":
                    forecasts_df.loc[forecasts_df['項目名'] == item, available_forecast_months] *= (1 - rate * 0.5)
                elif item in processor.ga_items:
                    forecasts_df.loc[forecasts_df['項目名'] == item, available_forecast_months] *= (1 - rate * 0.3)
                    
            st.session_state.adjusted_forecasts_df = forecasts_df.copy()
        
        # 補助科目合計の反映
        sub_accounts_df = processor.load_sub_accounts(st.session_state.selected_period_id, st.session_state.scenario)
        if not sub_accounts_df.empty:
            aggregated = sub_accounts_df.groupby(['parent_item', 'month'])['amount'].sum().reset_index()
            for _, row in aggregated.iterrows():
                parent = row['parent_item']
                month = row['month']
                amount = row['amount']
                forecasts_df.loc[forecasts_df['項目名'] == parent, month] = amount
        
        # PL計算
        split_idx = months.index(st.session_state.current_month) + 1 if st.session_state.current_month in months else 0
        pl_df = processor.calculate_pl(
            actuals_df,
            forecasts_df,
            split_idx,
            months
        )
        
        # 表示モードでフィルタ
        if st.session_state.display_mode == "要約":
            pl_display = pl_df[pl_df['タイプ'] == '要約']
        else:
            pl_display = pl_df
        
        # --------------------------------------------------------------------------------
        # ページコンテンツ
        # --------------------------------------------------------------------------------
        
        if st.session_state.page == "着地予測ダッシュボード":
            st.title("📊 着地予測ダッシュボード")
            
            st.markdown(f"""
            <div class="info-box">
                <strong>🏢 {st.session_state.selected_comp_name}</strong> | 
                第{st.session_state.selected_period_num}期 | 
                実績: {st.session_state.start_date} 〜 {st.session_state.current_month} | 
                シナリオ: <strong>{st.session_state.scenario}</strong>
            </div>
            """, unsafe_allow_html=True)
            
            # KPIサマリーカード
            col1, col2, col3, col4, col5 = st.columns(5)
            
            with col1:
                sales_total = pl_display[pl_display['項目名'] == '売上高']['合計'].iloc[0]
                st.markdown(f"""
                <div class="summary-card-blue">
                    <div class="card-title">売上高</div>
                    <div class="card-value">¥{safe_int(sales_total):,}</div>
                    <div class="card-subtitle">期末着地予測</div>
                </div>
                """, unsafe_allow_html=True)
            
            with col2:
                gp_total = pl_display[pl_display['項目名'] == '売上総損益金額']['合計'].iloc[0]
                gp_rate = (gp_total / sales_total * 100) if sales_total != 0 else 0
                st.markdown(f"""
                <div class="summary-card-green">
                    <div class="card-title">売上総利益</div>
                    <div class="card-value">¥{safe_int(gp_total):,}</div>
                    <div class="card-subtitle">粗利率: {gp_rate:.1f}%</div>
                </div>
                """, unsafe_allow_html=True)
            
            with col3:
                op_total = pl_display[pl_display['項目名'] == '営業損益金額']['合計'].iloc[0]
                op_rate = (op_total / sales_total * 100) if sales_total != 0 else 0
                st.markdown(f"""
                <div class="summary-card-orange">
                    <div class="card-title">営業利益</div>
                    <div class="card-value">¥{safe_int(op_total):,}</div>
                    <div class="card-subtitle">営業利益率: {op_rate:.1f}%</div>
                </div>
                """, unsafe_allow_html=True)
            
            with col4:
                ord_total = pl_display[pl_display['項目名'] == '経常損益金額']['合計'].iloc[0]
                ord_rate = (ord_total / sales_total * 100) if sales_total != 0 else 0
                st.markdown(f"""
                <div class="summary-card">
                    <div class="card-title">経常利益</div>
                    <div class="card-value">¥{safe_int(ord_total):,}</div>
                    <div class="card-subtitle">経常利益率: {ord_rate:.1f}%</div>
                </div>
                """, unsafe_allow_html=True)
            
            with col5:
                net_total = pl_display[pl_display['項目名'] == '当期純損益金額']['合計'].iloc[0]
                net_rate = (net_total / sales_total * 100) if sales_total != 0 else 0
                color_class = "summary-card-green" if net_total >= 0 else "summary-card-red"
                st.markdown(f"""
                <div class="{color_class}">
                    <div class="card-title">当期純利益</div>
                    <div class="card-value">¥{safe_int(net_total):,}</div>
                    <div class="card-subtitle">純利益率: {net_rate:.1f}%</div>
                </div>
                """, unsafe_allow_html=True)
            
            st.markdown("---")
            
            # タブで表示切り替え
            tab1, tab2 = st.tabs(["📊 損益計算書", "📈 グラフ分析"])
            
            with tab1:
                st.subheader("期末着地予測 損益計算書")
                
                # スタイル付きデータフレーム
                def highlight_summary(row):
                    if row['タイプ'] == '要約':
                        return ['background-color: #5db5f5; font-weight: bold'] * len(row)
                    return [''] * len(row)
                
                # タイプ列を使ってスタイルを適用してから削除
                styled_df = pl_display.style\
                    .apply(highlight_summary, axis=1)\
                    .format(lambda x: f"¥{safe_int(x):,}" if isinstance(x, (int, float)) else x)
                
                st.dataframe(styled_df, width="stretch", height=500)
                
            with tab2:
                st.subheader("月次推移グラフ")
                
                # グラフ用データの準備
                fig = make_subplots(specs=[[{"secondary_y": True}]])
                
                # 売上高（棒グラフ）
                fig.add_trace(
                    go.Bar(
                        x=months,
                        y=pl_df[pl_df['項目名'] == '売上高'][months].iloc[0],
                        name="売上高",
                        marker_color='#4facfe'
                    ),
                    secondary_y=False
                )
                
                # 営業利益（折れ線グラフ）
                fig.add_trace(
                    go.Scatter(
                        x=months,
                        y=pl_df[pl_df['項目名'] == '営業損益金額'][months].iloc[0],
                        name="営業利益",
                        line=dict(color='#f5576c', width=3)
                    ),
                    secondary_y=True
                )
                
                # 実績/予測の境界線
                try:
                    # add_vlineの代わりに、より安定したadd_shapeを使用して境界線を描画
                    fig.add_shape(
                        type="line",
                        x0=st.session_state.current_month,
                        x1=st.session_state.current_month,
                        y0=0,
                        y1=1,
                        yref="paper",
                        line=dict(color="gray", width=2, dash="dash")
                    )
                    # 境界線のラベルを追加
                    fig.add_annotation(
                        x=st.session_state.current_month,
                        y=1,
                        yref="paper",
                        text="実績/予測 境界",
                        showarrow=False,
                        xanchor="left",
                        textangle=-90
                    )
                except Exception as e:
                    # 万が一エラーが発生した場合は境界線なしで続行
                    st.sidebar.error(f"グラフ境界線の描画エラー: {e}")
                
                fig.update_layout(
                    title_text="売上高と営業利益の推移",
                    hovermode="x unified",
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
                )
                
                fig.update_yaxes(title_text="売上高 (円)", secondary_y=False)
                fig.update_yaxes(title_text="営業利益 (円)", secondary_y=True)
                
                st.plotly_chart(fig, width="stretch")
                
                # 費用構成の円グラフ
                st.subheader("費用構成分析（通期予測）")
                
                ga_items_data = pl_df[pl_df['項目名'].isin(processor.ga_items)]
                fig_pie = px.pie(
                    ga_items_data,
                    values='合計',
                    names='項目名',
                    title="販売管理費の内訳",
                    hole=0.4,
                    color_discrete_sequence=px.colors.qualitative.Pastel
                )
                st.plotly_chart(fig_pie, width="stretch")

        elif st.session_state.page == "損益計算書 (PL)":
            st.title("📄 損益計算書 (PL)")
            
            st.markdown(f"""
            <div class="info-box">
                <strong>🏢 {st.session_state.selected_comp_name}</strong> | 
                第{st.session_state.selected_period_num}期 | 
                実績締月: {st.session_state.current_month} | 
                シナリオ: <strong>{st.session_state.scenario}</strong>
            </div>
            """, unsafe_allow_html=True)
            
            # フィルタリング
            col1, col2 = st.columns([2, 1])
            with col1:
                search_term = st.text_input("🔍 項目名で検索", "")
            
            display_df = pl_display.copy()
            if search_term:
                display_df = display_df[display_df['項目名'].str.contains(search_term)]
            
            # フォーマット
            formatted_df = display_df.style\
                .format(lambda x: f"¥{safe_int(x):,}" if isinstance(x, (int, float)) else x)\
                .apply(lambda row: ['background-color: #f8f9fa; font-weight: bold' if row['タイプ'] == '要約' else '' for _ in row], axis=1)
            
            st.dataframe(formatted_df, width="stretch", height=700)
            
            # CSVダウンロード
            csv = display_df.to_csv(index=False).encode('utf-8-sig')
            st.download_button(
                "📥 CSVとしてダウンロード",
                csv,
                f"PL_{st.session_state.selected_comp_name}_第{st.session_state.selected_period_num}期.csv",
                "text/csv",
                key='download-csv'
            )

        elif st.session_state.page == "予測データ入力":
            st.title("🔮 予測データ入力")
            
            st.markdown(f"""
            <div class="info-box">
                <strong>シナリオ: {st.session_state.scenario}</strong> | 
                実績締月: {st.session_state.current_month} 以降のデータを編集してください。<br>
                💡 <strong>使い方:</strong> 項目をクリック → 行を追加/編集 → 自動保存
            </div>
            """, unsafe_allow_html=True)
            
            # 予測データを取得（キャッシュ付き）
            forecast_data = load_forecast_data_cached(
                st.session_state.selected_period_id,
                st.session_state.scenario,
                processor
            )
            
            # 補助科目データを取得（キャッシュ付き）
            sub_accounts_data = load_sub_accounts_cached(
                st.session_state.selected_period_id,
                st.session_state.scenario,
                processor
            )
            
            # 展開状態を管理
            if 'expanded_forecast_item' not in st.session_state:
                st.session_state.expanded_forecast_item = None
            
            # PLの構造を定義（カテゴリ別）
            pl_categories = {
                "売上": ["売上高"],
                "売上原価": ["売上原価"],
                "人件費": ["役員報酬", "給料手当", "賞与", "法定福利費", "福利厚生費"],
                "採用・外注": ["採用教育費", "外注費"],
                "販売費": ["荷造運賃", "広告宣伝費", "販売手数料", "販売促進費"],
                "一般管理費": [
                    "交際費", "会議費", "旅費交通費", "通信費", "消耗品費", 
                    "修繕費", "事務用品費", "水道光熱費", "新聞図書費", "諸会費",
                    "支払手数料", "車両費", "地代家賃", "賃借料", "保険料",
                    "租税公課", "支払報酬料", "研究開発費", "研修費", "減価償却費",
                    "貸倒損失(販)", "雑費", "少額交際費"
                ],
                "営業外・特別損益": [
                    "営業外収益合計", "営業外費用合計", 
                    "特別利益合計", "特別損失合計"
                ],
                "税金": ["法人税、住民税及び事業税"]
            }
            
            # カテゴリ選択
            selected_category = st.selectbox(
                "カテゴリを選択",
                list(pl_categories.keys()),
                key="forecast_category"
            )
            
            items_in_category = pl_categories[selected_category]
            
            # 項目を選択
            editable_items = [item for item in items_in_category if item not in processor.calculated_items]
            
            if not editable_items:
                st.warning("このカテゴリには編集可能な項目がありません。")
            else:
                selected_item = st.selectbox(
                    "編集する項目を選択",
                    editable_items,
                    key="forecast_item_select"
                )
                
                # テーブル形式でデータを表示・編集
                st.markdown(f"### 📊 {selected_item} の予測データ")
                
                # 基本項目データの準備
                item_row_data = {"項目名": selected_item, "タイプ": "要約"}
                item_data = forecast_data[forecast_data['項目名'] == selected_item]
                
                for month in months:
                    if not item_data.empty and month in item_data.columns:
                        val = item_data[month].iloc[0]
                        item_row_data[month] = float(val) if pd.notna(val) else 0.0
                    else:
                        item_row_data[month] = 0.0
                
                # 補助科目データの準備
                sub_rows = []
                if selected_item in processor.parent_items_with_sub_accounts:
                    item_subs = sub_accounts_data[sub_accounts_data['parent_item'] == selected_item]
                    for sub_name in item_subs['sub_account_name'].unique():
                        sub_row = {"項目名": f"  └ {sub_name}", "タイプ": "詳細"}
                        sub_data = item_subs[item_subs['sub_account_name'] == sub_name]
                        for month in months:
                            month_data = sub_data[sub_data['month'] == month]
                            if not month_data.empty:
                                val = month_data['amount'].iloc[0]
                                sub_row[month] = float(val) if pd.notna(val) else 0.0
                            else:
                                sub_row[month] = 0.0
                        sub_rows.append(sub_row)
                
                # DataFrameに変換
                all_rows = [item_row_data] + sub_rows
                edit_df = pd.DataFrame(all_rows)
                
                # 合計列を追加
                month_cols = [m for m in months if m in edit_df.columns]
                edit_df['合計'] = edit_df[month_cols].sum(axis=1)
                
                # データエディタで編集
                column_config = {
                    "項目名": st.column_config.TextColumn("項目名", disabled=True, width="medium"),
                    "タイプ": st.column_config.TextColumn("タイプ", disabled=True, width="small"),
                    "合計": st.column_config.NumberColumn("合計", format="¥%d", disabled=True, width="medium")
                }
                
                for month in month_cols:
                    column_config[month] = st.column_config.NumberColumn(
                        month,
                        format="¥%d",
                        width="small"
                    )
                
                edited_df = st.data_editor(
                    edit_df,
                    column_config=column_config,
                    use_container_width=True,
                    num_rows="dynamic",  # 行の追加・削除を許可
                    key=f"editor_{selected_item}"
                )
                
                # 保存ボタン
                col1, col2, col3 = st.columns([2, 2, 1])
                
                with col1:
                    if st.button("💾 変更を保存", type="primary", key="save_forecast_table"):
                        # 基本項目の保存
                        main_row = edited_df[edited_df['タイプ'] == '要約'].iloc[0]
                        main_values = {month: main_row[month] for month in month_cols}
                        
                        success, msg = processor.save_forecast_item(
                            st.session_state.selected_period_id,
                            st.session_state.scenario,
                            selected_item,
                            main_values
                        )
                        
                        if success:
                            # 補助科目の保存
                            sub_rows_df = edited_df[edited_df['タイプ'] == '詳細']
                            for _, row in sub_rows_df.iterrows():
                                sub_name = row['項目名'].replace('  └ ', '')
                                sub_values = {month: row[month] for month in month_cols}
                                processor.save_sub_account(
                                    st.session_state.selected_period_id,
                                    st.session_state.scenario,
                                    selected_item,
                                    sub_name,
                                    sub_values
                                )
                            
                            st.success("✅ データを保存しました")
                            # キャッシュクリア
                            st.cache_data.clear()
                            st.rerun()
                        else:
                            st.error(f"❌ 保存に失敗: {msg}")
                
                with col2:
                    if selected_item in processor.parent_items_with_sub_accounts:
                        if st.button("🗑️ 補助科目を全期から削除", key="delete_sub_all"):
                            # 削除する補助科目を選択
                            sub_names = [row['項目名'].replace('  └ ', '') for _, row in edited_df[edited_df['タイプ'] == '詳細'].iterrows()]
                            if sub_names:
                                selected_sub = st.selectbox("削除する補助科目", sub_names, key="sub_to_delete")
                                if st.button("確認：全期から削除", key="confirm_delete"):
                                    # TODO: 全期削除の実装
                                    st.warning("全期削除機能は実装中です")
                
                with col3:
                    if st.button("🔄 リセット"):
                        st.cache_data.clear()
                        st.rerun()
            
        
        
        elif st.session_state.page == "キャッシュフロー計算書 (CF)":
            st.title("💰 キャッシュフロー計算書")
            
            st.markdown("""
            <div class="info-box">
                <strong>💡 概要:</strong> 資金の流れを「営業活動」「投資活動」「財務活動」に分けて把握します。
            </div>
            """, unsafe_allow_html=True)
            
            # CFデータを計算
            cf_data = processor.calculate_cash_flow(st.session_state.selected_period_id)
            
            if cf_data:
                # 各カテゴリのサマリーカード
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    operating_cf_total = sum([v for v in cf_data.get("営業活動によるキャッシュフロー", {}).values() if pd.notna(v)])
                    st.markdown(f"""
                    <div class="summary-card-blue">
                        <div class="card-title">営業活動CF</div>
                        <div class="card-value">¥{safe_int(operating_cf_total):,}</div>
                        <div class="card-subtitle">本業で稼いだ現金</div>
                    </div>
                    """, unsafe_allow_html=True)
                
                with col2:
                    investing_cf_total = sum([v for v in cf_data.get("投資活動によるキャッシュフロー", {}).values() if pd.notna(v)])
                    st.markdown(f"""
                    <div class="summary-card-orange">
                        <div class="card-title">投資活動CF</div>
                        <div class="card-value">¥{safe_int(investing_cf_total):,}</div>
                        <div class="card-subtitle">設備投資などの支出</div>
                    </div>
                    """, unsafe_allow_html=True)
                
                with col3:
                    financing_cf_total = sum([v for v in cf_data.get("財務活動によるキャッシュフロー", {}).values() if pd.notna(v)])
                    st.markdown(f"""
                    <div class="summary-card-purple">
                        <div class="card-title">財務活動CF</div>
                        <div class="card-value">¥{safe_int(financing_cf_total):,}</div>
                        <div class="card-subtitle">借入・返済の収支</div>
                    </div>
                    """, unsafe_allow_html=True)
                
                # 詳細テーブル
                st.markdown("### 📊 月次キャッシュフロー推移")
                
                cf_rows = []
                for category, month_data in cf_data.items():
                    row = {"項目": category}
                    row.update(month_data)
                    cf_rows.append(row)
                
                if cf_rows:
                    cf_df = pd.DataFrame(cf_rows)
                    st.dataframe(cf_df, width="stretch", height=300)
                    
                    # グラフ
                    st.markdown("### 📈 キャッシュフロー推移グラフ")
                    fig = go.Figure()
                    
                    for category in cf_data.keys():
                        months_list = list(cf_data[category].keys())
                        values_list = [cf_data[category][m] for m in months_list]
                        
                        fig.add_trace(go.Scatter(
                            x=months_list,
                            y=values_list,
                            mode='lines+markers',
                            name=category,
                            line=dict(width=3)
                        ))
                    
                    fig.update_layout(
                        xaxis_title="月",
                        yaxis_title="金額 (円)",
                        hovermode='x unified',
                        height=500,
                        template="plotly_white"
                    )
                    
                    st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning("キャッシュフローデータがありません。")
        
        elif st.session_state.page == "経営指標ダッシュボード":
            st.title("📊 経営指標ダッシュボード")
            
            st.markdown("""
            <div class="info-box">
                <strong>💡 概要:</strong> 企業の収益性、効率性、安全性を数値で評価します。
            </div>
            """, unsafe_allow_html=True)
            
            # 経営指標を計算
            indicators = processor.calculate_financial_indicators(st.session_state.selected_period_id)
            
            if indicators:
                # 最新月の指標を取得
                latest_month = list(indicators.keys())[-1] if indicators else None
                
                if latest_month:
                    latest_indicators = indicators[latest_month]
                    
                    # KPIカード
                    st.markdown("### 📈 主要経営指標（最新月）")
                    
                    col1, col2, col3, col4 = st.columns(4)
                    
                    with col1:
                        st.markdown(f"""
                        <div class="kpi-card">
                            <div class="kpi-label">粗利率</div>
                            <div class="kpi-value">{latest_indicators['粗利率']:.1f}%</div>
                            <div class="kpi-change kpi-{'positive' if latest_indicators['粗利率'] > 30 else 'negative'}">
                                {'✓ 良好' if latest_indicators['粗利率'] > 30 else '△ 要改善'}
                            </div>
                        </div>
                        """, unsafe_allow_html=True)
                    
                    with col2:
                        st.markdown(f"""
                        <div class="kpi-card">
                            <div class="kpi-label">営業利益率</div>
                            <div class="kpi-value">{latest_indicators['営業利益率']:.1f}%</div>
                            <div class="kpi-change kpi-{'positive' if latest_indicators['営業利益率'] > 5 else 'negative'}">
                                {'✓ 良好' if latest_indicators['営業利益率'] > 5 else '△ 要改善'}
                            </div>
                        </div>
                        """, unsafe_allow_html=True)
                    
                    with col3:
                        st.markdown(f"""
                        <div class="kpi-card">
                            <div class="kpi-label">経常利益率</div>
                            <div class="kpi-value">{latest_indicators['経常利益率']:.1f}%</div>
                            <div class="kpi-change kpi-{'positive' if latest_indicators['経常利益率'] > 3 else 'negative'}">
                                {'✓ 良好' if latest_indicators['経常利益率'] > 3 else '△ 要改善'}
                            </div>
                        </div>
                        """, unsafe_allow_html=True)
                    
                    with col4:
                        st.markdown(f"""
                        <div class="kpi-card">
                            <div class="kpi-label">当期純利益率</div>
                            <div class="kpi-value">{latest_indicators['当期純利益率']:.1f}%</div>
                            <div class="kpi-change kpi-{'positive' if latest_indicators['当期純利益率'] > 2 else 'negative'}">
                                {'✓ 良好' if latest_indicators['当期純利益率'] > 2 else '△ 要改善'}
                            </div>
                        </div>
                        """, unsafe_allow_html=True)
                    
                    # 月次推移グラフ
                    st.markdown("### 📈 収益性指標の推移")
                    
                    months_list = list(indicators.keys())
                    
                    fig = go.Figure()
                    
                    fig.add_trace(go.Scatter(
                        x=months_list,
                        y=[indicators[m]['粗利率'] for m in months_list],
                        mode='lines+markers',
                        name='粗利率',
                        line=dict(color='#2e7d32', width=3)
                    ))
                    
                    fig.add_trace(go.Scatter(
                        x=months_list,
                        y=[indicators[m]['営業利益率'] for m in months_list],
                        mode='lines+markers',
                        name='営業利益率',
                        line=dict(color='#1976d2', width=3)
                    ))
                    
                    fig.add_trace(go.Scatter(
                        x=months_list,
                        y=[indicators[m]['経常利益率'] for m in months_list],
                        mode='lines+markers',
                        name='経常利益率',
                        line=dict(color='#f57c00', width=3)
                    ))
                    
                    fig.add_trace(go.Scatter(
                        x=months_list,
                        y=[indicators[m]['当期純利益率'] for m in months_list],
                        mode='lines+markers',
                        name='当期純利益率',
                        line=dict(color='#6a1b9a', width=3)
                    ))
                    
                    fig.update_layout(
                        xaxis_title="月",
                        yaxis_title="利益率 (%)",
                        hovermode='x unified',
                        height=500,
                        template="plotly_white"
                    )
                    
                    st.plotly_chart(fig, use_container_width=True)
                    
                    # 推奨改善アクション
                    st.markdown("### 💡 推奨改善アクション")
                    
                    if latest_indicators['粗利率'] < 30:
                        st.markdown("""
                        <div class="warning-box">
                            <strong>⚠️ 粗利率が低い</strong><br>
                            • 価格設定の見直し<br>
                            • 原価削減施策の検討<br>
                            • 高付加価値商品へのシフト
                        </div>
                        """, unsafe_allow_html=True)
                    
                    if latest_indicators['営業利益率'] < 5:
                        st.markdown("""
                        <div class="warning-box">
                            <strong>⚠️ 営業利益率が低い</strong><br>
                            • 販管費の見直し<br>
                            • 業務効率化の推進<br>
                            • 固定費の削減検討
                        </div>
                        """, unsafe_allow_html=True)
                    
                    if latest_indicators['経常利益率'] > 3 and latest_indicators['営業利益率'] > 5:
                        st.markdown("""
                        <div class="success-box">
                            <strong>✓ 良好な収益性</strong><br>
                            現在の収益構造を維持しつつ、さらなる成長を目指しましょう。
                        </div>
                        """, unsafe_allow_html=True)
            else:
                st.warning("経営指標データがありません。")
        
        elif st.session_state.page == "損益分岐点分析":
            st.title("📉 損益分岐点分析")
            
            st.markdown("""
            <div class="info-box">
                <strong>💡 概要:</strong> 赤字にならない最低売上高を計算し、経営の安全性を評価します。
            </div>
            """, unsafe_allow_html=True)
            
            # 損益分岐点を計算（予測データを使用）
            forecasts = load_forecast_data_cached(
                st.session_state.selected_period_id,
                st.session_state.scenario,
                processor
            )
            
            if not forecasts.empty:
                months = [col for col in forecasts.columns if col not in ['項目名']]
                
                # 売上高
                sales_row = forecasts[forecasts['項目名'] == '売上高']
                total_sales = 0
                if not sales_row.empty:
                    for month in months:
                        if month in sales_row.columns:
                            val = sales_row[month].iloc[0]
                            if pd.notna(val):
                                total_sales += float(val)
                
                # 変動費（売上原価）
                vc_row = forecasts[forecasts['項目名'] == '売上原価']
                total_vc = 0
                if not vc_row.empty:
                    for month in months:
                        if month in vc_row.columns:
                            val = vc_row[month].iloc[0]
                            if pd.notna(val):
                                total_vc += float(val)
                
                # 固定費（販管費）
                total_fc = 0
                for item in processor.ga_items:
                    item_row = forecasts[forecasts['項目名'] == item]
                    if not item_row.empty:
                        for month in months:
                            if month in item_row.columns:
                                val = item_row[month].iloc[0]
                                if pd.notna(val):
                                    total_fc += float(val)
                
                # 計算
                contribution_margin = total_sales - total_vc
                contribution_margin_ratio = (contribution_margin / total_sales * 100) if total_sales > 0 else 0
                breakeven_sales = (total_fc / (contribution_margin_ratio / 100)) if contribution_margin_ratio > 0 else 0
                safety_margin = total_sales - breakeven_sales
                safety_margin_ratio = (safety_margin / total_sales * 100) if total_sales > 0 else 0
                
                # サマリーカード
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.markdown(f"""
                    <div class="summary-card-blue">
                        <div class="card-title">損益分岐点売上高</div>
                        <div class="card-value">¥{safe_int(breakeven_sales):,}</div>
                        <div class="card-subtitle">この売上で利益ゼロ</div>
                    </div>
                    """, unsafe_allow_html=True)
                
                with col2:
                    st.markdown(f"""
                    <div class="summary-card-green">
                        <div class="card-title">安全余裕額</div>
                        <div class="card-value">¥{safe_int(safety_margin):,}</div>
                        <div class="card-subtitle">赤字までの余裕</div>
                    </div>
                    """, unsafe_allow_html=True)
                
                with col3:
                    st.markdown(f"""
                    <div class="summary-card-{'green' if safety_margin_ratio > 20 else 'orange'}">
                        <div class="card-title">安全余裕率</div>
                        <div class="card-value">{safety_margin_ratio:.1f}%</div>
                        <div class="card-subtitle">{'安全' if safety_margin_ratio > 20 else '注意'}</div>
                    </div>
                    """, unsafe_allow_html=True)
                
                # 詳細分析
                st.markdown("### 📊 詳細分析")
                
                col1, col2 = st.columns(2)
                
                with col1:
                    st.markdown("""
                    <div class="kpi-card">
                        <h4>費用構造</h4>
                    </div>
                    """, unsafe_allow_html=True)
                    
                    analysis_data = {
                        "項目": ["売上高", "変動費", "限界利益", "固定費", "営業利益"],
                        "金額": [total_sales, total_vc, contribution_margin, total_fc, contribution_margin - total_fc],
                        "構成比(%)": [
                            100,
                            (total_vc / total_sales * 100) if total_sales > 0 else 0,
                            (contribution_margin / total_sales * 100) if total_sales > 0 else 0,
                            (total_fc / total_sales * 100) if total_sales > 0 else 0,
                            ((contribution_margin - total_fc) / total_sales * 100) if total_sales > 0 else 0
                        ]
                    }
                    
                    analysis_df = pd.DataFrame(analysis_data)
                    st.dataframe(
                        analysis_df.style.format({"金額": "¥{:,.0f}", "構成比(%)": "{:.1f}%"}),
                        width="stretch"
                    )
                
                with col2:
                    st.markdown("""
                    <div class="kpi-card">
                        <h4>重要指標</h4>
                    </div>
                    """, unsafe_allow_html=True)
                    
                    metrics_data = {
                        "指標": ["限界利益率", "損益分岐点比率", "安全余裕率", "固定費回収率"],
                        "値": [
                            f"{contribution_margin_ratio:.1f}%",
                            f"{(breakeven_sales / total_sales * 100) if total_sales > 0 else 0:.1f}%",
                            f"{safety_margin_ratio:.1f}%",
                            f"{(contribution_margin / total_fc * 100) if total_fc > 0 else 0:.1f}%"
                        ],
                        "評価": [
                            "良好" if contribution_margin_ratio > 40 else "要改善",
                            "良好" if (breakeven_sales / total_sales * 100) < 80 else "要改善",
                            "良好" if safety_margin_ratio > 20 else "要注意",
                            "良好" if (contribution_margin / total_fc * 100) > 120 else "要改善"
                        ]
                    }
                    
                    metrics_df = pd.DataFrame(metrics_data)
                    st.dataframe(metrics_df, width="stretch")
                
                # グラフ
                st.markdown("### 📈 損益分岐点グラフ")
                
                # X軸（売上高の範囲）
                x_range = np.linspace(0, total_sales * 1.5, 100)
                
                # 総費用線（固定費 + 変動費）
                variable_cost_ratio = total_vc / total_sales if total_sales > 0 else 0
                total_cost_line = total_fc + (x_range * variable_cost_ratio)
                
                # 売上高線
                sales_line = x_range
                
                fig = go.Figure()
                
                # 売上高線
                fig.add_trace(go.Scatter(
                    x=x_range,
                    y=sales_line,
                    mode='lines',
                    name='売上高',
                    line=dict(color='#2e7d32', width=3)
                ))
                
                # 総費用線
                fig.add_trace(go.Scatter(
                    x=x_range,
                    y=total_cost_line,
                    mode='lines',
                    name='総費用',
                    line=dict(color='#c62828', width=3)
                ))
                
                # 損益分岐点
                fig.add_trace(go.Scatter(
                    x=[breakeven_sales],
                    y=[breakeven_sales],
                    mode='markers',
                    name='損益分岐点',
                    marker=dict(size=15, color='#f57c00')
                ))
                
                # 現在の売上
                current_total_cost = total_fc + (total_sales * variable_cost_ratio)
                fig.add_trace(go.Scatter(
                    x=[total_sales],
                    y=[current_total_cost],
                    mode='markers',
                    name='現在位置',
                    marker=dict(size=15, color='#1976d2', symbol='star')
                ))
                
                fig.update_layout(
                    xaxis_title="売上高 (円)",
                    yaxis_title="金額 (円)",
                    hovermode='closest',
                    height=500,
                    template="plotly_white"
                )
                
                st.plotly_chart(fig, use_container_width=True)
                
                # 改善提案
                st.markdown("### 💡 改善提案")
                
                if safety_margin_ratio < 10:
                    st.markdown("""
                    <div class="warning-box">
                        <strong>⚠️ 危険水準：安全余裕率が10%未満</strong><br>
                        <strong>至急対応が必要：</strong><br>
                        • 固定費の大幅削減を検討<br>
                        • 売上拡大策の即時実行<br>
                        • 変動費率の改善（仕入先交渉など）<br>
                        • 資金繰り計画の見直し
                    </div>
                    """, unsafe_allow_html=True)
                elif safety_margin_ratio < 20:
                    st.markdown("""
                    <div class="warning-box">
                        <strong>⚠️ 注意水準：安全余裕率が20%未満</strong><br>
                        <strong>改善施策：</strong><br>
                        • 固定費の削減余地を調査<br>
                        • 売上増加施策の検討<br>
                        • 利益率の高い商品の強化
                    </div>
                    """, unsafe_allow_html=True)
                else:
                    st.markdown("""
                    <div class="success-box">
                        <strong>✓ 良好な水準：安全余裕率が20%以上</strong><br>
                        現在の経営は安全です。さらなる成長を目指しましょう。
                    </div>
                    """, unsafe_allow_html=True)
            else:
                st.warning("予測データがありません。予測データを入力してください。")
        
        elif st.session_state.page == "予測 VS 実績比較":
            st.title("📊 予測 VS 実績比較")
            
            st.markdown("""
            <div class="info-box">
                <strong>💡 使い方:</strong> 予測値と実績値の差異を分析します。達成率や乖離額を確認できます。
            </div>
            """, unsafe_allow_html=True)
            
            # 実績データと予測データを取得
            actuals = actuals_df.copy()
            forecasts = load_forecast_data_cached(
                st.session_state.selected_period_id,
                st.session_state.scenario,
                processor
            )
            
            # 比較テーブルを作成
            comparison_rows = []
            
            for item in processor.all_items:
                actual_row = actuals[actuals['項目名'] == item]
                forecast_row = forecasts[forecasts['項目名'] == item]
                
                if actual_row.empty or forecast_row.empty:
                    continue
                
                row_data = {"項目名": item}
                
                # 実績合計
                actual_total = 0
                for month in months:
                    if month in actual_row.columns:
                        val = actual_row[month].iloc[0]
                        if pd.notna(val):
                            actual_total += float(val)
                
                # 予測合計
                forecast_total = 0
                for month in months:
                    if month in forecast_row.columns:
                        val = forecast_row[month].iloc[0]
                        if pd.notna(val):
                            forecast_total += float(val)
                
                # 差異計算
                diff = actual_total - forecast_total
                diff_rate = (diff / forecast_total * 100) if forecast_total != 0 else 0
                achievement_rate = (actual_total / forecast_total * 100) if forecast_total != 0 else 0
                
                row_data["実績"] = actual_total
                row_data["予測"] = forecast_total
                row_data["差異"] = diff
                row_data["差異率(%)"] = diff_rate
                row_data["達成率(%)"] = achievement_rate
                
                comparison_rows.append(row_data)
            
            comparison_df = pd.DataFrame(comparison_rows)
            
            # フォーマットして表示
            if not comparison_df.empty:
                formatted_df = comparison_df.style\
                    .format({
                        "実績": "¥{:,.0f}",
                        "予測": "¥{:,.0f}",
                        "差異": "¥{:,.0f}",
                        "差異率(%)": "{:.1f}%",
                        "達成率(%)": "{:.1f}%"
                    })\
                    .applymap(
                        lambda x: 'background-color: #d4edda' if isinstance(x, (int, float)) and x > 0 else 
                                  ('background-color: #f8d7da' if isinstance(x, (int, float)) and x < 0 else ''),
                        subset=['差異', '差異率(%)']
                    )
                
                st.dataframe(formatted_df, width="stretch", height=600)
                
                # CSVダウンロード
                csv = comparison_df.to_csv(index=False).encode('utf-8-sig')
                st.download_button(
                    "📥 比較結果をCSVダウンロード",
                    csv,
                    "forecast_vs_actual_comparison.csv",
                    "text/csv",
                    key='download_comparison'
                )
            else:
                st.warning("比較するデータがありません。")
        
        elif st.session_state.page == "期間比較分析":
            st.title("📈 期間比較分析")
            
            st.markdown("""
            <div class="info-box">
                <strong>💡 使い方:</strong> 異なる会計期間のデータを比較します。前期比、成長率などを確認できます。
            </div>
            """, unsafe_allow_html=True)
            
            # 期間選択
            all_periods = get_company_periods_cached(st.session_state.selected_comp_id, processor)
            
            if len(all_periods) < 2:
                st.warning("比較するには2期以上のデータが必要です。")
            else:
                col1, col2 = st.columns(2)
                
                with col1:
                    period1_id = st.selectbox(
                        "比較元の期",
                        all_periods['id'].tolist(),
                        format_func=lambda x: f"第{all_periods[all_periods['id']==x]['period_num'].iloc[0]}期",
                        key="period1"
                    )
                
                with col2:
                    period2_id = st.selectbox(
                        "比較先の期",
                        all_periods['id'].tolist(),
                        format_func=lambda x: f"第{all_periods[all_periods['id']==x]['period_num'].iloc[0]}期",
                        index=1 if len(all_periods) > 1 else 0,
                        key="period2"
                    )
                
                if period1_id == period2_id:
                    st.warning("異なる期を選択してください。")
                else:
                    # 両期間のデータを取得
                    data1 = load_actual_data_cached(period1_id, processor)
                    data2 = load_actual_data_cached(period2_id, processor)
                    
                    months1 = get_fiscal_months_cached(st.session_state.selected_comp_id, period1_id, processor)
                    months2 = get_fiscal_months_cached(st.session_state.selected_comp_id, period2_id, processor)
                    
                    # 比較テーブルを作成
                    period_comparison_rows = []
                    
                    for item in processor.all_items:
                        row1 = data1[data1['項目名'] == item]
                        row2 = data2[data2['項目名'] == item]
                        
                        if row1.empty or row2.empty:
                            continue
                        
                        # 合計計算
                        total1 = sum([float(row1[m].iloc[0]) if m in row1.columns and pd.notna(row1[m].iloc[0]) else 0 for m in months1])
                        total2 = sum([float(row2[m].iloc[0]) if m in row2.columns and pd.notna(row2[m].iloc[0]) else 0 for m in months2])
                        
                        diff = total2 - total1
                        growth_rate = (diff / total1 * 100) if total1 != 0 else 0
                        
                        period_comparison_rows.append({
                            "項目名": item,
                            f"第{all_periods[all_periods['id']==period1_id]['period_num'].iloc[0]}期": total1,
                            f"第{all_periods[all_periods['id']==period2_id]['period_num'].iloc[0]}期": total2,
                            "増減額": diff,
                            "成長率(%)": growth_rate
                        })
                    
                    period_comparison_df = pd.DataFrame(period_comparison_rows)
                    
                    if not period_comparison_df.empty:
                        formatted_period_df = period_comparison_df.style\
                            .format({
                                f"第{all_periods[all_periods['id']==period1_id]['period_num'].iloc[0]}期": "¥{:,.0f}",
                                f"第{all_periods[all_periods['id']==period2_id]['period_num'].iloc[0]}期": "¥{:,.0f}",
                                "増減額": "¥{:,.0f}",
                                "成長率(%)": "{:.1f}%"
                            })\
                            .applymap(
                                lambda x: 'background-color: #d4edda' if isinstance(x, (int, float)) and x > 0 else 
                                          ('background-color: #f8d7da' if isinstance(x, (int, float)) and x < 0 else ''),
                                subset=['増減額', '成長率(%)']
                            )
                        
                        st.dataframe(formatted_period_df, width="stretch", height=600)
                        
                        # CSVダウンロード
                        csv = period_comparison_df.to_csv(index=False).encode('utf-8-sig')
                        st.download_button(
                            "📥 期間比較結果をCSVダウンロード",
                            csv,
                            "period_comparison.csv",
                            "text/csv",
                            key='download_period_comparison'
                        )
                    else:
                        st.warning("比較するデータがありません。")
        
        elif st.session_state.page == "データインポート":
            st.title("📥 データインポート")
            
            # タブで実績データと予測データを分ける
            tab1, tab2 = st.tabs(["💰 実績データインポート", "📊 予測データインポート"])
            
            # ===== タブ1: 実績データインポート =====
            with tab1:
                st.markdown("""
                <div class="info-box">
                    <strong>💡 使い方:</strong> 弥生会計からエクスポートしたExcelファイルをアップロードしてください。
                </div>
                """, unsafe_allow_html=True)
                
                uploaded_file = st.file_uploader(
                    "Excelファイルを選択（実績データ）",
                    type=['xlsx', 'xls'],
                    help="弥生会計の月次推移表をアップロードしてください",
                    key="actual_upload"
                )
                
                # ファイルが削除された場合のキャッシュクリア
                if uploaded_file is None:
                    if 'imported_df' in st.session_state:
                        del st.session_state.imported_df
                    if 'show_import_button' in st.session_state:
                        del st.session_state.show_import_button
                
                if uploaded_file:
                    if 'imported_df' not in st.session_state:
                        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp_file:
                            tmp_file.write(uploaded_file.read())
                            temp_path = tmp_file.name
                            st.session_state.temp_path_to_delete = temp_path
                            
                        st.success(f"✅ ファイル **{uploaded_file.name}** を読み込みました")
                        
                        # fiscal_period_idを渡す
                        st.session_state.imported_df, info = processor.import_yayoi_excel(
                            temp_path, 
                            st.session_state.selected_period_id,
                            preview_only=True
                        )
                        st.session_state.show_import_button = True
                        
                        # 一時ファイルを削除
                        if os.path.exists(temp_path):
                            os.unlink(temp_path)
                        
                    if st.session_state.get('show_import_button'):
                        st.subheader("📋 インポートデータ プレビュー（直接編集可能）")
                        
                        st.markdown("""
                        <div class="info-box">
                            <strong>✏️ 編集:</strong> セルをダブルクリックして値を直接修正できます。
                        </div>
                        """, unsafe_allow_html=True)
                        
                        # 編集可能なデータエディタを使用
                        edited_df = st.data_editor(
                            st.session_state.imported_df,
                            width="stretch",
                            height=400,
                            num_rows="fixed",  # 行の追加・削除は不可
                            disabled=["項目名"],  # 項目名列は編集不可
                            column_config={
                                col: st.column_config.NumberColumn(
                                    format="¥%d",
                                    min_value=-999999999,
                                    max_value=999999999
                                ) for col in st.session_state.imported_df.columns if col != '項目名'
                            }
                        )
                        
                        # 編集後のデータを保存
                        st.session_state.imported_df = edited_df
                        
                        st.markdown("""
                        <div class="warning-box">
                            <strong>⚠️ 注意:</strong> 上記の内容でインポートを実行すると、現在の実績データは上書きされます。
                        </div>
                        """, unsafe_allow_html=True)
                        
                        if st.button("✅ 上記内容でインポートを実行", type="primary", key="import_actual"):
                            success, info = processor.save_extracted_data(
                                st.session_state.selected_period_id,
                                st.session_state.imported_df
                            )
                            if success:
                                st.success("✅ インポートが完了しました！")
                                # キャッシュクリア
                                for key in ['actuals_df', 'imported_df', 'show_import_button']:
                                    if key in st.session_state:
                                        del st.session_state[key]
                                st.rerun()
                            else:
                                st.error(f"❌ インポートに失敗しました: {info}")
            
            # ===== タブ2: 予測データインポート =====
            with tab2:
                st.markdown("""
                <div class="info-box">
                    <strong>💡 使い方:</strong><br>
                    1. テンプレートをダウンロード<br>
                    2. Excelで予測数値を入力<br>
                    3. ファイルをアップロード
                </div>
                """, unsafe_allow_html=True)
                
                # シナリオ選択
                forecast_scenario = st.selectbox(
                    "インポート先シナリオを選択",
                    ["現実", "楽観", "悲観"],
                    key="forecast_import_scenario"
                )
                
                # テンプレートダウンロード
                st.subheader("📥 ステップ1: テンプレートをダウンロード")
                
                template_df = processor.create_forecast_template(
                    st.session_state.selected_period_id,
                    forecast_scenario
                )
                
                if template_df is not None:
                    # Excelファイルとして出力
                    from io import BytesIO
                    output = BytesIO()
                    with pd.ExcelWriter(output, engine='openpyxl') as writer:
                        template_df.to_excel(writer, index=False, sheet_name='予測データ')
                    excel_data = output.getvalue()
                    
                    st.download_button(
                        label="📥 予測データテンプレートをダウンロード",
                        data=excel_data,
                        file_name=f"予測データテンプレート_{forecast_scenario}_{datetime.now().strftime('%Y%m%d')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        type="primary"
                    )
                    
                    st.info("""
                    💡 **テンプレートの使い方:**
                    - 各項目の予測数値を月ごとに入力してください
                    - 0のままの項目はインポートされません
                    - 項目名の列は変更しないでください
                    """)
                
                st.markdown("---")
                
                # ファイルアップロード
                st.subheader("📤 ステップ2: 入力済みファイルをアップロード")
                
                forecast_file = st.file_uploader(
                    "予測データExcelファイルを選択",
                    type=['xlsx', 'xls'],
                    help="入力済みのテンプレートファイルをアップロードしてください",
                    key="forecast_upload"
                )
                
                # ファイルが削除された場合のキャッシュクリア
                if forecast_file is None:
                    if 'forecast_imported_df' in st.session_state:
                        del st.session_state.forecast_imported_df
                    if 'show_forecast_import_button' in st.session_state:
                        del st.session_state.show_forecast_import_button
                
                if forecast_file:
                    if 'forecast_imported_df' not in st.session_state:
                        try:
                            # Excelファイルを読み込み
                            forecast_df = pd.read_excel(forecast_file)
                            
                            # 基本的なバリデーション
                            if '項目名' not in forecast_df.columns:
                                st.error("❌ テンプレート形式が正しくありません。「項目名」列が見つかりません。")
                            else:
                                st.success(f"✅ ファイル **{forecast_file.name}** を読み込みました")
                                st.session_state.forecast_imported_df = forecast_df
                                st.session_state.show_forecast_import_button = True
                        
                        except Exception as e:
                            st.error(f"❌ ファイルの読み込みに失敗しました: {str(e)}")
                    
                    if st.session_state.get('show_forecast_import_button'):
                        st.subheader("📋 インポートデータ プレビュー（直接編集可能）")
                        
                        st.markdown("""
                        <div class="info-box">
                            <strong>✏️ 編集:</strong> セルをダブルクリックして値を直接修正できます。
                        </div>
                        """, unsafe_allow_html=True)
                        
                        # 編集可能なデータエディタを使用
                        edited_forecast_df = st.data_editor(
                            st.session_state.forecast_imported_df,
                            width="stretch",
                            height=400,
                            num_rows="fixed",
                            disabled=["項目名"],
                            column_config={
                                col: st.column_config.NumberColumn(
                                    format="¥%d",
                                    min_value=-999999999,
                                    max_value=999999999
                                ) for col in st.session_state.forecast_imported_df.columns if col != '項目名'
                            }
                        )
                        
                        # 編集後のデータを保存
                        st.session_state.forecast_imported_df = edited_forecast_df
                        
                        st.markdown(f"""
                        <div class="warning-box">
                            <strong>⚠️ 注意:</strong> 上記の内容でインポートを実行すると、「{forecast_scenario}」シナリオの予測データが上書きされます。
                        </div>
                        """, unsafe_allow_html=True)
                        
                        if st.button("✅ 予測データをインポート", type="primary", key="import_forecast"):
                            success, info = processor.save_forecast_from_excel(
                                st.session_state.selected_period_id,
                                forecast_scenario,
                                st.session_state.forecast_imported_df
                            )
                            if success:
                                st.success(f"✅ {info}")
                                # キャッシュクリア
                                for key in ['forecasts_df', 'forecast_imported_df', 'show_forecast_import_button']:
                                    if key in st.session_state:
                                        del st.session_state[key]
                                st.rerun()
                            else:
                                st.error(f"❌ インポートに失敗しました: {info}")
        
        elif st.session_state.page == "シナリオ一括設定":
            st.title("🎯 シナリオ一括設定")
            
            st.markdown("""
            <div class="info-box">
                <strong>💡 使い方:</strong> 「現実」シナリオをベースに、「楽観」「悲観」シナリオの増減率を設定します。
                設定した増減率は全画面に即座に反映されます。
            </div>
            """, unsafe_allow_html=True)
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("### 📈 楽観シナリオ")
                st.markdown("""
                <div class="success-box">
                    <strong>想定される効果:</strong><br>
                    • 売上: 増加率そのまま適用<br>
                    • 売上原価: 増加率の50%を逆方向に適用<br>
                    • 販管費: 増加率の30%を逆方向に適用
                </div>
                """, unsafe_allow_html=True)
                
                new_opt_rate = st.number_input(
                    "楽観シナリオ増減率 (%)",
                    value=st.session_state.scenario_rates["楽観"] * 100,
                    min_value=-100.0,
                    max_value=100.0,
                    step=1.0,
                    key="opt_rate_input"
                ) / 100.0
                
                if st.button("💾 楽観シナリオ増減率を保存", type="primary"):
                    st.session_state.scenario_rates["楽観"] = new_opt_rate
                    st.success(f"✅ 楽観シナリオの増減率を **{new_opt_rate * 100:.1f}%** に設定しました")
                    st.rerun()
            
            with col2:
                st.markdown("### 📉 悲観シナリオ")
                st.markdown("""
                <div class="warning-box">
                    <strong>想定される効果:</strong><br>
                    • 売上: 減少率そのまま適用<br>
                    • 売上原価: 減少率の50%を逆方向に適用<br>
                    • 販管費: 減少率の30%を逆方向に適用
                </div>
                """, unsafe_allow_html=True)
                
                new_pes_rate = st.number_input(
                    "悲観シナリオ増減率 (%)",
                    value=st.session_state.scenario_rates["悲観"] * 100,
                    min_value=-100.0,
                    max_value=100.0,
                    step=1.0,
                    key="pes_rate_input"
                ) / 100.0
                
                if st.button("💾 悲観シナリオ増減率を保存", type="primary"):
                    st.session_state.scenario_rates["悲観"] = new_pes_rate
                    st.success(f"✅ 悲観シナリオの増減率を **{new_pes_rate * 100:.1f}%** に設定しました")
                    st.rerun()
            
            st.markdown("---")
            
            # 設定値サマリー
            st.subheader("📋 現在の設定値")
            
            summary_data = {
                "シナリオ": ["現実", "楽観", "悲観"],
                "増減率": [
                    f"{st.session_state.scenario_rates['現実'] * 100:.1f}%",
                    f"{st.session_state.scenario_rates['楽観'] * 100:.1f}%",
                    f"{st.session_state.scenario_rates['悲観'] * 100:.1f}%"
                ],
                "説明": [
                    "ベースとなる予測値",
                    "売上増加・費用削減を想定",
                    "売上減少・費用増加を想定"
                ]
            }
            
            st.table(pd.DataFrame(summary_data))
        

else:
    # 会社または期が未登録の場合
    if companies.empty:
        st.title("👋 ようこそ！財務予測シミュレーターへ")
        
        st.markdown("""
        <div style="background-color: #e3f2fd; padding: 2rem; border-radius: 10px; margin: 2rem 0;">
            <h3 style="color: #1976d2; margin-top: 0;">🚀 はじめての方へ</h3>
            <p style="font-size: 1.1rem; line-height: 1.8;">
                まずは以下の手順でセットアップしてください：
            </p>
            <div style="background-color: white; padding: 1.5rem; border-radius: 8px; margin: 1rem 0;">
                <strong style="font-size: 1.2rem; color: #1976d2;">📍 手順</strong><br><br>
                <strong style="color: #d32f2f;">1️⃣ 左サイドバーの「⚙️ システム設定」をクリック</strong><br>
                <span style="font-size: 0.9rem; color: #666;">← 左側のメニューから選択してください</span><br><br>
                <strong>2️⃣ 会社設定タブで会社名を入力</strong><br><br>
                <strong>3️⃣ 会計期間設定タブで期の情報を入力</strong><br><br>
                <strong>4️⃣ サイドバーで会社と期を選択</strong><br>
                <span style="font-size: 0.9rem; color: #666;">→ すべての機能が使えるようになります！</span>
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        # データベース接続状態を表示
        if processor.use_postgres:
            st.success("✅ Supabaseに接続済み - データは永続的に保存されます")
        else:
            st.info("ℹ️ ローカルモードで動作中")
            
    else:
        st.warning("### ⚠️ 会計期間が選択されていません")
        st.markdown("""
        <div class="warning-box">
            <strong>会計期間を登録してください</strong><br><br>
            左サイドバーの「システム設定」→「会計期間設定」タブから<br>
            会計期間を追加してください。
        </div>
        """, unsafe_allow_html=True)
