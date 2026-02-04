"""
財務予測シミュレーター
メインアプリケーション（完全版）
"""

import streamlit as st
import pandas as pd
import os
import sys

# ページ設定（最初に実行）
st.set_page_config(
    page_title="財務予測シミュレーター",
    page_icon="💰",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============ 環境変数設定 ============
DEBUG = os.getenv('DEBUG', 'false').lower() == 'true'

def debug_log(message: str):
    """デバッグ出力（環境変数で制御）"""
    if DEBUG:
        sys.stderr.write(message)
        sys.stderr.flush()

# ============ モジュールインポート（条件付き） ============

debug_log("="*80 + "\n")
debug_log("🚀 アプリ起動開始\n")

# データハンドラー
try:
    from data_handler import DataHandler
    DATA_HANDLER_AVAILABLE = True
    debug_log("✅ DataHandler インポート成功\n")
except ImportError as e:
    DATA_HANDLER_AVAILABLE = False
    debug_log(f"❌ DataHandler インポート失敗: {e}\n")

# 財務分析
try:
    from financial_analysis import FinancialAnalyzer
    FINANCIAL_ANALYSIS_AVAILABLE = True
    debug_log("✅ FinancialAnalyzer インポート成功\n")
except ImportError as e:
    FINANCIAL_ANALYSIS_AVAILABLE = False
    debug_log(f"⚠️ FinancialAnalyzer インポート失敗: {e}\n")

# 高度予測エンジン
try:
    from advanced_forecast_engine import get_advanced_forecast_engine
    ADVANCED_FORECAST_AVAILABLE = True
    debug_log("✅ AdvancedForecastEngine インポート成功\n")
except ImportError as e:
    ADVANCED_FORECAST_AVAILABLE = False
    debug_log(f"⚠️ AdvancedForecastEngine インポート失敗: {e}\n")

# 高度予測UI
try:
    from advanced_forecast_ui import show_advanced_forecast_page
    ADVANCED_FORECAST_UI_AVAILABLE = True
    debug_log("✅ AdvancedForecastUI インポート成功\n")
except ImportError as e:
    ADVANCED_FORECAST_UI_AVAILABLE = False
    debug_log(f"⚠️ AdvancedForecastUI インポート失敗: {e}\n")

# 予測入力ページ
try:
    from forecast_input_page_FINAL import show_forecast_input_page
    FORECAST_INPUT_AVAILABLE = True
    debug_log("✅ ForecastInputPage インポート成功\n")
except ImportError:
    FORECAST_INPUT_AVAILABLE = False
    debug_log(f"⚠️ ForecastInputPage インポート失敗\n")

debug_log("="*80 + "\n")

# ============ 初期化（遅延読み込み） ============

@st.cache_resource
def load_data_handler():
    """DataHandlerを遅延読み込み"""
    if not DATA_HANDLER_AVAILABLE:
        return None
    try:
        debug_log("📦 DataHandler初期化中...\n")
        handler = DataHandler()
        debug_log("✅ DataHandler初期化完了\n")
        return handler
    except Exception as e:
        debug_log(f"❌ DataHandler初期化エラー: {e}\n")
        return None

# ✅ data_handlerを先に初期化
data_handler = load_data_handler()

if data_handler is None:
    st.error("❌ データハンドラーの初期化に失敗しました")
    st.stop()

# ============ セッション状態の初期化 ============

if 'page' not in st.session_state:
    st.session_state.page = 'dashboard'

if 'authenticated' not in st.session_state:
    st.session_state.authenticated = False

if 'username' not in st.session_state:
    st.session_state.username = ''

# ============ 認証 ============

if not st.session_state.authenticated:
    st.title("🔐 ログイン")
    
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col2:
        st.markdown("### 財務予測シミュレーター")
        
        username = st.text_input("ユーザー名", key="login_username")
        password = st.text_input("パスワード", type="password", key="login_password")
        
        col_a, col_b = st.columns(2)
        
        with col_a:
            if st.button("ログイン", type="primary", use_container_width=True):
                if username and password:
                    st.session_state.authenticated = True
                    st.session_state.username = username
                    debug_log(f"👤 ログイン: {username}\n")
                    st.rerun()
                else:
                    st.error("ユーザー名とパスワードを入力してください")
        
        with col_b:
            if st.button("デモモード", use_container_width=True):
                st.session_state.authenticated = True
                st.session_state.username = "demo"
                debug_log("👤 デモモードでログイン\n")
                st.rerun()
    
    st.stop()

# ============ サイドバー ============

st.sidebar.title("📊 財務予測シミュレーター")
st.sidebar.write(f"👤 **{st.session_state.username}**")

# 会計期間選択
st.sidebar.markdown("---")
st.sidebar.subheader("📅 会計期間")

if 'selected_period_id' not in st.session_state:
    st.session_state.selected_period_id = 1

period_select = st.sidebar.selectbox(
    "会計期間を選択",
    [1, 2, 3],
    format_func=lambda x: f"FY{2024 + x-1}"
)
st.session_state.selected_period_id = period_select

# 実績締月の表示
split_info = data_handler.get_actual_vs_forecast_split(st.session_state.selected_period_id)
if split_info['has_actual']:
    st.sidebar.info(f"📅 実績締月: **{split_info['latest_actual_month']}月**")

# メニュー
st.sidebar.markdown("---")
st.sidebar.subheader("📋 メニュー")

menu_items = {
    'dashboard': ('📊', 'ダッシュボード'),
    'profitability': ('💰', '収益構造分析'),
    'metrics': ('📈', '経営指標'),
    'working_capital': ('🔄', '運転資本分析'),
    'forecast_input': ('📝', '予測値入力'),
    'advanced_forecast': ('🔮', 'AI自動予測'),
    'import': ('📥', 'データインポート')
}

for page_key, (icon, label) in menu_items.items():
    # AI自動予測は利用可能な場合のみ表示
    if page_key == 'advanced_forecast':
        if not (ADVANCED_FORECAST_AVAILABLE and ADVANCED_FORECAST_UI_AVAILABLE):
            continue
    
    # 予測値入力は利用可能な場合のみ表示
    if page_key == 'forecast_input':
        if not FORECAST_INPUT_AVAILABLE:
            continue
    
    if st.sidebar.button(f"{icon} {label}", key=f"nav_{page_key}", use_container_width=True):
        st.session_state.page = page_key
        debug_log(f"📄 ページ遷移: {page_key}\n")

# ログアウト
st.sidebar.markdown("---")
if st.sidebar.button("🚪 ログアウト", use_container_width=True):
    st.session_state.authenticated = False
    st.session_state.username = ''
    st.rerun()

# ============ メインエリア ============

debug_log(f"📄 表示ページ: {st.session_state.page}\n")

# ダッシュボード
if st.session_state.page == 'dashboard':
    st.title("📊 ダッシュボード")
    
    period_id = st.session_state.selected_period_id
    split_info = data_handler.get_actual_vs_forecast_split(period_id)
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("実績締月", f"{split_info['latest_actual_month']}月")
    with col2:
        st.metric("実績月数", f"{len(split_info['actual_months'])}ヶ月")
    with col3:
        st.metric("予測月数", f"{len(split_info['forecast_months'])}ヶ月")
    
    st.markdown("---")
    
    # 主要指標
    if FINANCIAL_ANALYSIS_AVAILABLE and split_info['has_actual']:
        st.subheader("📈 主要経営指標")
        
        analyzer = FinancialAnalyzer(data_handler)
        cumulative = data_handler.get_cumulative_actual_data(period_id, split_info['latest_actual_month'])
        
        if cumulative:
            metrics = analyzer.calculate_all_metrics(cumulative, {}, None, period_id)
            
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("売上高営業利益率", f"{metrics.get('売上高営業利益率', 0):.2f}%")
            with col2:
                st.metric("ROE", f"{metrics.get('ROE', 0):.2f}%")
            with col3:
                st.metric("自己資本比率", f"{metrics.get('自己資本比率', 0):.1f}%")
            with col4:
                st.metric("流動比率", f"{metrics.get('流動比率', 0):.1f}%")

# 収益構造分析
elif st.session_state.page == 'profitability':
    st.title("💰 収益構造分析")
    
    if FINANCIAL_ANALYSIS_AVAILABLE:
        period_id = st.session_state.selected_period_id
        analyzer = FinancialAnalyzer(data_handler)
        
        result = analyzer.analyze_profitability(period_id)
        
        if result and 'monthly_data' in result:
            df = result['monthly_data']
            
            import plotly.graph_objects as go
            
            fig = go.Figure()
            fig.add_trace(go.Scatter(x=df['month'], y=df['sales'], name='実際の売上', mode='lines+markers'))
            fig.add_trace(go.Scatter(x=df['month'], y=df['breakeven_sales'], name='損益分岐点', line=dict(dash='dash')))
            
            fig.update_layout(xaxis_title="月", yaxis_title="金額（円）", height=400)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("⚠️ 実績データがありません")

# 経営指標
elif st.session_state.page == 'metrics':
    st.title("📈 経営指標")
    
    if FINANCIAL_ANALYSIS_AVAILABLE:
        period_id = st.session_state.selected_period_id
        analyzer = FinancialAnalyzer(data_handler)
        
        split_info = data_handler.get_actual_vs_forecast_split(period_id)
        cumulative = data_handler.get_cumulative_actual_data(period_id, split_info['latest_actual_month'])
        
        if cumulative:
            metrics = analyzer.calculate_all_metrics(cumulative, {}, None, period_id)
            
            st.subheader("💰 収益性指標")
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("売上高営業利益率", f"{metrics.get('売上高営業利益率', 0):.2f}%")
            with col2:
                st.metric("ROE", f"{metrics.get('ROE', 0):.2f}%")
            with col3:
                st.metric("ROA", f"{metrics.get('ROA', 0):.2f}%")
            
            st.subheader("🛡️ 安全性指標")
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("流動比率", f"{metrics.get('流動比率', 0):.1f}%")
            with col2:
                st.metric("自己資本比率", f"{metrics.get('自己資本比率', 0):.1f}%")
            with col3:
                st.metric("負債比率", f"{metrics.get('負債比率', 0):.1f}%")

# 運転資本分析
elif st.session_state.page == 'working_capital':
    st.title("🔄 運転資本分析")
    
    if FINANCIAL_ANALYSIS_AVAILABLE:
        period_id = st.session_state.selected_period_id
        analyzer = FinancialAnalyzer(data_handler)
        
        split_info = data_handler.get_actual_vs_forecast_split(period_id)
        cumulative = data_handler.get_cumulative_actual_data(period_id, split_info['latest_actual_month'])
        
        if cumulative:
            wc_metrics = analyzer.calculate_working_capital(cumulative, {}, None, period_id)
            
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("CCC", f"{wc_metrics.get('CCC', 0):.0f}日")
            with col2:
                st.metric("売上債権回転期間", f"{wc_metrics.get('売上債権回転期間', 0):.0f}日")
            with col3:
                st.metric("棚卸資産回転期間", f"{wc_metrics.get('棚卸資産回転期間', 0):.0f}日")
            with col4:
                st.metric("仕入債務回転期間", f"{wc_metrics.get('仕入債務回転期間', 0):.0f}日")

# 予測値入力
elif st.session_state.page == 'forecast_input':
    if FORECAST_INPUT_AVAILABLE:
        show_forecast_input_page(data_handler, data_handler)
    else:
        st.error("❌ 予測入力モジュールが利用できません")

# AI自動予測
elif st.session_state.page == 'advanced_forecast':
    if ADVANCED_FORECAST_AVAILABLE and ADVANCED_FORECAST_UI_AVAILABLE:
        # ✅ ここで初期化（遅延初期化）
        if 'advanced_engine' not in st.session_state:
            debug_log("🔮 AdvancedForecastEngine 初期化中...\n")
            st.session_state.advanced_engine = get_advanced_forecast_engine(data_handler)
            debug_log("✅ AdvancedForecastEngine 初期化完了\n")
        
        show_advanced_forecast_page(data_handler, st.session_state.advanced_engine)
    else:
        st.error("❌ AI自動予測機能が利用できません")

# データインポート
elif st.session_state.page == 'import':
    st.title("📥 データインポート")
    
    uploaded_file = st.file_uploader("実績データファイルをアップロード", type=['xlsx', 'xls'])
    
    if uploaded_file:
        import tempfile
        
        with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp:
            tmp.write(uploaded_file.getvalue())
            tmp_path = tmp.name
        
        if st.button("📥 インポート実行", type="primary"):
            with st.spinner("インポート中..."):
                result = data_handler.import_actual_data_from_excel(tmp_path, st.session_state.selected_period_id)
            
            if result['success']:
                st.success(f"✅ {result['message']}")

st.sidebar.markdown("---")
st.sidebar.caption("© 2026 Financial Forecast Simulator")

debug_log("✅ ページ表示完了\n")
