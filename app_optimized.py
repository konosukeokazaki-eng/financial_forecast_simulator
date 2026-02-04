"""
最適化版 app.py テンプレート
- 遅延インポート
- キャッシュ活用
- デバッグ出力制御
"""

import streamlit as st
import pandas as pd
import os

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
        import sys
        sys.stderr.write(message)
        sys.stderr.flush()

# ============ 遅延インポート用関数 ============

@st.cache_resource
def load_data_handler():
    """DataHandlerを遅延読み込み"""
    from data_handler import DataHandler
    return DataHandler()

@st.cache_resource
def load_financial_analyzer(_data_handler):
    """FinancialAnalyzerを遅延読み込み"""
    from financial_analysis import FinancialAnalyzer
    return FinancialAnalyzer(_data_handler)

def load_visualization_helpers():
    """可視化ヘルパーを遅延読み込み（使う時だけ）"""
    import visualization_helpers as vh
    return vh

def load_cf_analyzer(_data_handler):
    """CFAnalyzerを遅延読み込み（使う時だけ）"""
    from cashflow import CashFlowAnalyzer
    return CashFlowAnalyzer(_data_handler)

# ============ 初期化 ============

debug_log("="*80 + "\n")
debug_log("🚀 アプリ起動開始\n")

# セッション状態の初期化
if 'page' not in st.session_state:
    st.session_state.page = 'dashboard'

if 'authenticated' not in st.session_state:
    st.session_state.authenticated = False

# DataHandlerの初期化（キャッシュ済み）
data_handler = load_data_handler()

debug_log("✅ DataHandler初期化完了\n")

# ============ 認証 ============

if not st.session_state.authenticated:
    st.title("🔐 ログイン")
    
    username = st.text_input("ユーザー名")
    password = st.text_input("パスワード", type="password")
    
    if st.button("ログイン"):
        # 簡易認証（本番環境では適切な認証を実装）
        if username and password:
            st.session_state.authenticated = True
            st.session_state.username = username
            st.rerun()
    
    st.stop()

# ============ サイドバー ============

st.sidebar.title("📊 財務予測シミュレーター")
st.sidebar.write(f"👤 {st.session_state.username}")

# 会計期間選択
periods = data_handler.get_fiscal_periods()  # キャッシュ済み
if periods:
    period_options = {f"{p['name']}": p['id'] for p in periods}
    selected_period_name = st.sidebar.selectbox(
        "会計期間",
        list(period_options.keys())
    )
    st.session_state.selected_period_id = period_options[selected_period_name]

# メニュー
st.sidebar.markdown("---")
st.sidebar.subheader("📋 メニュー")

menu_items = {
    'dashboard': '📊 ダッシュボード',
    'profitability': '💰 収益構造分析',
    'metrics': '📈 経営指標',
    'working_capital': '🔄 運転資本分析',
    'cashflow': '💵 キャッシュフロー',
    'forecast': '🔮 予測入力',
    'comparison': '📊 予実比較'
}

for page_key, page_label in menu_items.items():
    if st.sidebar.button(page_label, key=f"nav_{page_key}"):
        st.session_state.page = page_key

# ============ メインエリア ============

debug_log(f"📄 表示ページ: {st.session_state.page}\n")

# 各ページの表示（遅延ロード）

if st.session_state.page == 'dashboard':
    st.title("📊 ダッシュボード")
    
    # 必要なデータのみ読み込み
    period_id = st.session_state.get('selected_period_id')
    
    if period_id:
        # 実績締月情報を表示
        split_info = data_handler.get_actual_vs_forecast_split(period_id)  # キャッシュ済み
        
        st.info(f"""
        📅 **実績締月: {split_info['latest_actual_month']}月**
        - 実績月: {split_info['actual_months']}
        - 予測月: {split_info['forecast_months']}
        """)
        
        # 主要指標（段階的ロード）
        with st.expander("📈 主要経営指標", expanded=True):
            analyzer = load_financial_analyzer(data_handler)  # キャッシュ済み
            
            # データ取得（キャッシュ済み）
            pl_data = data_handler.load_pl_data(period_id)
            bs_data = data_handler.load_bs_data(period_id)
            
            if not pl_data.empty:
                # 累計データを辞書に変換
                cumulative = data_handler.get_cumulative_actual_data(
                    period_id, 
                    split_info['latest_actual_month']
                )
                
                # 経営指標計算（キャッシュ済み）
                metrics = analyzer.calculate_all_metrics(
                    cumulative, 
                    {},  # BS
                    None,
                    period_id
                )
                
                # 表示
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("ROE", f"{metrics.get('ROE', 0):.2f}%")
                with col2:
                    st.metric("ROA", f"{metrics.get('ROA', 0):.2f}%")
                with col3:
                    st.metric("自己資本比率", f"{metrics.get('自己資本比率', 0):.1f}%")
                with col4:
                    st.metric("流動比率", f"{metrics.get('流動比率', 0):.1f}%")

elif st.session_state.page == 'profitability':
    st.title("💰 収益構造分析")
    
    period_id = st.session_state.get('selected_period_id')
    
    if period_id:
        analyzer = load_financial_analyzer(data_handler)
        
        # 収益性分析（キャッシュ済み）
        with st.spinner("分析中..."):
            result = analyzer.analyze_profitability(period_id)
        
        if result and 'monthly_data' in result:
            df = result['monthly_data']
            
            # グラフ表示（遅延インポート）
            vh = load_visualization_helpers()
            
            st.subheader("📈 損益分岐点分析")
            
            # Plotlyのインポートは使う直前に
            import plotly.graph_objects as go
            
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=df['month'], 
                y=df['sales'], 
                name='実際の売上',
                mode='lines+markers'
            ))
            fig.add_trace(go.Scatter(
                x=df['month'], 
                y=df['breakeven_sales'], 
                name='損益分岐点',
                mode='lines',
                line=dict(dash='dash')
            ))
            
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("⚠️ 実績データがありません")

elif st.session_state.page == 'metrics':
    st.title("📈 経営指標")
    
    period_id = st.session_state.get('selected_period_id')
    
    if period_id:
        analyzer = load_financial_analyzer(data_handler)
        
        # データ取得
        split_info = data_handler.get_actual_vs_forecast_split(period_id)
        cumulative = data_handler.get_cumulative_actual_data(
            period_id,
            split_info['latest_actual_month']
        )
        
        # 経営指標計算（キャッシュ済み）
        metrics = analyzer.calculate_all_metrics(
            cumulative,
            {},
            None,
            period_id
        )
        
        # 表示
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

elif st.session_state.page == 'working_capital':
    st.title("🔄 運転資本分析")
    
    period_id = st.session_state.get('selected_period_id')
    
    if period_id:
        analyzer = load_financial_analyzer(data_handler)
        
        # データ取得
        split_info = data_handler.get_actual_vs_forecast_split(period_id)
        cumulative = data_handler.get_cumulative_actual_data(
            period_id,
            split_info['latest_actual_month']
        )
        
        # 運転資本分析（キャッシュ済み）
        wc_metrics = analyzer.calculate_working_capital(
            cumulative,
            {},
            None,
            period_id
        )
        
        # 表示
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("CCC", f"{wc_metrics.get('CCC', 0):.0f}日")
        with col2:
            st.metric("売上債権回転期間", f"{wc_metrics.get('売上債権回転期間', 0):.0f}日")
        with col3:
            st.metric("棚卸資産回転期間", f"{wc_metrics.get('棚卸資産回転期間', 0):.0f}日")
        with col4:
            st.metric("仕入債務回転期間", f"{wc_metrics.get('仕入債務回転期間', 0):.0f}日")

elif st.session_state.page == 'forecast':
    st.title("🔮 予測値入力")
    
    # 予測入力画面を遅延ロード
    from forecast_input_page import show_forecast_input_page
    show_forecast_input_page(data_handler, data_handler)

else:
    st.info("該当ページは準備中です")

# ============ フッター ============

st.sidebar.markdown("---")
st.sidebar.caption("© 2026 Financial Forecast Simulator")

debug_log("✅ ページ表示完了\n")
debug_log("="*80 + "\n")
