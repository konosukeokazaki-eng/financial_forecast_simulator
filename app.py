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

# ページ設定
st.set_page_config(
    page_title="財務予測シミュレーター",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# カスタムCSS
st.markdown("""
<style>
    /* メインコンテナ */
    .main {
        padding: 0rem 1rem;
    }
    
    /* タイトル */
    h1 {
        color: #1f77b4;
        font-weight: 700;
        margin-bottom: 0.5rem;
    }
    
    h2 {
        color: #2c3e50;
        font-weight: 600;
        border-bottom: 2px solid #1f77b4;
        padding-bottom: 0.5rem;
        margin-top: 2rem;
    }
    
    h3 {
        color: #34495e;
        font-weight: 600;
    }
    
    /* サマリーカード */
    .summary-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1.5rem;
        border-radius: 10px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        color: white;
        margin-bottom: 1rem;
    }
    
    .summary-card-green {
        background: linear-gradient(135deg, #43e97b 0%, #38f9d7 100%);
        padding: 1.5rem;
        border-radius: 10px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        color: white;
        margin-bottom: 1rem;
    }
    
    .summary-card-orange {
        background: linear-gradient(135deg, #fa709a 0%, #fee140 100%);
        padding: 1.5rem;
        border-radius: 10px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        color: white;
        margin-bottom: 1rem;
    }
    
    .summary-card-blue {
        background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%);
        padding: 1.5rem;
        border-radius: 10px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        color: white;
        margin-bottom: 1rem;
    }
    
    .summary-card-red {
        background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
        padding: 1.5rem;
        border-radius: 10px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        color: white;
        margin-bottom: 1rem;
    }
    
    .card-title {
        font-size: 0.9rem;
        font-weight: 500;
        opacity: 0.9;
        margin-bottom: 0.3rem;
    }
    
    .card-value {
        font-size: 2rem;
        font-weight: 700;
        margin: 0;
    }
    
    .card-subtitle {
        font-size: 0.85rem;
        opacity: 0.85;
        margin-top: 0.3rem;
    }
    
    /* サイドバー */
    .css-1d391kg {
        background-color: #f8f9fa;
    }
    
    /* ボタン */
    .stButton>button {
        border-radius: 20px;
        font-weight: 600;
        transition: all 0.3s ease;
    }
    
    .stButton>button:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 8px rgba(0,0,0,0.2);
    }
    
    /* データフレーム */
    .dataframe {
        font-size: 0.9rem;
    }
    
    /* インフォボックス */
    .info-box {
        background-color: #024270;
        padding: 1rem;
        border-left: 4px solid #1f77b4;
        border-radius: 4px;
        margin: 1rem 0;
    }
    
    .warning-box {
        background-color: #ff8ca1;
        padding: 1rem;
        border-left: 4px solid #ff7f0e;
        border-radius: 4px;
        margin: 1rem 0;
    }
    
    .success-box {
        background-color: #d4edda;
        padding: 1rem;
        border-left: 4px solid #2ca02c;
        border-radius: 4px;
        margin: 1rem 0;
    }
    
    /* タブ */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
    }
    
    .stTabs [data-baseweb="tab"] {
        border-radius: 4px 4px 0 0;
        padding: 10px 20px;
        font-weight: 600;
    }
</style>
""", unsafe_allow_html=True)

# 初期化
if 'page' not in st.session_state:
    st.session_state.page = "着地予測ダッシュボード"
if 'authenticated' not in st.session_state:
    st.session_state.authenticated = False
if 'username' not in st.session_state:
    st.session_state.username = ""

# --------------------------------------------------------------------------------
# シンプルな認証機能
# --------------------------------------------------------------------------------
def check_password():
    """パスワードチェック関数"""
    def password_entered():
        """パスワードが入力されたときの処理"""
        if st.session_state["password"] == st.secrets.get("password", "admin123"):
            st.session_state.authenticated = True
            st.session_state.username = "admin"
            del st.session_state["password"]  # パスワードを削除
        else:
            st.session_state.authenticated = False

    if not st.session_state.authenticated:
        # ログイン画面
        st.markdown("""
        <div style='text-align: center; padding: 2rem;'>
            <h1 style='color: #1f77b4; font-size: 3rem; margin-bottom: 1rem;'>📊</h1>
            <h1 style='color: #2c3e50;'>財務予測シミュレーター</h1>
            <p style='color: #7f8c8d; font-size: 1.1rem;'>ログインして開始してください</p>
        </div>
        """, unsafe_allow_html=True)
        
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            st.text_input(
                "パスワード",
                type="password",
                key="password",
                on_change=password_entered,
                placeholder="パスワードを入力してください"
            )
            
            if "password" in st.session_state:
                st.error("❌ パスワードが正しくありません")
        
        return False
    else:
        return True

# 認証チェック
if not check_password():
    st.stop()

# ログイン成功 - メインアプリケーション

# 初期化
if 'processor' not in st.session_state:
    st.session_state.processor = DataProcessor()
processor = st.session_state.processor

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
companies = processor.get_companies()
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
        for key in ['actuals_df', 'forecasts_df']:
            if key in st.session_state:
                del st.session_state[key]
    
    st.session_state.selected_comp_id = selected_comp_id
    st.session_state.selected_comp_name = selected_comp_name

    # 期選択
    periods = processor.get_company_periods(selected_comp_id)
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
                for key in ['actuals_df', 'forecasts_df']:
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
    
    # メニュー
    st.sidebar.markdown("---")
    st.sidebar.markdown("### 📋 メニュー")
    
    menu_options = [
        "着地予測ダッシュボード",
        "月次推移詳細",
        "予測データ入力",
        "実績データ入力",
        "データインポート",
        "シナリオ一括設定",
        "システム設定"
    ]
    
    st.session_state.page = st.sidebar.radio(
        "画面を選択",
        menu_options,
        label_visibility="collapsed"
    )

# --------------------------------------------------------------------------------
# 共通関数
# --------------------------------------------------------------------------------
def format_currency(val):
    """通貨フォーマット"""
    if val >= 0:
        return f"¥{val:,.0f}"
    else:
        return f"△¥{abs(val):,.0f}"

def color_negative_red(val):
    """負の値を赤色にする"""
    color = 'red' if val < 0 else 'black'
    return f'color: {color}'

# --------------------------------------------------------------------------------
# メインコンテンツ
# --------------------------------------------------------------------------------
if selected_period_id:
    # データの読み込み
    if 'actuals_df' not in st.session_state:
        st.session_state.actuals_df = processor.load_actual_data(selected_period_id)
    if 'forecasts_df' not in st.session_state:
        st.session_state.forecasts_df = processor.load_forecast_data(selected_period_id, st.session_state.scenario)
    
    actuals_df = st.session_state.actuals_df
    forecasts_df = st.session_state.forecasts_df
    
    # 会計期間の月リスト
    months = processor.get_fiscal_months(selected_period_id)
    
    # 実績と予測の切り替わり月を特定（実績がある最後の月）
    # 実績データがある月を特定
    actual_months_with_data = []
    for m in months:
        if m in actuals_df.columns and actuals_df[m].sum() != 0:
            actual_months_with_data.append(m)
    
    if actual_months_with_data:
        last_actual_month = actual_months_with_data[-1]
        split_index = months.index(last_actual_month) + 1
    else:
        split_index = 0
    
    # PL計算
    pl_df = processor.calculate_pl(actuals_df, forecasts_df, split_index, months)
    
    # ページごとの表示
    if st.session_state.page == "着地予測ダッシュボード":
        st.title("📊 着地予測ダッシュボード")
        
        # サマリー指標
        total_row = pl_df[pl_df['項目名'] == "当期純損益金額"]
        sales_row = pl_df[pl_df['項目名'] == "売上高"]
        op_row = pl_df[pl_df['項目名'] == "営業損益金額"]
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            val = sales_row['合計'].iloc[0]
            st.markdown(f"""
            <div class="summary-card-blue">
                <p class="card-title">通期売上高予測</p>
                <p class="card-value">{format_currency(val)}</p>
                <p class="card-subtitle">実績: {format_currency(sales_row['実績合計'].iloc[0])} / 予測: {format_currency(sales_row['予測合計'].iloc[0])}</p>
            </div>
            """, unsafe_allow_html=True)
            
        with col2:
            val = op_row['合計'].iloc[0]
            st.markdown(f"""
            <div class="summary-card-green">
                <p class="card-title">通期営業利益予測</p>
                <p class="card-value">{format_currency(val)}</p>
                <p class="card-subtitle">利益率: {(val/sales_row['合計'].iloc[0]*100 if sales_row['合計'].iloc[0] != 0 else 0):.1f}%</p>
            </div>
            """, unsafe_allow_html=True)
            
        with col3:
            val = total_row['合計'].iloc[0]
            st.markdown(f"""
            <div class="summary-card">
                <p class="card-title">通期純利益予測</p>
                <p class="card-value">{format_currency(val)}</p>
                <p class="card-subtitle">前月比: -</p>
            </div>
            """, unsafe_allow_html=True)
            
        with col4:
            # 進捗率（月数ベース）
            progress = (split_index / len(months)) * 100
            st.markdown(f"""
            <div class="summary-card-orange">
                <p class="card-title">会計期間進捗</p>
                <p class="card-value">{progress:.0f}%</p>
                <p class="card-subtitle">{split_index}ヶ月経過 / 残り{len(months)-split_index}ヶ月</p>
            </div>
            """, unsafe_allow_html=True)
            
        # グラフ表示
        st.markdown("### 📈 月次推移グラフ")
        
        # グラフ用データの準備
        plot_df = pl_df[pl_df['タイプ'] == "要約"].copy()
        plot_data = []
        for m in months:
            for _, row in plot_df.iterrows():
                plot_data.append({
                    '月': m,
                    '項目': row['項目名'],
                    '金額': row[m],
                    '種別': '実績' if months.index(m) < split_index else '予測'
                })
        plot_df_long = pd.DataFrame(plot_data)
        
        fig = px.bar(
            plot_df_long[plot_df_long['項目'] == "売上高"],
            x='月', y='金額', color='種別',
            title="売上高推移",
            color_discrete_map={'実績': '#1f77b4', '予測': '#aec7e8'}
        )
        
        # 利益ラインを追加
        profit_data = plot_df_long[plot_df_long['項目'] == "営業損益金額"]
        fig.add_trace(go.Scatter(
            x=profit_data['月'], y=profit_data['金額'],
            name='営業利益', line=dict(color='#ff7f0e', width=3)
        ))
        
        st.plotly_chart(fig, use_container_width=True)
        
        # 簡易PL表示
        st.markdown("### 📋 損益計算書サマリー")
        summary_pl = pl_df[pl_df['タイプ'] == "要約"][['項目名', '実績合計', '予測合計', '合計']]
        st.dataframe(
            summary_pl.style.format({
                '実績合計': format_currency,
                '予測合計': format_currency,
                '合計': format_currency
            }),
            use_container_width=True
        )

    elif st.session_state.page == "月次推移詳細":
        st.title("📅 月次推移詳細")
        
        # フィルター
        show_type = st.radio("表示項目", ["すべて", "要約のみ", "詳細のみ"], horizontal=True)
        
        display_df = pl_df.copy()
        if show_type == "要約のみ":
            display_df = display_df[display_df['タイプ'] == "要約"]
        elif show_type == "詳細のみ":
            display_df = display_df[display_df['タイプ'] == "詳細"]
            
        # 表示列の選択
        cols = ['項目名'] + months + ['合計']
        
        # スタイル適用
        st.dataframe(
            display_df[cols].style.format({m: format_currency for m in months + ['合計']})
            .applymap(color_negative_red, subset=months + ['合計']),
            use_container_width=True,
            height=600
        )
        
        # CSVダウンロード
        csv = display_df[cols].to_csv(index=False).encode('utf_8_sig')
        st.download_button(
            "📥 CSVとしてダウンロード",
            csv,
            f"financial_report_{selected_comp_name}_{selected_period_num}.csv",
            "text/csv",
            key='download-csv'
        )

    elif st.session_state.page == "予測データ入力":
        st.title("🔮 予測データ入力")
        
        tab1, tab2 = st.tabs(["主要項目入力", "補助科目入力"])
        
        with tab1:
            st.subheader(f"シナリオ: {st.session_state.scenario}")
            
            st.markdown("""
            <div class="info-box">
                <strong>💡 使い方:</strong> 各項目の月次予測値を入力してください。
                補助科目が設定されている項目は、補助科目の合計が自動的に反映されます。
            </div>
            """, unsafe_allow_html=True)
            
            # 編集可能な項目
            editable_items = [item for item in processor.all_items if item not in processor.calculated_items]
            
            selected_item = st.selectbox("編集する項目", editable_items)
            
            st.markdown(f"### {selected_item} の予測値入力")
            
            # 現在の値を表示
            current_values = forecasts_df[forecasts_df['項目名'] == selected_item]
            
            # 入力フォーム
            with st.form(f"forecast_form_{selected_item}"):
                cols = st.columns(4)
                new_values = {}
                
                col_count = 4
                for i, month in enumerate(months):
                    col_idx = i % col_count
                    with cols[col_idx]:
                        current_val = 0
                        if not current_values.empty and month in current_values.columns:
                            current_val = current_values[month].iloc[0]
                        
                        new_val = st.number_input(
                            f"{month}",
                            value=float(current_val),
                            step=10000.0,
                            format="%.0f",
                            key=f"forecast_{selected_item}_{month}"
                        )
                        new_values[month] = new_val
                
                if st.form_submit_button("💾 保存", type="primary"):
                    success = processor.save_forecast_item(
                        st.session_state.selected_period_id,
                        st.session_state.scenario,
                        selected_item,
                        new_values
                    )
                    if success:
                        st.success("✅ 保存しました")
                        st.rerun()
                    else:
                        st.error("❌ 保存に失敗しました")
            
        with tab2:
            st.subheader("補助科目入力")
            
            st.markdown("""
            <div class="info-box">
                <strong>💡 使い方:</strong> 販売管理費の各項目について、詳細な内訳(補助科目)を入力できます。
            </div>
            """, unsafe_allow_html=True)
            
            parent_item = st.selectbox("親項目を選択", processor.ga_items)
            
            # 既存の補助科目を取得
            existing_subs = processor.get_sub_accounts_for_parent(
                st.session_state.selected_period_id,
                st.session_state.scenario,
                parent_item
            )
            
            # 補助科目追加
            st.markdown("#### 新規補助科目追加")
            new_sub_name = st.text_input("補助科目名", key="new_sub_name")
            
            if new_sub_name:
                st.markdown(f"**{new_sub_name}** の月次入力")
                
                cols = st.columns(4)
                sub_values = {}
                
                for i, month in enumerate(months):
                    with cols[i % 4]:
                        val = st.number_input(
                            f"{month}",
                            value=0.0,
                            step=1000.0,
                            format="%.0f",
                            key=f"sub_{parent_item}_{new_sub_name}_{month}"
                        )
                        sub_values[month] = val
                
                if st.button("💾 補助科目を追加", type="primary"):
                    success = processor.save_sub_account(
                        st.session_state.selected_period_id,
                        st.session_state.scenario,
                        parent_item,
                        new_sub_name,
                        sub_values
                    )
                    if success:
                        st.success("✅ 追加しました")
                        st.rerun()
                    else:
                        st.error("❌ 追加に失敗しました")
            
            # 既存補助科目の表示・編集
            if not existing_subs.empty:
                st.markdown("#### 既存補助科目")
                
                for sub_name in existing_subs['sub_account_name'].unique():
                    with st.expander(f"📌 {sub_name}"):
                        sub_data = existing_subs[existing_subs['sub_account_name'] == sub_name]
                        
                        # 月次データ表示
                        display_data = {}
                        for month in months:
                            matching = sub_data[sub_data['month'] == month]
                            if not matching.empty:
                                display_data[month] = matching['amount'].iloc[0]
                            else:
                                display_data[month] = 0
                        
                        df_display = pd.DataFrame([display_data])
                        st.dataframe(
                            df_display.style.format(format_currency),
                            use_container_width=True
                        )
                        
                        if st.button(f"🗑️ {sub_name}を削除", key=f"del_{sub_name}"):
                            processor.delete_sub_account(
                                st.session_state.selected_period_id,
                                st.session_state.scenario,
                                parent_item,
                                sub_name
                            )
                            st.success("削除しました")
                            st.rerun()
    
    elif st.session_state.page == "実績データ入力":
        st.title("⌨️ 実績データ入力")
        
        st.markdown("""
        <div class="info-box">
            <strong>💡 使い方:</strong> 月次の実績データを入力してください。
        </div>
        """, unsafe_allow_html=True)
        
        # 編集可能な項目
        editable_items = [item for item in processor.all_items if item not in processor.calculated_items]
        
        selected_item = st.selectbox("編集する項目", editable_items)
        
        st.markdown(f"### {selected_item} の実績値入力")
        
        cols = st.columns(4)
        new_values = {}
        current_values = actuals_df[actuals_df['項目名'] == selected_item]
        
        for i, month in enumerate(months):
            with cols[i % 4]:
                current_val = 0
                if not current_values.empty and month in current_values.columns:
                    current_val = current_values[month].iloc[0]
                
                new_val = st.number_input(
                    f"{month}",
                    value=float(current_val),
                    step=10000.0,
                    format="%.0f",
                    key=f"actual_{selected_item}_{month}"
                )
                new_values[month] = new_val
        
        if st.button("💾 保存", type="primary"):
            success = processor.save_actual_item(
                st.session_state.selected_period_id,
                selected_item,
                new_values
            )
            if success:
                st.success("✅ 保存しました")
                st.rerun()
            else:
                st.error("❌ 保存に失敗しました")
    
    elif st.session_state.page == "データインポート":
        st.title("📥 データインポート")
        
        st.markdown("""
        <div class="info-box">
            <strong>💡 使い方:</strong> 弥生会計からエクスポートしたExcelファイルをアップロードしてください。
        </div>
        """, unsafe_allow_html=True)
        
        uploaded_file = st.file_uploader(
            "Excel fileを選択",
            type=['xlsx', 'xls'],
            help="弥生会計の月次推移表をアップロードしてください"
        )
        
        if 'show_import_button' not in st.session_state:
            st.session_state.show_import_button = False
        
        if uploaded_file:
            with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp_file:
                tmp_file.write(uploaded_file.read())
                temp_path = tmp_file.name
                st.session_state.temp_path_to_delete = temp_path
                
            st.success(f"✅ ファイル **{uploaded_file.name}** を読み込みました")
            
            if 'imported_df' not in st.session_state:
                # fiscal_period_idを渡す
                st.session_state.imported_df, info = processor.import_yayoi_excel(
                    temp_path, 
                    st.session_state.selected_period_id,
                    preview_only=True
                )
                st.session_state.show_import_button = True
                
            if st.session_state.show_import_button:
                st.subheader("📋 インポートデータ プレビュー（直接編集可能）")
                
                st.markdown("""
                <div class="info-box">
                    <strong>✏️ 編集:</strong> セルをダブルクリックして値を直接修正できます。
                </div>
                """, unsafe_allow_html=True)
                
                # 編集可能なデータエディタを使用
                edited_df = st.data_editor(
                    st.session_state.imported_df,
                    use_container_width=True,
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
                
                if st.button("✅ 上記内容でインポートを実行", type="primary"):
                    success, info = processor.save_extracted_data(
                        st.session_state.selected_period_id,
                        st.session_state.imported_df
                    )
                    if success:
                        st.success("✅ インポートが完了しました！")
                        del st.session_state.imported_df
                        del st.session_state.show_import_button
                        
                        if 'temp_path_to_delete' in st.session_state:
                            os.unlink(st.session_state.temp_path_to_delete)
                            del st.session_state.temp_path_to_delete
                            
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
            売上高を増加させ、費用を減少させるシナリオです。
            """)
            optimistic_rate = st.slider("売上増加率 (%)", 0, 50, 10, key="opt_rate")
            
        with col2:
            st.markdown("### 📉 悲観シナリオ")
            st.markdown("""
            売上高を減少させ、費用を増加させるシナリオです。
            """)
            pessimistic_rate = st.slider("売上減少率 (%)", 0, 50, 10, key="pess_rate")
            
        if st.button("🚀 シナリオを生成して保存", type="primary"):
            # 現実シナリオをベースに生成
            base_forecast = processor.load_forecast_data(selected_period_id, "現実")
            
            # 楽観シナリオ
            opt_df = base_forecast.copy()
            for m in months:
                # 売上は増加
                opt_df.loc[opt_df['項目名'] == "売上高", m] *= (1 + optimistic_rate/100)
            
            processor.save_extracted_data(selected_period_id, opt_df) # TODO: scenario引数が必要
            
            st.success("✅ シナリオを生成しました（※実装中）")

    elif st.session_state.page == "システム設定":
        st.title("⚙️ システム設定")
        
        tab1, tab2 = st.tabs(["会社管理", "会計期管理"])
        
        with tab1:
            st.subheader("会社登録")
            new_comp_name = st.text_input("新しい会社名")
            if st.button("登録", key="add_comp"):
                if new_comp_name:
                    success, msg = processor.add_company(new_comp_name)
                    if success:
                        st.success(msg)
                        st.rerun()
                    else:
                        st.error(msg)
            
            st.subheader("登録済み会社一覧")
            st.dataframe(companies, use_container_width=True)
            
        with tab2:
            st.subheader("会計期登録")
            if not companies.empty:
                target_comp = st.selectbox("会社を選択", companies['name'].tolist(), key="setup_comp")
                target_comp_id = companies[companies['name'] == target_comp]['id'].iloc[0]
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    p_num = st.number_input("期数", min_value=1, value=1)
                with col2:
                    s_date = st.date_input("開始日")
                with col3:
                    e_date = st.date_input("終了日")
                    
                if st.button("会計期を登録"):
                    success, msg = processor.add_fiscal_period(
                        int(target_comp_id), p_num, s_date.strftime('%Y-%m-%d'), e_date.strftime('%Y-%m-%d')
                    )
                    if success:
                        st.success(msg)
                        st.rerun()
                    else:
                        st.error(msg)
                
                st.subheader("登録済み会計期")
                st.dataframe(processor.get_company_periods(target_comp_id), use_container_width=True)
            else:
                st.warning("先に会社を登録してください")

else:
    st.title("👋 ようこそ")
    st.info("左側のサイドバーから会社と会計期を選択してください。")
    
    if st.session_state.page == "システム設定":
        # システム設定ページは会社未選択でも表示
        st.title("⚙️ システム設定")
        # (上記と同じ設定UIを表示...)
