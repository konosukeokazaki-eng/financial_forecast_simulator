"""
3つの問題の修正実装コード
app.pyに追加・置き換えてください
"""

# ==================== 修正1: PLウォーターフォールの追加 ====================

def create_pl_waterfall(pl_data):
    """
    PLウォーターフォールチャートを作成
    
    Args:
        pl_data: DataFrame with columns ['科目', '金額']
    
    Returns:
        plotly figure
    """
    import plotly.graph_objects as go
    
    try:
        # データ抽出
        sales = float(pl_data.loc[pl_data['科目'] == '売上高', '金額'].values[0]) if '売上高' in pl_data['科目'].values else 0
        cogs = float(pl_data.loc[pl_data['科目'] == '売上原価', '金額'].values[0]) if '売上原価' in pl_data['科目'].values else 0
        sg_expense = float(pl_data.loc[pl_data['科目'] == '販売費及び一般管理費', '金額'].values[0]) if '販売費及び一般管理費' in pl_data['科目'].values else 0
        operating_profit = float(pl_data.loc[pl_data['科目'] == '営業損益金額', '金額'].values[0]) if '営業損益金額' in pl_data['科目'].values else 0
        
        # ウォーターフォールデータ
        x_labels = ['売上高', '売上原価', '売上総利益', '販管費', '営業利益']
        y_values = [sales, -cogs, sales - cogs, -sg_expense, operating_profit]
        measures = ['absolute', 'relative', 'total', 'relative', 'total']
        
        # グラフ作成
        fig = go.Figure(go.Waterfall(
            x=x_labels,
            y=y_values,
            measure=measures,
            text=[f"¥{abs(v):,.0f}" for v in y_values],
            textposition='outside',
            connector={"line": {"color": "rgb(63, 63, 63)"}},
            increasing={"marker": {"color": "#2ecc71"}},
            decreasing={"marker": {"color": "#e74c3c"}},
            totals={"marker": {"color": "#3498db"}}
        ))
        
        fig.update_layout(
            title="損益の流れ",
            showlegend=False,
            height=500,
            xaxis_title="",
            yaxis_title="金額（円）"
        )
        
        return fig
        
    except Exception as e:
        import sys
        sys.stderr.write(f"ウォーターフォール作成エラー: {e}\n")
        return None


# ==================== 修正2: BS Sankeyの追加 ====================

def create_bs_sankey(bs_data):
    """
    BS Sankey図を作成
    
    Args:
        bs_data: DataFrame with columns ['科目', '金額']
    
    Returns:
        plotly figure
    """
    import plotly.graph_objects as go
    
    try:
        # データ抽出
        current_assets = float(bs_data.loc[bs_data['科目'] == '流動資産', '金額'].values[0]) if '流動資産' in bs_data['科目'].values else 0
        fixed_assets = float(bs_data.loc[bs_data['科目'] == '固定資産', '金額'].values[0]) if '固定資産' in bs_data['科目'].values else 0
        current_liabilities = float(bs_data.loc[bs_data['科目'] == '流動負債', '金額'].values[0]) if '流動負債' in bs_data['科目'].values else 0
        fixed_liabilities = float(bs_data.loc[bs_data['科目'] == '固定負債', '金額'].values[0]) if '固定負債' in bs_data['科目'].values else 0
        equity = float(bs_data.loc[bs_data['科目'] == '純資産合計', '金額'].values[0]) if '純資産合計' in bs_data['科目'].values else 0
        
        # Sankeyデータ
        labels = ['調達', '流動資産', '固定資産', '流動負債', '固定負債', '純資産']
        
        sources = []
        targets = []
        values = []
        
        # 調達 → 資産
        if current_assets > 0:
            sources.append(0)
            targets.append(1)
            values.append(current_assets)
        
        if fixed_assets > 0:
            sources.append(0)
            targets.append(2)
            values.append(fixed_assets)
        
        # 負債・純資産 → 調達
        if current_liabilities > 0:
            sources.append(3)
            targets.append(0)
            values.append(current_liabilities)
        
        if fixed_liabilities > 0:
            sources.append(4)
            targets.append(0)
            values.append(fixed_liabilities)
        
        if equity > 0:
            sources.append(5)
            targets.append(0)
            values.append(equity)
        
        # グラフ作成
        fig = go.Figure(data=[go.Sankey(
            node=dict(
                pad=15,
                thickness=20,
                line=dict(color="black", width=0.5),
                label=labels,
                color=["#3498db", "#2ecc71", "#e67e22", "#e74c3c", "#9b59b6", "#1abc9c"]
            ),
            link=dict(
                source=sources,
                target=targets,
                value=values
            )
        )])
        
        fig.update_layout(
            title="貸借対照表の資金の流れ",
            height=600,
            font_size=12
        )
        
        return fig
        
    except Exception as e:
        import sys
        sys.stderr.write(f"Sankey作成エラー: {e}\n")
        return None


# ==================== 修正3: AI Forecastページの完全版 ====================

def show_ai_forecast_page_fixed():
    """AI Forecast画面（デバッグ機能付き）"""
    import streamlit as st
    
    st.title("🔮 AI自動予測")
    
    # デバッグ情報
    with st.expander("🔍 デバッグ情報"):
        st.write("**ADVANCED_FORECAST_AVAILABLE:**", globals().get('ADVANCED_FORECAST_AVAILABLE', False))
        st.write("**selected_period_id:**", st.session_state.get('selected_period_id', 'None'))
        st.write("**Session state keys:**", list(st.session_state.keys()))
        
        # ファイル確認
        import os
        st.write("**Current directory:**", os.getcwd())
        files = [f for f in os.listdir('.') if f.endswith('.py')]
        st.write("**Python files:**", files)
    
    # 機能が利用可能かチェック
    if not globals().get('ADVANCED_FORECAST_AVAILABLE', False):
        st.error("❌ AI予測機能が利用できません")
        st.info("""
        **必要なファイル:**
        - `advanced_forecast_engine.py`
        - `advanced_forecast_ui.py`
        
        これらのファイルをapp.pyと同じディレクトリに配置してください。
        """)
        return
    
    # 期間IDチェック
    if 'selected_period_id' not in st.session_state or st.session_state.selected_period_id is None:
        st.warning("⚠️ 会計期間が選択されていません")
        st.info("左サイドバーで会社と期を選択してください")
        return
    
    period_id = st.session_state.selected_period_id
    
    try:
        # DataHandlerAdapter作成
        if 'data_handler_adapter' not in st.session_state:
            from advanced_forecast_engine import get_advanced_forecast_engine
            
            class DataHandlerAdapter:
                def __init__(self, processor):
                    self._processor = processor
                    self._connection = None
                
                def _get_connection(self):
                    if self._connection is None:
                        if self._processor.use_postgres:
                            import psycopg2
                            self._connection = psycopg2.connect(self._processor.db_url)
                        else:
                            import sqlite3
                            self._connection = sqlite3.connect('financial_simulator.db')
                    return self._connection
                
                def get_actual_vs_forecast_split(self, period_id):
                    import pandas as pd
                    try:
                        conn = self._get_connection()
                        if self._processor.use_postgres:
                            query = "SELECT MAX(fiscal_month) as latest_actual FROM actuals WHERE fiscal_period_id = %s"
                            df = pd.read_sql_query(query, conn, params=(period_id,))
                        else:
                            query = "SELECT MAX(fiscal_month) as latest_actual FROM actuals WHERE fiscal_period_id = ?"
                            df = pd.read_sql_query(query, conn, params=(period_id,))
                        
                        latest_actual = df['latest_actual'].iloc[0] if not df.empty and not pd.isna(df['latest_actual'].iloc[0]) else 0
                        
                        if latest_actual and latest_actual > 0:
                            latest_actual = int(latest_actual)
                            actual_months = list(range(1, latest_actual + 1))
                            forecast_months = list(range(latest_actual + 1, 13))
                        else:
                            actual_months = []
                            forecast_months = list(range(1, 13))
                        
                        return {
                            'has_actual': latest_actual > 0,
                            'latest_actual_month': latest_actual,
                            'actual_months': actual_months,
                            'forecast_months': forecast_months
                        }
                    except Exception as e:
                        st.error(f"実績データ取得エラー: {e}")
                        return {
                            'has_actual': False,
                            'latest_actual_month': 0,
                            'actual_months': [],
                            'forecast_months': list(range(1, 13))
                        }
                
                def get_cumulative_actual_data(self, period_id, up_to_month):
                    try:
                        actuals_df = self._processor.load_actual_data(period_id)
                        if actuals_df is None or actuals_df.empty:
                            return {}
                        if 'fiscal_month' in actuals_df.columns:
                            actuals_df = actuals_df[actuals_df['fiscal_month'] <= up_to_month]
                        result = {}
                        for _, row in actuals_df.iterrows():
                            account = row.get('account_name', row.get('account', ''))
                            amount = row.get('amount', row.get('value', 0))
                            if account:
                                result[account] = amount
                        return result
                    except:
                        return {}
            
            st.session_state.data_handler_adapter = DataHandlerAdapter(st.session_state.processor)
        
        # AI予測エンジン初期化
        if 'advanced_engine' not in st.session_state:
            from advanced_forecast_engine import get_advanced_forecast_engine
            st.session_state.advanced_engine = get_advanced_forecast_engine(
                st.session_state.data_handler_adapter
            )
        
        # AI予測画面を表示
        from advanced_forecast_ui import show_advanced_forecast_page
        show_advanced_forecast_page(
            st.session_state.data_handler_adapter,
            st.session_state.advanced_engine
        )
        
    except Exception as e:
        st.error(f"❌ エラーが発生しました: {e}")
        
        with st.expander("詳細なエラー情報"):
            import traceback
            st.code(traceback.format_exc())


# ==================== app.pyへの適用方法 ====================

"""
【適用方法】

1. PLページにウォーターフォール追加：

if st.session_state.page == "損益計算書 (PL)":
    # ... 既存のコード ...
    
    # ウォーターフォール追加
    if not pl_df.empty:
        st.subheader("📊 ウォーターフォールチャート")
        fig = create_pl_waterfall(pl_df)
        if fig:
            st.plotly_chart(fig, use_container_width=True)


2. BSページにSankey追加：

if st.session_state.page == "貸借対照表 (BS)":
    # ... 既存のコード ...
    
    # Sankey追加
    if not bs_df.empty:
        st.subheader("📊 資金の流れ")
        fig = create_bs_sankey(bs_df)
        if fig:
            st.plotly_chart(fig, use_container_width=True)


3. AI Forecastページを置き換え：

elif st.session_state.page == "AI Forecast":
    show_ai_forecast_page_fixed()
"""
