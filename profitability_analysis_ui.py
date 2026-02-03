"""
収益構造分析画面
app.pyに統合するためのコード
"""

import streamlit as st
import plotly.graph_objects as go
from profitability_analyzer import ProfitabilityAnalyzer, analyze_profitability_from_db
from cfo_advisor import CFOAdvisor


def show_profitability_analysis_page(processor):
    """収益構造分析画面"""
    
    st.title("📊 収益構造分析")
    
    # 会計期間選択チェック
    if 'selected_period_id' not in st.session_state:
        st.warning("⚠️ 会計期間を選択してください")
        return
    
    period_id = st.session_state.selected_period_id
    
    # データベースから分析
    with st.spinner("分析中..."):
        cost_structure = analyze_profitability_from_db(period_id, processor)
    
    if not cost_structure or 'monthly_data' not in cost_structure:
        st.info("📊 データがありません。実績データをインポートしてください。")
        return
    
    df = cost_structure['monthly_data']
    
    if df.empty:
        st.info("📊 分析可能なデータがありません。")
        return
    
    # 最新月のデータ
    latest = df.iloc[-1]
    
    # ==================
    # KPIカード
    # ==================
    st.subheader("📈 主要指標")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        marginal_rate = latest.get('marginal_profit_rate', 0)
        delta_val = (marginal_rate - 0.35) * 100 if marginal_rate > 0 else None
        st.metric(
            label="限界利益率",
            value=f"{marginal_rate*100:.1f}%",
            delta=f"{delta_val:.1f}%" if delta_val is not None else None,
            help="(売上 - 変動費) ÷ 売上\n業界平均: 35%"
        )
    
    with col2:
        breakeven = latest.get('breakeven_sales', 0)
        st.metric(
            label="損益分岐点売上高",
            value=f"¥{breakeven/10000:.1f}万",
            help="固定費 ÷ 限界利益率"
        )
    
    with col3:
        safety_rate = latest.get('safety_rate', 0)
        delta_val = (safety_rate - 0.20) * 100
        st.metric(
            label="安全余裕率",
            value=f"{safety_rate*100:.1f}%",
            delta=f"{delta_val:.1f}%",
            help="(売上 - 損益分岐点) ÷ 売上\n目標: 20%以上"
        )
    
    with col4:
        fixed_costs = latest.get('fixed_costs', 0)
        st.metric(
            label="月間固定費",
            value=f"¥{fixed_costs/10000:.1f}万",
            help="販管費（固定費）"
        )
    
    # ==================
    # 損益分岐点チャート
    # ==================
    st.subheader("📈 損益分岐点分析")
    
    fig = go.Figure()
    
    # 実際の売上
    fig.add_trace(go.Scatter(
        x=df['month'],
        y=df['sales'],
        name='実際の売上',
        mode='lines+markers',
        line=dict(color='#1f77b4', width=3),
        marker=dict(size=8)
    ))
    
    # 損益分岐点
    fig.add_trace(go.Scatter(
        x=df['month'],
        y=df['breakeven_sales'],
        name='損益分岐点',
        mode='lines',
        line=dict(color='#ff7f0e', width=2, dash='dash')
    ))
    
    fig.update_layout(
        title='売上高と損益分岐点の推移',
        xaxis_title='月',
        yaxis_title='金額（円）',
        hovermode='x unified',
        height=400
    )
    
    st.plotly_chart(fig, width="stretch")
    
    # ==================
    # 限界利益率の推移
    # ==================
    st.subheader("📊 限界利益率の推移")
    
    fig2 = go.Figure()
    
    fig2.add_trace(go.Scatter(
        x=df['month'],
        y=df['marginal_profit_rate'] * 100,
        name='限界利益率',
        mode='lines+markers',
        line=dict(color='#2ecc71', width=3),
        marker=dict(size=8),
        fill='tozeroy',
        fillcolor='rgba(46, 204, 113, 0.1)'
    ))
    
    # 業界平均線
    fig2.add_hline(
        y=35, 
        line_dash="dash", 
        line_color="gray",
        annotation_text="業界平均35%"
    )
    
    fig2.update_layout(
        title='限界利益率の推移',
        xaxis_title='月',
        yaxis_title='限界利益率（%）',
        hovermode='x unified',
        height=350
    )
    
    st.plotly_chart(fig2, width="stretch")
    
    # ==================
    # 分析コメント
    # ==================
    st.subheader("💬 分析コメント")
    
    trend = cost_structure.get('trend', 'unknown')
    avg_rate = cost_structure.get('average_marginal_profit_rate', 0)
    
    if trend == 'improving':
        st.success(f"✅ 限界利益率は改善傾向です（平均{avg_rate*100:.1f}%）。収益構造が改善しています。")
    elif trend == 'deteriorating':
        st.warning(f"⚠️ 限界利益率は悪化傾向です（平均{avg_rate*100:.1f}%）。原価管理の強化が必要です。")
    else:
        st.info(f"📊 限界利益率は安定しています（平均{avg_rate*100:.1f}%）。")
    
    # 業界平均との比較
    if avg_rate < 0.30:
        st.error("🔴 限界利益率が30%を下回っています。値上げまたは原価削減の検討が必要です。")
    elif avg_rate < 0.35:
        st.warning("🟡 限界利益率は業界平均（35%）を下回っています。改善の余地があります。")
    else:
        st.success("🟢 限界利益率は業界平均以上です。良好な収益構造です。")
    
    # ==================
    # CFOアドバイス
    # ==================
    st.subheader("💡 CFOからのアドバイス")
    
    # CFOアドバイザー初期化
    advisor = CFOAdvisor()
    
    # 最新月の指標を取得
    metrics = {
        'sales': latest.get('sales', 0),
        'cogs': latest.get('cogs', 0),
        'marginal_profit_rate': latest.get('marginal_profit_rate', 0),
        'breakeven_sales': latest.get('breakeven_sales', 0),
        'safety_rate': latest.get('safety_rate', 0),
        'fixed_costs': latest.get('fixed_costs', 0),
        'operating_profit': latest.get('operating_profit', 0)
    }
    
    # 資金耐久月数を追加（もし利用可能なら）
    if 'cash_runway_months' in st.session_state:
        metrics['cash_runway_months'] = st.session_state.cash_runway_months
    
    # アドバイス生成
    messages = advisor.generate_advisory_messages(metrics)
    
    if not messages:
        st.info("✅ 現在、特に注意が必要な事項はありません。")
    else:
        for msg in messages:
            # レベルに応じた表示
            if msg['level'] == 'critical':
                with st.error(msg['title']):
                    st.markdown(f"**{msg['icon']} {msg['message']}**")
                    _display_actions(msg.get('actions', []))
            elif msg['level'] == 'warning':
                with st.warning(msg['title']):
                    st.markdown(f"**{msg['icon']} {msg['message']}**")
                    _display_actions(msg.get('actions', []))
            elif msg['level'] == 'success':
                with st.success(msg['title']):
                    st.markdown(f"**{msg['icon']} {msg['message']}**")
                    _display_actions(msg.get('actions', []))
            else:
                with st.info(msg['title']):
                    st.markdown(f"**{msg['icon']} {msg['message']}**")
                    _display_actions(msg.get('actions', []))
    
    # ==================
    # 詳細データ
    # ==================
    with st.expander("📋 詳細データを表示"):
        display_cols = ['month', 'sales', 'cogs', 'marginal_profit', 'marginal_profit_rate', 
                       'breakeven_sales', 'safety_rate', 'operating_profit']
        # 存在する列のみ表示
        available_cols = [col for col in display_cols if col in df.columns]
        st.dataframe(
            df[available_cols],
            use_container_width=True
        )


def _display_actions(actions: list):
    """推奨アクションを表示"""
    if actions:
        st.markdown("**推奨アクション:**")
        for action in actions:
            # 影響度バッジ
            if action['impact'] == 'high':
                badge = "🔴"
            elif action['impact'] == 'medium':
                badge = "🟡"
            else:
                badge = "🟢"
            
            st.markdown(f"{badge} **{action['title']}**")
            st.caption(f"   {action['detail']}")
            if 'difficulty' in action and 'timeframe' in action:
                st.caption(f"   実施難易度: {action['difficulty']} | 期間: {action['timeframe']}")


# ========================================
# app.pyへの統合手順
# ========================================
"""
1. app.pyの先頭に以下をインポート:
   from profitability_analyzer import ProfitabilityAnalyzer, analyze_profitability_from_db
   from cfo_advisor import CFOAdvisor

2. サイドバーメニューに追加（分析レポートセクション内）:
   if st.sidebar.button("収益構造分析", width="stretch", key="nav_profitability"):
       st.session_state.page = "profitability_analysis"

3. ページ表示部分に追加:
   elif st.session_state.page == "profitability_analysis":
       from profitability_analysis_ui import show_profitability_analysis_page
       show_profitability_analysis_page(processor)
"""
