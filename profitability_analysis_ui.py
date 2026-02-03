"""
収益構造分析画面
"""

import streamlit as st
import plotly.graph_objects as go
from profitability_analyzer import analyze_profitability_from_db
from cfo_advisor import CFOAdvisor
import sys


def show_profitability_analysis_page(processor):
    st.title("📊 収益構造分析")
    
    sys.stderr.write("\n" + "="*80 + "\n")
    sys.stderr.write("🖥️ show_profitability_analysis_page開始\n")
    sys.stderr.flush()
    
    # デバッグ: session_stateの内容を確認
    sys.stderr.write(f"   session_state keys: {list(st.session_state.keys())}\n")
    sys.stderr.flush()
    
    if 'selected_period_id' not in st.session_state:
        sys.stderr.write("❌ selected_period_id が session_state にありません\n")
        sys.stderr.flush()
        st.warning("⚠️ 会計期間を選択してください")
        st.info("サイドバーから会計期間を選択してください。")
        return
    
    period_id = st.session_state.selected_period_id
    sys.stderr.write(f"   選択された期間ID: {period_id}\n")
    sys.stderr.flush()
    
    with st.spinner("分析中..."):
        cost_structure = analyze_profitability_from_db(period_id, processor)
    
    sys.stderr.write(f"   cost_structure keys: {list(cost_structure.keys()) if cost_structure else 'None'}\n")
    sys.stderr.flush()
    
    if not cost_structure:
        sys.stderr.write("❌ cost_structureが空\n")
        sys.stderr.flush()
        st.error("❌ データ取得に失敗しました")
        st.info("📊 実績データをインポートしてください。")
        st.markdown("**手順:**")
        st.markdown("1. サイドバーから「データ取込」を選択")
        st.markdown("2. BS・PLファイルをアップロード")
        st.markdown("3. インポートを実行")
        return
    
    if 'monthly_data' not in cost_structure:
        sys.stderr.write("❌ monthly_dataが cost_structure にありません\n")
        sys.stderr.flush()
        st.error("❌ データ構造エラー")
        st.info("📊 実績データをインポートしてください。")
        return
    
    df = cost_structure['monthly_data']
    
    sys.stderr.write(f"   monthly_data shape: {df.shape if not df.empty else 'empty'}\n")
    sys.stderr.flush()
    
    if df.empty:
        sys.stderr.write("❌ monthly_dataが空\n")
        sys.stderr.flush()
        st.warning("⚠️ 分析可能なデータがありません")
        st.info("売上、売上原価、販管費のデータが必要です。")
        return
    
    sys.stderr.write("✅ データ取得成功 - 画面表示開始\n")
    sys.stderr.write("="*80 + "\n\n")
    sys.stderr.flush()
    
    latest = df.iloc[-1]
    
    st.subheader("📈 主要指標")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        marginal_rate = latest.get('marginal_profit_rate', 0)
        delta_val = (marginal_rate - 0.35) * 100 if marginal_rate > 0 else None
        st.metric("限界利益率", f"{marginal_rate*100:.1f}%", delta=f"{delta_val:.1f}%" if delta_val is not None else None)
    
    with col2:
        breakeven = latest.get('breakeven_sales', 0)
        st.metric("損益分岐点売上高", f"¥{breakeven/10000:.1f}万")
    
    with col3:
        safety_rate = latest.get('safety_rate', 0)
        delta_val = (safety_rate - 0.20) * 100
        st.metric("安全余裕率", f"{safety_rate*100:.1f}%", delta=f"{delta_val:.1f}%")
    
    with col4:
        fixed_costs = latest.get('fixed_costs', 0)
        st.metric("月間固定費", f"¥{fixed_costs/10000:.1f}万")
    
    st.subheader("📈 損益分岐点分析")
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df['month'], y=df['sales'], name='実際の売上', mode='lines+markers', line=dict(color='#1f77b4', width=3), marker=dict(size=8)))
    fig.add_trace(go.Scatter(x=df['month'], y=df['breakeven_sales'], name='損益分岐点', mode='lines', line=dict(color='#ff7f0e', width=2, dash='dash')))
    fig.update_layout(title='売上高と損益分岐点の推移', xaxis_title='月', yaxis_title='金額（円）', hovermode='x unified', height=400)
    st.plotly_chart(fig, use_container_width=True)
    
    st.subheader("📊 限界利益率の推移")
    fig2 = go.Figure()
    fig2.add_trace(go.Scatter(x=df['month'], y=df['marginal_profit_rate'] * 100, name='限界利益率', mode='lines+markers', line=dict(color='#2ecc71', width=3), marker=dict(size=8), fill='tozeroy', fillcolor='rgba(46, 204, 113, 0.1)'))
    fig2.add_hline(y=35, line_dash="dash", line_color="gray", annotation_text="業界平均35%")
    fig2.update_layout(title='限界利益率の推移', xaxis_title='月', yaxis_title='限界利益率（%）', hovermode='x unified', height=350)
    st.plotly_chart(fig2, use_container_width=True)
    
    st.subheader("💬 分析コメント")
    trend = cost_structure.get('trend', 'unknown')
    avg_rate = cost_structure.get('average_marginal_profit_rate', 0)
    
    if trend == 'improving':
        st.success(f"✅ 限界利益率は改善傾向です（平均{avg_rate*100:.1f}%）。")
    elif trend == 'deteriorating':
        st.warning(f"⚠️ 限界利益率は悪化傾向です（平均{avg_rate*100:.1f}%）。")
    else:
        st.info(f"📊 限界利益率は安定しています（平均{avg_rate*100:.1f}%）。")
    
    if avg_rate < 0.30:
        st.error("🔴 限界利益率が30%を下回っています。")
    elif avg_rate < 0.35:
        st.warning("🟡 限界利益率は業界平均（35%）を下回っています。")
    else:
        st.success("🟢 限界利益率は業界平均以上です。")
    
    st.subheader("💡 CFOからのアドバイス")
    advisor = CFOAdvisor()
    
    metrics = {
        'sales': latest.get('sales', 0),
        'cogs': latest.get('cogs', 0),
        'marginal_profit_rate': latest.get('marginal_profit_rate', 0),
        'breakeven_sales': latest.get('breakeven_sales', 0),
        'safety_rate': latest.get('safety_rate', 0),
        'fixed_costs': latest.get('fixed_costs', 0),
        'operating_profit': latest.get('operating_profit', 0)
    }
    
    if 'cash_runway_months' in st.session_state:
        metrics['cash_runway_months'] = st.session_state.cash_runway_months
    
    messages = advisor.generate_advisory_messages(metrics)
    
    if not messages:
        st.info("✅ 現在、特に注意が必要な事項はありません。")
    else:
        for msg in messages:
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
    
    with st.expander("📋 詳細データを表示"):
        display_cols = ['month', 'sales', 'cogs', 'marginal_profit', 'marginal_profit_rate', 'breakeven_sales', 'safety_rate', 'operating_profit']
        available_cols = [col for col in display_cols if col in df.columns]
        st.dataframe(df[available_cols], use_container_width=True)


def _display_actions(actions: list):
    if actions:
        st.markdown("**推奨アクション:**")
        for action in actions:
            badge = "🔴" if action['impact'] == 'high' else "🟡" if action['impact'] == 'medium' else "🟢"
            st.markdown(f"{badge} **{action['title']}**")
            st.caption(f"   {action['detail']}")
            if 'difficulty' in action and 'timeframe' in action:
                st.caption(f"   実施難易度: {action['difficulty']} | 期間: {action['timeframe']}")
