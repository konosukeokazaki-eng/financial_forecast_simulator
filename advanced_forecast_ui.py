"""
高度な予測結果表示画面
自動予測の結果を可視化し、手動調整も可能にする
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from typing import Dict


def show_advanced_forecast_page(data_handler, advanced_engine):
    """
    高度な予測画面を表示
    
    Args:
        data_handler: DataHandlerインスタンス
        advanced_engine: AdvancedForecastEngineインスタンス
    """
    st.title("🔮 AI自動予測")
    
    if 'selected_period_id' not in st.session_state:
        st.warning("⚠️ 会計期間を選択してください")
        return
    
    period_id = st.session_state.selected_period_id
    
    # 実績締月情報
    split_info = data_handler.get_actual_vs_forecast_split(period_id)
    latest_actual = split_info['latest_actual_month']
    
    st.info(f"📅 実績締月: **{latest_actual}月**  |  実績データから自動予測を生成します")
    
    # サイドバー設定
    with st.sidebar:
        st.markdown("### ⚙️ 予測設定")
        
        forecast_months = st.slider(
            "予測期間（月）",
            min_value=1,
            max_value=24,
            value=12,
            help="何ヶ月先まで予測するか"
        )
        
        method = st.selectbox(
            "予測手法",
            ["auto", "average", "exponential", "linear_regression", "arima"],
            format_func=lambda x: {
                "auto": "🤖 自動選択（推奨）",
                "average": "📊 平均法",
                "exponential": "📈 指数平滑法",
                "linear_regression": "📉 線形回帰",
                "arima": "🔬 ARIMA（高度）"
            }.get(x, x)
        )
        
        generate_scenarios = st.checkbox(
            "複数シナリオを生成",
            value=True,
            help="楽観・標準・悲観の3シナリオを生成"
        )
    
    # 予測実行ボタン
    if st.button("🚀 予測を実行", type="primary", use_container_width=True):
        with st.spinner("AI予測を生成中..."):
            result = advanced_engine.generate_forecast(
                period_id,
                forecast_months=forecast_months,
                method=method,
                scenarios=generate_scenarios
            )
        
        if result:
            st.session_state['forecast_result'] = result
            st.success("✅ 予測を生成しました！")
        else:
            st.error("❌ 予測生成に失敗しました")
    
    # 予測結果の表示
    if 'forecast_result' in st.session_state:
        result = st.session_state['forecast_result']
        
        # タブで分類
        tab1, tab2, tab3, tab4 = st.tabs([
            "📊 予測結果",
            "📈 精度評価",
            "🔍 詳細分析",
            "💾 保存・調整"
        ])
        
        with tab1:
            show_forecast_results(result, latest_actual)
        
        with tab2:
            show_accuracy_evaluation(result)
        
        with tab3:
            show_detailed_analysis(result, latest_actual)
        
        with tab4:
            show_save_and_adjust(result, data_handler, period_id)


def show_forecast_results(result: Dict, latest_actual: int):
    """予測結果を表示"""
    st.subheader("📊 予測結果")
    
    forecasts = result['forecasts']
    scenarios = result.get('scenarios')
    
    # メイン予測値の表示
    st.markdown("### 標準シナリオ")
    
    # データエディタで表示（編集可能）
    edited_forecast = st.data_editor(
        forecasts,
        use_container_width=True,
        num_rows="fixed"
    )
    
    # 保存用にセッションに格納
    st.session_state['edited_forecast'] = edited_forecast
    
    # グラフ表示（売上高）
    if '売上高' in forecasts['科目'].values:
        st.markdown("### 📈 売上高予測グラフ")
        
        sales_row = forecasts[forecasts['科目'] == '売上高'].iloc[0]
        month_cols = [col for col in forecasts.columns if '月' in col]
        
        months = [int(col.replace('月', '')) for col in month_cols]
        values = [sales_row[col] for col in month_cols]
        
        # Plotlyグラフ
        fig = go.Figure()
        
        # 予測値
        fig.add_trace(go.Scatter(
            x=months,
            y=values,
            mode='lines+markers',
            name='予測',
            line=dict(color='blue', width=3),
            marker=dict(size=8)
        ))
        
        # 実績締月の縦線
        fig.add_vline(
            x=latest_actual,
            line_dash="dash",
            line_color="red",
            annotation_text="実績締月"
        )
        
        fig.update_layout(
            xaxis_title="月",
            yaxis_title="金額（円）",
            hovermode='x unified',
            height=400
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    # シナリオ別予測
    if scenarios:
        st.markdown("### 🎯 複数シナリオ比較")
        
        scenario_names = ['悲観', '標準', '楽観']
        colors = ['red', 'blue', 'green']
        
        fig = go.Figure()
        
        for scenario_name, color in zip(scenario_names, colors):
            if scenario_name in scenarios:
                scenario_df = scenarios[scenario_name]
                if '売上高' in scenario_df['科目'].values:
                    sales_row = scenario_df[scenario_df['科目'] == '売上高'].iloc[0]
                    month_cols = [col for col in scenario_df.columns if '月' in col]
                    
                    months = [int(col.replace('月', '')) for col in month_cols]
                    values = [sales_row[col] for col in month_cols]
                    
                    fig.add_trace(go.Scatter(
                        x=months,
                        y=values,
                        mode='lines',
                        name=scenario_name,
                        line=dict(color=color, width=2)
                    ))
        
        fig.add_vline(
            x=latest_actual,
            line_dash="dash",
            line_color="gray"
        )
        
        fig.update_layout(
            xaxis_title="月",
            yaxis_title="売上高（円）",
            hovermode='x unified',
            height=400
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # 変動性の表示
        volatility = scenarios.get('volatility', 0)
        st.info(f"📊 **変動性:** {volatility*100:.1f}% （過去実績の標準偏差）")


def show_accuracy_evaluation(result: Dict):
    """精度評価を表示"""
    st.subheader("📈 予測精度の評価")
    
    accuracy = result.get('accuracy', {})
    method = result.get('method', 'unknown')
    
    if not accuracy or accuracy.get('mape') is None:
        st.warning("⚠️ 精度評価にはデータが不足しています（最低6ヶ月の実績が必要）")
        return
    
    # メトリクス表示
    col1, col2, col3 = st.columns(3)
    
    with col1:
        mape = accuracy.get('mape', 0)
        st.metric(
            "MAPE（平均絶対パーセント誤差）",
            f"{mape:.1f}%",
            help="予測誤差の平均。低いほど高精度。"
        )
    
    with col2:
        mae = accuracy.get('mae', 0)
        st.metric(
            "MAE（平均絶対誤差）",
            f"¥{mae:,.0f}",
            help="予測値と実績値の差の平均"
        )
    
    with col3:
        rmse = accuracy.get('rmse', 0)
        st.metric(
            "RMSE（二乗平均平方根誤差）",
            f"¥{rmse:,.0f}",
            help="大きな誤差に敏感な指標"
        )
    
    # 解釈
    interpretation = accuracy.get('interpretation', '')
    if interpretation:
        if 'high' in interpretation.lower() or '高' in interpretation:
            st.success(f"✅ {interpretation}")
        elif 'low' in interpretation.lower() or '低' in interpretation:
            st.error(f"⚠️ {interpretation}")
        else:
            st.info(f"ℹ️ {interpretation}")
    
    # 精度の説明
    with st.expander("📚 精度指標の説明"):
        st.markdown("""
        **MAPE（Mean Absolute Percentage Error）:**
        - 10%未満: 非常に高精度
        - 10-20%: 高精度
        - 20-30%: 中程度の精度
        - 30%以上: 低精度（要注意）
        
        **MAE（Mean Absolute Error）:**
        - 予測値と実績値の差の平均値
        - 金額ベースでの誤差を示す
        
        **RMSE（Root Mean Squared Error）:**
        - 大きな誤差により敏感な指標
        - 外れ値の影響を受けやすい
        """)
    
    # 使用した手法
    st.markdown(f"**使用した予測手法:** {method}")


def show_detailed_analysis(result: Dict, latest_actual: int):
    """詳細分析を表示"""
    st.subheader("🔍 詳細分析")
    
    # トレンド分析
    trend_info = result.get('trend', {})
    if trend_info:
        st.markdown("### 📈 トレンド分析")
        
        trend = trend_info.get('trend', 'stable')
        interpretation = trend_info.get('interpretation', '')
        
        if trend == 'up':
            st.success(f"📈 {interpretation}")
        elif trend == 'down':
            st.error(f"📉 {interpretation}")
        else:
            st.info(f"➡️ {interpretation}")
    
    # 季節性分析
    seasonality_info = result.get('seasonality', {})
    if seasonality_info:
        st.markdown("### 🌊 季節性分析")
        
        has_seasonality = seasonality_info.get('has_seasonality', False)
        
        if has_seasonality:
            st.success("✅ 季節性パターンが検出されました")
            
            indices = seasonality_info.get('indices', {})
            if indices:
                # 季節指数のグラフ
                months = list(indices.keys())
                values = list(indices.values())
                
                fig = go.Figure()
                fig.add_trace(go.Bar(
                    x=months,
                    y=values,
                    marker_color=['red' if v < 0 else 'green' for v in values]
                ))
                
                fig.update_layout(
                    xaxis_title="月",
                    yaxis_title="季節指数（%）",
                    title="月別の季節変動パターン",
                    height=300
                )
                
                st.plotly_chart(fig, use_container_width=True)
                
                # ピーク月・低調月
                peak_month = seasonality_info.get('peak_month')
                low_month = seasonality_info.get('low_month')
                
                col1, col2 = st.columns(2)
                with col1:
                    st.info(f"📈 **ピーク月:** {peak_month}月")
                with col2:
                    st.info(f"📉 **低調月:** {low_month}月")
        else:
            st.info("ℹ️ 明確な季節性パターンは検出されませんでした")
    
    # メタデータ
    metadata = result.get('metadata', {})
    if metadata:
        with st.expander("ℹ️ 予測情報"):
            st.json(metadata)


def show_save_and_adjust(result: Dict, data_handler, period_id: int):
    """保存・調整画面"""
    st.subheader("💾 予測の保存と調整")
    
    # 編集された予測を取得
    edited_forecast = st.session_state.get('edited_forecast')
    
    if edited_forecast is None:
        edited_forecast = result['forecasts']
    
    st.markdown("### 📝 予測値の確認")
    st.info("上の「予測結果」タブで値を編集できます。編集後、ここで保存してください。")
    
    # 保存ボタン
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("💾 予測をデータベースに保存", type="primary", use_container_width=True):
            success = save_forecast_to_db(data_handler, period_id, edited_forecast)
            
            if success:
                st.success("✅ 予測値をデータベースに保存しました")
                # キャッシュクリア
                st.cache_data.clear()
            else:
                st.error("❌ 保存に失敗しました")
    
    with col2:
        if st.button("📥 CSV出力", use_container_width=True):
            csv = edited_forecast.to_csv(index=False, encoding='utf-8-sig')
            st.download_button(
                label="💾 CSVをダウンロード",
                data=csv,
                file_name=f"forecast_{period_id}.csv",
                mime="text/csv"
            )


def save_forecast_to_db(data_handler, period_id: int, forecast_df: pd.DataFrame) -> bool:
    """予測値をデータベースに保存"""
    try:
        conn = data_handler._get_connection()
        cursor = conn.cursor()

        if data_handler.use_postgres:
            query = """
                INSERT INTO forecast_data
                (fiscal_period_id, item_name, month, forecast_value, created_at, updated_at)
                VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                ON CONFLICT (fiscal_period_id, item_name, month)
                DO UPDATE SET
                    forecast_value = EXCLUDED.forecast_value,
                    updated_at = CURRENT_TIMESTAMP
            """
        else:
            query = """
                INSERT INTO forecast_data
                (fiscal_period_id, item_name, month, forecast_value, created_at, updated_at)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                ON CONFLICT (fiscal_period_id, item_name, month)
                DO UPDATE SET
                    forecast_value = excluded.forecast_value,
                    updated_at = CURRENT_TIMESTAMP
            """

        item_col = forecast_df.columns[0]
        items = forecast_df[item_col].tolist()

        month_cols = [col for col in forecast_df.columns if '月' in col]

        for month_col in month_cols:
            month = int(month_col.replace('月', ''))

            for idx, item_name in enumerate(items):
                value = forecast_df.loc[idx, month_col]

                if pd.notna(value):
                    cursor.execute(query, (period_id, item_name, month, float(value)))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return True
        
    except Exception as e:
        import sys
        sys.stderr.write(f"❌ 予測保存エラー: {e}\n")
        import traceback
        traceback.print_exc(file=sys.stderr)
        return False
