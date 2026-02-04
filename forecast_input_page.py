"""
予測直接入力画面
ユーザーが予測値を月別・科目別に直接入力できる
"""

import streamlit as st
import pandas as pd
from typing import Dict, List
import sys


def show_forecast_input_page(processor, period_manager):
    """
    予測直接入力画面を表示
    
    Args:
        processor: DataProcessorインスタンス
        period_manager: ActualPeriodManagerインスタンス
    """
    st.title("📝 予測値の直接入力")
    
    if 'selected_period_id' not in st.session_state:
        st.warning("⚠️ 会計期間を選択してください")
        return
    
    period_id = st.session_state.selected_period_id
    
    # 実績締月情報を取得
    split_info = period_manager.get_actual_vs_forecast_split(period_id)
    latest_actual = split_info['latest_actual_month']
    forecast_months = split_info['forecast_months']
    
    if not forecast_months:
        st.info("📊 すべての月の実績データが入力済みです。予測入力の必要はありません。")
        return
    
    st.info(f"📅 最新実績締月: **{latest_actual}月**  |  予測入力対象: **{min(forecast_months)}月～{max(forecast_months)}月**")
    
    # タブで入力方法を選択
    tab1, tab2, tab3 = st.tabs(["📊 月別入力", "📋 一括入力", "🔄 自動予測から調整"])
    
    with tab1:
        show_monthly_input(processor, period_id, forecast_months)
    
    with tab2:
        show_bulk_input(processor, period_id, forecast_months)
    
    with tab3:
        show_auto_forecast_adjustment(processor, period_id, forecast_months, latest_actual)


def show_monthly_input(processor, period_id: int, forecast_months: List[int]):
    """月別入力タブ"""
    st.subheader("📊 月別に予測値を入力")
    
    # 入力する月を選択
    selected_month = st.selectbox(
        "入力する月を選択",
        forecast_months,
        format_func=lambda x: f"{x}月"
    )
    
    # 主要科目のリスト
    main_items = [
        "売上高",
        "売上原価",
        "売上総損益金額",
        "販売費及び一般管理費",
        "営業損益金額",
        "営業外収益合計",
        "営業外費用合計",
        "経常損益金額",
        "特別利益合計",
        "特別損失合計",
        "税引前当期純利益",
        "法人税等合計",
        "当期純利益"
    ]
    
    # 既存の予測データを取得
    existing_forecast = get_existing_forecast(processor, period_id, selected_month)
    
    st.markdown(f"### {selected_month}月の予測値入力")
    
    # 入力フォーム
    with st.form(f"forecast_form_{selected_month}"):
        input_values = {}
        
        col1, col2 = st.columns(2)
        
        for idx, item in enumerate(main_items):
            current_value = existing_forecast.get(item, 0)
            
            with (col1 if idx % 2 == 0 else col2):
                input_values[item] = st.number_input(
                    item,
                    value=float(current_value),
                    step=10000.0,
                    format="%.0f",
                    key=f"input_{selected_month}_{item}"
                )
        
        submitted = st.form_submit_button("💾 この月の予測を保存", use_container_width=True)
        
        if submitted:
            # データベースに保存
            success = save_forecast_data(processor, period_id, selected_month, input_values)
            
            if success:
                st.success(f"✅ {selected_month}月の予測値を保存しました")
                st.rerun()
            else:
                st.error("❌ 保存に失敗しました")


def show_bulk_input(processor, period_id: int, forecast_months: List[int]):
    """一括入力タブ"""
    st.subheader("📋 複数月の予測値を一括入力")
    
    # Excelテンプレートのダウンロード
    if st.button("📥 入力テンプレートをダウンロード"):
        template_df = create_forecast_template(forecast_months)
        
        # CSVとしてダウンロード
        csv = template_df.to_csv(index=False, encoding='utf-8-sig')
        st.download_button(
            label="💾 テンプレートCSVをダウンロード",
            data=csv,
            file_name=f"forecast_template_{period_id}.csv",
            mime="text/csv"
        )
    
    st.markdown("---")
    
    # ファイルアップロード
    uploaded_file = st.file_uploader(
        "📂 入力済みファイルをアップロード",
        type=['csv', 'xlsx'],
        help="テンプレートに予測値を入力してアップロードしてください"
    )
    
    if uploaded_file is not None:
        try:
            # ファイル読み込み
            if uploaded_file.name.endswith('.csv'):
                df = pd.read_csv(uploaded_file, encoding='utf-8-sig')
            else:
                df = pd.read_excel(uploaded_file)
            
            st.write("📊 アップロードされたデータ:")
            st.dataframe(df, use_container_width=True)
            
            if st.button("💾 一括保存", type="primary", use_container_width=True):
                success = save_bulk_forecast(processor, period_id, df)
                
                if success:
                    st.success("✅ 予測値を一括保存しました")
                    st.rerun()
                else:
                    st.error("❌ 保存に失敗しました")
        
        except Exception as e:
            st.error(f"❌ ファイル読み込みエラー: {e}")


def show_auto_forecast_adjustment(processor, period_id: int, forecast_months: List[int], 
                                  latest_actual: int):
    """自動予測から調整タブ"""
    st.subheader("🔄 自動予測を生成して調整")
    
    st.markdown("""
    実績データから自動的に予測値を生成し、それをベースに調整できます。
    
    **予測方法:**
    - 前年同月比法
    - 直近3ヶ月平均
    - トレンド分析
    """)
    
    # 予測方法を選択
    forecast_method = st.radio(
        "予測方法を選択",
        ["前年同月比（成長率考慮）", "直近3ヶ月平均", "トレンド分析（線形回帰）"],
        horizontal=True
    )
    
    # 成長率の調整（前年同月比の場合）
    growth_rate = 0
    if forecast_method == "前年同月比（成長率考慮）":
        growth_rate = st.slider(
            "成長率（%）",
            min_value=-50,
            max_value=100,
            value=0,
            step=5,
            help="前年同月比に対する成長率を設定します"
        ) / 100
    
    if st.button("🔮 自動予測を生成", type="primary", use_container_width=True):
        with st.spinner("予測計算中..."):
            # 自動予測を生成
            forecast_data = generate_auto_forecast(
                processor, 
                period_id, 
                forecast_months,
                latest_actual,
                method=forecast_method,
                growth_rate=growth_rate
            )
            
            if forecast_data is not None and not forecast_data.empty:
                st.session_state['auto_forecast'] = forecast_data
                st.success("✅ 自動予測を生成しました")
            else:
                st.error("❌ 予測生成に失敗しました")
    
    # 生成された予測を表示・編集
    if 'auto_forecast' in st.session_state:
        st.markdown("### 📊 生成された予測値")
        
        edited_df = st.data_editor(
            st.session_state['auto_forecast'],
            use_container_width=True,
            num_rows="fixed",
            column_config={
                "科目": st.column_config.TextColumn("科目", disabled=True),
                **{f"{m}月": st.column_config.NumberColumn(f"{m}月", format="¥%.0f") 
                   for m in forecast_months}
            }
        )
        
        col1, col2 = st.columns([1, 1])
        
        with col1:
            if st.button("💾 この予測を保存", type="primary", use_container_width=True):
                success = save_forecast_from_dataframe(processor, period_id, edited_df, forecast_months)
                
                if success:
                    st.success("✅ 予測値を保存しました")
                    del st.session_state['auto_forecast']
                    st.rerun()
                else:
                    st.error("❌ 保存に失敗しました")
        
        with col2:
            if st.button("🔄 予測をクリア"):
                del st.session_state['auto_forecast']
                st.rerun()


# ==================== ヘルパー関数 ====================

def get_existing_forecast(processor, period_id: int, month: int) -> Dict:
    """既存の予測データを取得"""
    try:
        conn = processor._get_connection()
        
        query = """
            SELECT item_name, forecast_value
            FROM forecast_data
            WHERE fiscal_period_id = %s AND month = %s
        """
        
        df = pd.read_sql_query(query, conn, params=(period_id, month))
        
        if df.empty:
            return {}
        
        return dict(zip(df['item_name'], df['forecast_value']))
        
    except:
        return {}


def save_forecast_data(processor, period_id: int, month: int, values: Dict) -> bool:
    """予測データを保存"""
    try:
        conn = processor._get_connection()
        cursor = conn.cursor()
        
        for item_name, value in values.items():
            # UPSERT（存在すれば更新、なければ挿入）
            query = """
                INSERT INTO forecast_data (fiscal_period_id, item_name, month, forecast_value, created_at, updated_at)
                VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                ON CONFLICT (fiscal_period_id, item_name, month) 
                DO UPDATE SET 
                    forecast_value = EXCLUDED.forecast_value,
                    updated_at = CURRENT_TIMESTAMP
            """
            
            cursor.execute(query, (period_id, item_name, month, value))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return True
        
    except Exception as e:
        sys.stderr.write(f"❌ 予測データ保存エラー: {e}\n")
        import traceback
        traceback.print_exc(file=sys.stderr)
        sys.stderr.flush()
        return False


def create_forecast_template(forecast_months: List[int]) -> pd.DataFrame:
    """予測入力テンプレートを作成"""
    items = [
        "売上高", "売上原価", "売上総損益金額",
        "販売費及び一般管理費", "営業損益金額",
        "営業外収益合計", "営業外費用合計", "経常損益金額",
        "特別利益合計", "特別損失合計", 
        "税引前当期純利益", "法人税等合計", "当期純利益"
    ]
    
    data = {'科目': items}
    for month in forecast_months:
        data[f'{month}月'] = [0] * len(items)
    
    return pd.DataFrame(data)


def save_bulk_forecast(processor, period_id: int, df: pd.DataFrame) -> bool:
    """一括予測データを保存"""
    try:
        conn = processor._get_connection()
        cursor = conn.cursor()
        
        # 科目列を取得
        item_col = df.columns[0]
        items = df[item_col].tolist()
        
        # 月列（2列目以降）
        month_cols = df.columns[1:]
        
        for month_col in month_cols:
            # "3月" → 3 に変換
            month = int(month_col.replace('月', ''))
            
            for idx, item_name in enumerate(items):
                value = df.loc[idx, month_col]
                
                if pd.notna(value):
                    query = """
                        INSERT INTO forecast_data (fiscal_period_id, item_name, month, forecast_value, created_at, updated_at)
                        VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                        ON CONFLICT (fiscal_period_id, item_name, month) 
                        DO UPDATE SET 
                            forecast_value = EXCLUDED.forecast_value,
                            updated_at = CURRENT_TIMESTAMP
                    """
                    
                    cursor.execute(query, (period_id, item_name, month, float(value)))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return True
        
    except Exception as e:
        sys.stderr.write(f"❌ 一括保存エラー: {e}\n")
        import traceback
        traceback.print_exc(file=sys.stderr)
        sys.stderr.flush()
        return False


def generate_auto_forecast(processor, period_id: int, forecast_months: List[int],
                          latest_actual: int, method: str, growth_rate: float = 0) -> pd.DataFrame:
    """自動予測を生成"""
    try:
        from forecast_engine import ForecastEngine
        
        # 実績データを取得
        conn = processor._get_connection()
        query = """
            SELECT item_name, month, amount
            FROM actual_data
            WHERE fiscal_period_id = %s
            ORDER BY month, item_name
        """
        
        df_actuals = pd.read_sql_query(query, conn, params=(period_id,))
        
        if df_actuals.empty:
            return None
        
        # month列を数値に変換
        if df_actuals['month'].dtype == 'object':
            df_actuals['month_num'] = df_actuals['month'].apply(
                lambda x: int(str(x).split('-')[1]) if '-' in str(x) else int(x)
            )
        else:
            df_actuals['month_num'] = df_actuals['month']
        
        # 科目ごとに予測
        engine = ForecastEngine(processor)
        
        forecast_results = {}
        
        for item_name in df_actuals['item_name'].unique():
            item_data = df_actuals[df_actuals['item_name'] == item_name].sort_values('month_num')
            actuals_list = item_data['amount'].tolist()
            
            # 予測実行
            if method == "前年同月比（成長率考慮）":
                forecasts = engine.forecast_sales_yoy(actuals_list, len(forecast_months))
                # 成長率を適用
                forecasts = [f * (1 + growth_rate) for f in forecasts]
            elif method == "直近3ヶ月平均":
                forecasts = engine._moving_average_forecast(actuals_list, len(forecast_months))
            else:  # トレンド分析
                forecasts = engine.forecast_sales_yoy(actuals_list, len(forecast_months))
            
            forecast_results[item_name] = forecasts
        
        # DataFrameに変換
        data = {'科目': list(forecast_results.keys())}
        for idx, month in enumerate(forecast_months):
            data[f'{month}月'] = [forecast_results[item][idx] for item in forecast_results.keys()]
        
        return pd.DataFrame(data)
        
    except Exception as e:
        sys.stderr.write(f"❌ 自動予測生成エラー: {e}\n")
        import traceback
        traceback.print_exc(file=sys.stderr)
        sys.stderr.flush()
        return None


def save_forecast_from_dataframe(processor, period_id: int, df: pd.DataFrame, 
                                 forecast_months: List[int]) -> bool:
    """DataFrameから予測データを保存"""
    try:
        conn = processor._get_connection()
        cursor = conn.cursor()
        
        item_col = df.columns[0]
        items = df[item_col].tolist()
        
        for month in forecast_months:
            month_col = f'{month}月'
            
            if month_col in df.columns:
                for idx, item_name in enumerate(items):
                    value = df.loc[idx, month_col]
                    
                    if pd.notna(value):
                        query = """
                            INSERT INTO forecast_data (fiscal_period_id, item_name, month, forecast_value, created_at, updated_at)
                            VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                            ON CONFLICT (fiscal_period_id, item_name, month) 
                            DO UPDATE SET 
                                forecast_value = EXCLUDED.forecast_value,
                                updated_at = CURRENT_TIMESTAMP
                        """
                        
                        cursor.execute(query, (period_id, item_name, month, float(value)))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return True
        
    except Exception as e:
        sys.stderr.write(f"❌ DataFrame保存エラー: {e}\n")
        return False
