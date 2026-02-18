"""
予測直接入力画面（format指定完全削除版）
"""

import streamlit as st
import pandas as pd
from typing import Dict, List
import sys


def show_forecast_input_page(processor, period_manager):
    """予測直接入力画面を表示"""
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
        st.info("📊 すべての月の実績データが入力済みです。")
        return
    
    st.info(f"📅 最新実績締月: **{latest_actual}月**  |  予測入力対象: **{min(forecast_months)}月～{max(forecast_months)}月**")
    
    # タブで入力方法を選択
    tab1, tab2 = st.tabs(["📊 月別入力", "📋 一括入力"])
    
    with tab1:
        show_monthly_input(processor, period_id, forecast_months)
    
    with tab2:
        show_bulk_input(processor, period_id, forecast_months)


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
        "販売費及び一般管理費",
        "営業損益金額",
        "経常損益金額",
        "当期純利益"
    ]
    
    # 既存の予測データを取得
    existing_forecast = get_existing_forecast(processor, period_id, selected_month)
    
    st.markdown(f"### {selected_month}月の予測値入力")
    
    # 入力フォーム
    with st.form(f"forecast_form_{selected_month}"):
        input_values = {}
        
        for item in main_items:
            current_value = existing_forecast.get(item, 0)
            
            # ⚠️ format指定を完全に削除
            input_val = st.number_input(
                item,
                value=float(current_value),
                step=10000.0,
                key=f"input_{selected_month}_{item}"
            )
            input_values[item] = input_val
            
            # 表示用（カンマ区切り）
            if input_val != 0:
                st.caption(f"💰 ¥{input_val:,.0f}")
        
        submitted = st.form_submit_button("💾 この月の予測を保存", use_container_width=True)
        
        if submitted:
            success = save_forecast_data(processor, period_id, selected_month, input_values)
            
            if success:
                st.success(f"✅ {selected_month}月の予測値を保存しました")
                st.rerun()
            else:
                st.error("❌ 保存に失敗しました")


def show_bulk_input(processor, period_id: int, forecast_months: List[int]):
    """一括入力タブ"""
    st.subheader("📋 複数月の予測値を一括入力")
    
    # テンプレート作成
    items = ["売上高", "売上原価", "販売費及び一般管理費", "営業損益金額", "経常損益金額", "当期純利益"]
    
    data = {'科目': items}
    for month in forecast_months:
        data[f'{month}月'] = [0] * len(items)
    
    template_df = pd.DataFrame(data)
    
    # テンプレートダウンロード
    if st.button("📥 入力テンプレートをダウンロード"):
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
        type=['csv', 'xlsx']
    )
    
    if uploaded_file is not None:
        try:
            if uploaded_file.name.endswith('.csv'):
                df = pd.read_csv(uploaded_file, encoding='utf-8-sig')
            else:
                df = pd.read_excel(uploaded_file)
            
            st.write("📊 アップロードされたデータ:")
            
            # ⚠️ data_editorでもformat指定を完全に削除
            edited_df = st.data_editor(
                df,
                use_container_width=True,
                num_rows="fixed",
                column_config={
                    "科目": st.column_config.TextColumn("科目", disabled=True)
                    # 他の列はデフォルト設定を使用（format指定なし）
                }
            )
            
            if st.button("💾 一括保存", type="primary", use_container_width=True):
                success = save_bulk_forecast(processor, period_id, edited_df)
                
                if success:
                    st.success("✅ 予測値を一括保存しました")
                    st.rerun()
                else:
                    st.error("❌ 保存に失敗しました")
        
        except Exception as e:
            st.error(f"❌ ファイル読み込みエラー: {e}")


# ==================== ヘルパー関数 ====================

def get_existing_forecast(processor, period_id: int, month: int) -> Dict:
    """既存の予測データを取得"""
    try:
        conn = processor._get_connection()
        placeholder = '%s' if processor.use_postgres else '?'

        query = f"""
            SELECT item_name, forecast_value
            FROM forecast_data
            WHERE fiscal_period_id = {placeholder} AND month = {placeholder}
        """

        df = pd.read_sql_query(query, conn, params=(period_id, month))
        
        if df.empty:
            return {}
        
        return dict(zip(df['item_name'], df['forecast_value']))
        
    except Exception as e:
        sys.stderr.write(f"予測データ取得エラー: {e}\n")
        return {}


def save_forecast_data(processor, period_id: int, month: int, values: Dict) -> bool:
    """予測データを保存"""
    try:
        conn = processor._get_connection()
        cursor = conn.cursor()

        if processor.use_postgres:
            query = """
                INSERT INTO forecast_data (fiscal_period_id, item_name, month, forecast_value, created_at, updated_at)
                VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                ON CONFLICT (fiscal_period_id, item_name, month)
                DO UPDATE SET
                    forecast_value = EXCLUDED.forecast_value,
                    updated_at = CURRENT_TIMESTAMP
            """
        else:
            query = """
                INSERT INTO forecast_data (fiscal_period_id, item_name, month, forecast_value, created_at, updated_at)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                ON CONFLICT (fiscal_period_id, item_name, month)
                DO UPDATE SET
                    forecast_value = excluded.forecast_value,
                    updated_at = CURRENT_TIMESTAMP
            """

        for item_name, value in values.items():
            cursor.execute(query, (period_id, item_name, month, value))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        # キャッシュクリア
        st.cache_data.clear()
        
        return True
        
    except Exception as e:
        sys.stderr.write(f"❌ 予測データ保存エラー: {e}\n")
        import traceback
        traceback.print_exc(file=sys.stderr)
        return False


def save_bulk_forecast(processor, period_id: int, df: pd.DataFrame) -> bool:
    """一括予測データを保存"""
    try:
        conn = processor._get_connection()
        cursor = conn.cursor()

        if processor.use_postgres:
            query = """
                INSERT INTO forecast_data (fiscal_period_id, item_name, month, forecast_value, created_at, updated_at)
                VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                ON CONFLICT (fiscal_period_id, item_name, month)
                DO UPDATE SET
                    forecast_value = EXCLUDED.forecast_value,
                    updated_at = CURRENT_TIMESTAMP
            """
        else:
            query = """
                INSERT INTO forecast_data (fiscal_period_id, item_name, month, forecast_value, created_at, updated_at)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                ON CONFLICT (fiscal_period_id, item_name, month)
                DO UPDATE SET
                    forecast_value = excluded.forecast_value,
                    updated_at = CURRENT_TIMESTAMP
            """

        item_col = df.columns[0]
        items = df[item_col].tolist()

        month_cols = df.columns[1:]

        for month_col in month_cols:
            # "3月" → 3 に変換
            month_str = str(month_col).replace('月', '')
            try:
                month = int(month_str)
            except Exception:
                continue

            for idx, item_name in enumerate(items):
                value = df.loc[idx, month_col]

                if pd.notna(value) and value != 0:
                    cursor.execute(query, (period_id, item_name, month, float(value)))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        # キャッシュクリア
        st.cache_data.clear()
        
        return True
        
    except Exception as e:
        sys.stderr.write(f"❌ 一括保存エラー: {e}\n")
        import traceback
        traceback.print_exc(file=sys.stderr)
        return False
