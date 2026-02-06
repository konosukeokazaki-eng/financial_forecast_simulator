"""
app.py完全版生成スクリプト
元のapp.pyにAI自動予測機能を統合
"""

def generate_complete_app(original_app_path, output_path):
    """
    元のapp.pyにAI自動予測機能を追加した完全版を生成
    
    Args:
        original_app_path: 元のapp.pyのパス
        output_path: 出力するapp.pyのパス
    """
    
    print("🚀 app.py完全版を生成しています...")
    
    # 元のapp.pyを読み込み
    with open(original_app_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    # Part 1: インポート追加（16行目の後）
    import_addition = '''
# 🆕 AI自動予測機能の追加
try:
    from advanced_forecast_engine import get_advanced_forecast_engine
    from advanced_forecast_ui import show_advanced_forecast_page
    ADVANCED_FORECAST_AVAILABLE = True
except ImportError:
    ADVANCED_FORECAST_AVAILABLE = False

'''
    
    # Part 2: メニュー追加（824行目付近を探す）
    menu_addition = '''    
    # 🆕 AI自動予測を追加
    if ADVANCED_FORECAST_AVAILABLE:
        if st.sidebar.button("🔮 AI自動予測", width="stretch", key="nav_advanced_forecast"):
            st.session_state.page = "AI自動予測"
'''
    
    # Part 3: ページ処理追加（最後に）
    page_addition = '''
# 🆕 AI自動予測ページ
elif st.session_state.page == "AI自動予測":
    if ADVANCED_FORECAST_AVAILABLE:
        # DataProcessorをDataHandler互換にするアダプター
        class DataHandlerAdapter:
            """data_processorをdata_handler互換にする"""
            
            def __init__(self, processor):
                self._processor = processor
                self._connection = None
            
            def _get_connection(self):
                """データベース接続を取得"""
                if self._connection is None:
                    if self._processor.use_postgres:
                        import psycopg2
                        self._connection = psycopg2.connect(self._processor.db_url)
                    else:
                        import sqlite3
                        self._connection = sqlite3.connect('financial_simulator.db')
                return self._connection
            
            def get_actual_vs_forecast_split(self, period_id):
                """実績締月情報を取得"""
                try:
                    conn = self._get_connection()
                    
                    # 実績データの最新月を取得
                    if self._processor.use_postgres:
                        query = """
                            SELECT MAX(fiscal_month) as latest_actual
                            FROM actuals
                            WHERE fiscal_period_id = %s
                        """
                        df = pd.read_sql_query(query, conn, params=(period_id,))
                    else:
                        query = """
                            SELECT MAX(fiscal_month) as latest_actual
                            FROM actuals
                            WHERE fiscal_period_id = ?
                        """
                        df = pd.read_sql_query(query, conn, params=(period_id,))
                    
                    latest_actual = df['latest_actual'].iloc[0] if not df.empty and not pd.isna(df['latest_actual'].iloc[0]) else 0
                    
                    # 月リストを生成
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
                    import sys
                    sys.stderr.write(f"❌ get_actual_vs_forecast_split error: {e}\\n")
                    sys.stderr.flush()
                    return {
                        'has_actual': False,
                        'latest_actual_month': 0,
                        'actual_months': [],
                        'forecast_months': list(range(1, 13))
                    }
            
            def get_cumulative_actual_data(self, period_id, up_to_month):
                """累計実績データを取得"""
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
                    
                except Exception as e:
                    import sys
                    sys.stderr.write(f"❌ get_cumulative_actual_data error: {e}\\n")
                    sys.stderr.flush()
                    return {}
        
        # アダプターを作成
        if 'data_handler_adapter' not in st.session_state:
            st.session_state.data_handler_adapter = DataHandlerAdapter(processor)
        
        data_handler_adapter = st.session_state.data_handler_adapter
        
        # AI予測エンジンの初期化（遅延初期化）
        if 'advanced_engine' not in st.session_state:
            st.session_state.advanced_engine = get_advanced_forecast_engine(data_handler_adapter)
        
        # AI予測画面を表示
        show_advanced_forecast_page(
            data_handler_adapter,
            st.session_state.advanced_engine
        )
        
    else:
        st.error("❌ AI自動予測機能が利用できません")
        st.info("""
        AI自動予測機能を使用するには、以下のファイルを配置してください：
        
        📁 必要なファイル:
        - advanced_forecast_engine.py
        - advanced_forecast_ui.py
        
        これらのファイルをアプリと同じディレクトリに配置してください。
        """)
        
        with st.expander("📖 配置方法"):
            st.markdown("""
            1. `advanced_forecast_engine.py` をダウンロード
            2. `advanced_forecast_ui.py` をダウンロード
            3. アプリの実行ディレクトリに配置
            4. アプリを再起動
            """)
'''
    
    # 新しいファイルを作成
    new_lines = []
    
    # Part 1: 16行目までそのまま、その後インポート追加
    for i, line in enumerate(lines):
        new_lines.append(line)
        if i == 15:  # 16行目の後（0-indexed）
            new_lines.append(import_addition)
    
    # Part 2: 824行目付近で「収益構造分析」の後にメニュー追加
    final_lines = []
    for i, line in enumerate(new_lines):
        final_lines.append(line)
        # 「収益構造分析」ボタンの後に追加
        if '収益構造分析' in line and 'st.sidebar.button' in line:
            # 次の行を確認
            if i + 1 < len(new_lines):
                next_line = new_lines[i + 1]
                # すでに追加されていないか確認
                if 'AI自動予測' not in next_line:
                    final_lines.append(menu_addition)
    
    # Part 3: 最後にページ処理を追加（elifの前に追加）
    # 最後のelseの前に追加
    output_lines = []
    last_else_index = -1
    
    for i, line in enumerate(final_lines):
        if i == len(final_lines) - 1:
            # 最後の行の前に追加
            output_lines.append(page_addition)
        output_lines.append(line)
    
    # ファイルに書き込み
    with open(output_path, 'w', encoding='utf-8') as f:
        f.writelines(output_lines)
    
    print(f"✅ 完全版app.pyを生成しました: {output_path}")
    print(f"📊 総行数: {len(output_lines)}行")
    print()
    print("📝 次のステップ:")
    print("1. advanced_forecast_engine.py を配置")
    print("2. advanced_forecast_ui.py を配置")
    print(f"3. streamlit run {output_path}")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("使い方: python generate_complete_app.py <元のapp.pyのパス> [出力パス]")
        print()
        print("例:")
        print("  python generate_complete_app.py app__12_.py app_complete.py")
        print("  python generate_complete_app.py app.py")
        sys.exit(1)
    
    original_path = sys.argv[1]
    output_path = sys.argv[2] if len(sys.argv) > 2 else "app_complete.py"
    
    try:
        generate_complete_app(original_path, output_path)
    except FileNotFoundError:
        print(f"❌ エラー: ファイルが見つかりません: {original_path}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ エラー: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
