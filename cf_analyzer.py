"""
キャッシュフロー分析モジュール
BS（貸借対照表）とPL（損益計算書）からキャッシュフロー計算書を生成し、将来予測を行う
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sys

class CashFlowAnalyzer:
    """キャッシュフロー分析クラス"""
    
    def __init__(self, processor):
        """
        初期化
        
        Args:
            processor: DataProcessorインスタンス
        """
        self.processor = processor
        
    def load_bs_from_yayoi(self, file_path, sheet_name='貸･事業所(合計)'):
        """
        弥生会計のBS（貸借対照表）を読み込む
        
        Args:
            file_path: Excelファイルパス
            sheet_name: シート名
            
        Returns:
            DataFrame: BS データ
        """
        try:
            sys.stderr.write(f"📊 BS読み込み開始: {file_path}\n")
            sys.stderr.flush()
            
            # pandasで直接読み込み（シンプルな方法）
            try:
                # header=Noneで全データを取得
                df_raw = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
                
                sys.stderr.write(f"   読み込み完了: {len(df_raw)}行 × {len(df_raw.columns)}列\n")
                sys.stderr.flush()
                
                if len(df_raw) < 9:
                    sys.stderr.write(f"❌ データ行数が不足: {len(df_raw)}行（最低9行必要）\n")
                    sys.stderr.flush()
                    return pd.DataFrame()
                
                # 7行目をヘッダーとして取得（インデックス7）
                header_row = df_raw.iloc[7]
                # 先頭・末尾の空白を削除
                header_list = [str(x).strip() if pd.notna(x) else f"Col_{i}" for i, x in enumerate(header_row)]
                sys.stderr.write(f"   ヘッダー行（7行目）: {header_list[:5]}...\n")
                sys.stderr.flush()
                
                # 8行目以降をデータとして取得（インデックス8以降）
                df_data = df_raw.iloc[8:].copy()
                df_data.columns = header_list
                df_data = df_data.reset_index(drop=True)
                
                sys.stderr.write(f"   データ行数: {len(df_data)}\n")
                sys.stderr.flush()
                
            except Exception as e:
                sys.stderr.write(f"❌ Excel読み込みエラー: {e}\n")
                import traceback
                traceback.print_exc(file=sys.stderr)
                sys.stderr.flush()
                return pd.DataFrame()
            
            # 最初の列名を取得
            if len(df_data.columns) == 0:
                sys.stderr.write(f"❌ 列が存在しません\n")
                sys.stderr.flush()
                return pd.DataFrame()
            
            first_col = df_data.columns[0]
            
            # 月度列を特定
            month_cols = []
            for col in df_data.columns:
                if col is not None and pd.notna(col) and '月度' in str(col):
                    month_cols.append(col)
            
            sys.stderr.write(f"   勘定科目列: {first_col}\n")
            sys.stderr.write(f"   月度列数: {len(month_cols)}\n")
            sys.stderr.write(f"   月度列: {month_cols[:3] if len(month_cols) > 3 else month_cols}...\n")
            sys.stderr.flush()
            
            if len(month_cols) == 0:
                sys.stderr.write(f"❌ 月度列が見つかりません\n")
                sys.stderr.write(f"   全列名: {df_data.columns.tolist()}\n")
                sys.stderr.flush()
                return pd.DataFrame()
            
            # BSの主要項目を抽出
            bs_data = {
                '勘定科目': [],
                '項目タイプ': []
            }
            
            # 月度データを初期化
            for month_col in month_cols:
                bs_data[month_col] = []
            
            current_type = None
            item_count = 0
            
            for idx in range(len(df_data)):
                try:
                    # 行を取得
                    row = df_data.iloc[idx]
                    
                    # 最初の列の値を取得
                    account_value = row.iloc[0]
                    
                    # NaNチェック
                    if pd.isna(account_value):
                        continue
                    
                    account_str = str(account_value).strip()
                    
                    # 空文字チェック
                    if not account_str or account_str == 'nan' or account_str == 'None':
                        continue
                    
                    # 項目タイプを判定
                    if '[' in account_str and ']' in account_str:
                        # 大分類（例: [現金･預金]）
                        current_type = account_str.replace('[', '').replace(']', '')
                        continue
                    
                    # データを記録（合計行も個別項目も全て記録）
                    bs_data['勘定科目'].append(account_str)
                    bs_data['項目タイプ'].append(current_type if current_type else '不明')
                    
                    for month_col in month_cols:
                        val = row[month_col]
                        bs_data[month_col].append(float(val) if pd.notna(val) and val != '' else 0)
                    
                    item_count += 1
                    
                except Exception as e:
                    sys.stderr.write(f"⚠️ 行{idx}の処理エラー（スキップ）: {e}\n")
                    sys.stderr.flush()
                    continue
            
            result_df = pd.DataFrame(bs_data)
            
            sys.stderr.write(f"✅ BS読み込み完了: {len(result_df)}項目\n")
            if not result_df.empty:
                unique_types = result_df['項目タイプ'].unique().tolist()
                sys.stderr.write(f"   項目タイプ: {unique_types[:5] if len(unique_types) > 5 else unique_types}\n")
            sys.stderr.flush()
            
            return result_df
            
        except Exception as e:
            sys.stderr.write(f"❌ BS読み込みエラー: {e}\n")
            import traceback
            traceback.print_exc(file=sys.stderr)
            sys.stderr.flush()
            return pd.DataFrame()
    
    def calculate_cash_flow(self, pl_data, bs_data, bs_previous=None):
        """
        間接法でキャッシュフロー計算書を生成
        
        Args:
            pl_data: PLデータ（DataFrameまたは辞書）
            bs_data: 当期BSデータ
            bs_previous: 前期BSデータ（ない場合は前月BSを使用）
            
        Returns:
            dict: キャッシュフロー計算書
        """
        try:
            sys.stderr.write("💰 キャッシュフロー計算開始\n")
            sys.stderr.flush()
            
            cf_statement = {}
            
            # 月度列を取得
            month_cols = [col for col in bs_data.columns if '月度' in str(col)]
            
            for month_col in month_cols:
                month_cf = self._calculate_monthly_cf(pl_data, bs_data, month_col, bs_previous)
                cf_statement[month_col] = month_cf
            
            sys.stderr.write(f"✅ キャッシュフロー計算完了: {len(cf_statement)}ヶ月\n")
            sys.stderr.flush()
            
            return cf_statement
            
        except Exception as e:
            sys.stderr.write(f"❌ キャッシュフロー計算エラー: {e}\n")
            import traceback
            traceback.print_exc(file=sys.stderr)
            sys.stderr.flush()
            return {}
    
    def _calculate_monthly_cf(self, pl_data, bs_data, month_col, bs_previous):
        """
        月次キャッシュフロー計算
        
        Args:
            pl_data: PLデータ
            bs_data: 当月BSデータ
            month_col: 月度列名
            bs_previous: 前月BSデータ
            
        Returns:
            dict: 月次CF計算結果
        """
        cf = {
            '営業CF': {},
            '投資CF': {},
            '財務CF': {},
            '現金増減': 0,
            '期首現金': 0,
            '期末現金': 0
        }
        
        # BS項目を取得
        def get_bs_value(bs_df, item_name, month):
            row = bs_df[bs_df['勘定科目'].str.contains(item_name, na=False)]
            if not row.empty and month in row.columns:
                return float(row[month].iloc[0]) if pd.notna(row[month].iloc[0]) else 0
            return 0
        
        # PL項目を取得
        def get_pl_value(pl_df, item_name, month):
            if isinstance(pl_df, pd.DataFrame):
                row = pl_df[pl_df['項目名'] == item_name]
                if not row.empty and month in row.columns:
                    return float(row[month].iloc[0]) if pd.notna(row[month].iloc[0]) else 0
            return 0
        
        # === 営業活動によるキャッシュフロー ===
        
        # 税引前利益（経常利益で代用）
        profit_before_tax = get_pl_value(pl_data, '経常損益金額', month_col)
        cf['営業CF']['税引前利益'] = profit_before_tax
        
        # 減価償却費（簡易計算: 固定資産の減少分）
        # ※本来は減価償却費を直接取得すべき
        depreciation = 0  # TODO: PLから取得
        cf['営業CF']['減価償却費'] = depreciation
        
        # 売上債権の増減
        ar_current = get_bs_value(bs_data, '売掛金', month_col)
        ar_previous = 0
        if bs_previous is not None:
            ar_previous = get_bs_value(bs_previous, '売掛金', month_col)
        ar_change = ar_current - ar_previous
        cf['営業CF']['売上債権の増減'] = -ar_change  # 増加はマイナス
        
        # 棚卸資産の増減
        inv_current = get_bs_value(bs_data, '棚卸資産合計', month_col)
        inv_previous = 0
        if bs_previous is not None:
            inv_previous = get_bs_value(bs_previous, '棚卸資産合計', month_col)
        inv_change = inv_current - inv_previous
        cf['営業CF']['棚卸資産の増減'] = -inv_change  # 増加はマイナス
        
        # 買入債務の増減
        ap_current = get_bs_value(bs_data, '買掛金', month_col)
        ap_previous = 0
        if bs_previous is not None:
            ap_previous = get_bs_value(bs_previous, '買掛金', month_col)
        ap_change = ap_current - ap_previous
        cf['営業CF']['買入債務の増減'] = ap_change  # 増加はプラス
        
        # 法人税の支払（簡易: 税引前利益 × 30%）
        tax_rate = 0.30
        tax_paid = profit_before_tax * tax_rate if profit_before_tax > 0 else 0
        cf['営業CF']['法人税の支払'] = -tax_paid
        
        # 営業CF合計
        operating_cf = (
            cf['営業CF']['税引前利益'] +
            cf['営業CF']['減価償却費'] +
            cf['営業CF']['売上債権の増減'] +
            cf['営業CF']['棚卸資産の増減'] +
            cf['営業CF']['買入債務の増減'] +
            cf['営業CF']['法人税の支払']
        )
        cf['営業CF']['合計'] = operating_cf
        
        # === 投資活動によるキャッシュフロー ===
        
        # 固定資産の取得（簡易: 固定資産の増加分）
        fa_current = get_bs_value(bs_data, '固定資産合計', month_col)
        fa_previous = 0
        if bs_previous is not None:
            fa_previous = get_bs_value(bs_previous, '固定資産合計', month_col)
        capex = -(fa_current - fa_previous) if fa_current > fa_previous else 0
        cf['投資CF']['固定資産の取得'] = capex
        
        # 投資CF合計
        investing_cf = cf['投資CF']['固定資産の取得']
        cf['投資CF']['合計'] = investing_cf
        
        # === 財務活動によるキャッシュフロー ===
        
        # 借入金の返済（簡易: 借入金の減少分）
        debt_current = get_bs_value(bs_data, '借入金', month_col)
        debt_previous = 0
        if bs_previous is not None:
            debt_previous = get_bs_value(bs_previous, '借入金', month_col)
        debt_repayment = -(debt_current - debt_previous) if debt_current < debt_previous else 0
        cf['財務CF']['借入金の返済'] = debt_repayment
        
        # 配当金の支払
        dividend = 0  # TODO: PLから取得
        cf['財務CF']['配当金の支払'] = dividend
        
        # 財務CF合計
        financing_cf = (
            cf['財務CF']['借入金の返済'] +
            cf['財務CF']['配当金の支払']
        )
        cf['財務CF']['合計'] = financing_cf
        
        # === 現金増減 ===
        net_change = operating_cf + investing_cf + financing_cf
        cf['現金増減'] = net_change
        
        # 現金残高
        cash_current = get_bs_value(bs_data, '現金･預金合計', month_col)
        cash_previous = 0
        if bs_previous is not None:
            cash_previous = get_bs_value(bs_previous, '現金･預金合計', month_col)
        
        cf['期首現金'] = cash_previous
        cf['期末現金'] = cash_current
        
        return cf
    
    def calculate_cfo_kpis(self, cf_data, bs_data, pl_data):
        """
        CFO特化指標を計算
        
        Args:
            cf_data: キャッシュフローデータ
            bs_data: BSデータ
            pl_data: PLデータ
            
        Returns:
            dict: CFO KPI
        """
        try:
            kpis = {}
            
            month_cols = [col for col in bs_data.columns if '月度' in str(col)]
            
            for month_col in month_cols:
                month_kpi = {}
                
                if month_col in cf_data:
                    cf_month = cf_data[month_col]
                    
                    # 税引後キャッシュ = 営業CF - 法人税
                    operating_cf = cf_month['営業CF'].get('合計', 0)
                    month_kpi['税引後キャッシュ'] = operating_cf
                    
                    # フリーキャッシュフロー = 営業CF + 投資CF
                    fcf = cf_month['営業CF'].get('合計', 0) + cf_month['投資CF'].get('合計', 0)
                    month_kpi['FCF'] = fcf
                    
                    # 現金残高
                    cash_balance = cf_month.get('期末現金', 0)
                    month_kpi['現金残高'] = cash_balance
                    
                    # 資金耐久月数（簡易: 固定費を販管費として計算）
                    # TODO: 固定費を正確に計算
                    fixed_cost_monthly = 10000000  # 仮の固定費
                    if fixed_cost_monthly > 0:
                        months_runway = cash_balance / fixed_cost_monthly
                        month_kpi['資金耐久月数'] = months_runway
                    else:
                        month_kpi['資金耐久月数'] = 999
                    
                    kpis[month_col] = month_kpi
            
            return kpis
            
        except Exception as e:
            sys.stderr.write(f"❌ CFO KPI計算エラー: {e}\n")
            import traceback
            traceback.print_exc(file=sys.stderr)
            sys.stderr.flush()
            return {}
    
    def forecast_cash_flow(self, historical_cf, months_ahead=12):
        """
        将来のキャッシュフローを予測
        
        Args:
            historical_cf: 過去のCFデータ
            months_ahead: 予測期間（月数）
            
        Returns:
            dict: 予測CF（楽観・標準・悲観）
        """
        try:
            sys.stderr.write(f"🔮 キャッシュフロー予測開始: {months_ahead}ヶ月先まで\n")
            sys.stderr.flush()
            
            # 過去データから営業CFの平均を計算
            operating_cfs = []
            for month, cf in historical_cf.items():
                if '営業CF' in cf and '合計' in cf['営業CF']:
                    operating_cfs.append(cf['営業CF']['合計'])
            
            if not operating_cfs:
                sys.stderr.write("⚠️ 過去データなし - 予測不可\n")
                sys.stderr.flush()
                return {}
            
            avg_operating_cf = np.mean(operating_cfs)
            std_operating_cf = np.std(operating_cfs)
            
            sys.stderr.write(f"   平均営業CF: ¥{avg_operating_cf:,.0f}\n")
            sys.stderr.write(f"   標準偏差: ¥{std_operating_cf:,.0f}\n")
            sys.stderr.flush()
            
            # 3シナリオを生成
            forecasts = {
                '楽観': [],
                '標準': [],
                '悲観': []
            }
            
            # 現在の現金残高を取得
            last_month = list(historical_cf.keys())[-1]
            current_cash = historical_cf[last_month].get('期末現金', 0)
            
            for i in range(months_ahead):
                # 標準シナリオ: 平均値
                standard_cf = avg_operating_cf
                
                # 楽観シナリオ: 平均 + 15%
                optimistic_cf = avg_operating_cf * 1.15
                
                # 悲観シナリオ: 平均 - 15%
                pessimistic_cf = avg_operating_cf * 0.85
                
                # 現金残高を累積
                current_cash_standard = current_cash + standard_cf * (i + 1)
                current_cash_optimistic = current_cash + optimistic_cf * (i + 1)
                current_cash_pessimistic = current_cash + pessimistic_cf * (i + 1)
                
                forecasts['標準'].append({
                    '月': i + 1,
                    '営業CF': standard_cf,
                    '現金残高': current_cash_standard
                })
                
                forecasts['楽観'].append({
                    '月': i + 1,
                    '営業CF': optimistic_cf,
                    '現金残高': current_cash_optimistic
                })
                
                forecasts['悲観'].append({
                    '月': i + 1,
                    '営業CF': pessimistic_cf,
                    '現金残高': current_cash_pessimistic
                })
            
            sys.stderr.write(f"✅ 予測完了: 3シナリオ × {months_ahead}ヶ月\n")
            sys.stderr.flush()
            
            return forecasts
            
        except Exception as e:
            sys.stderr.write(f"❌ CF予測エラー: {e}\n")
            import traceback
            traceback.print_exc(file=sys.stderr)
            sys.stderr.flush()
            return {}
    
    def check_alerts(self, current_kpis, cf_data):
        """
        アラートをチェック
        
        Args:
            current_kpis: 現在のKPI
            cf_data: CFデータ
            
        Returns:
            list: アラートリスト
        """
        alerts = []
        
        try:
            # 最新月のKPIを取得
            if not current_kpis:
                return alerts
            
            latest_month = list(current_kpis.keys())[-1]
            kpi = current_kpis[latest_month]
            
            # 資金耐久月数チェック
            runway = kpi.get('資金耐久月数', 999)
            if runway < 3:
                alerts.append({
                    'level': 'critical',
                    'type': '資金危険',
                    'message': f'現金残高が固定費の{runway:.1f}ヶ月分しかありません',
                    'action': '追加融資または固定費削減を至急検討してください'
                })
            elif runway < 6:
                alerts.append({
                    'level': 'warning',
                    'type': '資金注意',
                    'message': f'現金残高が固定費の{runway:.1f}ヶ月分です',
                    'action': '資金計画の見直しを推奨します'
                })
            
            # 営業CFチェック
            if latest_month in cf_data:
                operating_cf = cf_data[latest_month]['営業CF'].get('合計', 0)
                if operating_cf < 0:
                    alerts.append({
                        'level': 'critical',
                        'type': '営業CF赤字',
                        'message': '営業活動でキャッシュを生み出せていません',
                        'action': '本業の収益性改善が必要です'
                    })
            
            # FCFチェック
            fcf = kpi.get('FCF', 0)
            if fcf < 0:
                alerts.append({
                    'level': 'warning',
                    'type': 'FCF赤字',
                    'message': 'フリーキャッシュフローがマイナスです',
                    'action': '投資を抑制するか、営業CFを改善してください'
                })
            
        except Exception as e:
            sys.stderr.write(f"❌ アラートチェックエラー: {e}\n")
            sys.stderr.flush()
        
        return alerts
    
    def suggest_actions(self, alerts, current_kpis):
        """
        状況に応じた具体的アクションを提案
        
        Args:
            alerts: アラートリスト
            current_kpis: 現在のKPI
            
        Returns:
            list: アクション提案リスト
        """
        suggestions = []
        
        # アラートに基づく提案
        for alert in alerts:
            if alert['type'] == '資金危険' or alert['type'] == '資金注意':
                suggestions.extend([
                    {
                        'category': '資金確保',
                        'priority': 'high',
                        'action': '売上債権の早期回収',
                        'detail': '請求サイトの短縮交渉、ファクタリングの検討'
                    },
                    {
                        'category': '資金確保',
                        'priority': 'high',
                        'action': '在庫の適正化',
                        'detail': '滞留在庫の処分、発注タイミングの見直し'
                    },
                    {
                        'category': '資金確保',
                        'priority': 'medium',
                        'action': '追加融資の検討',
                        'detail': '銀行折衝資料の作成、事業計画書の準備'
                    }
                ])
            
            elif alert['type'] == '営業CF赤字':
                suggestions.extend([
                    {
                        'category': '収益改善',
                        'priority': 'critical',
                        'action': '売上の増加',
                        'detail': '新規営業強化、既存顧客の単価アップ'
                    },
                    {
                        'category': '収益改善',
                        'priority': 'critical',
                        'action': '原価率の改善',
                        'detail': '仕入先の見直し、業務効率化'
                    },
                    {
                        'category': '収益改善',
                        'priority': 'high',
                        'action': '固定費の削減',
                        'detail': '人件費・家賃の見直し、不要な支出の削減'
                    }
                ])
        
        return suggestions
    
    def save_bs_to_db(self, period_id, bs_data):
        """
        BSデータをデータベースに保存
        
        Args:
            period_id: 会計期間ID
            bs_data: BSデータ（DataFrame）
            
        Returns:
            bool: 成功/失敗
        """
        try:
            import sqlite3
            
            conn = self.processor.get_connection()
            cursor = conn.cursor()
            
            # 月度列を取得
            month_cols = [col for col in bs_data.columns if '月度' in str(col)]
            
            for month_col in month_cols:
                month = month_col.replace('月度', '月')
                
                # BS項目から値を取得
                def get_value(item_name):
                    row = bs_data[bs_data['勘定科目'].str.contains(item_name, na=False)]
                    if not row.empty and month_col in row.columns:
                        val = row[month_col].iloc[0]
                        return float(val) if pd.notna(val) else 0
                    return 0
                
                # データを準備
                data = {
                    'fiscal_period_id': period_id,
                    'month': month,
                    'cash_and_deposits': get_value('現金･預金合計'),
                    'accounts_receivable': get_value('売掛金'),
                    'inventory': get_value('棚卸資産合計'),
                    'other_current_assets': get_value('他流動資産'),
                    'fixed_assets': get_value('固定資産合計'),
                    'accounts_payable': get_value('買掛金'),
                    'short_term_debt': get_value('短期借入金'),
                    'long_term_debt': get_value('長期借入金'),
                }
                
                # UPSERTクエリ
                cursor.execute("""
                    INSERT INTO balance_sheet 
                    (fiscal_period_id, month, cash_and_deposits, accounts_receivable, 
                     inventory, other_current_assets, fixed_assets, accounts_payable,
                     short_term_debt, long_term_debt)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(fiscal_period_id, month) 
                    DO UPDATE SET
                        cash_and_deposits = excluded.cash_and_deposits,
                        accounts_receivable = excluded.accounts_receivable,
                        inventory = excluded.inventory,
                        other_current_assets = excluded.other_current_assets,
                        fixed_assets = excluded.fixed_assets,
                        accounts_payable = excluded.accounts_payable,
                        short_term_debt = excluded.short_term_debt,
                        long_term_debt = excluded.long_term_debt
                """, (
                    data['fiscal_period_id'], data['month'], 
                    data['cash_and_deposits'], data['accounts_receivable'],
                    data['inventory'], data['other_current_assets'], 
                    data['fixed_assets'], data['accounts_payable'],
                    data['short_term_debt'], data['long_term_debt']
                ))
            
            conn.commit()
            sys.stderr.write(f"✅ BS保存完了: {len(month_cols)}ヶ月\n")
            sys.stderr.flush()
            return True
            
        except Exception as e:
            sys.stderr.write(f"❌ BS保存エラー: {e}\n")
            import traceback
            traceback.print_exc(file=sys.stderr)
            sys.stderr.flush()
            return False
    
    def save_cf_to_db(self, period_id, cf_data):
        """
        CFデータをデータベースに保存
        
        Args:
            period_id: 会計期間ID
            cf_data: CFデータ（辞書）
            
        Returns:
            bool: 成功/失敗
        """
        try:
            import sqlite3
            
            conn = self.processor.get_connection()
            cursor = conn.cursor()
            
            for month_key, cf_month in cf_data.items():
                month = month_key.replace('月度', '月')
                
                # データを準備
                operating_cf_data = cf_month.get('営業CF', {})
                investing_cf_data = cf_month.get('投資CF', {})
                financing_cf_data = cf_month.get('財務CF', {})
                
                data = {
                    'fiscal_period_id': period_id,
                    'month': month,
                    'operating_cf': operating_cf_data.get('合計', 0),
                    'profit_before_tax': operating_cf_data.get('税引前利益', 0),
                    'depreciation': operating_cf_data.get('減価償却費', 0),
                    'ar_change': operating_cf_data.get('売上債権の増減', 0),
                    'inventory_change': operating_cf_data.get('棚卸資産の増減', 0),
                    'ap_change': operating_cf_data.get('買入債務の増減', 0),
                    'tax_paid': operating_cf_data.get('法人税の支払', 0),
                    'investing_cf': investing_cf_data.get('合計', 0),
                    'capex': investing_cf_data.get('固定資産の取得', 0),
                    'financing_cf': financing_cf_data.get('合計', 0),
                    'debt_repayment': financing_cf_data.get('借入金の返済', 0),
                    'dividend_paid': financing_cf_data.get('配当金の支払', 0),
                    'net_cash_change': cf_month.get('現金増減', 0),
                    'beginning_cash': cf_month.get('期首現金', 0),
                    'ending_cash': cf_month.get('期末現金', 0)
                }
                
                # UPSERTクエリ
                cursor.execute("""
                    INSERT INTO cash_flow_statement 
                    (fiscal_period_id, month, operating_cf, profit_before_tax, 
                     depreciation, ar_change, inventory_change, ap_change, tax_paid,
                     investing_cf, capex, financing_cf, debt_repayment, dividend_paid,
                     net_cash_change, beginning_cash, ending_cash)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(fiscal_period_id, month) 
                    DO UPDATE SET
                        operating_cf = excluded.operating_cf,
                        profit_before_tax = excluded.profit_before_tax,
                        depreciation = excluded.depreciation,
                        ar_change = excluded.ar_change,
                        inventory_change = excluded.inventory_change,
                        ap_change = excluded.ap_change,
                        tax_paid = excluded.tax_paid,
                        investing_cf = excluded.investing_cf,
                        capex = excluded.capex,
                        financing_cf = excluded.financing_cf,
                        debt_repayment = excluded.debt_repayment,
                        dividend_paid = excluded.dividend_paid,
                        net_cash_change = excluded.net_cash_change,
                        beginning_cash = excluded.beginning_cash,
                        ending_cash = excluded.ending_cash
                """, (
                    data['fiscal_period_id'], data['month'],
                    data['operating_cf'], data['profit_before_tax'],
                    data['depreciation'], data['ar_change'],
                    data['inventory_change'], data['ap_change'], data['tax_paid'],
                    data['investing_cf'], data['capex'],
                    data['financing_cf'], data['debt_repayment'], data['dividend_paid'],
                    data['net_cash_change'], data['beginning_cash'], data['ending_cash']
                ))
            
            conn.commit()
            sys.stderr.write(f"✅ CF保存完了: {len(cf_data)}ヶ月\n")
            sys.stderr.flush()
            return True
            
        except Exception as e:
            sys.stderr.write(f"❌ CF保存エラー: {e}\n")
            import traceback
            traceback.print_exc(file=sys.stderr)
            sys.stderr.flush()
            return False
    
    def load_cf_from_db(self, period_id):
        """
        データベースからCFデータを読み込み
        
        Args:
            period_id: 会計期間ID
            
        Returns:
            dict: CFデータ
        """
        try:
            import sqlite3
            import pandas as pd
            
            conn = self.processor.get_connection()
            
            # CFデータを読み込み
            query = """
                SELECT * FROM cash_flow_statement 
                WHERE fiscal_period_id = ? 
                ORDER BY month
            """
            df = pd.read_sql_query(query, conn, params=(period_id,))
            
            if df.empty:
                return {}
            
            # 辞書形式に変換
            cf_data = {}
            for _, row in df.iterrows():
                month_key = row['month'] + '度'
                cf_data[month_key] = {
                    '営業CF': {
                        '合計': row['operating_cf'],
                        '税引前利益': row['profit_before_tax'],
                        '減価償却費': row['depreciation'],
                        '売上債権の増減': row['ar_change'],
                        '棚卸資産の増減': row['inventory_change'],
                        '買入債務の増減': row['ap_change'],
                        '法人税の支払': row['tax_paid']
                    },
                    '投資CF': {
                        '合計': row['investing_cf'],
                        '固定資産の取得': row['capex']
                    },
                    '財務CF': {
                        '合計': row['financing_cf'],
                        '借入金の返済': row['debt_repayment'],
                        '配当金の支払': row['dividend_paid']
                    },
                    '現金増減': row['net_cash_change'],
                    '期首現金': row['beginning_cash'],
                    '期末現金': row['ending_cash']
                }
            
            sys.stderr.write(f"✅ CF読み込み完了: {len(cf_data)}ヶ月\n")
            sys.stderr.flush()
            return cf_data
            
        except Exception as e:
            sys.stderr.write(f"❌ CF読み込みエラー: {e}\n")
            import traceback
            traceback.print_exc(file=sys.stderr)
            sys.stderr.flush()
            return {}
    
    def calculate_working_capital_metrics(self, bs_data, pl_data):
        """
        運転資本の指標を計算
        
        Args:
            bs_data: BSデータ
            pl_data: PLデータ
            
        Returns:
            dict: 運転資本指標
        """
        try:
            metrics = {}
            
            month_cols = [col for col in bs_data.columns if '月度' in str(col)]
            
            for month_col in month_cols:
                month = month_col.replace('月度', '月')
                
                # BS項目を取得
                def get_bs_value(item_name):
                    row = bs_data[bs_data['勘定科目'].str.contains(item_name, na=False)]
                    if not row.empty and month_col in row.columns:
                        val = row[month_col].iloc[0]
                        return float(val) if pd.notna(val) else 0
                    return 0
                
                # 運転資本 = (売上債権 + 棚卸資産) - 買入債務
                ar = get_bs_value('売掛金')
                inventory = get_bs_value('棚卸資産合計')
                ap = get_bs_value('買掛金')
                working_capital = (ar + inventory) - ap
                
                # 売上高を取得（月次）
                # TODO: PLデータから売上高を取得
                sales = 150000000 / 12  # 仮の値
                cogs = 100000000 / 12   # 仮の値
                
                # 回転日数
                ar_days = (ar / sales * 30) if sales > 0 else 0
                inventory_days = (inventory / cogs * 30) if cogs > 0 else 0
                ap_days = (ap / cogs * 30) if cogs > 0 else 0
                
                # CCC（キャッシュコンバージョンサイクル）
                ccc = ar_days + inventory_days - ap_days
                
                metrics[month] = {
                    '運転資本': working_capital,
                    '売上債権': ar,
                    '棚卸資産': inventory,
                    '買入債務': ap,
                    '売上債権回転日数': ar_days,
                    '棚卸資産回転日数': inventory_days,
                    '買入債務回転日数': ap_days,
                    'CCC': ccc
                }
            
            return metrics
            
        except Exception as e:
            sys.stderr.write(f"❌ 運転資本計算エラー: {e}\n")
            import traceback
            traceback.print_exc(file=sys.stderr)
            sys.stderr.flush()
            return {}
