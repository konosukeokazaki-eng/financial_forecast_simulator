"""
収益性分析モジュール
損益分岐点分析、限界利益率分析を実施
"""

import pandas as pd
import numpy as np
from typing import Dict, Optional
import sys


def analyze_profitability_from_db(conn, period_id: int) -> Optional[Dict]:
    """
    データベースから実績データを取得して収益性分析を実行
    
    Args:
        conn: データベース接続
        period_id: 会計期間ID
        
    Returns:
        Dict: 分析結果
    """
    try:
        print(f"🚀 analyze_profitability_from_db開始")
        print(f"   期間ID: {period_id}")
        
        # プレースホルダーの判定
        placeholder = '%s'  # PostgreSQL用
        print(f"   プレースホルダー: {placeholder}")
        
        # actual_dataテーブルから実績データを取得（amountカラムを使用）
        query = """
            SELECT item_name, month, amount
            FROM actual_data
            WHERE fiscal_period_id = %s
            ORDER BY month, item_name
        """
        
        print(f"   SQL実行中...")
        df = pd.read_sql_query(query, conn, params=(period_id,))
        
        # 後続処理との互換性のため、カラム名を'value'にリネーム
        df = df.rename(columns={'amount': 'value'})
        
        if df.empty:
            print(f"⚠️  実績データが見つかりません")
            return None
        
        print(f"✅ データ取得成功: {len(df)}行")
        
        # ピボットして月次×科目の形式に変換
        df_pivot = df.pivot(index='month', columns='item_name', values='value').fillna(0)
        
        print(f"   ピボット後の形状: {df_pivot.shape}")
        print(f"   科目: {list(df_pivot.columns)}")
        
        # 必要な科目が存在するか確認
        required_items = ['売上高', '売上原価', '販売費及び一般管理費']
        missing_items = [item for item in required_items if item not in df_pivot.columns]
        
        if missing_items:
            print(f"⚠️  必須科目が不足: {missing_items}")
            # 不足している科目は0で補完
            for item in missing_items:
                df_pivot[item] = 0
        
        # 月次分析データを作成
        monthly_data = []
        
        for month in df_pivot.index:
            sales = df_pivot.loc[month, '売上高'] if '売上高' in df_pivot.columns else 0
            cogs = df_pivot.loc[month, '売上原価'] if '売上原価' in df_pivot.columns else 0
            sg_expenses = df_pivot.loc[month, '販売費及び一般管理費'] if '販売費及び一般管理費' in df_pivot.columns else 0
            
            # 限界利益 = 売上 - 変動費（ここでは売上原価を変動費と仮定）
            marginal_profit = sales - cogs
            marginal_profit_rate = marginal_profit / sales if sales > 0 else 0
            
            # 固定費（販管費を固定費と仮定）
            fixed_costs = sg_expenses
            
            # 営業利益
            operating_profit = marginal_profit - fixed_costs
            
            # 損益分岐点売上高 = 固定費 / 限界利益率
            breakeven_sales = fixed_costs / marginal_profit_rate if marginal_profit_rate > 0 else 0
            
            # 安全余裕率 = (実際の売上 - 損益分岐点売上高) / 実際の売上
            safety_rate = (sales - breakeven_sales) / sales if sales > 0 else 0
            
            monthly_data.append({
                'month': int(month),
                'sales': float(sales),
                'cogs': float(cogs),
                'marginal_profit': float(marginal_profit),
                'marginal_profit_rate': float(marginal_profit_rate),
                'fixed_costs': float(fixed_costs),
                'operating_profit': float(operating_profit),
                'breakeven_sales': float(breakeven_sales),
                'safety_rate': float(safety_rate)
            })
        
        df_monthly = pd.DataFrame(monthly_data)
        
        if df_monthly.empty:
            print(f"⚠️  月次データの生成に失敗")
            return None
        
        # トレンド分析
        if len(df_monthly) >= 3:
            # 最近3ヶ月の平均と最初3ヶ月の平均を比較
            recent_avg = df_monthly.tail(3)['marginal_profit_rate'].mean()
            initial_avg = df_monthly.head(3)['marginal_profit_rate'].mean()
            
            if recent_avg > initial_avg * 1.05:
                trend = 'improving'
            elif recent_avg < initial_avg * 0.95:
                trend = 'deteriorating'
            else:
                trend = 'stable'
        else:
            trend = 'insufficient_data'
        
        # 全体の平均限界利益率
        avg_marginal_rate = df_monthly['marginal_profit_rate'].mean()
        
        result = {
            'monthly_data': df_monthly,
            'trend': trend,
            'average_marginal_profit_rate': float(avg_marginal_rate),
            'latest_month': int(df_monthly.iloc[-1]['month']),
            'total_months': len(df_monthly)
        }
        
        print(f"✅ 分析完了")
        print(f"   トレンド: {trend}")
        print(f"   平均限界利益率: {avg_marginal_rate*100:.2f}%")
        print(f"   対象月数: {len(df_monthly)}ヶ月")
        
        return result
        
    except Exception as e:
        print(f"❌ analyze_profitability_from_dbエラー: {e}")
        import traceback
        traceback.print_exc()
        return None


def calculate_cost_structure(df: pd.DataFrame) -> Dict:
    """
    費用構造を分析
    
    Args:
        df: 月次PL/BSデータ
        
    Returns:
        Dict: 変動費率、固定費などの分析結果
    """
    try:
        # 売上と原価の関係から変動費率を推定
        if 'sales' not in df.columns or 'cogs' not in df.columns:
            return {}
        
        # 線形回帰で変動費率を推定
        from sklearn.linear_model import LinearRegression
        
        X = df[['sales']].values
        y = df['cogs'].values
        
        model = LinearRegression()
        model.fit(X, y)
        
        variable_cost_rate = model.coef_[0]
        fixed_cost_intercept = model.intercept_
        
        return {
            'variable_cost_rate': float(variable_cost_rate),
            'estimated_fixed_costs': float(fixed_cost_intercept),
            'r_squared': float(model.score(X, y))
        }
        
    except Exception as e:
        print(f"❌ 費用構造分析エラー: {e}")
        return {}


def identify_improvement_opportunities(monthly_data: pd.DataFrame) -> list:
    """
    改善機会を特定
    
    Args:
        monthly_data: 月次分析データ
        
    Returns:
        list: 改善提案のリスト
    """
    opportunities = []
    
    if monthly_data.empty:
        return opportunities
    
    latest = monthly_data.iloc[-1]
    
    # 限界利益率が低い
    if latest['marginal_profit_rate'] < 0.30:
        opportunities.append({
            'category': 'profitability',
            'severity': 'high',
            'message': '限界利益率が30%を下回っています',
            'suggestions': [
                '値上げの検討',
                '仕入先との価格交渉',
                '高粗利商品へのシフト'
            ]
        })
    
    # 安全余裕率が低い
    if latest['safety_rate'] < 0.20:
        opportunities.append({
            'category': 'risk',
            'severity': 'high',
            'message': '安全余裕率が20%を下回っています',
            'suggestions': [
                '固定費の削減',
                '売上の増加施策',
                '損益分岐点の引き下げ'
            ]
        })
    
    # 赤字
    if latest['operating_profit'] < 0:
        opportunities.append({
            'category': 'urgent',
            'severity': 'critical',
            'message': '営業赤字です',
            'suggestions': [
                '緊急の固定費削減',
                '不採算事業の見直し',
                '価格改定の即時実行'
            ]
        })
    
    return opportunities
