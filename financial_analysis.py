"""
統合財務分析モジュール
profitability_analyzer + metrics_calculator + working_capital_analyzer を統合
キャッシュ最適化済み
"""

import pandas as pd
import numpy as np
import streamlit as st
from typing import Dict, Optional, List


class FinancialAnalyzer:
    """統合財務分析クラス"""
    
    def __init__(self, data_handler=None):
        self.data_handler = data_handler
    
    # ==================== 収益性分析 ====================
    
    @st.cache_data(ttl=300)
    def analyze_profitability(_self, period_id: int) -> Optional[Dict]:
        """
        収益性分析を実行（キャッシュ付き）
        
        Args:
            period_id: 会計期間ID
            
        Returns:
            Dict: 分析結果
        """
        if not _self.data_handler:
            return None
        
        try:
            conn = _self.data_handler._get_connection()
            placeholder = '%s' if _self.data_handler.use_postgres else '?'

            query = f"""
                SELECT item_name, month, amount
                FROM actual_data
                WHERE fiscal_period_id = {placeholder}
                ORDER BY month, item_name
            """

            df = pd.read_sql_query(query, conn, params=(period_id,))
            df = df.rename(columns={'amount': 'value'})
            
            if df.empty:
                return None
            
            # month列を月番号に変換
            if df['month'].dtype == 'object':
                df['month_num'] = df['month'].apply(
                    lambda x: int(str(x).split('-')[1]) if '-' in str(x) else int(x)
                )
            else:
                df['month_num'] = df['month']
            
            # ピボット
            df_pivot = df.pivot(index='month_num', columns='item_name', values='value').fillna(0)
            
            # 月次分析
            monthly_data = []
            
            for month_num in df_pivot.index:
                sales = df_pivot.loc[month_num, '売上高'] if '売上高' in df_pivot.columns else 0
                cogs = df_pivot.loc[month_num, '売上原価'] if '売上原価' in df_pivot.columns else 0
                sg_expenses = df_pivot.loc[month_num, '販売費及び一般管理費'] if '販売費及び一般管理費' in df_pivot.columns else 0
                
                marginal_profit = sales - cogs
                marginal_profit_rate = marginal_profit / sales if sales > 0 else 0
                fixed_costs = sg_expenses
                operating_profit = marginal_profit - fixed_costs
                breakeven_sales = fixed_costs / marginal_profit_rate if marginal_profit_rate > 0 else 0
                safety_rate = (sales - breakeven_sales) / sales if sales > 0 else 0
                
                monthly_data.append({
                    'month': int(month_num),
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
            
            # トレンド分析
            if len(df_monthly) >= 3:
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
            
            avg_marginal_rate = df_monthly['marginal_profit_rate'].mean()
            
            return {
                'monthly_data': df_monthly,
                'trend': trend,
                'average_marginal_profit_rate': float(avg_marginal_rate),
                'latest_month': int(df_monthly.iloc[-1]['month']),
                'total_months': len(df_monthly)
            }
            
        except Exception as e:
            return None
    
    # ==================== 経営指標計算 ====================
    
    @st.cache_data(ttl=300)
    def calculate_all_metrics(_self, pl_data: Dict, bs_data: Dict, 
                             bs_previous: Optional[Dict] = None,
                             period_id: int = None) -> Dict:
        """
        すべての経営指標を計算（キャッシュ付き）
        
        Args:
            pl_data: PLデータ
            bs_data: BSデータ
            bs_previous: 前期BSデータ
            period_id: 会計期間ID
            
        Returns:
            Dict: 経営指標
        """
        metrics = {}
        
        # 実績締月までのデータで計算
        if _self.data_handler and period_id:
            latest_actual = _self.data_handler.get_latest_actual_month(period_id)
            cumulative_data = _self.data_handler.get_cumulative_actual_data(period_id, latest_actual)
            
            if cumulative_data:
                pl_data = {**pl_data, **cumulative_data}
        
        # 収益性指標
        metrics.update(_self._calculate_profitability_ratios(pl_data, bs_data, bs_previous))
        
        # 安全性指標
        metrics.update(_self._calculate_safety_ratios(bs_data))
        
        # 効率性指標
        metrics.update(_self._calculate_efficiency_ratios(pl_data, bs_data, bs_previous))
        
        return metrics
    
    def _calculate_profitability_ratios(self, pl_data: Dict, bs_data: Dict, 
                                       bs_previous: Optional[Dict] = None) -> Dict:
        """収益性指標を計算"""
        metrics = {}
        
        sales = pl_data.get('売上高', 0)
        cogs = pl_data.get('売上原価', 0)
        gross_profit = sales - cogs
        operating_profit = pl_data.get('営業損益金額', 0)
        ordinary_profit = pl_data.get('経常損益金額', 0)
        net_income = pl_data.get('当期純利益', 0)
        
        total_assets = bs_data.get('資産合計', 0)
        equity = bs_data.get('純資産合計', 0)
        
        if bs_previous:
            total_assets_prev = bs_previous.get('資産合計', total_assets)
            equity_prev = bs_previous.get('純資産合計', equity)
            avg_total_assets = (total_assets + total_assets_prev) / 2
            avg_equity = (equity + equity_prev) / 2
        else:
            avg_total_assets = total_assets
            avg_equity = equity
        
        # 各種利益率
        if sales > 0:
            metrics['売上高総利益率'] = (gross_profit / sales) * 100
            metrics['売上高営業利益率'] = (operating_profit / sales) * 100
            metrics['売上高経常利益率'] = (ordinary_profit / sales) * 100
            metrics['売上高当期純利益率'] = (net_income / sales) * 100
        else:
            metrics['売上高総利益率'] = 0
            metrics['売上高営業利益率'] = 0
            metrics['売上高経常利益率'] = 0
            metrics['売上高当期純利益率'] = 0
        
        # ROE・ROA
        metrics['ROE'] = (net_income / avg_equity * 100) if avg_equity > 0 else 0
        metrics['ROA'] = (net_income / avg_total_assets * 100) if avg_total_assets > 0 else 0
        
        return metrics
    
    def _calculate_safety_ratios(self, bs_data: Dict) -> Dict:
        """安全性指標を計算"""
        metrics = {}
        
        current_assets = bs_data.get('流動資産合計', 0)
        current_liabilities = bs_data.get('流動負債合計', 0)
        fixed_assets = bs_data.get('固定資産合計', 0)
        total_assets = bs_data.get('資産合計', 0)
        total_liabilities = bs_data.get('負債合計', 0)
        equity = bs_data.get('純資産合計', 0)
        
        # 流動比率
        metrics['流動比率'] = (current_assets / current_liabilities * 100) if current_liabilities > 0 else 999
        
        # 固定比率
        metrics['固定比率'] = (fixed_assets / equity * 100) if equity > 0 else 999
        
        # 自己資本比率
        metrics['自己資本比率'] = (equity / total_assets * 100) if total_assets > 0 else 0
        
        # 負債比率
        metrics['負債比率'] = (total_liabilities / equity * 100) if equity > 0 else 999
        
        return metrics
    
    def _calculate_efficiency_ratios(self, pl_data: Dict, bs_data: Dict, 
                                    bs_previous: Optional[Dict] = None) -> Dict:
        """効率性指標を計算"""
        metrics = {}
        
        sales = pl_data.get('売上高', 0)
        cogs = pl_data.get('売上原価', 0)
        
        total_assets = bs_data.get('資産合計', 0)
        receivables = bs_data.get('売掛金', 0)
        inventory = bs_data.get('棚卸資産', 0)
        
        if bs_previous:
            total_assets_prev = bs_previous.get('資産合計', total_assets)
            receivables_prev = bs_previous.get('売掛金', receivables)
            inventory_prev = bs_previous.get('棚卸資産', inventory)
            
            avg_total_assets = (total_assets + total_assets_prev) / 2
            avg_receivables = (receivables + receivables_prev) / 2
            avg_inventory = (inventory + inventory_prev) / 2
        else:
            avg_total_assets = total_assets
            avg_receivables = receivables
            avg_inventory = inventory
        
        # 回転率
        metrics['総資産回転率'] = sales / avg_total_assets if avg_total_assets > 0 else 0
        
        if avg_receivables > 0:
            metrics['売上債権回転率'] = sales / avg_receivables
            metrics['売上債権回転期間'] = 365 / metrics['売上債権回転率']
        else:
            metrics['売上債権回転率'] = 0
            metrics['売上債権回転期間'] = 0
        
        if avg_inventory > 0:
            metrics['棚卸資産回転率'] = cogs / avg_inventory
            metrics['棚卸資産回転期間'] = 365 / metrics['棚卸資産回転率']
        else:
            metrics['棚卸資産回転率'] = 0
            metrics['棚卸資産回転期間'] = 0
        
        return metrics
    
    # ==================== 運転資本分析 ====================
    
    @st.cache_data(ttl=300)
    def calculate_working_capital(_self, pl_data: Dict, bs_data: Dict,
                                  bs_previous: Optional[Dict] = None,
                                  period_id: int = None) -> Dict:
        """
        運転資本分析を実行（キャッシュ付き）
        
        Args:
            pl_data: PLデータ
            bs_data: BSデータ
            bs_previous: 前期BSデータ
            period_id: 会計期間ID
            
        Returns:
            Dict: 運転資本指標
        """
        metrics = {}
        
        # 実績締月までのデータで計算
        actual_months = 12
        if _self.data_handler and period_id:
            latest_actual = _self.data_handler.get_latest_actual_month(period_id)
            actual_months = latest_actual
            cumulative_data = _self.data_handler.get_cumulative_actual_data(period_id, latest_actual)
            
            if cumulative_data:
                pl_data = {**pl_data, **cumulative_data}
        
        sales = pl_data.get('売上高', 0)
        cogs = pl_data.get('売上原価', 0)
        
        # BSデータ
        receivables = bs_data.get('売掛金', 0)
        inventory = bs_data.get('棚卸資産', 0)
        payables = bs_data.get('買掛金', 0)
        
        # 平均値
        if bs_previous:
            receivables_prev = bs_previous.get('売掛金', receivables)
            inventory_prev = bs_previous.get('棚卸資産', inventory)
            payables_prev = bs_previous.get('買掛金', payables)
            
            avg_receivables = (receivables + receivables_prev) / 2
            avg_inventory = (inventory + inventory_prev) / 2
            avg_payables = (payables + payables_prev) / 2
        else:
            avg_receivables = receivables
            avg_inventory = inventory
            avg_payables = payables
        
        # 回転期間
        if sales > 0:
            metrics['売上債権回転期間'] = (avg_receivables / (sales / 365))
        else:
            metrics['売上債権回転期間'] = 0
        
        if cogs > 0:
            metrics['棚卸資産回転期間'] = (avg_inventory / (cogs / 365))
            metrics['仕入債務回転期間'] = (avg_payables / (cogs / 365))
        else:
            metrics['棚卸資産回転期間'] = 0
            metrics['仕入債務回転期間'] = 0
        
        # CCC
        metrics['CCC'] = (metrics['売上債権回転期間'] + 
                         metrics['棚卸資産回転期間'] - 
                         metrics['仕入債務回転期間'])
        
        # 運転資本
        current_assets = bs_data.get('流動資産合計', 0)
        current_liabilities = bs_data.get('流動負債合計', 0)
        metrics['運転資本'] = current_assets - current_liabilities
        
        return metrics


# ==================== グローバル関数（後方互換性） ====================

_analyzer_instance = None

def get_financial_analyzer(data_handler=None):
    """シングルトンのFinancialAnalyzerを取得"""
    global _analyzer_instance
    if _analyzer_instance is None:
        _analyzer_instance = FinancialAnalyzer(data_handler)
    return _analyzer_instance
