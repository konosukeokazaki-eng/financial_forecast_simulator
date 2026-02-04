"""
経営指標計算モジュール
ROE, ROA, 流動比率, 自己資本比率, 各種利益率等を正確に計算
"""

import pandas as pd
from typing import Dict, Optional
import sys


class MetricsCalculator:
    """経営指標計算クラス"""
    
    def __init__(self):
        pass
    
    def calculate_all_metrics(self, pl_data: Dict, bs_data: Dict, 
                             bs_previous: Optional[Dict] = None) -> Dict:
        """
        すべての経営指標を計算
        
        Args:
            pl_data: PL（損益計算書）データ
            bs_data: BS（貸借対照表）データ
            bs_previous: 前期BSデータ（平均計算用）
            
        Returns:
            Dict: 各種経営指標
        """
        try:
            metrics = {}
            
            # ============ 収益性指標 ============
            metrics.update(self.calculate_profitability_ratios(pl_data, bs_data, bs_previous))
            
            # ============ 安全性指標 ============
            metrics.update(self.calculate_safety_ratios(bs_data))
            
            # ============ 効率性指標 ============
            metrics.update(self.calculate_efficiency_ratios(pl_data, bs_data, bs_previous))
            
            # ============ 成長性指標 ============
            if bs_previous:
                metrics.update(self.calculate_growth_ratios(pl_data, bs_data, bs_previous))
            
            # ============ キャッシュフロー指標 ============
            # cf_dataがあれば追加
            
            return metrics
            
        except Exception as e:
            sys.stderr.write(f"❌ 経営指標計算エラー: {e}\n")
            import traceback
            traceback.print_exc(file=sys.stderr)
            sys.stderr.flush()
            return {}
    
    def calculate_profitability_ratios(self, pl_data: Dict, bs_data: Dict, 
                                      bs_previous: Optional[Dict] = None) -> Dict:
        """
        収益性指標を計算
        
        Returns:
            Dict: 売上高総利益率、営業利益率、経常利益率、当期純利益率、ROE、ROA
        """
        metrics = {}
        
        # PLデータ取得
        sales = pl_data.get('売上高', 0)
        cogs = pl_data.get('売上原価', 0)
        gross_profit = sales - cogs
        operating_profit = pl_data.get('営業損益金額', gross_profit - pl_data.get('販売費及び一般管理費', 0))
        ordinary_profit = pl_data.get('経常損益金額', operating_profit)
        net_income = pl_data.get('当期純利益', ordinary_profit)
        
        # BSデータ取得
        total_assets = bs_data.get('資産合計', 0)
        equity = bs_data.get('純資産合計', 0)
        
        # 平均総資産・平均純資産（前期データがあれば）
        if bs_previous:
            total_assets_prev = bs_previous.get('資産合計', total_assets)
            equity_prev = bs_previous.get('純資産合計', equity)
            avg_total_assets = (total_assets + total_assets_prev) / 2
            avg_equity = (equity + equity_prev) / 2
        else:
            avg_total_assets = total_assets
            avg_equity = equity
        
        # 売上高総利益率（粗利率）
        if sales > 0:
            metrics['売上高総利益率'] = (gross_profit / sales) * 100
        else:
            metrics['売上高総利益率'] = 0
        
        # 売上高営業利益率
        if sales > 0:
            metrics['売上高営業利益率'] = (operating_profit / sales) * 100
        else:
            metrics['売上高営業利益率'] = 0
        
        # 売上高経常利益率
        if sales > 0:
            metrics['売上高経常利益率'] = (ordinary_profit / sales) * 100
        else:
            metrics['売上高経常利益率'] = 0
        
        # 売上高当期純利益率
        if sales > 0:
            metrics['売上高当期純利益率'] = (net_income / sales) * 100
        else:
            metrics['売上高当期純利益率'] = 0
        
        # ROE（自己資本利益率）= 当期純利益 / 自己資本 × 100
        if avg_equity > 0:
            metrics['ROE'] = (net_income / avg_equity) * 100
        else:
            metrics['ROE'] = 0
        
        # ROA（総資産利益率）= 当期純利益 / 総資産 × 100
        if avg_total_assets > 0:
            metrics['ROA'] = (net_income / avg_total_assets) * 100
        else:
            metrics['ROA'] = 0
        
        # 総資産経常利益率
        if avg_total_assets > 0:
            metrics['総資産経常利益率'] = (ordinary_profit / avg_total_assets) * 100
        else:
            metrics['総資産経常利益率'] = 0
        
        return metrics
    
    def calculate_safety_ratios(self, bs_data: Dict) -> Dict:
        """
        安全性指標を計算
        
        Returns:
            Dict: 流動比率、当座比率、固定比率、自己資本比率、負債比率
        """
        metrics = {}
        
        # BSデータ取得
        current_assets = bs_data.get('流動資産合計', 0)
        current_liabilities = bs_data.get('流動負債合計', 0)
        quick_assets = bs_data.get('当座資産', current_assets - bs_data.get('棚卸資産', 0))
        fixed_assets = bs_data.get('固定資産合計', 0)
        total_assets = bs_data.get('資産合計', current_assets + fixed_assets)
        total_liabilities = bs_data.get('負債合計', 0)
        equity = bs_data.get('純資産合計', 0)
        
        # 流動比率 = 流動資産 / 流動負債 × 100
        if current_liabilities > 0:
            metrics['流動比率'] = (current_assets / current_liabilities) * 100
        else:
            metrics['流動比率'] = 999
        
        # 当座比率 = 当座資産 / 流動負債 × 100
        if current_liabilities > 0:
            metrics['当座比率'] = (quick_assets / current_liabilities) * 100
        else:
            metrics['当座比率'] = 999
        
        # 固定比率 = 固定資産 / 自己資本 × 100
        if equity > 0:
            metrics['固定比率'] = (fixed_assets / equity) * 100
        else:
            metrics['固定比率'] = 999
        
        # 自己資本比率 = 自己資本 / 総資産 × 100
        if total_assets > 0:
            metrics['自己資本比率'] = (equity / total_assets) * 100
        else:
            metrics['自己資本比率'] = 0
        
        # 負債比率 = 負債合計 / 自己資本 × 100
        if equity > 0:
            metrics['負債比率'] = (total_liabilities / equity) * 100
        else:
            metrics['負債比率'] = 999
        
        # インタレストカバレッジレシオ = 営業利益 / 支払利息
        # （PLデータが必要なため、ここでは計算しない）
        
        return metrics
    
    def calculate_efficiency_ratios(self, pl_data: Dict, bs_data: Dict, 
                                   bs_previous: Optional[Dict] = None) -> Dict:
        """
        効率性指標を計算
        
        Returns:
            Dict: 総資産回転率、売上債権回転率、棚卸資産回転率、固定資産回転率
        """
        metrics = {}
        
        # PLデータ
        sales = pl_data.get('売上高', 0)
        cogs = pl_data.get('売上原価', 0)
        
        # BSデータ
        total_assets = bs_data.get('資産合計', 0)
        receivables = bs_data.get('売掛金', 0)
        inventory = bs_data.get('棚卸資産', 0)
        fixed_assets = bs_data.get('固定資産合計', 0)
        
        # 平均値計算（前期データがあれば）
        if bs_previous:
            total_assets_prev = bs_previous.get('資産合計', total_assets)
            receivables_prev = bs_previous.get('売掛金', receivables)
            inventory_prev = bs_previous.get('棚卸資産', inventory)
            fixed_assets_prev = bs_previous.get('固定資産合計', fixed_assets)
            
            avg_total_assets = (total_assets + total_assets_prev) / 2
            avg_receivables = (receivables + receivables_prev) / 2
            avg_inventory = (inventory + inventory_prev) / 2
            avg_fixed_assets = (fixed_assets + fixed_assets_prev) / 2
        else:
            avg_total_assets = total_assets
            avg_receivables = receivables
            avg_inventory = inventory
            avg_fixed_assets = fixed_assets
        
        # 総資産回転率 = 売上高 / 総資産（回）
        if avg_total_assets > 0:
            metrics['総資産回転率'] = sales / avg_total_assets
        else:
            metrics['総資産回転率'] = 0
        
        # 売上債権回転率 = 売上高 / 売掛金（回）
        if avg_receivables > 0:
            metrics['売上債権回転率'] = sales / avg_receivables
            # 売上債権回転期間（日）
            metrics['売上債権回転期間'] = 365 / metrics['売上債権回転率']
        else:
            metrics['売上債権回転率'] = 0
            metrics['売上債権回転期間'] = 0
        
        # 棚卸資産回転率 = 売上原価 / 棚卸資産（回）
        if avg_inventory > 0:
            metrics['棚卸資産回転率'] = cogs / avg_inventory
            # 棚卸資産回転期間（日）
            metrics['棚卸資産回転期間'] = 365 / metrics['棚卸資産回転率']
        else:
            metrics['棚卸資産回転率'] = 0
            metrics['棚卸資産回転期間'] = 0
        
        # 固定資産回転率 = 売上高 / 固定資産（回）
        if avg_fixed_assets > 0:
            metrics['固定資産回転率'] = sales / avg_fixed_assets
        else:
            metrics['固定資産回転率'] = 0
        
        return metrics
    
    def calculate_growth_ratios(self, pl_data: Dict, bs_data: Dict, 
                               bs_previous: Dict) -> Dict:
        """
        成長性指標を計算
        
        Returns:
            Dict: 売上高成長率、営業利益成長率、総資産成長率等
        """
        metrics = {}
        
        # 当期データ
        sales = pl_data.get('売上高', 0)
        operating_profit = pl_data.get('営業損益金額', 0)
        total_assets = bs_data.get('資産合計', 0)
        equity = bs_data.get('純資産合計', 0)
        
        # 前期データ（仮に前期PLデータがないため、成長率計算は制限される）
        # 実際には前期PLも必要
        total_assets_prev = bs_previous.get('資産合計', 0)
        equity_prev = bs_previous.get('純資産合計', 0)
        
        # 総資産成長率
        if total_assets_prev > 0:
            metrics['総資産成長率'] = ((total_assets - total_assets_prev) / total_assets_prev) * 100
        else:
            metrics['総資産成長率'] = 0
        
        # 自己資本成長率
        if equity_prev > 0:
            metrics['自己資本成長率'] = ((equity - equity_prev) / equity_prev) * 100
        else:
            metrics['自己資本成長率'] = 0
        
        return metrics
    
    def format_metrics_for_display(self, metrics: Dict) -> pd.DataFrame:
        """
        経営指標を表示用DataFrameに変換
        
        Args:
            metrics: 計算済み経営指標
            
        Returns:
            DataFrame: 表示用
        """
        data = []
        
        # カテゴリ別に整理
        categories = {
            '収益性指標': [
                '売上高総利益率', '売上高営業利益率', '売上高経常利益率', 
                '売上高当期純利益率', 'ROE', 'ROA', '総資産経常利益率'
            ],
            '安全性指標': [
                '流動比率', '当座比率', '固定比率', '自己資本比率', '負債比率'
            ],
            '効率性指標': [
                '総資産回転率', '売上債権回転率', '売上債権回転期間',
                '棚卸資産回転率', '棚卸資産回転期間', '固定資産回転率'
            ],
            '成長性指標': [
                '総資産成長率', '自己資本成長率'
            ]
        }
        
        for category, metric_names in categories.items():
            for metric_name in metric_names:
                if metric_name in metrics:
                    value = metrics[metric_name]
                    
                    # 単位を決定
                    if '回転率' in metric_name and '期間' not in metric_name:
                        unit = '回'
                        formatted_value = f"{value:.2f}"
                    elif '期間' in metric_name:
                        unit = '日'
                        formatted_value = f"{value:.1f}"
                    elif metric_name in ['ROE', 'ROA']:
                        unit = '%'
                        formatted_value = f"{value:.2f}"
                    else:
                        unit = '%'
                        formatted_value = f"{value:.1f}"
                    
                    data.append({
                        'カテゴリ': category,
                        '指標名': metric_name,
                        '値': formatted_value,
                        '単位': unit
                    })
        
        return pd.DataFrame(data)
    
    def get_metric_benchmarks(self) -> Dict:
        """
        業界標準・ベンチマーク値を取得
        
        Returns:
            Dict: 各指標の業界平均値
        """
        return {
            '売上高総利益率': {'良好': 40, '標準': 30, '注意': 20},
            '売上高営業利益率': {'良好': 10, '標準': 5, '注意': 0},
            '売上高経常利益率': {'良好': 10, '標準': 5, '注意': 0},
            'ROE': {'良好': 10, '標準': 5, '注意': 0},
            'ROA': {'良好': 5, '標準': 3, '注意': 0},
            '流動比率': {'良好': 200, '標準': 150, '注意': 100},
            '当座比率': {'良好': 100, '標準': 80, '注意': 50},
            '自己資本比率': {'良好': 50, '標準': 30, '注意': 20},
            '負債比率': {'良好': 100, '標準': 200, '注意': 300},
            '総資産回転率': {'良好': 1.5, '標準': 1.0, '注意': 0.5},
        }
    
    def evaluate_metrics(self, metrics: Dict) -> Dict:
        """
        経営指標を評価（良好/標準/注意）
        
        Args:
            metrics: 計算済み経営指標
            
        Returns:
            Dict: 評価結果
        """
        benchmarks = self.get_metric_benchmarks()
        evaluations = {}
        
        for metric_name, value in metrics.items():
            if metric_name in benchmarks:
                bench = benchmarks[metric_name]
                
                if metric_name == '負債比率':
                    # 負債比率は低いほど良い
                    if value <= bench['良好']:
                        evaluations[metric_name] = {'評価': '良好', 'レベル': 3, 'アイコン': '🟢'}
                    elif value <= bench['標準']:
                        evaluations[metric_name] = {'評価': '標準', 'レベル': 2, 'アイコン': '🟡'}
                    else:
                        evaluations[metric_name] = {'評価': '要改善', 'レベル': 1, 'アイコン': '🔴'}
                else:
                    # その他は高いほど良い
                    if value >= bench['良好']:
                        evaluations[metric_name] = {'評価': '良好', 'レベル': 3, 'アイコン': '🟢'}
                    elif value >= bench['標準']:
                        evaluations[metric_name] = {'評価': '標準', 'レベル': 2, 'アイコン': '🟡'}
                    else:
                        evaluations[metric_name] = {'評価': '要改善', 'レベル': 1, 'アイコン': '🔴'}
        
        return evaluations
