"""
運転資本分析モジュール
CCC（Cash Conversion Cycle）、運転資本回転期間等を詳細計算
"""

import pandas as pd
import numpy as np
from typing import Dict, Optional, List
import sys


class WorkingCapitalAnalyzer:
    """運転資本分析クラス"""
    
    def __init__(self):
        pass
    
    def calculate_working_capital_metrics(self, pl_data: Dict, bs_data: Dict,
                                         bs_previous: Optional[Dict] = None,
                                         period_months: int = 12) -> Dict:
        """
        運転資本関連指標を計算
        
        Args:
            pl_data: PL データ
            bs_data: BS データ
            bs_previous: 前期BSデータ
            period_months: 期間（月数）
            
        Returns:
            Dict: 運転資本指標
        """
        try:
            metrics = {}
            
            # ============ 基本データ取得 ============
            
            # PLデータ
            sales = pl_data.get('売上高', 0)
            cogs = pl_data.get('売上原価', 0)
            
            # 月平均に換算
            monthly_sales = sales / period_months if period_months > 0 else sales
            monthly_cogs = cogs / period_months if period_months > 0 else cogs
            
            # BSデータ（流動資産）
            cash = bs_data.get('現金･預金合計', 0)
            receivables = bs_data.get('売掛金', 0)
            notes_receivable = bs_data.get('受取手形', 0)
            inventory = bs_data.get('棚卸資産', 0)
            other_current_assets = bs_data.get('その他流動資産', 0)
            
            # BSデータ（流動負債）
            payables = bs_data.get('買掛金', 0)
            notes_payable = bs_data.get('支払手形', 0)
            short_term_debt = bs_data.get('短期借入金', 0)
            other_current_liabilities = bs_data.get('その他流動負債', 0)
            
            # 平均値計算（前期データがあれば）
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
            
            # ============ 運転資本 ============
            
            current_assets = cash + receivables + notes_receivable + inventory + other_current_assets
            current_liabilities = payables + notes_payable + short_term_debt + other_current_liabilities
            
            working_capital = current_assets - current_liabilities
            metrics['運転資本'] = working_capital
            
            # 純運転資本（有利子負債を除く）
            net_working_capital = (receivables + inventory) - (payables + notes_payable)
            metrics['純運転資本'] = net_working_capital
            
            # ============ 回転期間分析 ============
            
            # 売上債権回転期間（日）= 売掛金 / (売上高 / 365)
            if sales > 0:
                receivables_days = (avg_receivables / (sales / 365))
                metrics['売上債権回転期間'] = receivables_days
            else:
                metrics['売上債権回転期間'] = 0
            
            # 棚卸資産回転期間（日）= 棚卸資産 / (売上原価 / 365)
            if cogs > 0:
                inventory_days = (avg_inventory / (cogs / 365))
                metrics['棚卸資産回転期間'] = inventory_days
            else:
                metrics['棚卸資産回転期間'] = 0
            
            # 仕入債務回転期間（日）= 買掛金 / (売上原価 / 365)
            # 注: 仕入高が理想だが、データがないため売上原価で代用
            if cogs > 0:
                payables_days = (avg_payables / (cogs / 365))
                metrics['仕入債務回転期間'] = payables_days
            else:
                metrics['仕入債務回転期間'] = 0
            
            # ============ CCC（Cash Conversion Cycle）============
            
            ccc = (metrics['売上債権回転期間'] + 
                   metrics['棚卸資産回転期間'] - 
                   metrics['仕入債務回転期間'])
            metrics['CCC'] = ccc
            
            # ============ 運転資本比率 ============
            
            # 運転資本比率 = 運転資本 / 売上高 × 100
            if sales > 0:
                metrics['運転資本比率'] = (working_capital / sales) * 100
            else:
                metrics['運転資本比率'] = 0
            
            # 運転資本回転率 = 売上高 / 運転資本
            if working_capital > 0:
                metrics['運転資本回転率'] = sales / working_capital
            else:
                metrics['運転資本回転率'] = 0
            
            # ============ 各項目の売上高比率 ============
            
            if sales > 0:
                metrics['売掛金_売上高比率'] = (receivables / sales) * 100
                metrics['棚卸資産_売上高比率'] = (inventory / sales) * 100
                metrics['買掛金_売上高比率'] = (payables / sales) * 100
            else:
                metrics['売掛金_売上高比率'] = 0
                metrics['棚卸資産_売上高比率'] = 0
                metrics['買掛金_売上高比率'] = 0
            
            # ============ キャッシュギャップ分析 ============
            
            # 現金ベースの運転資本
            cash_working_capital = cash + receivables + inventory - payables - notes_payable
            metrics['現金ベース運転資本'] = cash_working_capital
            
            # 現金化日数 = 売上債権回転期間 + 棚卸資産回転期間
            metrics['現金化日数'] = metrics['売上債権回転期間'] + metrics['棚卸資産回転期間']
            
            # 支払猶予日数 = 仕入債務回転期間
            metrics['支払猶予日数'] = metrics['仕入債務回転期間']
            
            return metrics
            
        except Exception as e:
            sys.stderr.write(f"❌ 運転資本分析エラー: {e}\n")
            import traceback
            traceback.print_exc(file=sys.stderr)
            sys.stderr.flush()
            return {}
    
    def analyze_working_capital_trend(self, historical_data: List[Dict]) -> Dict:
        """
        運転資本の推移分析
        
        Args:
            historical_data: 過去データのリスト
            
        Returns:
            Dict: トレンド分析結果
        """
        if not historical_data or len(historical_data) < 2:
            return {}
        
        try:
            analysis = {}
            
            # CCCの推移
            ccc_values = [d.get('CCC', 0) for d in historical_data]
            
            # トレンド判定
            if len(ccc_values) >= 3:
                recent_avg = np.mean(ccc_values[-3:])
                initial_avg = np.mean(ccc_values[:3])
                
                ccc_change = recent_avg - initial_avg
                ccc_change_pct = (ccc_change / initial_avg * 100) if initial_avg != 0 else 0
                
                analysis['CCC推移'] = {
                    '初期平均': initial_avg,
                    '最近平均': recent_avg,
                    '変化': ccc_change,
                    '変化率': ccc_change_pct,
                    'トレンド': 'improving' if ccc_change < 0 else ('deteriorating' if ccc_change > 0 else 'stable')
                }
            
            # 運転資本の推移
            wc_values = [d.get('運転資本', 0) for d in historical_data]
            
            if len(wc_values) >= 2:
                wc_change = wc_values[-1] - wc_values[0]
                wc_change_pct = (wc_change / wc_values[0] * 100) if wc_values[0] != 0 else 0
                
                analysis['運転資本推移'] = {
                    '期首': wc_values[0],
                    '期末': wc_values[-1],
                    '変化': wc_change,
                    '変化率': wc_change_pct
                }
            
            return analysis
            
        except Exception as e:
            sys.stderr.write(f"❌ 運転資本トレンド分析エラー: {e}\n")
            return {}
    
    def get_working_capital_recommendations(self, metrics: Dict) -> List[Dict]:
        """
        運転資本改善の提案を生成
        
        Args:
            metrics: 計算済み運転資本指標
            
        Returns:
            List[Dict]: 改善提案のリスト
        """
        recommendations = []
        
        ccc = metrics.get('CCC', 0)
        receivables_days = metrics.get('売上債権回転期間', 0)
        inventory_days = metrics.get('棚卸資産回転期間', 0)
        payables_days = metrics.get('仕入債務回転期間', 0)
        
        # CCCが長い（資金繰り悪化）
        if ccc > 90:
            recommendations.append({
                'カテゴリ': '資金繰り',
                'レベル': 'critical',
                'アイコン': '🔴',
                'タイトル': 'CCCが90日超過',
                'メッセージ': f'現在のCCC: {ccc:.0f}日。資金繰りが圧迫されています。',
                'アクション': [
                    '売掛金回収の早期化（回収サイト短縮交渉）',
                    '在庫回転率の向上（適正在庫の維持）',
                    '仕入支払条件の延長交渉'
                ]
            })
        elif ccc > 60:
            recommendations.append({
                'カテゴリ': '資金繰り',
                'レベル': 'warning',
                'アイコン': '🟡',
                'タイトル': 'CCC改善余地あり',
                'メッセージ': f'現在のCCC: {ccc:.0f}日。標準的ですが改善の余地があります。',
                'アクション': [
                    '売掛金管理の強化',
                    '在庫管理の効率化'
                ]
            })
        
        # 売上債権回転期間が長い
        if receivables_days > 60:
            recommendations.append({
                'カテゴリ': '売掛金管理',
                'レベル': 'warning',
                'アイコン': '🟡',
                'タイトル': '売掛金回収期間が長い',
                'メッセージ': f'現在の回収期間: {receivables_days:.0f}日',
                'アクション': [
                    '回収サイトの短縮交渉（60日→45日など）',
                    '早期入金割引制度の導入',
                    '与信管理の厳格化',
                    '請求書発行の迅速化'
                ]
            })
        
        # 棚卸資産回転期間が長い
        if inventory_days > 60:
            recommendations.append({
                'カテゴリ': '在庫管理',
                'レベル': 'warning',
                'アイコン': '🟡',
                'タイトル': '在庫回転期間が長い',
                'メッセージ': f'現在の在庫回転期間: {inventory_days:.0f}日',
                'アクション': [
                    'ABC分析による適正在庫の設定',
                    '死蔵在庫の処分',
                    'ジャストインタイム発注の導入',
                    '需要予測精度の向上'
                ]
            })
        
        # 仕入債務回転期間が短い（支払いが早すぎる）
        if payables_days < 30:
            recommendations.append({
                'カテゴリ': '買掛金管理',
                'レベル': 'info',
                'アイコン': '💡',
                'タイトル': '支払条件の改善余地',
                'メッセージ': f'現在の支払期間: {payables_days:.0f}日',
                'アクション': [
                    '仕入先との支払サイト延長交渉',
                    '手形・月末締め翌月払いの活用',
                    '取引条件の見直し'
                ]
            })
        
        # CCCが短い（良好）
        if ccc < 30:
            recommendations.append({
                'カテゴリ': '資金繰り',
                'レベル': 'success',
                'アイコン': '🟢',
                'タイトル': 'CCCが良好',
                'メッセージ': f'現在のCCC: {ccc:.0f}日。資金効率が高い状態です。',
                'アクション': [
                    '現状の運転資本管理体制を維持',
                    '余剰資金の戦略的活用を検討'
                ]
            })
        
        return recommendations
    
    def create_working_capital_dashboard_data(self, metrics: Dict) -> Dict:
        """
        ダッシュボード表示用データを作成
        
        Args:
            metrics: 計算済み指標
            
        Returns:
            Dict: ダッシュボード用データ
        """
        dashboard = {
            'summary': {
                'CCC': {
                    '値': metrics.get('CCC', 0),
                    '単位': '日',
                    'ベンチマーク': 60,
                    '説明': '現金化サイクル'
                },
                '売上債権回転期間': {
                    '値': metrics.get('売上債権回転期間', 0),
                    '単位': '日',
                    'ベンチマーク': 45,
                    '説明': '売掛金回収期間'
                },
                '棚卸資産回転期間': {
                    '値': metrics.get('棚卸資産回転期間', 0),
                    '単位': '日',
                    'ベンチマーク': 30,
                    '説明': '在庫保有期間'
                },
                '仕入債務回転期間': {
                    '値': metrics.get('仕入債務回転期間', 0),
                    '単位': '日',
                    'ベンチマーク': 45,
                    '説明': '買掛金支払期間'
                }
            },
            'working_capital': {
                '運転資本': metrics.get('運転資本', 0),
                '純運転資本': metrics.get('純運転資本', 0),
                '運転資本比率': metrics.get('運転資本比率', 0),
                '運転資本回転率': metrics.get('運転資本回転率', 0)
            },
            'components': {
                '売掛金比率': metrics.get('売掛金_売上高比率', 0),
                '棚卸資産比率': metrics.get('棚卸資産_売上高比率', 0),
                '買掛金比率': metrics.get('買掛金_売上高比率', 0)
            }
        }
        
        return dashboard
    
    def format_for_display(self, metrics: Dict) -> pd.DataFrame:
        """
        表示用DataFrameに変換
        
        Args:
            metrics: 計算済み指標
            
        Returns:
            DataFrame: 表示用
        """
        data = []
        
        display_metrics = {
            'CCC': ('CCC（キャッシュ変換サイクル）', '日', '短いほど良好'),
            '売上債権回転期間': ('売上債権回転期間', '日', '短いほど良好'),
            '棚卸資産回転期間': ('棚卸資産回転期間', '日', '短いほど良好'),
            '仕入債務回転期間': ('仕入債務回転期間', '日', '長いほど良好'),
            '運転資本': ('運転資本', '円', 'プラスが健全'),
            '純運転資本': ('純運転資本', '円', 'プラスが健全'),
            '運転資本比率': ('運転資本比率', '%', '適正水準10-20%'),
            '運転資本回転率': ('運転資本回転率', '回', '高いほど良好')
        }
        
        for key, (name, unit, note) in display_metrics.items():
            if key in metrics:
                value = metrics[key]
                
                if unit == '円':
                    formatted_value = f"¥{value:,.0f}"
                elif unit == '日':
                    formatted_value = f"{value:.1f}"
                elif unit == '%':
                    formatted_value = f"{value:.1f}"
                elif unit == '回':
                    formatted_value = f"{value:.2f}"
                else:
                    formatted_value = f"{value:.2f}"
                
                data.append({
                    '指標名': name,
                    '値': formatted_value,
                    '単位': unit,
                    '備考': note
                })
        
        return pd.DataFrame(data)
