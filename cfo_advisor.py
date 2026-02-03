"""
CFOアドバイザリーエンジン (CFO Advisor)
財務指標から自動的にアドバイスを生成
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional
import sys


class CFOAdvisor:
    """CFOアドバイザリーエンジン"""
    
    def __init__(self):
        """初期化"""
        self.advisory_rules = self._initialize_rules()
    
    def _initialize_rules(self) -> List[Dict]:
        """アドバイザリールールを初期化"""
        return [
            {'id': 'deficit_risk', 'priority': 1, 'check': self._check_deficit_risk},
            {'id': 'cash_shortage_risk', 'priority': 1, 'check': self._check_cash_shortage},
            {'id': 'profitability_low', 'priority': 2, 'check': self._check_profitability},
            {'id': 'working_capital_deterioration', 'priority': 2, 'check': self._check_working_capital},
            {'id': 'investment_opportunity', 'priority': 3, 'check': self._check_investment_opportunity}
        ]
    
    def generate_advisory_messages(self, metrics: Dict) -> List[Dict]:
        """
        財務指標から自動的にアドバイスを生成
        
        Args:
            metrics: 財務指標の辞書
            
        Returns:
            List[Dict]: アドバイスメッセージのリスト
        """
        try:
            sys.stderr.write("💡 CFOアドバイス生成開始\n")
            sys.stderr.flush()
            
            messages = []
            
            # 各ルールをチェック
            for rule in self.advisory_rules:
                try:
                    message = rule['check'](metrics)
                    if message:
                        message['priority'] = rule['priority']
                        message['rule_id'] = rule['id']
                        messages.append(message)
                except Exception as e:
                    sys.stderr.write(f"⚠️ ルール{rule['id']}エラー: {e}\n")
                    sys.stderr.flush()
                    continue
            
            # 優先度でソート
            messages.sort(key=lambda x: x['priority'])
            
            sys.stderr.write(f"✅ {len(messages)}件のアドバイス生成\n")
            sys.stderr.flush()
            
            return messages
            
        except Exception as e:
            sys.stderr.write(f"❌ アドバイス生成エラー: {e}\n")
            import traceback
            traceback.print_exc(file=sys.stderr)
            sys.stderr.flush()
            return []
    
    def _check_deficit_risk(self, metrics: Dict) -> Optional[Dict]:
        """赤字リスクをチェック"""
        try:
            current_sales = metrics.get('sales', 0)
            breakeven_sales = metrics.get('breakeven_sales', 0)
            marginal_profit_rate = metrics.get('marginal_profit_rate', 0)
            fixed_costs = metrics.get('fixed_costs', 0)
            
            if breakeven_sales == 0 or current_sales == 0:
                return None
            
            # 売上が損益分岐点を下回る
            if current_sales < breakeven_sales:
                shortage = breakeven_sales - current_sales
                shortage_rate = shortage / breakeven_sales * 100
                
                # 対策の効果試算
                price_increase_10pct = current_sales * 0.10 * 0.95  # 5%需要減を考慮
                cost_reduction_10pct = fixed_costs * 0.10
                
                return {
                    'level': 'critical',
                    'icon': '🚨',
                    'title': '赤字リスク',
                    'message': f'固定費回収に月売上{breakeven_sales:,.0f}円必要ですが、現在{current_sales:,.0f}円で{shortage:,.0f}円（{shortage_rate:.1f}%）不足しています。',
                    'details': {
                        'current_sales': current_sales,
                        'breakeven_sales': breakeven_sales,
                        'shortage': shortage,
                        'shortage_rate': shortage_rate
                    },
                    'actions': [
                        {
                            'title': '値上げ',
                            'detail': f'10%値上げで月+{price_increase_10pct:,.0f}円（数量減5%考慮）',
                            'impact': 'high',
                            'difficulty': 'medium',
                            'timeframe': '即時〜1ヶ月'
                        },
                        {
                            'title': '固定費削減',
                            'detail': f'人件費・家賃見直しで月-{cost_reduction_10pct:,.0f}円（10%削減）',
                            'impact': 'high',
                            'difficulty': 'medium',
                            'timeframe': '1〜3ヶ月'
                        },
                        {
                            'title': '商品構成見直し',
                            'detail': f'高粗利商品の売上比率を向上（限界利益率{marginal_profit_rate*100:.1f}% → 目標{(marginal_profit_rate+0.05)*100:.1f}%）',
                            'impact': 'medium',
                            'difficulty': 'low',
                            'timeframe': '1〜2ヶ月'
                        }
                    ]
                }
            
            # 安全余裕率が低い（20%未満）
            safety_rate = metrics.get('safety_rate', 0)
            if 0 < safety_rate < 0.20:
                return {
                    'level': 'warning',
                    'icon': '⚠️',
                    'title': '安全余裕率が低い',
                    'message': f'安全余裕率{safety_rate*100:.1f}%は危険水域です。売上が{safety_rate*100:.1f}%減少すると赤字になります。',
                    'details': {
                        'safety_rate': safety_rate,
                        'safe_threshold': 0.20
                    },
                    'actions': [
                        {
                            'title': '売上増加',
                            'detail': f'目標月+{current_sales*0.10:,.0f}円（10%増）で安全余裕率を30%に改善',
                            'impact': 'high',
                            'difficulty': 'high',
                            'timeframe': '3〜6ヶ月'
                        },
                        {
                            'title': '固定費削減',
                            'detail': '損益分岐点を下げて安全余裕率を向上',
                            'impact': 'medium',
                            'difficulty': 'medium',
                            'timeframe': '1〜3ヶ月'
                        }
                    ]
                }
            
            return None
            
        except Exception as e:
            return None
    
    def _check_cash_shortage(self, metrics: Dict) -> Optional[Dict]:
        """資金不足リスクをチェック"""
        try:
            cash_runway = metrics.get('cash_runway_months', 0)
            
            if cash_runway == 0:
                return None
            
            # 資金耐久月数が3ヶ月未満
            if cash_runway < 3:
                return {
                    'level': 'critical',
                    'icon': '🚨',
                    'title': '資金危機',
                    'message': f'資金耐久月数が{cash_runway:.1f}ヶ月と危険水域です。即座の資金調達が必要です。',
                    'details': {
                        'cash_runway': cash_runway,
                        'critical_threshold': 3
                    },
                    'actions': [
                        {
                            'title': '融資実行',
                            'detail': '銀行融資または日本政策金融公庫の活用',
                            'impact': 'high',
                            'difficulty': 'medium',
                            'timeframe': '即時〜1ヶ月'
                        },
                        {
                            'title': '売掛金早期回収',
                            'detail': '回収サイト短縮交渉（90日→60日）',
                            'impact': 'medium',
                            'difficulty': 'medium',
                            'timeframe': '即時〜1ヶ月'
                        },
                        {
                            'title': '役員報酬減額',
                            'detail': '一時的な役員報酬の削減',
                            'impact': 'medium',
                            'difficulty': 'low',
                            'timeframe': '即時'
                        }
                    ]
                }
            
            # 資金耐久月数が6ヶ月未満
            elif cash_runway < 6:
                return {
                    'level': 'warning',
                    'icon': '⚠️',
                    'title': '資金注意',
                    'message': f'資金耐久月数が{cash_runway:.1f}ヶ月です。資金繰りに注意が必要です。',
                    'details': {
                        'cash_runway': cash_runway,
                        'safe_threshold': 6
                    },
                    'actions': [
                        {
                            'title': '営業CFの改善',
                            'detail': '売掛金回収の強化と在庫圧縮',
                            'impact': 'high',
                            'difficulty': 'medium',
                            'timeframe': '1〜2ヶ月'
                        },
                        {
                            'title': '固定費の見直し',
                            'detail': '不要な支出の削減',
                            'impact': 'medium',
                            'difficulty': 'low',
                            'timeframe': '即時〜1ヶ月'
                        }
                    ]
                }
            
            return None
            
        except Exception as e:
            return None
    
    def _check_profitability(self, metrics: Dict) -> Optional[Dict]:
        """収益性をチェック"""
        try:
            marginal_profit_rate = metrics.get('marginal_profit_rate', 0)
            
            if marginal_profit_rate == 0:
                return None
            
            # 限界利益率が30%未満
            if marginal_profit_rate < 0.30:
                return {
                    'level': 'warning',
                    'icon': '⚠️',
                    'title': '利益率改善必要',
                    'message': f'限界利益率{marginal_profit_rate*100:.1f}%は業界平均(35%)を下回っています。収益構造の改善が必要です。',
                    'details': {
                        'marginal_profit_rate': marginal_profit_rate,
                        'industry_average': 0.35,
                        'gap': 0.35 - marginal_profit_rate
                    },
                    'actions': [
                        {
                            'title': '値上げ',
                            'detail': '価格弾力性を分析して適正価格を設定',
                            'impact': 'high',
                            'difficulty': 'medium',
                            'timeframe': '1〜2ヶ月'
                        },
                        {
                            'title': '仕入先交渉',
                            'detail': '変動費率の削減（仕入価格の見直し）',
                            'impact': 'medium',
                            'difficulty': 'medium',
                            'timeframe': '1〜3ヶ月'
                        },
                        {
                            'title': '高付加価値化',
                            'detail': '商品・サービスの差別化で価格競争から脱却',
                            'impact': 'high',
                            'difficulty': 'high',
                            'timeframe': '3〜6ヶ月'
                        }
                    ]
                }
            
            return None
            
        except Exception as e:
            return None
    
    def _check_working_capital(self, metrics: Dict) -> Optional[Dict]:
        """運転資本をチェック"""
        try:
            operating_profit = metrics.get('operating_profit', 0)
            operating_cf = metrics.get('operating_cf', 0)
            
            # 黒字倒産リスク
            if operating_profit > 0 and operating_cf < 0:
                cf_gap = abs(operating_cf)
                
                return {
                    'level': 'critical',
                    'icon': '🚨',
                    'title': '黒字倒産リスク',
                    'message': f'営業利益は{operating_profit:,.0f}円の黒字ですが、営業CFは{operating_cf:,.0f}円の赤字です。売掛金・在庫の増加が現金を圧迫しています。',
                    'details': {
                        'operating_profit': operating_profit,
                        'operating_cf': operating_cf,
                        'cf_gap': cf_gap
                    },
                    'actions': [
                        {
                            'title': '売掛金の早期回収',
                            'detail': '回収サイト短縮交渉（90日→60日）または前受金の活用',
                            'impact': 'high',
                            'difficulty': 'medium',
                            'timeframe': '即時〜1ヶ月'
                        },
                        {
                            'title': '在庫の圧縮',
                            'detail': '発注量の最適化と在庫回転率の改善',
                            'impact': 'high',
                            'difficulty': 'medium',
                            'timeframe': '1〜2ヶ月'
                        },
                        {
                            'title': 'サブスクモデル化',
                            'detail': '前受金モデルへの転換でCF改善',
                            'impact': 'high',
                            'difficulty': 'high',
                            'timeframe': '3〜6ヶ月'
                        }
                    ]
                }
            
            return None
            
        except Exception as e:
            return None
    
    def _check_investment_opportunity(self, metrics: Dict) -> Optional[Dict]:
        """投資機会をチェック"""
        try:
            cash_runway = metrics.get('cash_runway_months', 0)
            marginal_profit_rate = metrics.get('marginal_profit_rate', 0)
            operating_profit = metrics.get('operating_profit', 0)
            
            # 投資余力の判定
            if cash_runway > 12 and marginal_profit_rate > 0.35 and operating_profit > 0:
                # 投資可能額の試算
                investable_amount = operating_profit * 0.5
                
                return {
                    'level': 'success',
                    'icon': '🟢',
                    'title': '成長投資の好機',
                    'message': f'財務基盤が盤石です（資金耐久{cash_runway:.1f}ヶ月、限界利益率{marginal_profit_rate*100:.1f}%）。次なる成長に向けた投資を検討できます。',
                    'details': {
                        'cash_runway': cash_runway,
                        'marginal_profit_rate': marginal_profit_rate,
                        'operating_profit': operating_profit,
                        'investable_amount': investable_amount
                    },
                    'actions': [
                        {
                            'title': '新規採用',
                            'detail': f'営業2名・エンジニア1名の採用（月額投資: 約{investable_amount*0.4:,.0f}円）',
                            'impact': 'high',
                            'difficulty': 'medium',
                            'timeframe': '1〜3ヶ月'
                        },
                        {
                            'title': 'マーケティング投資',
                            'detail': f'Web広告・SEO強化（月額投資: 約{investable_amount*0.3:,.0f}円）',
                            'impact': 'high',
                            'difficulty': 'low',
                            'timeframe': '即時〜1ヶ月'
                        },
                        {
                            'title': '新規事業開発',
                            'detail': f'新商品・新サービスの開発（月額投資: 約{investable_amount*0.3:,.0f}円）',
                            'impact': 'high',
                            'difficulty': 'high',
                            'timeframe': '3〜6ヶ月'
                        }
                    ]
                }
            
            return None
            
        except Exception as e:
            return None
