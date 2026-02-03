"""
CFOアドバイザリーエンジン
"""

from typing import Dict, List, Optional


class CFOAdvisor:
    
    def __init__(self):
        self.advisory_rules = [
            {'id': 'deficit_risk', 'priority': 1, 'check': self._check_deficit_risk},
            {'id': 'cash_shortage_risk', 'priority': 1, 'check': self._check_cash_shortage},
            {'id': 'profitability_low', 'priority': 2, 'check': self._check_profitability},
            {'id': 'working_capital_deterioration', 'priority': 2, 'check': self._check_working_capital},
            {'id': 'investment_opportunity', 'priority': 3, 'check': self._check_investment_opportunity}
        ]
    
    def generate_advisory_messages(self, metrics: Dict) -> List[Dict]:
        messages = []
        for rule in self.advisory_rules:
            try:
                message = rule['check'](metrics)
                if message:
                    message['priority'] = rule['priority']
                    message['rule_id'] = rule['id']
                    messages.append(message)
            except:
                continue
        messages.sort(key=lambda x: x['priority'])
        return messages
    
    def _check_deficit_risk(self, metrics: Dict) -> Optional[Dict]:
        current_sales = metrics.get('sales', 0)
        breakeven_sales = metrics.get('breakeven_sales', 0)
        marginal_profit_rate = metrics.get('marginal_profit_rate', 0)
        fixed_costs = metrics.get('fixed_costs', 0)
        
        if breakeven_sales == 0 or current_sales == 0:
            return None
        
        if current_sales < breakeven_sales:
            shortage = breakeven_sales - current_sales
            shortage_rate = shortage / breakeven_sales * 100
            price_increase_10pct = current_sales * 0.10 * 0.95
            cost_reduction_10pct = fixed_costs * 0.10
            
            return {
                'level': 'critical',
                'icon': '🚨',
                'title': '赤字リスク',
                'message': f'固定費回収に月売上{breakeven_sales:,.0f}円必要ですが、現在{current_sales:,.0f}円で{shortage:,.0f}円（{shortage_rate:.1f}%）不足しています。',
                'actions': [
                    {'title': '値上げ', 'detail': f'10%値上げで月+{price_increase_10pct:,.0f}円', 'impact': 'high', 'difficulty': 'medium', 'timeframe': '即時〜1ヶ月'},
                    {'title': '固定費削減', 'detail': f'人件費・家賃見直しで月-{cost_reduction_10pct:,.0f}円', 'impact': 'high', 'difficulty': 'medium', 'timeframe': '1〜3ヶ月'},
                    {'title': '商品構成見直し', 'detail': f'限界利益率{marginal_profit_rate*100:.1f}% → 目標{(marginal_profit_rate+0.05)*100:.1f}%', 'impact': 'medium', 'difficulty': 'low', 'timeframe': '1〜2ヶ月'}
                ]
            }
        
        safety_rate = metrics.get('safety_rate', 0)
        if 0 < safety_rate < 0.20:
            return {
                'level': 'warning',
                'icon': '⚠️',
                'title': '安全余裕率が低い',
                'message': f'安全余裕率{safety_rate*100:.1f}%は危険水域です。',
                'actions': [
                    {'title': '売上増加', 'detail': f'目標月+{current_sales*0.10:,.0f}円（10%増）', 'impact': 'high', 'difficulty': 'high', 'timeframe': '3〜6ヶ月'},
                    {'title': '固定費削減', 'detail': '損益分岐点を下げて安全余裕率を向上', 'impact': 'medium', 'difficulty': 'medium', 'timeframe': '1〜3ヶ月'}
                ]
            }
        return None
    
    def _check_cash_shortage(self, metrics: Dict) -> Optional[Dict]:
        cash_runway = metrics.get('cash_runway_months', 0)
        if cash_runway == 0:
            return None
        
        if cash_runway < 3:
            return {
                'level': 'critical',
                'icon': '🚨',
                'title': '資金危機',
                'message': f'資金耐久月数が{cash_runway:.1f}ヶ月と危険水域です。',
                'actions': [
                    {'title': '融資実行', 'detail': '銀行融資または日本政策金融公庫の活用', 'impact': 'high', 'difficulty': 'medium', 'timeframe': '即時〜1ヶ月'},
                    {'title': '売掛金早期回収', 'detail': '回収サイト短縮交渉', 'impact': 'medium', 'difficulty': 'medium', 'timeframe': '即時〜1ヶ月'}
                ]
            }
        elif cash_runway < 6:
            return {
                'level': 'warning',
                'icon': '⚠️',
                'title': '資金注意',
                'message': f'資金耐久月数が{cash_runway:.1f}ヶ月です。',
                'actions': [
                    {'title': '営業CFの改善', 'detail': '売掛金回収の強化と在庫圧縮', 'impact': 'high', 'difficulty': 'medium', 'timeframe': '1〜2ヶ月'}
                ]
            }
        return None
    
    def _check_profitability(self, metrics: Dict) -> Optional[Dict]:
        marginal_profit_rate = metrics.get('marginal_profit_rate', 0)
        if marginal_profit_rate == 0:
            return None
        
        if marginal_profit_rate < 0.30:
            return {
                'level': 'warning',
                'icon': '⚠️',
                'title': '利益率改善必要',
                'message': f'限界利益率{marginal_profit_rate*100:.1f}%は業界平均(35%)を下回っています。',
                'actions': [
                    {'title': '値上げ', 'detail': '価格弾力性を分析して適正価格を設定', 'impact': 'high', 'difficulty': 'medium', 'timeframe': '1〜2ヶ月'},
                    {'title': '仕入先交渉', 'detail': '変動費率の削減', 'impact': 'medium', 'difficulty': 'medium', 'timeframe': '1〜3ヶ月'}
                ]
            }
        return None
    
    def _check_working_capital(self, metrics: Dict) -> Optional[Dict]:
        operating_profit = metrics.get('operating_profit', 0)
        operating_cf = metrics.get('operating_cf', 0)
        
        if operating_profit > 0 and operating_cf < 0:
            cf_gap = abs(operating_cf)
            return {
                'level': 'critical',
                'icon': '🚨',
                'title': '黒字倒産リスク',
                'message': f'営業利益は{operating_profit:,.0f}円の黒字ですが、営業CFは{operating_cf:,.0f}円の赤字です。',
                'actions': [
                    {'title': '売掛金の早期回収', 'detail': '回収サイト短縮交渉', 'impact': 'high', 'difficulty': 'medium', 'timeframe': '即時〜1ヶ月'},
                    {'title': '在庫の圧縮', 'detail': '発注量の最適化', 'impact': 'high', 'difficulty': 'medium', 'timeframe': '1〜2ヶ月'}
                ]
            }
        return None
    
    def _check_investment_opportunity(self, metrics: Dict) -> Optional[Dict]:
        cash_runway = metrics.get('cash_runway_months', 0)
        marginal_profit_rate = metrics.get('marginal_profit_rate', 0)
        operating_profit = metrics.get('operating_profit', 0)
        
        if cash_runway > 12 and marginal_profit_rate > 0.35 and operating_profit > 0:
            investable_amount = operating_profit * 0.5
            return {
                'level': 'success',
                'icon': '🟢',
                'title': '成長投資の好機',
                'message': f'財務基盤が盤石です（資金耐久{cash_runway:.1f}ヶ月、限界利益率{marginal_profit_rate*100:.1f}%）。',
                'actions': [
                    {'title': '新規採用', 'detail': f'月額投資: 約{investable_amount*0.4:,.0f}円', 'impact': 'high', 'difficulty': 'medium', 'timeframe': '1〜3ヶ月'},
                    {'title': 'マーケティング投資', 'detail': f'月額投資: 約{investable_amount*0.3:,.0f}円', 'impact': 'high', 'difficulty': 'low', 'timeframe': '即時〜1ヶ月'}
                ]
            }
        return None
