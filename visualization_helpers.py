"""
UI改善とグラフ可視化ヘルパーモジュール
ウォーターフォール、Sankey図、スタイリング等
"""

import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
from typing import Dict, List, Optional


# ==================== スタイリング関連 ====================

def apply_custom_styles():
    """カスタムCSSスタイルを適用"""
    st.markdown("""
    <style>
    /* データフレームのスタイル */
    .dataframe {
        font-size: 14px !important;
    }
    
    .dataframe td {
        background-color: #ffffff !important;
        color: #262730 !important;
        padding: 8px !important;
    }
    
    .dataframe th {
        background-color: #f0f2f6 !important;
        color: #262730 !important;
        font-weight: bold !important;
        padding: 10px !important;
    }
    
    /* メトリクスカードのスタイル */
    div[data-testid="stMetricValue"] {
        font-size: 28px !important;
        font-weight: 600 !important;
    }
    
    div[data-testid="stMetricDelta"] {
        font-size: 16px !important;
    }
    
    /* テーブルヘッダー固定 */
    .stDataFrame {
        max-height: 600px;
        overflow-y: auto;
    }
    </style>
    """, unsafe_allow_html=True)


def style_comparison_dataframe(df: pd.DataFrame, variance_col: str = '差異') -> pd.DataFrame:
    """
    予実比較・期間比較のDataFrameにスタイルを適用
    
    Args:
        df: 元のDataFrame
        variance_col: 差異列の名前
        
    Returns:
        スタイル適用済みDataFrame
    """
    def highlight_variance(val):
        """差異に応じて背景色を設定"""
        if pd.isna(val):
            return ''
        
        try:
            num_val = float(val)
            if num_val > 0:
                # 正の差異: 薄い緑
                return 'background-color: #d4edda; color: #155724; font-weight: bold'
            elif num_val < 0:
                # 負の差異: 薄い赤
                return 'background-color: #f8d7da; color: #721c24; font-weight: bold'
            else:
                return ''
        except:
            return ''
    
    # 差異列が存在する場合のみスタイル適用
    if variance_col in df.columns:
        styled = df.style.applymap(highlight_variance, subset=[variance_col])
        return styled
    
    return df


def create_styled_table(df: pd.DataFrame, title: str = "") -> go.Figure:
    """
    Plotlyテーブルで見やすい表を作成
    
    Args:
        df: データ
        title: タイトル
        
    Returns:
        Plotly Figure
    """
    fig = go.Figure(data=[go.Table(
        header=dict(
            values=[f'<b>{col}</b>' for col in df.columns],
            fill_color='#4a90e2',
            font=dict(color='white', size=14),
            align='left',
            height=40
        ),
        cells=dict(
            values=[df[col] for col in df.columns],
            fill_color='lavender',
            font=dict(color='#262730', size=13),
            align='left',
            height=30
        )
    )])
    
    fig.update_layout(
        title=title,
        height=min(400, 50 + len(df) * 35),
        margin=dict(l=0, r=0, t=40, b=0)
    )
    
    return fig


# ==================== ウォーターフォールグラフ ====================

def create_pl_waterfall(pl_data: Dict, month: str = "最新月") -> go.Figure:
    """
    損益計算書のウォーターフォールグラフを作成
    
    Args:
        pl_data: PL データ辞書
        month: 表示する月
        
    Returns:
        Plotly Figure
    """
    # データを準備
    labels = []
    values = []
    measures = []
    
    # 売上高
    sales = pl_data.get('売上高', 0)
    labels.append('売上高')
    values.append(sales)
    measures.append('absolute')
    
    # 売上原価（マイナス）
    cogs = pl_data.get('売上原価', 0)
    labels.append('売上原価')
    values.append(-cogs)
    measures.append('relative')
    
    # 売上総利益
    gross_profit = sales - cogs
    labels.append('売上総利益')
    values.append(0)
    measures.append('total')
    
    # 販管費（マイナス）
    sg_expense = pl_data.get('販売費及び一般管理費', 0)
    labels.append('販管費')
    values.append(-sg_expense)
    measures.append('relative')
    
    # 営業利益
    operating_profit = gross_profit - sg_expense
    labels.append('営業利益')
    values.append(0)
    measures.append('total')
    
    # 営業外損益
    non_op_income = pl_data.get('営業外収益合計', 0)
    non_op_expense = pl_data.get('営業外費用合計', 0)
    
    if non_op_income > 0:
        labels.append('営業外収益')
        values.append(non_op_income)
        measures.append('relative')
    
    if non_op_expense > 0:
        labels.append('営業外費用')
        values.append(-non_op_expense)
        measures.append('relative')
    
    # 経常利益
    ordinary_profit = operating_profit + non_op_income - non_op_expense
    labels.append('経常利益')
    values.append(0)
    measures.append('total')
    
    # 特別損益
    sp_income = pl_data.get('特別利益合計', 0)
    sp_loss = pl_data.get('特別損失合計', 0)
    
    if sp_income > 0:
        labels.append('特別利益')
        values.append(sp_income)
        measures.append('relative')
    
    if sp_loss > 0:
        labels.append('特別損失')
        values.append(-sp_loss)
        measures.append('relative')
    
    # 税引前当期純利益
    labels.append('税引前利益')
    values.append(0)
    measures.append('total')
    
    # 法人税等
    tax = pl_data.get('法人税等合計', 0)
    if tax > 0:
        labels.append('法人税等')
        values.append(-tax)
        measures.append('relative')
    
    # 当期純利益
    labels.append('当期純利益')
    values.append(0)
    measures.append('total')
    
    # ウォーターフォール作成
    fig = go.Figure(go.Waterfall(
        orientation="v",
        measure=measures,
        x=labels,
        y=values,
        textposition="outside",
        text=[f"¥{v/10000:.0f}万" if v != 0 else "" for v in values],
        connector={"line": {"color": "rgb(63, 63, 63)"}},
        increasing={"marker": {"color": "#2ecc71"}},
        decreasing={"marker": {"color": "#e74c3c"}},
        totals={"marker": {"color": "#3498db"}}
    ))
    
    fig.update_layout(
        title=f"損益計算書 ウォーターフォール ({month})",
        xaxis_title="",
        yaxis_title="金額（円）",
        height=500,
        showlegend=False,
        hovermode='x unified'
    )
    
    return fig


# ==================== 貸借対照表BOX可視化 ====================

def create_bs_sankey(bs_data: Dict) -> go.Figure:
    """
    貸借対照表のSankey図（フロー図）を作成
    
    Args:
        bs_data: BS データ辞書
        
    Returns:
        Plotly Figure
    """
    # 資産側
    current_assets = bs_data.get('流動資産合計', 0)
    fixed_assets = bs_data.get('固定資産合計', 0)
    total_assets = current_assets + fixed_assets
    
    # 負債・純資産側
    current_liabilities = bs_data.get('流動負債合計', 0)
    fixed_liabilities = bs_data.get('固定負債合計', 0)
    total_liabilities = current_liabilities + fixed_liabilities
    equity = bs_data.get('純資産合計', 0)
    
    # ノードラベル
    labels = [
        "総資産",           # 0
        "流動資産",         # 1
        "固定資産",         # 2
        "流動負債",         # 3
        "固定負債",         # 4
        "純資産"            # 5
    ]
    
    # リンク（source → target）
    sources = [0, 0, 0, 0]
    targets = [1, 2, 3, 5]
    values = [current_assets, fixed_assets, total_liabilities, equity]
    
    # 色設定
    node_colors = [
        "#3498db",  # 総資産
        "#5dade2",  # 流動資産
        "#85c1e9",  # 固定資産
        "#e74c3c",  # 流動負債
        "#ec7063",  # 固定負債
        "#2ecc71"   # 純資産
    ]
    
    link_colors = [
        "rgba(93, 173, 226, 0.4)",   # 流動資産
        "rgba(133, 193, 233, 0.4)",  # 固定資産
        "rgba(231, 76, 60, 0.4)",    # 負債
        "rgba(46, 204, 113, 0.4)"    # 純資産
    ]
    
    fig = go.Figure(data=[go.Sankey(
        node=dict(
            pad=20,
            thickness=30,
            line=dict(color="black", width=2),
            label=[f"{label}<br>¥{val/10000:.0f}万" for label, val in zip(labels, 
                   [total_assets, current_assets, fixed_assets, 
                    current_liabilities, fixed_liabilities, equity])],
            color=node_colors,
            customdata=[f"¥{val:,.0f}" for val in 
                       [total_assets, current_assets, fixed_assets, 
                        current_liabilities, fixed_liabilities, equity]],
            hovertemplate='%{label}<br>%{customdata}<extra></extra>'
        ),
        link=dict(
            source=sources,
            target=targets,
            value=values,
            color=link_colors,
            customdata=[f"¥{v:,.0f}" for v in values],
            hovertemplate='%{source.label} → %{target.label}<br>%{customdata}<extra></extra>'
        )
    )])
    
    fig.update_layout(
        title="貸借対照表 バランスシート構造",
        font=dict(size=12),
        height=400
    )
    
    return fig


def create_bs_treemap(bs_data: Dict) -> go.Figure:
    """
    貸借対照表のTreemap（ボックス可視化）
    
    Args:
        bs_data: BS データ辞書
        
    Returns:
        Plotly Figure
    """
    # データ準備
    labels = []
    parents = []
    values = []
    colors = []
    
    # 総資産
    total_assets = bs_data.get('流動資産合計', 0) + bs_data.get('固定資産合計', 0)
    
    # ルート
    labels.append("貸借対照表")
    parents.append("")
    values.append(total_assets)
    colors.append("#ecf0f1")
    
    # 資産側
    labels.append("資産")
    parents.append("貸借対照表")
    values.append(total_assets)
    colors.append("#3498db")
    
    # 流動資産
    current_assets = bs_data.get('流動資産合計', 0)
    labels.append("流動資産")
    parents.append("資産")
    values.append(current_assets)
    colors.append("#5dade2")
    
    # 流動資産の内訳
    cash = bs_data.get('現金･預金合計', 0)
    if cash > 0:
        labels.append("現金")
        parents.append("流動資産")
        values.append(cash)
        colors.append("#85c1e9")
    
    receivables = bs_data.get('売掛金', 0)
    if receivables > 0:
        labels.append("売掛金")
        parents.append("流動資産")
        values.append(receivables)
        colors.append("#85c1e9")
    
    inventory = bs_data.get('棚卸資産', 0)
    if inventory > 0:
        labels.append("棚卸資産")
        parents.append("流動資産")
        values.append(inventory)
        colors.append("#85c1e9")
    
    # 固定資産
    fixed_assets = bs_data.get('固定資産合計', 0)
    labels.append("固定資産")
    parents.append("資産")
    values.append(fixed_assets)
    colors.append("#85c1e9")
    
    # 負債・純資産側
    labels.append("負債・純資産")
    parents.append("貸借対照表")
    values.append(total_assets)
    colors.append("#95a5a6")
    
    # 負債
    total_liabilities = bs_data.get('流動負債合計', 0) + bs_data.get('固定負債合計', 0)
    if total_liabilities > 0:
        labels.append("負債")
        parents.append("負債・純資産")
        values.append(total_liabilities)
        colors.append("#e74c3c")
    
    # 純資産
    equity = bs_data.get('純資産合計', 0)
    if equity > 0:
        labels.append("純資産")
        parents.append("負債・純資産")
        values.append(equity)
        colors.append("#2ecc71")
    
    fig = go.Figure(go.Treemap(
        labels=labels,
        parents=parents,
        values=values,
        marker=dict(colors=colors, line=dict(width=2)),
        textinfo="label+value+percent parent",
        texttemplate="<b>%{label}</b><br>¥%{value:,.0f}<br>%{percentParent}",
        hovertemplate='<b>%{label}</b><br>金額: ¥%{value:,.0f}<br>構成比: %{percentParent}<extra></extra>'
    ))
    
    fig.update_layout(
        title="貸借対照表 ボックス可視化",
        height=600,
        margin=dict(t=50, l=0, r=0, b=0)
    )
    
    return fig


# ==================== キャッシュフロー可視化 ====================

def create_cf_waterfall(cf_data: Dict) -> go.Figure:
    """
    キャッシュフロー計算書のウォーターフォール
    
    Args:
        cf_data: CF データ辞書
        
    Returns:
        Plotly Figure
    """
    labels = ["期首現金"]
    values = [cf_data.get('期首現金', 0)]
    measures = ['absolute']
    
    # 営業CF
    operating_cf = cf_data.get('営業CF', {}).get('合計', 0)
    labels.append('営業CF')
    values.append(operating_cf)
    measures.append('relative')
    
    # 投資CF
    investing_cf = cf_data.get('投資CF', {}).get('合計', 0)
    labels.append('投資CF')
    values.append(investing_cf)
    measures.append('relative')
    
    # 財務CF
    financing_cf = cf_data.get('財務CF', {}).get('合計', 0)
    labels.append('財務CF')
    values.append(financing_cf)
    measures.append('relative')
    
    # 期末現金
    labels.append('期末現金')
    values.append(0)
    measures.append('total')
    
    fig = go.Figure(go.Waterfall(
        orientation="v",
        measure=measures,
        x=labels,
        y=values,
        textposition="outside",
        text=[f"¥{v/10000:.0f}万" if v != 0 else "" for v in values],
        connector={"line": {"color": "rgb(63, 63, 63)"}},
        increasing={"marker": {"color": "#2ecc71"}},
        decreasing={"marker": {"color": "#e74c3c"}},
        totals={"marker": {"color": "#3498db"}}
    ))
    
    fig.update_layout(
        title="キャッシュフロー ウォーターフォール",
        xaxis_title="",
        yaxis_title="金額（円）",
        height=400,
        showlegend=False
    )
    
    return fig


# ==================== 経営指標ダッシュボード ====================

def create_kpi_gauge(value: float, title: str, max_value: float = 100, 
                     threshold_good: float = 70, threshold_warning: float = 40) -> go.Figure:
    """
    KPIゲージチャートを作成
    
    Args:
        value: 現在の値
        title: タイトル
        max_value: 最大値
        threshold_good: 良好の閾値
        threshold_warning: 警告の閾値
        
    Returns:
        Plotly Figure
    """
    # 色を決定
    if value >= threshold_good:
        color = "#2ecc71"  # 緑
    elif value >= threshold_warning:
        color = "#f39c12"  # 黄色
    else:
        color = "#e74c3c"  # 赤
    
    fig = go.Figure(go.Indicator(
        mode="gauge+number+delta",
        value=value,
        title={'text': title, 'font': {'size': 16}},
        delta={'reference': threshold_good},
        gauge={
            'axis': {'range': [None, max_value]},
            'bar': {'color': color},
            'steps': [
                {'range': [0, threshold_warning], 'color': "#ffcccc"},
                {'range': [threshold_warning, threshold_good], 'color': "#fff9cc"},
                {'range': [threshold_good, max_value], 'color': "#ccffcc"}
            ],
            'threshold': {
                'line': {'color': "black", 'width': 2},
                'thickness': 0.75,
                'value': threshold_good
            }
        }
    ))
    
    fig.update_layout(
        height=250,
        margin=dict(l=20, r=20, t=50, b=20)
    )
    
    return fig
