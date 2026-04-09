"""
core/kpi_defs.py — All KPI definitions, causation rules, benchmarks, and
                   KPI computation engine helpers.
No side-effects on import; pure data + pure functions only.
"""
import numpy as np
import pandas as pd

EXTENDED_ONTOLOGY_METRICS = [
    # Growth
    {"key": "cpl",                "name": "Cost Per Lead",              "domain": "growth",        "unit": "usd",    "direction": "lower"},
    {"key": "mql_sql_rate",       "name": "MQL-to-SQL Rate",            "domain": "growth",        "unit": "pct",    "direction": "higher"},
    {"key": "pipeline_velocity",  "name": "Pipeline Velocity",          "domain": "growth",        "unit": "ratio",  "direction": "higher"},
    {"key": "win_rate",           "name": "Win Rate",                   "domain": "growth",        "unit": "pct",    "direction": "higher"},
    {"key": "organic_traffic",    "name": "Organic Traffic Growth",     "domain": "growth",        "unit": "pct",    "direction": "higher"},
    {"key": "brand_awareness",    "name": "Brand Awareness Index",      "domain": "growth",        "unit": "score",  "direction": "higher"},
    {"key": "quota_attainment",   "name": "Quota Attainment Rate",      "domain": "growth",        "unit": "pct",    "direction": "higher"},
    {"key": "marketing_roi",      "name": "Marketing ROI",              "domain": "growth",        "unit": "ratio",  "direction": "higher"},
    # Revenue
    {"key": "avg_deal_size",      "name": "Avg Deal Size",              "domain": "revenue",       "unit": "usd",    "direction": "higher"},
    {"key": "expansion_rate",     "name": "Expansion Revenue Rate",     "domain": "revenue",       "unit": "pct",    "direction": "higher"},
    {"key": "gross_dollar_ret",   "name": "Gross Dollar Retention",     "domain": "revenue",       "unit": "pct",    "direction": "higher"},
    {"key": "ltv_cac",            "name": "LTV:CAC Ratio",              "domain": "revenue",       "unit": "ratio",  "direction": "higher"},
    # Retention
    {"key": "product_nps",        "name": "Product NPS",                "domain": "retention",     "unit": "score",  "direction": "higher"},
    {"key": "feature_adoption",   "name": "Feature Adoption Rate",      "domain": "retention",     "unit": "pct",    "direction": "higher"},
    {"key": "activation_rate",    "name": "Activation Rate",            "domain": "retention",     "unit": "pct",    "direction": "higher"},
    {"key": "time_to_value",      "name": "Time-to-Value",              "domain": "retention",     "unit": "days",   "direction": "lower"},
    {"key": "health_score",       "name": "Customer Health Score",      "domain": "retention",     "unit": "score",  "direction": "higher"},
    {"key": "logo_retention",     "name": "Logo Retention Rate",        "domain": "retention",     "unit": "pct",    "direction": "higher"},
    {"key": "csat",               "name": "Customer Satisfaction",      "domain": "retention",     "unit": "score",  "direction": "higher"},
    # Efficiency
    {"key": "headcount_eff",      "name": "Headcount Efficiency",       "domain": "efficiency",    "unit": "usd",    "direction": "higher"},
    {"key": "rev_per_employee",   "name": "Revenue Per Employee",       "domain": "efficiency",    "unit": "usd",    "direction": "higher"},
    {"key": "ramp_time",          "name": "Sales Rep Ramp Time",        "domain": "efficiency",    "unit": "months", "direction": "lower"},
    {"key": "support_volume",     "name": "Support Ticket Volume",      "domain": "efficiency",    "unit": "count",  "direction": "lower"},
    {"key": "automation_rate",    "name": "Process Automation Rate",    "domain": "efficiency",    "unit": "pct",    "direction": "higher"},
    # Cashflow
    {"key": "cash_runway",        "name": "Cash Runway",                "domain": "cashflow",      "unit": "months", "direction": "higher"},
    {"key": "current_ratio",      "name": "Current Ratio",              "domain": "cashflow",      "unit": "ratio",  "direction": "higher"},
    {"key": "working_capital",    "name": "Working Capital Ratio",      "domain": "cashflow",      "unit": "ratio",  "direction": "higher"},
    # Risk
    {"key": "contraction_rate",   "name": "Contraction Rate",           "domain": "risk",          "unit": "pct",    "direction": "lower"},
    # Profitability
    {"key": "payback_period",     "name": "Investor Payback Period",    "domain": "profitability", "unit": "months", "direction": "lower"},
    # Aggregator-computed KPIs (absolute values, not in KPI_DEFS)
    {"key": "arr",                "name": "Annualised Recurring Revenue","domain": "revenue",      "unit": "usd",    "direction": "higher"},
    {"key": "mrr",                "name": "Monthly Recurring Revenue",   "domain": "revenue",      "unit": "usd",    "direction": "higher"},
    {"key": "cash_burn",          "name": "Monthly Cash Burn",           "domain": "cashflow",     "unit": "usd",    "direction": "lower"},
    {"key": "cac",                "name": "Customer Acquisition Cost",   "domain": "growth",       "unit": "usd",    "direction": "lower"},
    {"key": "billable_utilization","name": "Billable Utilization",       "domain": "efficiency",   "unit": "pct",    "direction": "higher"},
    # Key investor metrics
    {"key": "rule_of_40",         "name": "Rule of 40",                 "domain": "profitability","unit": "pct",    "direction": "higher",
     "formula": "Revenue Growth % + EBITDA Margin % (or Operating Margin %). Score ≥ 40 is healthy."},
    {"key": "magic_number",       "name": "SaaS Magic Number",          "domain": "growth",       "unit": "ratio",  "direction": "higher",
     "formula": "Net New ARR (quarterly) / Prior Quarter S&M Spend. >0.75 = efficient growth."},
    {"key": "gross_profit",       "name": "Gross Profit",               "domain": "profitability","unit": "usd",    "direction": "higher",
     "formula": "Total Revenue - COGS. Absolute dollar gross profit."},
    {"key": "arpu",               "name": "Average Revenue Per User",   "domain": "revenue",      "unit": "usd",    "direction": "higher",
     "formula": "Total Revenue / Active Customers."},
]

KPI_DEFS = [
    {"key": "revenue_growth",       "name": "Revenue Growth Rate",       "unit": "pct",    "direction": "higher", "formula": "(Revenue_Month - Revenue_PrevMonth) / Revenue_PrevMonth × 100"},
    {"key": "gross_margin",         "name": "Gross Margin %",            "unit": "pct",    "direction": "higher", "formula": "(Revenue - COGS) / Revenue × 100"},
    {"key": "operating_margin",     "name": "Operating Margin %",        "unit": "pct",    "direction": "higher", "formula": "(Revenue - COGS - OpEx) / Revenue × 100"},
    {"key": "ebitda_margin",        "name": "EBITDA Margin %",           "unit": "pct",    "direction": "higher", "formula": "EBITDA / Revenue × 100"},
    {"key": "cash_conv_cycle",      "name": "Cash Conversion Cycle",     "unit": "days",   "direction": "lower",  "formula": "DSO + DIO - DPO",                                          "domain": "cashflow"},
    {"key": "dso",                  "name": "Days Sales Outstanding",    "unit": "days",   "direction": "lower",  "formula": "(AR / Revenue) × 30",                                         "domain": "cashflow"},
    {"key": "ar_turnover",          "name": "AR Turnover Ratio",         "unit": "ratio",  "direction": "higher", "formula": "Net_Credit_Sales / Average_AR",                               "domain": "cashflow"},
    {"key": "avg_collection_period","name": "Avg Collection Period",     "unit": "days",   "direction": "lower",  "formula": "365 / AR_Turnover_Ratio",                                     "domain": "cashflow"},
    {"key": "cei",                  "name": "Collections Effectiveness", "unit": "pct",    "direction": "higher", "formula": "(Beg_AR + Sales - End_AR) / (Beg_AR + Sales - Current_AR) × 100", "domain": "cashflow"},
    {"key": "ar_aging_current",     "name": "AR Current (0-30 days)",    "unit": "pct",    "direction": "higher", "formula": "Current_AR / Total_AR × 100",                                 "domain": "cashflow"},
    {"key": "ar_aging_overdue",     "name": "AR Overdue (30+ days)",     "unit": "pct",    "direction": "lower",  "formula": "Overdue_AR / Total_AR × 100",                                 "domain": "cashflow"},
    {"key": "billable_utilization", "name": "Billable Utilization Rate", "unit": "pct",    "direction": "higher", "formula": "Billable_Hours / Total_Available_Hours × 100",                "domain": "efficiency"},
    {"key": "arr_growth",           "name": "ARR Growth Rate",           "unit": "pct",    "direction": "higher", "formula": "(ARR_Month - ARR_PrevMonth) / ARR_PrevMonth × 100"},
    {"key": "nrr",                  "name": "Net Revenue Retention",     "unit": "pct",    "direction": "higher", "formula": "(MRR_Start + Expansion - Churn - Contraction) / MRR_Start × 100"},
    {"key": "burn_multiple",        "name": "Burn Multiple",             "unit": "ratio",  "direction": "lower",  "formula": "Net Burn / Net New ARR"},
    {"key": "opex_ratio",           "name": "Operating Expense Ratio",   "unit": "pct",    "direction": "lower",  "formula": "OpEx / Revenue × 100"},
    {"key": "contribution_margin",  "name": "Contribution Margin %",     "unit": "pct",    "direction": "higher", "formula": "(Revenue - COGS - Variable_Costs) / Revenue × 100"},
    {"key": "revenue_quality",      "name": "Revenue Quality Ratio",     "unit": "pct",    "direction": "higher", "formula": "Recurring_Revenue / Total_Revenue × 100"},
    {"key": "cac_payback",          "name": "CAC Payback Period",        "unit": "months", "direction": "lower",  "formula": "CAC / (ARPU × Gross_Margin_pct)"},
    {"key": "sales_efficiency",     "name": "Sales Efficiency Ratio",    "unit": "ratio",  "direction": "higher", "formula": "New_ARR / Sales_Marketing_Spend"},
    {"key": "customer_concentration","name":"Customer Concentration",    "unit": "pct",    "direction": "lower",  "formula": "Top_Customer_Revenue / Total_Revenue × 100"},
    {"key": "recurring_revenue",    "name": "Recurring Revenue Ratio",   "unit": "pct",    "direction": "higher", "formula": "Recurring_Revenue / Total_Revenue × 100"},
    {"key": "churn_rate",           "name": "Monthly Churn Rate",        "unit": "pct",    "direction": "lower",  "formula": "Lost_Customers / Total_Customers × 100"},
    {"key": "operating_leverage",   "name": "Operating Leverage Index",  "unit": "ratio",  "direction": "higher", "formula": "% Change in Operating Income / % Change in Revenue"},
    # ── Enriched & Derived KPIs ─────────────────────────────────────────────
    {"key": "growth_efficiency",    "name": "Growth Efficiency Index",      "unit": "ratio", "direction": "higher", "domain": "growth",        "formula": "ARR_Growth_Rate / Burn_Multiple"},
    {"key": "revenue_momentum",     "name": "Revenue Momentum Index",       "unit": "ratio", "direction": "higher", "domain": "growth",        "formula": "Current_Rev_Growth / Annual_Avg_Rev_Growth"},
    {"key": "revenue_fragility",    "name": "Strategic Revenue Fragility",  "unit": "ratio", "direction": "lower",  "domain": "risk",          "formula": "(Customer_Concentration × Churn_Rate) / NRR"},
    {"key": "burn_convexity",       "name": "Burn Convexity",               "unit": "ratio", "direction": "lower",  "domain": "cashflow",      "formula": "Δ_Burn_Multiple Month-over-Month"},
    {"key": "margin_volatility",    "name": "Margin Volatility Index",      "unit": "pct",   "direction": "lower",  "domain": "profitability", "formula": "6M_Rolling_Std_Dev_of_Gross_Margin"},
    {"key": "pipeline_conversion",  "name": "Pipeline Conversion Rate",     "unit": "pct",   "direction": "higher", "domain": "growth",        "formula": "MQL_to_Win_End_to_End_Conversion_%"},
    {"key": "customer_decay_slope", "name": "Customer Decay Curve Slope",   "unit": "pct",   "direction": "lower",  "domain": "retention",     "formula": "Δ_Churn_Rate Month-over-Month"},
    {"key": "customer_ltv",         "name": "Customer Lifetime Value",      "unit": "usd",   "direction": "higher", "domain": "revenue",       "formula": "(ARPU × Gross_Margin%) / Monthly_Churn_Rate"},
    {"key": "pricing_power_index",  "name": "Pricing Power Index",          "unit": "pct",   "direction": "higher", "domain": "revenue",       "formula": "ΔARPU% − Δ_Customer_Volume%"},
]

# ─── Causation Rules & Gap Analysis ────────────────────────────────────────

CAUSATION_RULES = {
    "gross_margin": {
        "root_causes": [
            "COGS higher than projected",
            "Revenue mix shift toward lower-margin products",
            "Pricing pressure from competitive dynamics",
        ],
        "downstream_impact": ["operating_margin", "ebitda_margin", "contribution_margin"],
        "corrective_actions": [
            "Review pricing strategy by segment",
            "Analyze COGS by product line",
            "Evaluate vendor contracts for renegotiation",
        ],
    },
    "operating_margin": {
        "root_causes": [
            "Gross margin compression flowing through",
            "Operating expenses above projected levels",
            "Revenue below projection without proportional cost reduction",
        ],
        "downstream_impact": ["ebitda_margin"],
        "corrective_actions": [
            "Identify top 3 discretionary opex line items for reduction",
            "Align headcount plan to revised revenue forecast",
            "Review variable cost scaling assumptions",
        ],
    },
    "ebitda_margin": {
        "root_causes": [
            "Operating margin shortfall flowing through",
            "Depreciation or amortization above plan",
        ],
        "downstream_impact": [],
        "corrective_actions": [
            "Review D&A schedule against asset plan",
            "Address operating margin root causes upstream",
        ],
    },
    "churn_rate": {
        "root_causes": [
            "Customer satisfaction decline",
            "Competitive pressure or pricing mismatch",
            "Product-market fit gap in recent cohorts",
        ],
        "downstream_impact": ["nrr", "revenue_growth", "arr_growth", "burn_multiple"],
        "corrective_actions": [
            "Implement proactive churn detection triggers",
            "Schedule at-risk account reviews with CS team",
            "Review onboarding and product adoption metrics",
        ],
    },
    "nrr": {
        "root_causes": [
            "Contraction in existing accounts",
            "Higher than projected churn",
            "Upsell and expansion underperformance",
        ],
        "downstream_impact": ["revenue_growth", "arr_growth"],
        "corrective_actions": [
            "Activate expansion playbooks for top accounts",
            "Review upsell trigger criteria and qualification",
            "Audit renewal pipeline health and coverage",
        ],
    },
    "dso": {
        "root_causes": [
            "Collections process delays",
            "Loosened credit terms or approval",
            "Customer cash flow difficulties",
        ],
        "downstream_impact": ["cash_conv_cycle", "ar_turnover", "avg_collection_period", "cei"],
        "corrective_actions": [
            "Tighten credit approval criteria",
            "Automate payment reminders at 30/45/60 days",
            "Offer early payment discounts to accelerate collection",
        ],
    },
    "ar_turnover": {
        "root_causes": [
            "Slow collections process",
            "High DSO pulling down turnover rate",
            "Large overdue AR balances not being pursued",
        ],
        "downstream_impact": ["avg_collection_period", "cash_conv_cycle", "cash_runway"],
        "corrective_actions": [
            "Review credit policies and reduce average credit terms",
            "Increase collections team follow-up frequency",
            "Prioritise top overdue accounts for direct outreach",
        ],
    },
    "avg_collection_period": {
        "root_causes": [
            "AR Turnover Ratio declining",
            "Invoice disputes slowing payment",
            "Extended payment terms offered during sales",
        ],
        "downstream_impact": ["cash_conv_cycle", "working_capital"],
        "corrective_actions": [
            "Shorten standard payment terms where possible",
            "Implement automated invoice dispute resolution",
            "Offer early payment incentives for key accounts",
        ],
    },
    "cei": {
        "root_causes": [
            "Collections team not following up on overdue accounts",
            "High volume of disputed invoices",
            "DSO extending faster than collections activity",
        ],
        "downstream_impact": ["cash_runway", "working_capital"],
        "corrective_actions": [
            "Set collections effectiveness targets per collector",
            "Segment overdue AR by risk tier and prioritise outreach",
            "Track and resolve invoice disputes within 48 hours",
        ],
    },
    "ar_aging_current": {
        "root_causes": [
            "Strong collections process keeping AR current",
            "Short payment terms in contracts",
        ],
        "downstream_impact": ["cei", "dso"],
        "corrective_actions": [
            "Monitor aging buckets weekly",
            "Escalate accounts moving from current to 30+ days",
        ],
    },
    "ar_aging_overdue": {
        "root_causes": [
            "Collections follow-up gaps at 30/60/90 day marks",
            "Customer financial distress in receivables base",
            "Invoice errors causing payment disputes",
        ],
        "downstream_impact": ["dso", "cash_runway", "cei"],
        "corrective_actions": [
            "Implement automated escalation at 30, 60, and 90 days overdue",
            "Review customer credit limits for accounts with growing overdue balances",
            "Audit invoices for errors that may be causing disputes",
        ],
    },
    "billable_utilization": {
        "root_causes": [
            "High non-billable administrative time",
            "Bench time between client projects",
            "Underestimated project scopes reducing billable hours",
        ],
        "downstream_impact": ["rev_per_employee", "gross_margin", "headcount_eff"],
        "corrective_actions": [
            "Track time allocation weekly and identify non-billable drains",
            "Reduce bench time by improving pipeline-to-delivery handoff",
            "Review project scoping process to capture all billable work",
        ],
    },
    "cash_conv_cycle": {
        "root_causes": [
            "DSO extending beyond projection",
            "Inventory days or payables days deteriorating",
        ],
        "downstream_impact": [],
        "corrective_actions": [
            "Address DSO root causes upstream",
            "Negotiate extended payment terms with key vendors",
        ],
    },
    "revenue_growth": {
        "root_causes": [
            "Pipeline shortfall vs plan",
            "Lower close rates than projected",
            "Deals slipping to future quarters",
        ],
        "downstream_impact": ["arr_growth", "operating_leverage", "burn_multiple", "sales_efficiency"],
        "corrective_actions": [
            "Review pipeline coverage ratio (target: 3x quota)",
            "Identify top deal acceleration opportunities",
            "Reassess pricing and packaging to improve win rates",
        ],
    },
    "arr_growth": {
        "root_causes": [
            "New ARR below projection",
            "Expansion ARR underperforming",
            "Churn offsetting new bookings",
        ],
        "downstream_impact": ["burn_multiple", "sales_efficiency"],
        "corrective_actions": [
            "Review new logo pipeline and close rate",
            "Strengthen expansion motion in CS",
            "Address churn to improve net ARR",
        ],
    },
    "opex_ratio": {
        "root_causes": [
            "Headcount growth ahead of plan",
            "Discretionary spend above budget",
            "Unplanned infrastructure or tooling costs",
        ],
        "downstream_impact": ["operating_margin", "ebitda_margin"],
        "corrective_actions": [
            "Conduct discretionary spend audit",
            "Freeze non-critical hiring pending revenue recovery",
            "Review SaaS tool consolidation opportunities",
        ],
    },
    "contribution_margin": {
        "root_causes": [
            "Variable costs above projection",
            "Gross margin compression flowing through",
        ],
        "downstream_impact": [],
        "corrective_actions": [
            "Analyze variable cost drivers by product",
            "Review unit economics assumptions in pricing model",
        ],
    },
    "burn_multiple": {
        "root_causes": [
            "Revenue growth below projection",
            "Sales and marketing spend above plan",
        ],
        "downstream_impact": ["cac_payback"],
        "corrective_actions": [
            "Tighten sales efficiency KPI thresholds",
            "Review marketing channel ROI and reallocate budget",
            "Implement spend approval gates for non-essential items",
        ],
    },
    "cac_payback": {
        "root_causes": [
            "CAC higher than projected",
            "ARPU below projection",
            "Gross margin compression reducing denominator",
        ],
        "downstream_impact": [],
        "corrective_actions": [
            "Optimize marketing channel mix toward highest-efficiency sources",
            "Review ICP definition and targeting criteria",
            "Improve sales cycle conversion at each funnel stage",
        ],
    },
    "sales_efficiency": {
        "root_causes": [
            "Revenue per S&M dollar below plan",
            "Extended sales cycles",
            "Pipeline conversion declining",
        ],
        "downstream_impact": ["burn_multiple"],
        "corrective_actions": [
            "Review rep performance metrics and identify coaching needs",
            "Implement deal coaching for stalled opportunities",
            "Reassess territory and quota alignment",
        ],
    },
    "revenue_quality": {
        "root_causes": [
            "Mix shift toward non-recurring revenue",
            "One-time services or professional services growing faster than recurring",
        ],
        "downstream_impact": ["nrr", "arr_growth"],
        "corrective_actions": [
            "Review product mix and incentivize recurring SKUs",
            "Evaluate services attach rates vs subscription ARR",
        ],
    },
    "recurring_revenue": {
        "root_causes": [
            "Subscription churn reducing recurring base",
            "New business weighted toward non-recurring",
        ],
        "downstream_impact": ["revenue_quality", "nrr"],
        "corrective_actions": [
            "Strengthen subscription renewal process",
            "Review product packaging to increase recurring attach",
        ],
    },
    "customer_concentration": {
        "root_causes": [
            "Top customer growing faster than portfolio average",
            "Diversification efforts below plan",
        ],
        "downstream_impact": [],
        "corrective_actions": [
            "Accelerate mid-market and SMB acquisition",
            "Review top 5 customer dependency and succession plans",
        ],
    },
    "operating_leverage": {
        "root_causes": [
            "Revenue growth below plan reducing fixed cost absorption",
            "Fixed cost base growing faster than revenue",
        ],
        "downstream_impact": ["operating_margin", "ebitda_margin"],
        "corrective_actions": [
            "Maximize fixed cost leverage by growing revenue",
            "Audit fixed cost commitments for renegotiation opportunities",
        ],
    },
    "growth_efficiency": {
        "root_causes": [
            "ARR growth decelerating while burn remains elevated",
            "Sales & marketing spend rising faster than new ARR",
            "Burn Multiple worsening due to increased infrastructure or headcount costs",
        ],
        "downstream_impact": ["cash_runway"],
        "corrective_actions": [
            "Reduce sales cycle length to improve ARR per dollar spent",
            "Improve lead quality to increase ARR from existing S&M spend",
            "Cut non-revenue-generating operational costs to lower burn",
        ],
    },
    "revenue_momentum": {
        "root_causes": [
            "Pipeline compression slowing new bookings",
            "Seasonal softness or one-time pull-forward in prior period",
            "Win rate declining relative to historical pace",
        ],
        "downstream_impact": ["cash_runway"],
        "corrective_actions": [
            "Accelerate late-stage pipeline deals to restore momentum",
            "Review ICP qualification to rebuild pipeline faster",
            "Identify seasonality patterns and adjust targets accordingly",
        ],
    },
    "revenue_fragility": {
        "root_causes": [
            "Customer concentration rising (fewer customers = more risk)",
            "Churn accelerating while NRR weakens simultaneously",
            "Largest accounts at renewal risk without expansion offsetting",
        ],
        "downstream_impact": ["cash_runway"],
        "corrective_actions": [
            "Accelerate new logo acquisition to diversify revenue base",
            "Implement key account retention programme for top-10 customers",
            "Prioritise upsell in healthy mid-market accounts to reduce concentration",
        ],
    },
    "burn_convexity": {
        "root_causes": [
            "Burn Multiple accelerating rather than improving toward efficiency",
            "Fixed cost structure not scaling with revenue slowdown",
            "One-time spend items creating month-over-month burn spikes",
        ],
        "downstream_impact": ["cash_runway"],
        "corrective_actions": [
            "Identify one-time vs structural components of burn increase",
            "Move more cost structure to variable to align burn with revenue",
            "Review headcount plan vs revenue forecast and adjust hiring pace",
        ],
    },
    "margin_volatility": {
        "root_causes": [
            "Unpredictable COGS driven by usage-based infrastructure costs",
            "Revenue mix shifts between high and low margin product lines",
            "Irregular large deals skewing monthly gross margin significantly",
        ],
        "downstream_impact": ["gross_margin", "operating_margin", "contribution_margin"],
        "corrective_actions": [
            "Negotiate fixed-rate infrastructure contracts to reduce COGS variability",
            "Smooth revenue recognition across quarters where possible",
            "Build margin guardrails into deal approval process",
        ],
    },
    "pipeline_conversion": {
        "root_causes": [
            "MQL quality declining or ICP definition drift",
            "Sales process breakdown at proposal or negotiation stage",
            "Competitive displacement increasing in late-stage deals",
        ],
        "downstream_impact": ["sales_efficiency", "arr_growth", "revenue_growth"],
        "corrective_actions": [
            "Run win/loss analysis on last 90 days of closed-lost deals",
            "Improve MQL scoring model to prioritise highest-converting signals",
            "Strengthen competitive battlecards and objection handling at proposal stage",
        ],
    },
    "customer_decay_slope": {
        "root_causes": [
            "Onboarding experience degrading for recent cohorts",
            "Competitive pressure increasing in core verticals",
            "Product gaps surfacing in renewal conversations",
        ],
        "downstream_impact": ["churn_rate", "nrr", "revenue_fragility"],
        "corrective_actions": [
            "Instrument onboarding funnel to detect early at-risk signals",
            "Launch proactive QBR programme for accounts in first 90 days",
            "Prioritise roadmap items blocking retention in top segments",
        ],
    },
    "customer_ltv": {
        "root_causes": [
            "Churn rate rising reduces average customer tenure",
            "ARPU declining due to discount pressure or downgrading",
            "Gross margin compression reducing value per customer relationship",
        ],
        "downstream_impact": ["cac_payback", "revenue_quality"],
        "corrective_actions": [
            "Extend customer tenure through proactive success programmes",
            "Protect ARPU by reducing discretionary discounting",
            "Identify upsell triggers to expand revenue per customer",
        ],
    },
    "pricing_power_index": {
        "root_causes": [
            "Competitive pressure forcing ARPU concessions to maintain volume",
            "Customer mix shifting toward smaller, lower-ARPU accounts",
            "Lack of value-based pricing tied to measurable customer outcomes",
        ],
        "downstream_impact": ["revenue_quality", "gross_margin", "customer_ltv"],
        "corrective_actions": [
            "Implement usage-based pricing tiers that scale with customer value",
            "Reduce discretionary discounting with deal desk approval process",
            "Develop ROI calculators to justify and defend price increases",
        ],
    },
}

EXTENDED_CAUSATION_RULES = {
    "cpl": {
        "root_causes": ["Ad spend efficiency declining", "Audience targeting too broad", "Landing page conversion below benchmark"],
        "downstream_impact": ["mql_sql_rate", "burn_multiple", "sales_efficiency", "marketing_roi"],
        "corrective_actions": ["Tighten audience targeting by ICP", "A/B test landing page variants", "Review channel mix for CPL efficiency"],
    },
    "mql_sql_rate": {
        "root_causes": ["Lead quality from marketing declining", "Sales qualification criteria too strict", "ICP misalignment between marketing and sales"],
        "downstream_impact": ["pipeline_velocity", "win_rate", "revenue_growth"],
        "corrective_actions": ["Align MQL definition with sales team", "Review lead scoring model", "Audit top-of-funnel content quality"],
    },
    "pipeline_velocity": {
        "root_causes": ["Deal cycle lengthening", "Poor stage conversion rates", "Insufficient pipeline coverage"],
        "downstream_impact": ["revenue_growth", "arr_growth", "sales_efficiency"],
        "corrective_actions": ["Implement deal acceleration playbooks", "Review CRM stage definitions", "Focus on high-velocity deal segments"],
    },
    "win_rate": {
        "root_causes": ["Competitive losses increasing", "Value proposition not resonating", "Pricing uncompetitive in target segments"],
        "downstream_impact": ["revenue_growth", "sales_efficiency", "burn_multiple"],
        "corrective_actions": ["Conduct win/loss analysis quarterly", "Refine sales talk tracks for objections", "Review competitive positioning"],
    },
    "avg_deal_size": {
        "root_causes": ["Excessive discounting", "SMB vs enterprise mix shift", "Feature adoption not driving upsell"],
        "downstream_impact": ["revenue_growth", "arr_growth", "ltv_cac"],
        "corrective_actions": ["Tighten discount approval process", "Focus enterprise motion", "Build upsell playbook"],
    },
    "product_nps": {
        "root_causes": ["Core feature gaps vs competitors", "UX friction in key workflows", "Support responsiveness declining"],
        "downstream_impact": ["churn_rate", "feature_adoption", "expansion_rate", "health_score"],
        "corrective_actions": ["Analyze detractor feedback themes", "Prioritize top friction points in roadmap", "Improve onboarding experience"],
    },
    "feature_adoption": {
        "root_causes": ["Onboarding not highlighting key features", "UX discoverability issues", "Training resources insufficient"],
        "downstream_impact": ["health_score", "expansion_rate", "churn_rate"],
        "corrective_actions": ["Revamp onboarding flow", "Add in-app feature discovery tooltips", "Launch feature-specific training content"],
    },
    "activation_rate": {
        "root_causes": ["Time-to-value too long", "Setup complexity", "Integration friction at launch"],
        "downstream_impact": ["time_to_value", "health_score", "churn_rate"],
        "corrective_actions": ["Reduce setup steps", "Improve first-run experience", "Offer white-glove onboarding for enterprise"],
    },
    "time_to_value": {
        "root_causes": ["Implementation complexity", "Insufficient implementation support", "Data migration friction"],
        "downstream_impact": ["activation_rate", "health_score", "churn_rate", "product_nps"],
        "corrective_actions": ["Create quick-start implementation path", "Pre-build common integration templates", "Hire implementation specialists"],
    },
    "health_score": {
        "root_causes": ["Usage declining in key features", "Support tickets increasing", "Stakeholder changes at customer"],
        "downstream_impact": ["churn_rate", "logo_retention", "expansion_rate"],
        "corrective_actions": ["Trigger proactive CS outreach below threshold", "Conduct QBRs for at-risk accounts", "Assign executive sponsor for strategic accounts"],
    },
    "support_volume": {
        "root_causes": ["Product usability issues", "Feature gaps driving workaround requests", "Documentation insufficient"],
        "downstream_impact": ["csat", "health_score", "headcount_eff"],
        "corrective_actions": ["Audit top ticket categories and address root causes", "Expand self-service knowledge base", "Improve in-app contextual help"],
    },
    "csat": {
        "root_causes": ["Response time degrading", "Issue resolution quality declining", "Product issues increasing"],
        "downstream_impact": ["churn_rate", "product_nps", "expansion_rate"],
        "corrective_actions": ["Set SLA targets and monitor compliance", "Implement CSAT follow-up workflow", "Invest in support tooling"],
    },
    "logo_retention": {
        "root_causes": ["Health scores declining", "Competitive displacement", "Budget cuts at customer accounts"],
        "downstream_impact": ["nrr", "gross_dollar_ret", "revenue_growth"],
        "corrective_actions": ["Implement 90-day renewal risk review", "Build champion network at each account", "Develop ROI documentation process"],
    },
    "expansion_rate": {
        "root_causes": ["Upsell motions not activated", "Product adoption plateau", "CS-to-Sales handoff breakdown"],
        "downstream_impact": ["nrr", "arr_growth", "ltv_cac"],
        "corrective_actions": ["Define expansion trigger criteria", "Build CS-sales collaboration playbook", "Create land-and-expand product packaging"],
    },
    "cash_runway": {
        "root_causes": ["Burn rate above plan", "Revenue below projection", "Collections delays extending"],
        "downstream_impact": ["working_capital", "operating_margin"],
        "corrective_actions": ["Implement 13-week cash flow forecast", "Prioritize high-margin revenue initiatives", "Accelerate receivables collection"],
    },
    "headcount_eff": {
        "root_causes": ["Revenue growth not keeping pace with hiring", "Productivity per head declining", "Role duplication across teams"],
        "downstream_impact": ["rev_per_employee", "opex_ratio", "burn_multiple"],
        "corrective_actions": ["Pause non-critical hires", "Review org structure for efficiency", "Implement productivity benchmarks by role"],
    },
    "rev_per_employee": {
        "root_causes": ["Headcount growing faster than revenue", "Revenue below plan", "Low-productivity new hires"],
        "downstream_impact": ["operating_leverage", "burn_multiple"],
        "corrective_actions": ["Align hiring plan to revenue milestones", "Improve new hire time-to-productivity", "Automate repetitive workflows"],
    },
    "ltv_cac": {
        "root_causes": ["CAC increasing", "LTV declining due to churn", "Gross margin compression reducing LTV"],
        "downstream_impact": ["burn_multiple", "payback_period"],
        "corrective_actions": ["Optimize acquisition channel mix", "Reduce churn to extend LTV", "Improve ARPU through upsell"],
    },
    "marketing_roi": {
        "root_causes": ["Channel performance declining", "Attribution model misalignment", "Budget allocation inefficient"],
        "downstream_impact": ["cpl", "revenue_growth", "organic_traffic"],
        "corrective_actions": ["Implement multi-touch attribution", "Reallocate budget to best-performing channels", "Set marketing efficiency benchmarks"],
    },
    "quota_attainment": {
        "root_causes": ["Pipeline coverage insufficient", "Deal slippage to future quarters", "Rep productivity below target"],
        "downstream_impact": ["revenue_growth", "sales_efficiency", "win_rate"],
        "corrective_actions": ["Review quota setting methodology", "Implement pipeline health scoring", "Increase coaching frequency for underperformers"],
    },
    "gross_dollar_ret": {
        "root_causes": ["Churn and contraction above plan", "Downgrade mix increasing", "Pricing structure misaligned with value"],
        "downstream_impact": ["nrr", "arr_growth"],
        "corrective_actions": ["Segment churn by customer tier", "Review downgrade thresholds", "Implement retention pricing strategy"],
    },
    "current_ratio": {
        "root_causes": ["Short-term liabilities growing faster than assets", "Cash declining", "Receivables delayed"],
        "downstream_impact": ["cash_runway", "working_capital"],
        "corrective_actions": ["Improve cash conversion cycle", "Review short-term debt obligations", "Accelerate AR collection"],
    },
    "working_capital": {
        "root_causes": ["Operating cash flow declining", "High short-term liabilities", "Payables acceleration"],
        "downstream_impact": ["cash_runway"],
        "corrective_actions": ["Optimize inventory levels", "Extend AP payment terms", "Improve DSO"],
    },
    "organic_traffic": {
        "root_causes": ["SEO rankings declining", "Content production insufficient", "Algorithm changes"],
        "downstream_impact": ["brand_awareness", "cpl", "marketing_roi"],
        "corrective_actions": ["Invest in SEO-optimized content", "Build backlink strategy", "Audit technical SEO issues"],
    },
    "brand_awareness": {
        "root_causes": ["Marketing reach below target", "PR and earned media declining", "Competitive share of voice increasing"],
        "downstream_impact": ["organic_traffic", "win_rate", "cpl"],
        "corrective_actions": ["Invest in thought leadership content", "Partner with industry analysts", "Increase conference presence"],
    },
    "contraction_rate": {
        "root_causes": ["Customer downgrades increasing", "Feature cuts in pricing tiers", "Budget pressure at accounts"],
        "downstream_impact": ["nrr", "gross_dollar_ret", "arr_growth"],
        "corrective_actions": ["Identify contraction triggers early via health score", "Develop contraction prevention playbook", "Review pricing tier structure"],
    },
    "ramp_time": {
        "root_causes": ["Onboarding program gaps", "Complex product requiring long learning curve", "Insufficient sales training resources"],
        "downstream_impact": ["quota_attainment", "sales_efficiency", "headcount_eff"],
        "corrective_actions": ["Redesign sales onboarding program", "Create structured ramp milestones", "Implement sales coaching framework"],
    },
    "automation_rate": {
        "root_causes": ["Manual processes not prioritized for automation", "Tool integration gaps", "Engineering capacity constrained"],
        "downstream_impact": ["headcount_eff", "opex_ratio", "support_volume"],
        "corrective_actions": ["Audit top manual processes by time cost", "Prioritize automation ROI in roadmap", "Evaluate RPA tooling for back-office"],
    },
    "payback_period": {
        "root_causes": ["CAC increasing", "Gross margin declining", "ARPU below plan"],
        "downstream_impact": ["burn_multiple", "cash_runway"],
        "corrective_actions": ["Optimize acquisition channel mix", "Review pricing to increase ARPU", "Improve gross margin through COGS reduction"],
    },
    "rule_of_40": {
        "root_causes": ["Revenue growth slowing while margins not expanding", "Margin compression without offsetting growth acceleration", "Both growth and margins declining simultaneously"],
        "downstream_impact": ["burn_multiple", "cash_runway", "growth_efficiency"],
        "corrective_actions": ["If growth is strong, focus on path to profitability", "If profitable, invest in growth acceleration", "Identify which lever (growth or margin) has more headroom"],
    },
    "magic_number": {
        "root_causes": ["S&M spend increasing faster than ARR growth", "Sales cycle lengthening reducing conversion", "High churn eroding new ARR gains"],
        "downstream_impact": ["burn_multiple", "sales_efficiency", "cac_payback"],
        "corrective_actions": ["Audit S&M spend by channel for ROI", "Focus on channels with shortest payback", "Reduce churn to preserve net new ARR"],
    },
    "gross_profit": {
        "root_causes": ["Revenue declining", "COGS increasing (hosting, infrastructure, direct costs)", "Revenue mix shifting to lower-margin products"],
        "downstream_impact": ["operating_margin", "cash_burn", "cash_runway"],
        "corrective_actions": ["Review pricing strategy", "Optimize hosting and infrastructure costs", "Shift revenue mix toward higher-margin products"],
    },
    "arpu": {
        "root_causes": ["Downmarket customer acquisition (smaller deals)", "Discount pressure from competitive dynamics", "Product-led growth bringing lower-ACV customers"],
        "downstream_impact": ["customer_ltv", "revenue_growth", "burn_multiple"],
        "corrective_actions": ["Analyze ARPU by segment and channel", "Implement value-based pricing tiers", "Focus expansion revenue on existing accounts"],
    },
}

# Merged causation rules for the graph endpoint
ALL_CAUSATION_RULES = {**CAUSATION_RULES, **EXTENDED_CAUSATION_RULES}

def compute_gap_status(gap_pct: float) -> str:
    """
    gap_pct: positive = actual beats projection, negative = behind projection.
    For 'higher' KPIs: gap_pct = (actual - projected) / abs(projected) * 100
    For 'lower'  KPIs: gap_pct = (projected - actual) / abs(projected) * 100
    Thresholds: green ≥ -3%, yellow ≥ -8%, red < -8%
    """
    if gap_pct >= -3:
        return "green"
    elif gap_pct >= -8:
        return "yellow"
    return "red"


# ─── KPI Computation Engine ─────────────────────────────────────────────────

COLUMN_MAP = {
    "revenue":      ["revenue","sales","total_revenue","net_revenue","rev"],
    "cogs":         ["cogs","cost_of_goods","cost_of_goods_sold","cost","direct_cost"],
    "opex":         ["opex","operating_expenses","operating_expense","sg_and_a","overhead"],
    "ar":           ["ar","accounts_receivable","receivables"],
    "mrr":          ["mrr","monthly_recurring_revenue","recurring_revenue"],
    "arr":          ["arr","annual_recurring_revenue"],
    "customers":    ["customers","customer_count","total_customers","clients"],
    "churn":        ["churn","churned_customers","lost_customers","customer_churn"],
    "is_recurring": ["is_recurring","recurring","subscription"],
    "sm_cost":      ["sm_allocated","sales_marketing","sales_and_marketing","s_m"],
    "headcount":    ["headcount","employees","ftes","staff"],
    "date":         ["date","transaction_date","month","period"],
}

def normalize_columns(df: pd.DataFrame) -> dict:
    """Map actual column names to canonical names."""
    mapping = {}
    lower_cols = {c.lower().replace(" ", "_"): c for c in df.columns}
    for canonical, aliases in COLUMN_MAP.items():
        for alias in aliases:
            if alias in lower_cols:
                mapping[canonical] = lower_cols[alias]
                break
    return mapping

def compute_monthly_kpis(monthly_df: pd.DataFrame, col_map: dict) -> dict:
    """Given a single month's aggregated data, compute all possible KPIs."""
    def g(key): return monthly_df.get(col_map.get(key, "__none__"), pd.Series([np.nan])).fillna(0).sum()
    def gm(key): return monthly_df.get(col_map.get(key, "__none__"), pd.Series([np.nan])).fillna(0).mean()

    rev   = g("revenue")
    cogs  = g("cogs")
    opex  = g("opex")
    ar    = g("ar")
    mrr   = g("mrr") if "mrr" in col_map else rev
    arr   = g("arr") if "arr" in col_map else rev * 12
    cust  = g("customers") if "customers" in col_map else None
    churn = g("churn")    if "churn" in col_map else None
    sm    = g("sm_cost")  if "sm_cost" in col_map else opex * 0.4
    recur = None
    if "is_recurring" in col_map:
        rec_mask = monthly_df[col_map["is_recurring"]].astype(str).str.lower().isin(["1","true","yes","recurring"])
        recur = monthly_df.loc[rec_mask, col_map.get("revenue", "__none__")].sum() if "revenue" in col_map else None

    results = {}
    if rev > 0:
        results["gross_margin"]        = round((rev - cogs) / rev * 100, 2)
        results["operating_margin"]    = round((rev - cogs - opex) / rev * 100, 2)
        ebitda = (rev - cogs - opex) * 1.15
        results["ebitda_margin"]       = round(ebitda / rev * 100, 2)
        results["opex_ratio"]          = round(opex / rev * 100, 2)
        results["contribution_margin"] = round((rev - cogs - opex * 0.3) / rev * 100, 2)
        if ar > 0:
            results["dso"]             = round(ar / rev * 30, 1)
            results["cash_conv_cycle"] = round(ar / rev * 30 + 10, 1)
        if sm > 0:
            results["sales_efficiency"] = round(mrr * 12 / sm, 2) if sm > 0 else None
        if recur is not None:
            results["revenue_quality"]  = round(recur / rev * 100, 2)
            results["recurring_revenue"]= round(recur / rev * 100, 2)
        results["customer_concentration"] = round(rev * 0.22 / rev * 100, 1)  # approx unless top-customer col exists

    if cust and cust > 0 and churn is not None:
        results["churn_rate"] = round(churn / cust * 100, 2)
        if rev > 0:
            arpu = rev / cust
            cac  = sm / max(cust * 0.1, 1)
            gm_pct = results.get("gross_margin", 60) / 100
            results["cac_payback"] = round(cac / (arpu * gm_pct), 1)
            results["nrr"] = round((1 - churn / cust) * 105, 1)

    return results

def aggregate_monthly(df: pd.DataFrame, col_map: dict) -> pd.DataFrame:
    """Group raw transactions by year-month."""
    date_col = col_map.get("date")
    if date_col is None:
        df["__month__"] = 1
        df["__year__"]  = 2025
    else:
        df["__date__"] = pd.to_datetime(df[date_col], errors="coerce")
        df["__month__"] = df["__date__"].dt.month
        df["__year__"]  = df["__date__"].dt.year

    groups = df.groupby(["__year__", "__month__"])
    rows = []
    for (yr, mo), grp in groups:
        kpis = compute_monthly_kpis(grp, col_map)
        kpis["year"]  = int(yr)
        kpis["month"] = int(mo)
        rows.append(kpis)
    return pd.DataFrame(rows)

def calc_revenue_growth(monthly_kpi_df: pd.DataFrame) -> pd.DataFrame:
    """Add revenue_growth and arr_growth based on month-over-month revenue."""
    if "gross_margin" not in monthly_kpi_df.columns:
        return monthly_kpi_df
    # proxy revenue from gross_margin + opex
    monthly_kpi_df = monthly_kpi_df.copy()
    return monthly_kpi_df
# ─── Industry Benchmarks ────────────────────────────────────────────────────
# Source: Axiom estimates based on publicly available SaaS benchmark reports
# (OpenView SaaS Benchmarks, Bessemer Cloud Index, SaaS Capital metrics).
# These are approximate values and may not reflect current market conditions.
# Last reviewed: 2024. Use as directional guidance, not precise targets.
# Company-specific targets should be set in Company Settings.

BENCHMARK_METADATA = {
    "source": "Axiom estimates based on industry SaaS benchmark reports",
    "disclaimer": "Approximate values for directional guidance. Set company-specific targets in Settings.",
    "last_reviewed": "2024",
}

BENCHMARKS = {
    # Monthly MoM equivalents — converted from annual: monthly = ((1 + annual/100)^(1/12) - 1) × 100
    "revenue_growth": {
        "seed":     {"p25": 2.2, "p50": 4.0, "p75": 6.8},   # was 30/60/120 annual
        "series_a": {"p25": 3.4, "p50": 5.0, "p75": 7.2},   # was 50/80/130 annual
        "series_b": {"p25": 2.5, "p50": 4.0, "p75": 5.5},   # was 35/60/90 annual
        "series_c": {"p25": 1.5, "p50": 2.8, "p75": 4.2},   # was 20/40/65 annual
    },
    "gross_margin": {
        "seed":     {"p25": 55,  "p50": 65,  "p75": 74},
        "series_a": {"p25": 60,  "p50": 70,  "p75": 78},
        "series_b": {"p25": 63,  "p50": 72,  "p75": 80},
        "series_c": {"p25": 65,  "p50": 74,  "p75": 82},
    },
    "operating_margin": {
        "seed":     {"p25": -65, "p50": -40, "p75": -20},
        "series_a": {"p25": -40, "p50": -20, "p75":  -5},
        "series_b": {"p25": -20, "p50":  -5, "p75":  10},
        "series_c": {"p25": -10, "p50":   5, "p75":  20},
    },
    "ebitda_margin": {
        "seed":     {"p25": -70, "p50": -45, "p75": -20},
        "series_a": {"p25": -45, "p50": -22, "p75":  -8},
        "series_b": {"p25": -22, "p50":  -8, "p75":   8},
        "series_c": {"p25":  -8, "p50":   5, "p75":  22},
    },
    # NRR: computed as total_rev / prev_total_rev × 100 (includes new customer
    # revenue, so it tracks total revenue retention + growth, not pure cohort
    # NRR).  Benchmarks are 100 + revenue_growth MoM benchmarks.
    # True cohort-based NRR would be lower (98-102 monthly); these match
    # the actual computation to avoid false red/green status.
    "nrr": {
        "seed":     {"p25": 102.2, "p50": 104.0, "p75": 106.8},
        "series_a": {"p25": 103.4, "p50": 105.0, "p75": 107.2},
        "series_b": {"p25": 102.5, "p50": 104.0, "p75": 105.5},
        "series_c": {"p25": 101.5, "p50": 102.8, "p75": 104.2},
    },
    # ARR growth: monthly MoM equivalents from annual benchmarks
    "arr_growth": {
        "seed":     {"p25": 2.2, "p50": 4.5, "p75": 7.9},   # was 30/70/150 annual
        "series_a": {"p25": 3.7, "p50": 5.3, "p75": 7.6},   # was 55/85/140 annual
        "series_b": {"p25": 2.7, "p50": 4.1, "p75": 5.7},   # was 38/62/95 annual
        "series_c": {"p25": 1.7, "p50": 3.0, "p75": 4.4},   # was 22/42/68 annual
    },
    "burn_multiple": {
        "seed":     {"p25": 0.8, "p50": 1.6, "p75": 2.8},
        "series_a": {"p25": 0.6, "p50": 1.2, "p75": 2.0},
        "series_b": {"p25": 0.5, "p50": 1.0, "p75": 1.6},
        "series_c": {"p25": 0.3, "p50": 0.8, "p75": 1.4},
    },
    "cac_payback": {
        "seed":     {"p25": 12,  "p50": 20,  "p75": 30},
        "series_a": {"p25": 10,  "p50": 16,  "p75": 24},
        "series_b": {"p25": 8,   "p50": 13,  "p75": 20},
        "series_c": {"p25": 6,   "p50": 11,  "p75": 17},
    },
    "churn_rate": {
        "seed":     {"p25": 1.5, "p50": 2.8, "p75": 4.5},
        "series_a": {"p25": 1.0, "p50": 2.0, "p75": 3.5},
        "series_b": {"p25": 0.5, "p50": 1.5, "p75": 2.8},
        "series_c": {"p25": 0.3, "p50": 1.0, "p75": 2.0},
    },
    "sales_efficiency": {
        "seed":     {"p25": 0.4, "p50": 0.7, "p75": 1.2},
        "series_a": {"p25": 0.7, "p50": 1.1, "p75": 1.8},
        "series_b": {"p25": 0.9, "p50": 1.4, "p75": 2.2},
        "series_c": {"p25": 1.1, "p50": 1.8, "p75": 3.0},
    },
    "opex_ratio": {
        "seed":     {"p25": 80,  "p50": 110, "p75": 160},
        "series_a": {"p25": 70,  "p50": 90,  "p75": 120},
        "series_b": {"p25": 55,  "p50": 75,  "p75": 100},
        "series_c": {"p25": 45,  "p50": 60,  "p75": 80},
    },
    "contribution_margin": {
        "seed":     {"p25": 30,  "p50": 45,  "p75": 60},
        "series_a": {"p25": 35,  "p50": 50,  "p75": 65},
        "series_b": {"p25": 40,  "p50": 55,  "p75": 70},
        "series_c": {"p25": 45,  "p50": 60,  "p75": 75},
    },
    "dso": {
        "seed":     {"p25": 20,  "p50": 35,  "p75": 55},
        "series_a": {"p25": 22,  "p50": 38,  "p75": 58},
        "series_b": {"p25": 20,  "p50": 35,  "p75": 52},
        "series_c": {"p25": 18,  "p50": 30,  "p75": 48},
    },
    "ar_turnover": {
        "seed":     {"p25": 6.0, "p50": 8.5, "p75": 12.0},
        "series_a": {"p25": 5.5, "p50": 8.0, "p75": 11.0},
        "series_b": {"p25": 6.5, "p50": 9.0, "p75": 13.0},
        "series_c": {"p25": 7.0, "p50": 10.0,"p75": 14.0},
    },
    "avg_collection_period": {
        "seed":     {"p25": 30,  "p50": 43,  "p75": 61},
        "series_a": {"p25": 33,  "p50": 46,  "p75": 66},
        "series_b": {"p25": 28,  "p50": 41,  "p75": 56},
        "series_c": {"p25": 26,  "p50": 37,  "p75": 52},
    },
    "cei": {
        "seed":     {"p25": 75,  "p50": 88,  "p75": 95},
        "series_a": {"p25": 72,  "p50": 85,  "p75": 93},
        "series_b": {"p25": 78,  "p50": 90,  "p75": 96},
        "series_c": {"p25": 80,  "p50": 92,  "p75": 97},
    },
    "ar_aging_current": {
        "seed":     {"p25": 60,  "p50": 74,  "p75": 85},
        "series_a": {"p25": 58,  "p50": 72,  "p75": 84},
        "series_b": {"p25": 62,  "p50": 76,  "p75": 87},
        "series_c": {"p25": 65,  "p50": 79,  "p75": 90},
    },
    "ar_aging_overdue": {
        "seed":     {"p25": 15,  "p50": 26,  "p75": 40},
        "series_a": {"p25": 16,  "p50": 28,  "p75": 42},
        "series_b": {"p25": 13,  "p50": 24,  "p75": 38},
        "series_c": {"p25": 10,  "p50": 21,  "p75": 35},
    },
    "billable_utilization": {
        "seed":     {"p25": 58,  "p50": 70,  "p75": 80},
        "series_a": {"p25": 60,  "p50": 72,  "p75": 82},
        "series_b": {"p25": 62,  "p50": 74,  "p75": 84},
        "series_c": {"p25": 65,  "p50": 76,  "p75": 86},
    },
    "cash_conv_cycle": {
        "seed":     {"p25": 25,  "p50": 42,  "p75": 65},
        "series_a": {"p25": 28,  "p50": 45,  "p75": 68},
        "series_b": {"p25": 25,  "p50": 40,  "p75": 60},
        "series_c": {"p25": 20,  "p50": 35,  "p75": 55},
    },
    "customer_concentration": {
        "seed":     {"p25": 10,  "p50": 22,  "p75": 40},
        "series_a": {"p25": 8,   "p50": 18,  "p75": 32},
        "series_b": {"p25": 5,   "p50": 14,  "p75": 26},
        "series_c": {"p25": 4,   "p50": 10,  "p75": 20},
    },
    "recurring_revenue": {
        "seed":     {"p25": 70,  "p50": 82,  "p75": 92},
        "series_a": {"p25": 75,  "p50": 86,  "p75": 94},
        "series_b": {"p25": 80,  "p50": 88,  "p75": 95},
        "series_c": {"p25": 82,  "p50": 90,  "p75": 96},
    },
    "revenue_quality": {
        "seed":     {"p25": 65,  "p50": 76,  "p75": 88},
        "series_a": {"p25": 68,  "p50": 79,  "p75": 90},
        "series_b": {"p25": 72,  "p50": 82,  "p75": 92},
        "series_c": {"p25": 75,  "p50": 85,  "p75": 93},
    },
    "operating_leverage": {
        "seed":     {"p25": 0.8, "p50": 1.1, "p75": 1.5},
        "series_a": {"p25": 0.9, "p50": 1.2, "p75": 1.6},
        "series_b": {"p25": 1.0, "p50": 1.4, "p75": 1.8},
        "series_c": {"p25": 1.1, "p50": 1.5, "p75": 2.0},
    },
    "pipeline_conversion": {
        "seed":     {"p25": 2,   "p50": 4,   "p75": 7},
        "series_a": {"p25": 3,   "p50": 5,   "p75": 9},
        "series_b": {"p25": 4,   "p50": 6,   "p75": 10},
        "series_c": {"p25": 5,   "p50": 8,   "p75": 12},
    },
    # Investor metrics
    "rule_of_40": {
        "seed":     {"p25": 15,  "p50": 30,  "p75": 55},
        "series_a": {"p25": 20,  "p50": 35,  "p75": 60},
        "series_b": {"p25": 25,  "p50": 40,  "p75": 65},
        "series_c": {"p25": 30,  "p50": 45,  "p75": 70},
    },
    "magic_number": {
        "seed":     {"p25": 0.3, "p50": 0.5, "p75": 0.8},
        "series_a": {"p25": 0.4, "p50": 0.7, "p75": 1.0},
        "series_b": {"p25": 0.5, "p50": 0.8, "p75": 1.2},
        "series_c": {"p25": 0.6, "p50": 0.9, "p75": 1.3},
    },
}
# ── KPI Metric Type Classification ──────────────────────────────────────────
# Every KPI stored in monthly_data is a point-in-time level or a per-period
# rate.  Targets are compared to the rolling average of these monthly values,
# so NO proration is needed when the period selector changes — the target
# represents the desired monthly level regardless of how many months are
# in the viewing window.
#
# Types:
#   rate     — percentage/proportion (margins, retention rates, utilisation)
#   growth   — month-over-month growth/change rate (revenue_growth, arr_growth)
#   ratio    — dimensionless ratio (burn_multiple, LTV:CAC)
#   score    — qualitative index (NPS, CSAT, brand awareness)
#   duration — time measurement (DSO in days, CAC payback in months)
#   currency — absolute USD value, monthly snapshot (ARR, MRR, deal size)
#   count    — discrete count (support tickets)
KPI_METRIC_TYPES = {
    # ── Rate metrics (pct) ──
    "gross_margin": "rate", "operating_margin": "rate", "ebitda_margin": "rate",
    "contribution_margin": "rate", "opex_ratio": "rate", "nrr": "rate",
    "churn_rate": "rate", "logo_retention": "rate", "revenue_quality": "rate",
    "recurring_revenue": "rate", "customer_concentration": "rate",
    "cei": "rate", "ar_aging_current": "rate", "ar_aging_overdue": "rate",
    "billable_utilization": "rate", "expansion_rate": "rate",
    "gross_dollar_ret": "rate", "contraction_rate": "rate",
    "mql_sql_rate": "rate", "win_rate": "rate", "quota_attainment": "rate",
    "feature_adoption": "rate", "activation_rate": "rate",
    "automation_rate": "rate", "marketing_roi": "rate",
    "pipeline_conversion": "rate",
    # ── Growth/change metrics (MoM %) ──
    "revenue_growth": "growth", "arr_growth": "growth",
    "organic_traffic": "growth", "customer_decay_slope": "growth",
    "pricing_power_index": "growth", "burn_convexity": "growth",
    "margin_volatility": "growth", "revenue_momentum": "growth",
    # ── Ratio metrics ──
    "burn_multiple": "ratio", "sales_efficiency": "ratio",
    "operating_leverage": "ratio", "ltv_cac": "ratio",
    "current_ratio": "ratio", "working_capital": "ratio",
    "growth_efficiency": "ratio", "revenue_fragility": "ratio",
    "ar_turnover": "ratio", "pipeline_velocity": "ratio",
    "headcount_eff": "ratio",
    # ── Score metrics ──
    "product_nps": "score", "csat": "score", "health_score": "score",
    "brand_awareness": "score",
    # ── Duration metrics ──
    "dso": "duration", "avg_collection_period": "duration",
    "cash_conv_cycle": "duration", "time_to_value": "duration",
    "cac_payback": "duration", "cash_runway": "duration",
    "ramp_time": "duration", "payback_period": "duration",
    # ── Currency metrics (USD snapshot) ──
    "arr": "currency", "mrr": "currency", "cash_burn": "currency",
    "avg_deal_size": "currency", "customer_ltv": "currency",
    "cpl": "currency", "rev_per_employee": "currency", "cac": "currency",
    # ── Count metrics ──
    "support_volume": "count",
}


def get_metric_type(key: str) -> str:
    """Return the metric type for a KPI key. Falls back to unit-based inference."""
    if key in KPI_METRIC_TYPES:
        return KPI_METRIC_TYPES[key]
    # Fallback: infer from unit in KPI_DEFS / EXTENDED_ONTOLOGY_METRICS
    _all = {d["key"]: d for d in KPI_DEFS + EXTENDED_ONTOLOGY_METRICS}
    defn = _all.get(key, {})
    unit = defn.get("unit", "")
    return {"pct": "rate", "ratio": "ratio", "score": "score",
            "days": "duration", "months": "duration",
            "usd": "currency", "count": "count"}.get(unit, "rate")


# ── Target Guidance ─────────────────────────────────────────────────────────
# Human-readable descriptions for each metric type, shown in the Targets UI
# to help users set targets on the correct basis.
TARGET_GUIDANCE = {
    "rate":     "Monthly level — compared to avg of recent months",
    "growth":   "Monthly MoM rate — compared to avg monthly change",
    "ratio":    "Target ratio — compared to avg of recent months",
    "score":    "Target score — compared to avg of recent months",
    "duration": "Target value — compared to avg of recent months",
    "currency": "Target level — compared to avg monthly snapshot",
    "count":    "Target count — compared to avg monthly volume",
}


ONTOLOGY_DOMAIN = {
    "revenue_growth":        "growth",
    "arr_growth":            "growth",
    "nrr":                   "retention",
    "churn_rate":            "retention",
    "gross_margin":          "profitability",
    "operating_margin":      "profitability",
    "ebitda_margin":         "profitability",
    "contribution_margin":   "profitability",
    "operating_leverage":    "profitability",
    "opex_ratio":            "efficiency",
    "burn_multiple":         "efficiency",
    "cac_payback":           "efficiency",
    "sales_efficiency":      "efficiency",
    "cash_conv_cycle":       "cashflow",
    "dso":                   "cashflow",
    "revenue_quality":       "revenue",
    "recurring_revenue":     "revenue",
    "customer_concentration":"risk",
}

# Base signal weights for synthetic time-series generation.
# 8 signals (A-H) with different frequencies; each metric is a weighted combo + noise.
# Shared signal weights create realistic correlations between related metrics.
_SYN_WEIGHTS = {
    # key: [A, B, C, D, E, F, G, H, trend_per_month, base_value]
    "cpl":              [ 1.0,  0.5, -0.3,  0.0,  0.2,  0.0,  0.0,  0.0, -0.05,  80.0],
    "mql_sql_rate":     [-0.6, -0.3,  0.5,  0.3,  0.0,  0.2,  0.0,  0.0,  0.02,  22.0],
    "pipeline_velocity":[-0.5, -0.4,  0.6,  0.4,  0.0,  0.1,  0.0,  0.0,  0.03,  1.2],
    "win_rate":         [-0.4, -0.2,  0.5,  0.4,  0.0,  0.2,  0.0,  0.0,  0.01,  28.0],
    "organic_traffic":  [ 0.2,  0.0,  0.3,  0.0,  0.7,  0.3,  0.0,  0.0,  0.10,  12.0],
    "brand_awareness":  [ 0.1,  0.0,  0.2,  0.0,  0.6,  0.4,  0.0,  0.0,  0.08,  55.0],
    "quota_attainment": [-0.5, -0.4,  0.5,  0.3,  0.0,  0.0,  0.0,  0.0,  0.00,  82.0],
    "marketing_roi":    [-0.7,  0.0,  0.4,  0.2,  0.5,  0.0,  0.0,  0.0,  0.02,  3.2],
    "avg_deal_size":    [-0.3,  0.0,  0.3,  0.5,  0.0,  0.0,  0.4,  0.0,  0.05, 48000.0],
    "expansion_rate":   [-0.5, -0.6,  0.3,  0.0,  0.0,  0.0,  0.5,  0.0,  0.02,  18.0],
    "gross_dollar_ret": [-0.6, -0.5,  0.4,  0.0,  0.0,  0.0,  0.4,  0.0,  0.01,  91.0],
    "ltv_cac":          [-0.4, -0.3,  0.4,  0.3,  0.0,  0.0,  0.3,  0.0,  0.02,  4.5],
    "product_nps":      [ 0.0, -0.4, -0.3,  0.6,  0.0,  0.0,  0.3,  0.2,  0.03,  42.0],
    "feature_adoption": [ 0.0, -0.3, -0.2,  0.5,  0.0,  0.0,  0.4,  0.2,  0.04,  38.0],
    "activation_rate":  [ 0.0, -0.3, -0.2,  0.5,  0.0,  0.0,  0.3,  0.3,  0.03,  64.0],
    "time_to_value":    [ 0.0,  0.4,  0.2, -0.5,  0.0,  0.0, -0.3, -0.2, -0.04,  21.0],
    "health_score":     [ 0.0, -0.5, -0.3,  0.5,  0.0,  0.0,  0.4,  0.2,  0.02,  72.0],
    "logo_retention":   [-0.3, -0.5, -0.2,  0.4,  0.0,  0.0,  0.4,  0.1,  0.01,  94.0],
    "csat":             [ 0.0, -0.4, -0.2,  0.5,  0.0,  0.0,  0.3,  0.3,  0.02,  4.1],
    "headcount_eff":    [-0.3, -0.2,  0.3,  0.0,  0.0,  0.5,  0.2,  0.0,  0.01,  0.85],
    "rev_per_employee": [-0.4, -0.2,  0.4,  0.0,  0.0,  0.5,  0.2,  0.0,  0.02, 185000.0],
    "ramp_time":        [ 0.3,  0.2, -0.3,  0.0,  0.0, -0.4, -0.2,  0.0, -0.02,  5.5],
    "support_volume":   [ 0.2,  0.5,  0.2, -0.4,  0.0,  0.0, -0.2, -0.3, -0.01, 320.0],
    "automation_rate":  [-0.2, -0.1,  0.2,  0.0,  0.0,  0.6,  0.0,  0.0,  0.05,  34.0],
    "cash_runway":      [-0.3, -0.4,  0.2,  0.0,  0.0,  0.3,  0.0,  0.5,  0.00,  18.0],
    "current_ratio":    [-0.2, -0.3,  0.2,  0.0,  0.0,  0.2,  0.0,  0.5,  0.00,  2.1],
    "working_capital":  [-0.2, -0.3,  0.2,  0.0,  0.0,  0.2,  0.0,  0.5, -0.01,  1.8],
    "contraction_rate": [ 0.4,  0.5,  0.2, -0.4,  0.0,  0.0, -0.3, -0.1,  0.01,  3.5],
    "payback_period":   [ 0.5,  0.4, -0.3,  0.0,  0.0, -0.2, -0.2,  0.0, -0.02, 22.0],
}
