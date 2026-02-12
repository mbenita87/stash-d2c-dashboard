"""Chart: D2C Test Funnel - Funnel analysis for Test group only."""

from typing import Dict, Any
import pandas as pd
import plotly.graph_objects as go
from utils.bigquery_client import run_query


def build_funnel_query(filters: Dict[str, Any]) -> str:
    """
    Build SQL query for D2C Test group funnel metrics.
    Only includes Test group users (20% based on hash).
    """
    start_date = filters.get('start_date')
    end_date = filters.get('end_date')

    # Build OS filter
    os_filter = ""
    if filters.get('mp_os'):
        os_values = ", ".join([f"'{os}'" for os in filters['mp_os']])
        os_filter = f"AND ce.mp_os IN ({os_values})"

    # Build version filter
    version_filter = ""
    if filters.get('version'):
        version_values = ", ".join([str(v) for v in filters['version']])
        version_filter = f"AND ce.version_float IN ({version_values})"

    query = f"""
    WITH d2c_test_users AS (
        -- Only Test group users (hash_bucket < 20)
        SELECT
            p.distinct_id
        FROM `yotam-395120.peerplay.dim_player` p
        JOIN (
            SELECT distinct_id, MAX(version_float) as latest_version
            FROM `yotam-395120.peerplay.vmp_master_event_normalized`
            WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
              AND version_float IS NOT NULL AND version_float > 0
            GROUP BY distinct_id
        ) v ON p.distinct_id = v.distinct_id
        WHERE p.first_country = 'US'
          AND v.latest_version > 0.378
          AND DATE_DIFF(CURRENT_DATE(), DATE(p.first_event_time), DAY) > 3
          AND MOD(ABS(FARM_FINGERPRINT(p.distinct_id)), 100) < 20
    ),
    funnel_events AS (
        SELECT
            ce.distinct_id,
            ce.purchase_funnel_id,
            ce.mp_event_name,
            ce.payment_platform,
            ce.cta_name,
            ce.price_usd,
            ce.purchase_id,
            ce.google_order_number
        FROM `yotam-395120.peerplay.vmp_master_event_normalized` ce
        INNER JOIN d2c_test_users t ON ce.distinct_id = t.distinct_id
        WHERE ce.date >= '{start_date}'
          AND ce.date <= '{end_date}'
          {os_filter}
          {version_filter}
          AND ce.mp_event_name IN ('purchase_click', 'click_pre_purchase', 'purchase_successful')
    ),
    funnel_metrics AS (
        SELECT
            -- Purchase Clicks (start of funnel)
            COUNT(DISTINCT CASE
                WHEN mp_event_name = 'purchase_click'
                THEN purchase_funnel_id
            END) as purchase_clicks,

            COUNT(DISTINCT CASE
                WHEN mp_event_name = 'purchase_click'
                THEN distinct_id
            END) as purchase_click_users,

            -- PP Continue (by platform)
            COUNT(DISTINCT CASE
                WHEN mp_event_name = 'click_pre_purchase' AND cta_name = 'continue' AND payment_platform = 'stash'
                THEN purchase_funnel_id
            END) as stash_continue,

            COUNT(DISTINCT CASE
                WHEN mp_event_name = 'click_pre_purchase' AND cta_name = 'continue' AND payment_platform = 'apple'
                THEN purchase_funnel_id
            END) as apple_continue,

            COUNT(DISTINCT CASE
                WHEN mp_event_name = 'click_pre_purchase' AND cta_name = 'continue' AND payment_platform = 'googleplay'
                THEN purchase_funnel_id
            END) as google_continue,

            -- Purchase Success (by platform)
            COUNT(DISTINCT CASE
                WHEN mp_event_name = 'purchase_successful' AND payment_platform = 'stash'
                THEN purchase_funnel_id
            END) as stash_purchases,

            COUNT(DISTINCT CASE
                WHEN mp_event_name = 'purchase_successful' AND payment_platform = 'apple'
                  AND purchase_id IS NOT NULL AND purchase_id != ''
                THEN purchase_funnel_id
            END) as apple_purchases,

            COUNT(DISTINCT CASE
                WHEN mp_event_name = 'purchase_successful' AND payment_platform = 'googleplay'
                  AND google_order_number IS NOT NULL AND google_order_number != ''
                THEN purchase_funnel_id
            END) as google_purchases,

            -- Revenue (by platform)
            SUM(CASE
                WHEN mp_event_name = 'purchase_successful' AND payment_platform = 'stash'
                THEN COALESCE(price_usd, 0) ELSE 0
            END) as stash_revenue,

            SUM(CASE
                WHEN mp_event_name = 'purchase_successful' AND payment_platform = 'apple'
                  AND purchase_id IS NOT NULL AND purchase_id != ''
                THEN COALESCE(price_usd, 0) ELSE 0
            END) as apple_revenue,

            SUM(CASE
                WHEN mp_event_name = 'purchase_successful' AND payment_platform = 'googleplay'
                  AND google_order_number IS NOT NULL AND google_order_number != ''
                THEN COALESCE(price_usd, 0) ELSE 0
            END) as google_revenue,

            -- Paying users (by platform)
            COUNT(DISTINCT CASE
                WHEN mp_event_name = 'purchase_successful' AND payment_platform = 'stash'
                THEN distinct_id
            END) as stash_paying_users,

            COUNT(DISTINCT CASE
                WHEN mp_event_name = 'purchase_successful' AND payment_platform = 'apple'
                  AND purchase_id IS NOT NULL AND purchase_id != ''
                THEN distinct_id
            END) as apple_paying_users,

            COUNT(DISTINCT CASE
                WHEN mp_event_name = 'purchase_successful' AND payment_platform = 'googleplay'
                  AND google_order_number IS NOT NULL AND google_order_number != ''
                THEN distinct_id
            END) as google_paying_users
        FROM funnel_events
    )
    SELECT * FROM funnel_metrics
    """
    return query


def build_daily_funnel_query(filters: Dict[str, Any]) -> str:
    """Build query for daily funnel metrics for timeline chart."""
    start_date = filters.get('start_date')
    end_date = filters.get('end_date')

    # Build OS filter
    os_filter = ""
    if filters.get('mp_os'):
        os_values = ", ".join([f"'{os}'" for os in filters['mp_os']])
        os_filter = f"AND ce.mp_os IN ({os_values})"

    # Build version filter
    version_filter = ""
    if filters.get('version'):
        version_values = ", ".join([str(v) for v in filters['version']])
        version_filter = f"AND ce.version_float IN ({version_values})"

    query = f"""
    WITH d2c_test_users AS (
        SELECT p.distinct_id
        FROM `yotam-395120.peerplay.dim_player` p
        JOIN (
            SELECT distinct_id, MAX(version_float) as latest_version
            FROM `yotam-395120.peerplay.vmp_master_event_normalized`
            WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
              AND version_float IS NOT NULL AND version_float > 0
            GROUP BY distinct_id
        ) v ON p.distinct_id = v.distinct_id
        WHERE p.first_country = 'US'
          AND v.latest_version > 0.378
          AND DATE_DIFF(CURRENT_DATE(), DATE(p.first_event_time), DAY) > 3
          AND MOD(ABS(FARM_FINGERPRINT(p.distinct_id)), 100) < 20
    )
    SELECT
        ce.date as event_date,

        -- Purchase Clicks
        COUNT(DISTINCT CASE WHEN ce.mp_event_name = 'purchase_click' THEN ce.purchase_funnel_id END) as purchase_clicks,

        -- Stash funnel
        COUNT(DISTINCT CASE
            WHEN ce.mp_event_name = 'click_pre_purchase' AND ce.cta_name = 'continue' AND ce.payment_platform = 'stash'
            THEN ce.purchase_funnel_id
        END) as stash_continue,
        COUNT(DISTINCT CASE
            WHEN ce.mp_event_name = 'purchase_successful' AND ce.payment_platform = 'stash'
            THEN ce.purchase_funnel_id
        END) as stash_purchases,
        SUM(CASE
            WHEN ce.mp_event_name = 'purchase_successful' AND ce.payment_platform = 'stash'
            THEN COALESCE(ce.price_usd, 0) ELSE 0
        END) as stash_revenue,

        -- IAP funnel (Apple + Google)
        COUNT(DISTINCT CASE
            WHEN ce.mp_event_name = 'click_pre_purchase' AND ce.cta_name = 'continue' AND ce.payment_platform IN ('apple', 'googleplay')
            THEN ce.purchase_funnel_id
        END) as iap_continue,
        COUNT(DISTINCT CASE
            WHEN ce.mp_event_name = 'purchase_successful' AND ce.payment_platform IN ('apple', 'googleplay')
              AND ((ce.payment_platform = 'apple' AND ce.purchase_id IS NOT NULL AND ce.purchase_id != '')
                OR (ce.payment_platform = 'googleplay' AND ce.google_order_number IS NOT NULL AND ce.google_order_number != ''))
            THEN ce.purchase_funnel_id
        END) as iap_purchases,
        SUM(CASE
            WHEN ce.mp_event_name = 'purchase_successful'
              AND ((ce.payment_platform = 'apple' AND ce.purchase_id IS NOT NULL AND ce.purchase_id != '')
                OR (ce.payment_platform = 'googleplay' AND ce.google_order_number IS NOT NULL AND ce.google_order_number != ''))
            THEN COALESCE(ce.price_usd, 0) ELSE 0
        END) as iap_revenue

    FROM `yotam-395120.peerplay.vmp_master_event_normalized` ce
    INNER JOIN d2c_test_users t ON ce.distinct_id = t.distinct_id
    WHERE ce.date >= '{start_date}'
      AND ce.date <= '{end_date}'
      {os_filter}
      {version_filter}
      AND ce.mp_event_name IN ('purchase_click', 'click_pre_purchase', 'purchase_successful')
    GROUP BY ce.date
    ORDER BY ce.date
    """
    return query


def get_funnel_data(filters: Dict[str, Any]) -> pd.DataFrame:
    """Execute funnel query and return results."""
    query = build_funnel_query(filters)
    return run_query(query)


def get_daily_funnel_data(filters: Dict[str, Any]) -> pd.DataFrame:
    """Execute daily funnel query and return results."""
    query = build_daily_funnel_query(filters)
    return run_query(query)


def create_funnel_charts(df: pd.DataFrame) -> tuple:
    """
    Create two separate funnel visualizations - Stash and IAP side by side.

    Returns:
        Tuple of (stash_fig, iap_fig) - two separate Plotly figures
    """
    if df.empty:
        return go.Figure(), go.Figure()

    row = df.iloc[0]

    purchase_clicks = row['purchase_clicks']
    stash_continue = row['stash_continue']
    apple_continue = row['apple_continue']
    google_continue = row['google_continue']
    stash_purchases = row['stash_purchases']
    apple_purchases = row['apple_purchases']
    google_purchases = row['google_purchases']

    iap_continue = apple_continue + google_continue
    iap_purchases = apple_purchases + google_purchases

    # Stash (D2C) Funnel - Green colors
    fig_stash = go.Figure()
    fig_stash.add_trace(go.Funnel(
        name='Stash (D2C)',
        y=['Purchase Click', 'Continue to Stash', 'Purchase Success'],
        x=[purchase_clicks, stash_continue, stash_purchases],
        textposition="inside",
        textinfo="value+percent initial",
        marker=dict(color=['#27ae60', '#2ecc71', '#58d68d']),
        connector=dict(line=dict(color="#27ae60", width=2))
    ))
    fig_stash.update_layout(
        title="💚 Stash (D2C) Funnel",
        height=400,
        showlegend=False
    )

    # IAP (Apple/Google) Funnel - Red/Orange colors
    fig_iap = go.Figure()
    fig_iap.add_trace(go.Funnel(
        name='IAP (Apple/Google)',
        y=['Purchase Click', 'Continue to IAP', 'Purchase Success'],
        x=[purchase_clicks, iap_continue, iap_purchases],
        textposition="inside",
        textinfo="value+percent initial",
        marker=dict(color=['#e74c3c', '#c0392b', '#a93226']),
        connector=dict(line=dict(color="#e74c3c", width=2))
    ))
    fig_iap.update_layout(
        title="🔴 IAP (Apple/Google) Funnel",
        height=400,
        showlegend=False
    )

    return fig_stash, fig_iap


def create_prepurchase_choice_pie(df: pd.DataFrame) -> go.Figure:
    """
    Create a pie chart showing the distribution of pre-purchase choices.
    Shows how many users stayed with Stash (default) vs switched to IAP.
    """
    if df.empty:
        return go.Figure()

    row = df.iloc[0]

    stash_continue = row['stash_continue']
    iap_continue = row['apple_continue'] + row['google_continue']

    total_continue = stash_continue + iap_continue

    fig = go.Figure()

    fig.add_trace(go.Pie(
        labels=['💚 Stash (Default)', '🔴 IAP (Switched)'],
        values=[stash_continue, iap_continue],
        marker=dict(colors=['#2ecc71', '#e74c3c']),
        textinfo='label+percent+value',
        textposition='inside',
        insidetextorientation='horizontal',
        hole=0.3,  # Donut style
        textfont=dict(size=14)
    ))

    fig.update_layout(
        title=f"🥧 Pre-Purchase Choice Distribution (Total: {int(total_continue):,})",
        height=400,
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=-0.1, xanchor="center", x=0.5)
    )

    return fig


def create_daily_chart(df: pd.DataFrame, metric: str, title: str) -> go.Figure:
    """Create daily timeline chart for a specific metric."""
    if df.empty:
        return go.Figure()

    fig = go.Figure()

    if metric == 'revenue':
        fig.add_trace(go.Scatter(
            x=df['event_date'],
            y=df['stash_revenue'],
            name='Stash Revenue',
            mode='lines+markers',
            line=dict(color='#2ecc71', width=2)
        ))
        fig.add_trace(go.Scatter(
            x=df['event_date'],
            y=df['iap_revenue'],
            name='IAP Revenue',
            mode='lines+markers',
            line=dict(color='#e74c3c', width=2)
        ))
    elif metric == 'purchases':
        fig.add_trace(go.Scatter(
            x=df['event_date'],
            y=df['stash_purchases'],
            name='Stash Purchases',
            mode='lines+markers',
            line=dict(color='#2ecc71', width=2)
        ))
        fig.add_trace(go.Scatter(
            x=df['event_date'],
            y=df['iap_purchases'],
            name='IAP Purchases',
            mode='lines+markers',
            line=dict(color='#e74c3c', width=2)
        ))
    elif metric == 'conversion':
        # Calculate conversion rates
        df['stash_conversion'] = (df['stash_purchases'] / df['stash_continue'] * 100).fillna(0)
        df['iap_conversion'] = (df['iap_purchases'] / df['iap_continue'] * 100).fillna(0)

        fig.add_trace(go.Scatter(
            x=df['event_date'],
            y=df['stash_conversion'],
            name='Stash Conversion %',
            mode='lines+markers',
            line=dict(color='#2ecc71', width=2)
        ))
        fig.add_trace(go.Scatter(
            x=df['event_date'],
            y=df['iap_conversion'],
            name='IAP Conversion %',
            mode='lines+markers',
            line=dict(color='#e74c3c', width=2)
        ))

    fig.update_layout(
        title=title,
        height=350,
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        xaxis_title="Date",
        yaxis_title=metric.capitalize()
    )

    return fig


def build_d2c_first_vs_repeat_query(filters: Dict[str, Any]) -> str:
    """
    Build query to track first-time vs repeat D2C (Stash) purchasers by day.
    """
    start_date = filters.get('start_date')
    end_date = filters.get('end_date')

    # Build OS filter
    os_filter = ""
    if filters.get('mp_os'):
        os_values = ", ".join([f"'{os}'" for os in filters['mp_os']])
        os_filter = f"AND ce.mp_os IN ({os_values})"

    # Build version filter
    version_filter = ""
    if filters.get('version'):
        version_values = ", ".join([str(v) for v in filters['version']])
        version_filter = f"AND ce.version_float IN ({version_values})"

    query = f"""
    WITH d2c_test_users AS (
        -- Only Test group users (hash_bucket < 20)
        SELECT p.distinct_id
        FROM `yotam-395120.peerplay.dim_player` p
        JOIN (
            SELECT distinct_id, MAX(version_float) as latest_version
            FROM `yotam-395120.peerplay.vmp_master_event_normalized`
            WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
              AND version_float IS NOT NULL AND version_float > 0
            GROUP BY distinct_id
        ) v ON p.distinct_id = v.distinct_id
        WHERE p.first_country = 'US'
          AND v.latest_version > 0.378
          AND DATE_DIFF(CURRENT_DATE(), DATE(p.first_event_time), DAY) > 3
          AND MOD(ABS(FARM_FINGERPRINT(p.distinct_id)), 100) < 20
    ),
    stash_purchases AS (
        -- All Stash purchases with purchase ranking per user
        -- Look back 1 year to correctly identify first vs repeat purchases
        SELECT
            ce.distinct_id,
            ce.date as purchase_date,
            ce.purchase_funnel_id,
            ROW_NUMBER() OVER (PARTITION BY ce.distinct_id ORDER BY ce.date, ce.purchase_funnel_id) as purchase_number
        FROM `yotam-395120.peerplay.vmp_master_event_normalized` ce
        INNER JOIN d2c_test_users t ON ce.distinct_id = t.distinct_id
        WHERE ce.date >= DATE_SUB(DATE('{start_date}'), INTERVAL 365 DAY)
          AND ce.date <= '{end_date}'
          AND ce.mp_event_name = 'purchase_successful'
          AND ce.payment_platform = 'stash'
          {os_filter}
          {version_filter}
    ),
    daily_categorized AS (
        SELECT
            purchase_date,
            distinct_id,
            CASE WHEN purchase_number = 1 THEN 'first' ELSE 'repeat' END as purchase_type
        FROM stash_purchases
        WHERE purchase_date >= '{start_date}'
          AND purchase_date <= '{end_date}'
    )
    SELECT
        purchase_date as event_date,
        COUNT(DISTINCT CASE WHEN purchase_type = 'first' THEN distinct_id END) as first_purchase_users,
        COUNT(DISTINCT CASE WHEN purchase_type = 'repeat' THEN distinct_id END) as repeat_purchase_users
    FROM daily_categorized
    GROUP BY purchase_date
    ORDER BY purchase_date
    """
    return query


def get_d2c_first_vs_repeat_data(filters: Dict[str, Any]) -> pd.DataFrame:
    """Execute first vs repeat purchase query and return results."""
    query = build_d2c_first_vs_repeat_query(filters)
    return run_query(query)


def create_first_vs_repeat_chart(df: pd.DataFrame) -> go.Figure:
    """Create daily chart showing first-time vs repeat D2C purchasers."""
    if df.empty:
        return go.Figure()

    fig = go.Figure()

    # First purchase users - blue
    fig.add_trace(go.Scatter(
        x=df['event_date'],
        y=df['first_purchase_users'],
        name='First D2C Purchase',
        mode='lines+markers',
        line=dict(color='#3498db', width=2),
        marker=dict(size=8)
    ))

    # Repeat purchase users - orange
    fig.add_trace(go.Scatter(
        x=df['event_date'],
        y=df['repeat_purchase_users'],
        name='Repeat D2C Purchase (2nd+)',
        mode='lines+markers',
        line=dict(color='#e67e22', width=2),
        marker=dict(size=8)
    ))

    fig.update_layout(
        title="Daily D2C Purchasers: First Purchase vs Repeat",
        height=350,
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        xaxis_title="Date",
        yaxis_title="Users"
    )

    return fig


def build_d2c_adoption_funnel_query(filters: Dict[str, Any]) -> str:
    """
    Build query to show D2C adoption funnel - users by purchase number (1st, 2nd, 3rd, 4th+).
    """
    start_date = filters.get('start_date')
    end_date = filters.get('end_date')

    # Build OS filter
    os_filter = ""
    if filters.get('mp_os'):
        os_values = ", ".join([f"'{os}'" for os in filters['mp_os']])
        os_filter = f"AND ce.mp_os IN ({os_values})"

    # Build version filter
    version_filter = ""
    if filters.get('version'):
        version_values = ", ".join([str(v) for v in filters['version']])
        version_filter = f"AND ce.version_float IN ({version_values})"

    query = f"""
    WITH d2c_test_users AS (
        -- Only Test group users (hash_bucket < 20)
        SELECT p.distinct_id
        FROM `yotam-395120.peerplay.dim_player` p
        JOIN (
            SELECT distinct_id, MAX(version_float) as latest_version
            FROM `yotam-395120.peerplay.vmp_master_event_normalized`
            WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
              AND version_float IS NOT NULL AND version_float > 0
            GROUP BY distinct_id
        ) v ON p.distinct_id = v.distinct_id
        WHERE p.first_country = 'US'
          AND v.latest_version > 0.378
          AND DATE_DIFF(CURRENT_DATE(), DATE(p.first_event_time), DAY) > 3
          AND MOD(ABS(FARM_FINGERPRINT(p.distinct_id)), 100) < 20
    ),
    stash_purchases AS (
        -- All Stash purchases with purchase ranking per user
        SELECT
            ce.distinct_id,
            ce.date as purchase_date,
            ROW_NUMBER() OVER (PARTITION BY ce.distinct_id ORDER BY ce.date, ce.purchase_funnel_id) as purchase_number
        FROM `yotam-395120.peerplay.vmp_master_event_normalized` ce
        INNER JOIN d2c_test_users t ON ce.distinct_id = t.distinct_id
        WHERE ce.date >= DATE_SUB(DATE('{start_date}'), INTERVAL 365 DAY)
          AND ce.date <= '{end_date}'
          AND ce.mp_event_name = 'purchase_successful'
          AND ce.payment_platform = 'stash'
          {os_filter}
          {version_filter}
    ),
    user_max_purchase AS (
        -- Get the maximum purchase number for each user (within the date range)
        SELECT
            distinct_id,
            MAX(purchase_number) as max_purchase_num
        FROM stash_purchases
        WHERE purchase_date >= '{start_date}'
          AND purchase_date <= '{end_date}'
        GROUP BY distinct_id
    )
    SELECT
        COUNT(DISTINCT CASE WHEN max_purchase_num >= 1 THEN distinct_id END) as users_1st_purchase,
        COUNT(DISTINCT CASE WHEN max_purchase_num >= 2 THEN distinct_id END) as users_2nd_purchase,
        COUNT(DISTINCT CASE WHEN max_purchase_num >= 3 THEN distinct_id END) as users_3rd_purchase,
        COUNT(DISTINCT CASE WHEN max_purchase_num >= 4 THEN distinct_id END) as users_4th_plus_purchase
    FROM user_max_purchase
    """
    return query


def get_d2c_adoption_funnel_data(filters: Dict[str, Any]) -> pd.DataFrame:
    """Execute D2C adoption funnel query and return results."""
    query = build_d2c_adoption_funnel_query(filters)
    return run_query(query)


def build_d2c_atv_by_purchase_number_query(filters: Dict[str, Any]) -> str:
    """
    Build query to show Average Transaction Value (ATV) by purchase number.
    Shows if users spend more/less on 1st, 2nd, 3rd, 4th+ purchases.
    """
    start_date = filters.get('start_date')
    end_date = filters.get('end_date')

    # Build OS filter
    os_filter = ""
    if filters.get('mp_os'):
        os_values = ", ".join([f"'{os}'" for os in filters['mp_os']])
        os_filter = f"AND ce.mp_os IN ({os_values})"

    # Build version filter
    version_filter = ""
    if filters.get('version'):
        version_values = ", ".join([str(v) for v in filters['version']])
        version_filter = f"AND ce.version_float IN ({version_values})"

    query = f"""
    WITH d2c_test_users AS (
        SELECT p.distinct_id
        FROM `yotam-395120.peerplay.dim_player` p
        JOIN (
            SELECT distinct_id, MAX(version_float) as latest_version
            FROM `yotam-395120.peerplay.vmp_master_event_normalized`
            WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
              AND version_float IS NOT NULL AND version_float > 0
            GROUP BY distinct_id
        ) v ON p.distinct_id = v.distinct_id
        WHERE p.first_country = 'US'
          AND v.latest_version > 0.378
          AND DATE_DIFF(CURRENT_DATE(), DATE(p.first_event_time), DAY) > 3
          AND MOD(ABS(FARM_FINGERPRINT(p.distinct_id)), 100) < 20
    ),
    stash_purchases AS (
        SELECT
            ce.distinct_id,
            ce.date as purchase_date,
            ce.price_usd,
            ROW_NUMBER() OVER (PARTITION BY ce.distinct_id ORDER BY ce.date, ce.purchase_funnel_id) as purchase_number
        FROM `yotam-395120.peerplay.vmp_master_event_normalized` ce
        INNER JOIN d2c_test_users t ON ce.distinct_id = t.distinct_id
        WHERE ce.date >= DATE_SUB(DATE('{start_date}'), INTERVAL 365 DAY)
          AND ce.date <= '{end_date}'
          AND ce.mp_event_name = 'purchase_successful'
          AND ce.payment_platform = 'stash'
          {os_filter}
          {version_filter}
    )
    SELECT
        CASE
            WHEN purchase_number = 1 THEN '1st Purchase'
            WHEN purchase_number = 2 THEN '2nd Purchase'
            WHEN purchase_number = 3 THEN '3rd Purchase'
            ELSE '4th+ Purchase'
        END as purchase_tier,
        CASE
            WHEN purchase_number = 1 THEN 1
            WHEN purchase_number = 2 THEN 2
            WHEN purchase_number = 3 THEN 3
            ELSE 4
        END as tier_order,
        COUNT(*) as num_purchases,
        SUM(COALESCE(price_usd, 0)) as total_revenue,
        AVG(COALESCE(price_usd, 0)) as avg_transaction_value
    FROM stash_purchases
    WHERE purchase_date >= '{start_date}'
      AND purchase_date <= '{end_date}'
    GROUP BY 1, 2
    ORDER BY tier_order
    """
    return query


def get_d2c_atv_by_purchase_number(filters: Dict[str, Any]) -> pd.DataFrame:
    """Execute ATV by purchase number query."""
    query = build_d2c_atv_by_purchase_number_query(filters)
    return run_query(query)


def create_atv_by_purchase_chart(df: pd.DataFrame) -> go.Figure:
    """Create bar chart showing ATV by purchase number."""
    if df.empty:
        return go.Figure()

    fig = go.Figure()

    fig.add_trace(go.Bar(
        x=df['purchase_tier'],
        y=df['avg_transaction_value'],
        text=[f"${v:.2f}" for v in df['avg_transaction_value']],
        textposition='outside',
        marker=dict(color=['#3498db', '#2ecc71', '#f39c12', '#9b59b6'])
    ))

    fig.update_layout(
        title="Average Transaction Value by Purchase Number",
        height=350,
        xaxis_title="Purchase Number",
        yaxis_title="Average Transaction Value ($)",
        showlegend=False
    )

    return fig


def create_d2c_adoption_funnel_chart(df: pd.DataFrame) -> tuple:
    """
    Create funnel chart showing D2C adoption from 1st to 2nd to 3rd to 4th+ purchase.
    Returns tuple of (figure, metrics_dict) for display in app.
    """
    if df.empty:
        return go.Figure(), {}

    row = df.iloc[0]

    users_1st = int(row['users_1st_purchase'])
    users_2nd = int(row['users_2nd_purchase'])
    users_3rd = int(row['users_3rd_purchase'])
    users_4th_plus = int(row['users_4th_plus_purchase'])

    # Calculate retention rates (step-by-step)
    retention_1_to_2 = (users_2nd / users_1st * 100) if users_1st > 0 else 0
    retention_2_to_3 = (users_3rd / users_2nd * 100) if users_2nd > 0 else 0
    retention_3_to_4 = (users_4th_plus / users_3rd * 100) if users_3rd > 0 else 0

    # Calculate cumulative retention (from 1st purchase)
    cumulative_2nd = (users_2nd / users_1st * 100) if users_1st > 0 else 0
    cumulative_3rd = (users_3rd / users_1st * 100) if users_1st > 0 else 0
    cumulative_4th = (users_4th_plus / users_1st * 100) if users_1st > 0 else 0

    metrics = {
        'users_1st': users_1st,
        'users_2nd': users_2nd,
        'users_3rd': users_3rd,
        'users_4th_plus': users_4th_plus,
        'retention_1_to_2': retention_1_to_2,
        'retention_2_to_3': retention_2_to_3,
        'retention_3_to_4': retention_3_to_4,
        'cumulative_2nd': cumulative_2nd,
        'cumulative_3rd': cumulative_3rd,
        'cumulative_4th': cumulative_4th
    }

    fig = go.Figure()

    fig.add_trace(go.Funnel(
        name='D2C Adoption',
        y=['1st D2C Purchase', '2nd D2C Purchase', '3rd D2C Purchase', '4th+ D2C Purchase'],
        x=[users_1st, users_2nd, users_3rd, users_4th_plus],
        textposition="inside",
        textinfo="value+percent initial",
        marker=dict(color=['#2ecc71', '#27ae60', '#1e8449', '#145a32']),
        connector=dict(line=dict(color="#27ae60", width=2))
    ))

    fig.update_layout(
        title="D2C Adoption Funnel: Repeat Purchase Retention",
        height=400,
        showlegend=False
    )

    return fig, metrics


def build_time_to_first_d2c_purchase_query(filters: Dict[str, Any]) -> str:
    """
    Build query to show distribution of days from install to first D2C purchase.
    """
    start_date = filters.get('start_date')
    end_date = filters.get('end_date')

    # Build OS filter
    os_filter = ""
    if filters.get('mp_os'):
        os_values = ", ".join([f"'{os}'" for os in filters['mp_os']])
        os_filter = f"AND ce.mp_os IN ({os_values})"

    # Build version filter
    version_filter = ""
    if filters.get('version'):
        version_values = ", ".join([str(v) for v in filters['version']])
        version_filter = f"AND ce.version_float IN ({version_values})"

    query = f"""
    WITH d2c_test_users AS (
        SELECT
            p.distinct_id,
            DATE(p.first_event_time) as install_date
        FROM `yotam-395120.peerplay.dim_player` p
        JOIN (
            SELECT distinct_id, MAX(version_float) as latest_version
            FROM `yotam-395120.peerplay.vmp_master_event_normalized`
            WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
              AND version_float IS NOT NULL AND version_float > 0
            GROUP BY distinct_id
        ) v ON p.distinct_id = v.distinct_id
        WHERE p.first_country = 'US'
          AND v.latest_version > 0.378
          AND DATE_DIFF(CURRENT_DATE(), DATE(p.first_event_time), DAY) > 3
          AND MOD(ABS(FARM_FINGERPRINT(p.distinct_id)), 100) < 20
    ),
    first_d2c_purchase AS (
        SELECT
            ce.distinct_id,
            MIN(ce.date) as first_purchase_date
        FROM `yotam-395120.peerplay.vmp_master_event_normalized` ce
        INNER JOIN d2c_test_users t ON ce.distinct_id = t.distinct_id
        WHERE ce.date >= DATE_SUB(DATE('{start_date}'), INTERVAL 365 DAY)
          AND ce.date <= '{end_date}'
          AND ce.mp_event_name = 'purchase_successful'
          AND ce.payment_platform = 'stash'
          {os_filter}
          {version_filter}
        GROUP BY ce.distinct_id
    ),
    time_to_purchase AS (
        SELECT
            t.distinct_id,
            t.install_date,
            f.first_purchase_date,
            DATE_DIFF(f.first_purchase_date, t.install_date, DAY) as days_to_first_purchase
        FROM d2c_test_users t
        INNER JOIN first_d2c_purchase f ON t.distinct_id = f.distinct_id
        WHERE f.first_purchase_date >= '{start_date}'
          AND f.first_purchase_date <= '{end_date}'
    )
    SELECT
        CASE
            WHEN days_to_first_purchase = 0 THEN 'Day 0 (Same day)'
            WHEN days_to_first_purchase = 1 THEN 'Day 1'
            WHEN days_to_first_purchase BETWEEN 2 AND 3 THEN 'Day 2-3'
            WHEN days_to_first_purchase BETWEEN 4 AND 7 THEN 'Day 4-7'
            WHEN days_to_first_purchase BETWEEN 8 AND 14 THEN 'Day 8-14'
            WHEN days_to_first_purchase BETWEEN 15 AND 30 THEN 'Day 15-30'
            ELSE 'Day 31+'
        END as days_bucket,
        CASE
            WHEN days_to_first_purchase = 0 THEN 1
            WHEN days_to_first_purchase = 1 THEN 2
            WHEN days_to_first_purchase BETWEEN 2 AND 3 THEN 3
            WHEN days_to_first_purchase BETWEEN 4 AND 7 THEN 4
            WHEN days_to_first_purchase BETWEEN 8 AND 14 THEN 5
            WHEN days_to_first_purchase BETWEEN 15 AND 30 THEN 6
            ELSE 7
        END as bucket_order,
        COUNT(*) as users,
        AVG(days_to_first_purchase) as avg_days
    FROM time_to_purchase
    GROUP BY 1, 2
    ORDER BY bucket_order
    """
    return query


def get_time_to_first_d2c_purchase(filters: Dict[str, Any]) -> pd.DataFrame:
    """Execute time to first D2C purchase query."""
    query = build_time_to_first_d2c_purchase_query(filters)
    return run_query(query)


def create_time_to_first_purchase_chart(df: pd.DataFrame) -> go.Figure:
    """Create bar chart showing distribution of time to first D2C purchase."""
    if df.empty:
        return go.Figure()

    total_users = df['users'].sum()

    fig = go.Figure()

    fig.add_trace(go.Bar(
        x=df['days_bucket'],
        y=df['users'],
        text=[f"{u:,} ({u/total_users*100:.1f}%)" for u in df['users']],
        textposition='outside',
        marker=dict(color='#3498db')
    ))

    fig.update_layout(
        title=f"Time to First D2C Purchase (Total: {total_users:,} users)",
        height=350,
        xaxis_title="Days Since Install",
        yaxis_title="Number of Users",
        showlegend=False
    )

    return fig


def get_time_to_first_purchase_stats(df: pd.DataFrame) -> dict:
    """Calculate summary statistics for time to first purchase."""
    if df.empty:
        return {}

    total_users = df['users'].sum()

    # Calculate weighted average
    weighted_avg = sum(df['avg_days'] * df['users']) / total_users if total_users > 0 else 0

    # Calculate users in each bucket
    day_0_users = df[df['days_bucket'] == 'Day 0 (Same day)']['users'].sum() if 'Day 0 (Same day)' in df['days_bucket'].values else 0
    week_1_users = df[df['bucket_order'] <= 4]['users'].sum()  # Day 0-7

    return {
        'total_users': total_users,
        'avg_days': weighted_avg,
        'day_0_pct': (day_0_users / total_users * 100) if total_users > 0 else 0,
        'week_1_pct': (week_1_users / total_users * 100) if total_users > 0 else 0
    }


def build_stash_funnel_execution_query(filters: Dict[str, Any]) -> str:
    """
    Build query for Stash funnel executions for D2C Test group only.
    Tracks the full Stash purchase funnel from purchase_click to rewards_store.
    """
    start_date = filters.get('start_date')
    end_date = filters.get('end_date')

    # Build OS filter
    os_filter = ""
    if filters.get('mp_os'):
        os_values = ", ".join([f"'{os}'" for os in filters['mp_os']])
        os_filter = f"AND ce.mp_os IN ({os_values})"

    # Build version filter
    version_filter = ""
    if filters.get('version'):
        version_values = ", ".join([str(v) for v in filters['version']])
        version_filter = f"AND ce.version_float IN ({version_values})"

    query = f"""
    WITH d2c_test_users AS (
        -- Only Test group users (hash_bucket < 20)
        SELECT p.distinct_id
        FROM `yotam-395120.peerplay.dim_player` p
        JOIN (
            SELECT distinct_id, MAX(version_float) as latest_version
            FROM `yotam-395120.peerplay.vmp_master_event_normalized`
            WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
              AND version_float IS NOT NULL AND version_float > 0
            GROUP BY distinct_id
        ) v ON p.distinct_id = v.distinct_id
        WHERE p.first_country = 'US'
          AND v.latest_version > 0.378
          AND DATE_DIFF(CURRENT_DATE(), DATE(p.first_event_time), DAY) > 3
          AND MOD(ABS(FARM_FINGERPRINT(p.distinct_id)), 100) < 20
    ),
    client_events AS (
        SELECT
            ce.distinct_id,
            ce.mp_event_name,
            ce.purchase_funnel_id,
            ce.cta_name,
            ce.payment_platform
        FROM `yotam-395120.peerplay.vmp_master_event_normalized` ce
        INNER JOIN d2c_test_users t ON ce.distinct_id = t.distinct_id
        WHERE ce.date >= '{start_date}'
          AND ce.date <= '{end_date}'
          {os_filter}
          {version_filter}
          AND ce.purchase_funnel_id IS NOT NULL
    ),
    server_events AS (
        SELECT DISTINCT
            se.distinct_id,
            se.event_name,
            se.transaction_id,
            se.cta_name
        FROM `yotam-395120.peerplay.verification_service_events` se
        INNER JOIN d2c_test_users t ON se.distinct_id = t.distinct_id
        WHERE se.date >= '{start_date}'
          AND se.date <= '{end_date}'
          AND se.transaction_id IS NOT NULL
    )
    SELECT
      -- Step 1: Purchase Click
      (SELECT COUNT(DISTINCT purchase_funnel_id) FROM client_events
       WHERE mp_event_name = 'purchase_click') as funnels_purchase_click,

      -- Step 2: Changed Selection (select_stash or select_iap)
      (SELECT COUNT(DISTINCT purchase_funnel_id) FROM client_events
       WHERE mp_event_name = 'click_pre_purchase' AND cta_name IN ('select_stash', 'select_iap')) as funnels_changed_selection,

      -- Step 3: Stash Continue
      (SELECT COUNT(DISTINCT purchase_funnel_id) FROM client_events
       WHERE mp_event_name = 'click_pre_purchase' AND cta_name = 'continue' AND payment_platform = 'stash') as funnels_stash_continue,

      -- Step 4: Native Popup
      (SELECT COUNT(DISTINCT purchase_funnel_id) FROM client_events
       WHERE mp_event_name = 'purchase_native_popup_impression' AND payment_platform = 'stash') as funnels_native_popup,

      -- Step 5: Webform Impression
      (SELECT COUNT(DISTINCT transaction_id) FROM server_events
       WHERE event_name = 'stash_form_webhook_impression_checkout_loading_started') as funnels_webform_impression,

      -- Step 6: Webform Add New Card
      (SELECT COUNT(DISTINCT transaction_id) FROM server_events
       WHERE event_name = 'stash_form_webhook_click_in_add_new_card') as funnels_webform_add_card,

      -- Step 7: Webform Pay Click
      (SELECT COUNT(DISTINCT transaction_id) FROM server_events
       WHERE event_name = 'stash_form_webhook_click_in_checkout' AND cta_name = 'pay') as funnels_webform_pay_click,

      -- Step 8: Webform Purchase Successful
      (SELECT COUNT(DISTINCT transaction_id) FROM server_events
       WHERE event_name = 'stash_webhook_purchase_succeeded') as funnels_webform_success,

      -- Step 9: Client Purchase Successful
      (SELECT COUNT(DISTINCT purchase_funnel_id) FROM client_events
       WHERE mp_event_name = 'purchase_successful' AND payment_platform = 'stash') as funnels_client_success,

      -- Step 10: Validation Request
      (SELECT COUNT(DISTINCT purchase_funnel_id) FROM client_events
       WHERE mp_event_name = 'purchase_verification_request' AND payment_platform = 'stash') as funnels_validation_request,

      -- Step 11: Validation Approval
      (SELECT COUNT(DISTINCT purchase_funnel_id) FROM client_events
       WHERE mp_event_name = 'purchase_verification_approval' AND payment_platform = 'stash') as funnels_validation_approval,

      -- Step 12: Rewards Granted
      (SELECT COUNT(DISTINCT purchase_funnel_id) FROM client_events
       WHERE mp_event_name = 'rewards_store' AND payment_platform = 'stash') as funnels_rewards_granted
    """
    return query


def get_stash_funnel_execution_data(filters: Dict[str, Any]) -> pd.DataFrame:
    """Execute Stash funnel execution query for D2C Test group."""
    query = build_stash_funnel_execution_query(filters)
    return run_query(query)


def create_stash_funnel_execution_chart(df: pd.DataFrame) -> go.Figure:
    """Create bar chart showing Stash funnel execution percentages."""
    if df.empty:
        return go.Figure()

    row = df.iloc[0]

    # Base count (purchase clicks)
    base_count = row['funnels_purchase_click']
    if base_count == 0:
        return go.Figure()

    # Build metrics with counts and percentages
    metrics = [
        ("Purchase Click", row['funnels_purchase_click'], 100.0),
        ("Changed Selection", row['funnels_changed_selection'], (row['funnels_changed_selection'] / base_count) * 100),
        ("Stash Continue", row['funnels_stash_continue'], (row['funnels_stash_continue'] / base_count) * 100),
        ("Native Popup", row['funnels_native_popup'], (row['funnels_native_popup'] / base_count) * 100),
        ("Webform Impression", row['funnels_webform_impression'], (row['funnels_webform_impression'] / base_count) * 100),
        ("Webform Add Card", row['funnels_webform_add_card'], (row['funnels_webform_add_card'] / base_count) * 100),
        ("Webform Pay Click", row['funnels_webform_pay_click'], (row['funnels_webform_pay_click'] / base_count) * 100),
        ("Webform Success", row['funnels_webform_success'], (row['funnels_webform_success'] / base_count) * 100),
        ("Client Success", row['funnels_client_success'], (row['funnels_client_success'] / base_count) * 100),
        ("Validation Request", row['funnels_validation_request'], (row['funnels_validation_request'] / base_count) * 100),
        ("Validation Approval", row['funnels_validation_approval'], (row['funnels_validation_approval'] / base_count) * 100),
        ("Rewards Granted", row['funnels_rewards_granted'], (row['funnels_rewards_granted'] / base_count) * 100),
    ]

    labels = [m[0] for m in metrics]
    percentages = [m[2] for m in metrics]
    counts = [int(m[1]) for m in metrics]

    # Format text to show both count and percentage
    text_labels = [f"{c:,}<br>({p:.1f}%)" for c, p in zip(counts, percentages)]

    fig = go.Figure()

    fig.add_trace(go.Bar(
        x=labels,
        y=percentages,
        text=text_labels,
        textposition='outside',
        marker=dict(color='#2ecc71'),
        hovertemplate='%{x}<br>Count: %{customdata:,}<br>Percentage: %{y:.1f}%<extra></extra>',
        customdata=counts
    ))

    fig.update_layout(
        title="Stash Funnel Executions (% from Purchase Click) - Test Group Only",
        height=500,
        xaxis_title="Funnel Step",
        yaxis_title="Percentage (%)",
        xaxis_tickangle=-45,
        showlegend=False,
        yaxis=dict(range=[0, max(percentages) * 1.15])  # Add 15% headroom for labels
    )

    return fig


def get_stash_funnel_metrics(df: pd.DataFrame) -> dict:
    """Extract key metrics from the Stash funnel data."""
    if df.empty:
        return {}

    row = df.iloc[0]
    base_count = row['funnels_purchase_click']

    if base_count == 0:
        return {}

    # Calculate key conversion rates
    continue_rate = (row['funnels_stash_continue'] / base_count * 100) if base_count > 0 else 0
    webform_rate = (row['funnels_webform_impression'] / row['funnels_stash_continue'] * 100) if row['funnels_stash_continue'] > 0 else 0
    pay_click_rate = (row['funnels_webform_pay_click'] / row['funnels_webform_impression'] * 100) if row['funnels_webform_impression'] > 0 else 0
    success_rate = (row['funnels_client_success'] / row['funnels_webform_pay_click'] * 100) if row['funnels_webform_pay_click'] > 0 else 0
    overall_conversion = (row['funnels_client_success'] / base_count * 100) if base_count > 0 else 0

    return {
        'purchase_clicks': int(row['funnels_purchase_click']),
        'stash_continues': int(row['funnels_stash_continue']),
        'webform_impressions': int(row['funnels_webform_impression']),
        'purchases': int(row['funnels_client_success']),
        'continue_rate': continue_rate,
        'webform_rate': webform_rate,
        'pay_click_rate': pay_click_rate,
        'success_rate': success_rate,
        'overall_conversion': overall_conversion
    }
