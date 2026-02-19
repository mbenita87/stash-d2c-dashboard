"""Chart: D2C Test Funnel - Funnel analysis for Test group only."""

from typing import Dict, Any
import pandas as pd
import plotly.graph_objects as go
from utils.bigquery_client import run_query


def get_effective_start_date(filters: Dict[str, Any]) -> str:
    """
    Get the effective start date considering the test start date.
    Returns the later of start_date and test_start_date.
    If test hasn't started yet (test_start_date > end_date), returns start_date.
    """
    start_date = filters.get('start_date')
    end_date = filters.get('end_date')
    test_start_date = filters.get('test_start_date')

    # If no test_start_date, use start_date
    if not test_start_date:
        return start_date

    # If test_start_date is after end_date, test hasn't started yet - use original start_date
    if str(test_start_date) > str(end_date):
        return start_date

    # Compare as strings (YYYY-MM-DD format allows string comparison)
    if str(start_date) < str(test_start_date):
        return test_start_date
    return start_date


# Firebase segment CTE - reusable across all queries
FIREBASE_SEGMENT_CTE = """
    firebase_segment_events AS (
        SELECT
            distinct_id,
            CASE
                WHEN firebase_segments LIKE '%LiveOpsData.stash_test%' THEN 'test'
                WHEN firebase_segments LIKE '%LiveOpsData.stash_control%' THEN 'control'
            END as segment,
            ROW_NUMBER() OVER (PARTITION BY distinct_id ORDER BY date DESC, time DESC) as rn
        FROM `yotam-395120.peerplay.vmp_master_event_normalized`
        WHERE mp_event_name = 'dynamic_configuration_loaded'
          AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
          AND (firebase_segments LIKE '%LiveOpsData.stash_test%'
               OR firebase_segments LIKE '%LiveOpsData.stash_control%')
    ),
    d2c_test_users AS (
        -- Only Test group users (Firebase segment: stash_test)
        SELECT p.distinct_id
        FROM `yotam-395120.peerplay.dim_player` p
        INNER JOIN (
            SELECT distinct_id
            FROM firebase_segment_events
            WHERE rn = 1 AND segment = 'test'
        ) fs ON p.distinct_id = fs.distinct_id
        WHERE p.first_country = 'US'
    ),
    d2c_control_users AS (
        -- Only Control group users (Firebase segment: stash_control)
        SELECT p.distinct_id
        FROM `yotam-395120.peerplay.dim_player` p
        INNER JOIN (
            SELECT distinct_id
            FROM firebase_segment_events
            WHERE rn = 1 AND segment = 'control'
        ) fs ON p.distinct_id = fs.distinct_id
        WHERE p.first_country = 'US'
    ),
"""


def build_funnel_query(filters: Dict[str, Any]) -> str:
    """
    Build SQL query for D2C Test group funnel metrics.
    Only includes Test group users (Firebase segment: stash_test).
    Data is filtered to only include events after test start date.
    """
    start_date = get_effective_start_date(filters)
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
    WITH {FIREBASE_SEGMENT_CTE}
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
    -- Get funnels that had IAP continue (to filter IAP purchases)
    iap_continue_funnels AS (
        SELECT DISTINCT purchase_funnel_id
        FROM funnel_events
        WHERE mp_event_name = 'click_pre_purchase'
          AND cta_name = 'continue'
          AND payment_platform IN ('apple', 'googleplay')
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
            -- Stash: count all successful purchases
            COUNT(DISTINCT CASE
                WHEN mp_event_name = 'purchase_successful' AND payment_platform = 'stash'
                THEN purchase_funnel_id
            END) as stash_purchases,

            -- IAP: only count purchases that went through the pre-purchase flow (had IAP continue)
            COUNT(DISTINCT CASE
                WHEN mp_event_name = 'purchase_successful' AND payment_platform = 'apple'
                  AND purchase_id IS NOT NULL AND purchase_id != ''
                  AND purchase_funnel_id IN (SELECT purchase_funnel_id FROM iap_continue_funnels)
                THEN purchase_funnel_id
            END) as apple_purchases,

            COUNT(DISTINCT CASE
                WHEN mp_event_name = 'purchase_successful' AND payment_platform = 'googleplay'
                  AND google_order_number IS NOT NULL AND google_order_number != ''
                  AND purchase_funnel_id IN (SELECT purchase_funnel_id FROM iap_continue_funnels)
                THEN purchase_funnel_id
            END) as google_purchases,

            -- Revenue (by platform)
            SUM(CASE
                WHEN mp_event_name = 'purchase_successful' AND payment_platform = 'stash'
                THEN COALESCE(price_usd, 0) ELSE 0
            END) as stash_revenue,

            -- IAP Revenue: only count from purchases that went through the pre-purchase flow
            SUM(CASE
                WHEN mp_event_name = 'purchase_successful' AND payment_platform = 'apple'
                  AND purchase_id IS NOT NULL AND purchase_id != ''
                  AND purchase_funnel_id IN (SELECT purchase_funnel_id FROM iap_continue_funnels)
                THEN COALESCE(price_usd, 0) ELSE 0
            END) as apple_revenue,

            SUM(CASE
                WHEN mp_event_name = 'purchase_successful' AND payment_platform = 'googleplay'
                  AND google_order_number IS NOT NULL AND google_order_number != ''
                  AND purchase_funnel_id IN (SELECT purchase_funnel_id FROM iap_continue_funnels)
                THEN COALESCE(price_usd, 0) ELSE 0
            END) as google_revenue,

            -- Paying users (by platform)
            COUNT(DISTINCT CASE
                WHEN mp_event_name = 'purchase_successful' AND payment_platform = 'stash'
                THEN distinct_id
            END) as stash_paying_users,

            -- IAP Paying users: only count from purchases that went through the pre-purchase flow
            COUNT(DISTINCT CASE
                WHEN mp_event_name = 'purchase_successful' AND payment_platform = 'apple'
                  AND purchase_id IS NOT NULL AND purchase_id != ''
                  AND purchase_funnel_id IN (SELECT purchase_funnel_id FROM iap_continue_funnels)
                THEN distinct_id
            END) as apple_paying_users,

            COUNT(DISTINCT CASE
                WHEN mp_event_name = 'purchase_successful' AND payment_platform = 'googleplay'
                  AND google_order_number IS NOT NULL AND google_order_number != ''
                  AND purchase_funnel_id IN (SELECT purchase_funnel_id FROM iap_continue_funnels)
                THEN distinct_id
            END) as google_paying_users
        FROM funnel_events
    )
    SELECT * FROM funnel_metrics
    """
    return query


def build_daily_funnel_query(filters: Dict[str, Any]) -> str:
    """Build query for daily funnel metrics for timeline chart.
    Data is filtered to only include events after test start date."""
    start_date = get_effective_start_date(filters)
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
    WITH {FIREBASE_SEGMENT_CTE}
    dummy AS (SELECT 1)
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

    # Convert to native Python int to avoid Plotly issues with numpy types
    purchase_clicks = int(row['purchase_clicks']) if pd.notna(row['purchase_clicks']) else 0
    stash_continue = int(row['stash_continue']) if pd.notna(row['stash_continue']) else 0
    apple_continue = int(row['apple_continue']) if pd.notna(row['apple_continue']) else 0
    google_continue = int(row['google_continue']) if pd.notna(row['google_continue']) else 0
    stash_purchases = int(row['stash_purchases']) if pd.notna(row['stash_purchases']) else 0
    apple_purchases = int(row['apple_purchases']) if pd.notna(row['apple_purchases']) else 0
    google_purchases = int(row['google_purchases']) if pd.notna(row['google_purchases']) else 0

    iap_continue = apple_continue + google_continue
    iap_purchases = apple_purchases + google_purchases

    # Stash (D2C) Funnel - Green colors
    # Filter out zero values to avoid Plotly Funnel errors
    fig_stash = go.Figure()
    stash_labels = []
    stash_values = []
    stash_colors = []
    all_stash_labels = ['Purchase Click', 'Continue to Stash', 'Purchase Success']
    all_stash_values = [purchase_clicks, stash_continue, stash_purchases]
    all_stash_colors = ['#27ae60', '#2ecc71', '#58d68d']

    for label, value, color in zip(all_stash_labels, all_stash_values, all_stash_colors):
        if value > 0:
            stash_labels.append(label)
            stash_values.append(value)
            stash_colors.append(color)

    if stash_values:
        fig_stash.add_trace(go.Funnel(
            name='Stash (D2C)',
            y=stash_labels,
            x=stash_values,
            textposition="inside",
            textinfo="value+percent initial",
            marker=dict(color=stash_colors),
            connector=dict(line=dict(color="#27ae60", width=2))
        ))
    fig_stash.update_layout(
        title="💚 Stash (D2C) Funnel",
        height=400,
        showlegend=False
    )

    # IAP (Apple/Google) Funnel - Red/Orange colors
    # Filter out zero values to avoid Plotly Funnel errors
    fig_iap = go.Figure()
    iap_labels = []
    iap_values = []
    iap_colors = []
    all_iap_labels = ['Purchase Click', 'Continue to IAP', 'Purchase Success']
    all_iap_values = [purchase_clicks, iap_continue, iap_purchases]
    all_iap_colors = ['#e74c3c', '#c0392b', '#a93226']

    for label, value, color in zip(all_iap_labels, all_iap_values, all_iap_colors):
        if value > 0:
            iap_labels.append(label)
            iap_values.append(value)
            iap_colors.append(color)

    if iap_values:
        fig_iap.add_trace(go.Funnel(
            name='IAP (Apple/Google)',
            y=iap_labels,
            x=iap_values,
            textposition="inside",
            textinfo="value+percent initial",
            marker=dict(color=iap_colors),
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

    # Convert to native Python int to avoid Plotly issues with numpy types
    stash_continue = int(row['stash_continue']) if pd.notna(row['stash_continue']) else 0
    apple_continue = int(row['apple_continue']) if pd.notna(row['apple_continue']) else 0
    google_continue = int(row['google_continue']) if pd.notna(row['google_continue']) else 0
    iap_continue = apple_continue + google_continue

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

    # Convert BigQuery types to native Python types to avoid Plotly issues
    # Convert event_date to string to ensure compatibility
    event_dates = [str(d) if d is not None else None for d in df['event_date'].tolist()]

    fig = go.Figure()

    if metric == 'revenue':
        stash_rev = [float(v) if pd.notna(v) and v is not None else 0.0 for v in df['stash_revenue'].tolist()]
        iap_rev = [float(v) if pd.notna(v) and v is not None else 0.0 for v in df['iap_revenue'].tolist()]
        fig.add_trace(go.Scatter(
            x=event_dates,
            y=stash_rev,
            name='Stash Revenue',
            mode='lines+markers',
            line=dict(color='#2ecc71', width=2)
        ))
        fig.add_trace(go.Scatter(
            x=event_dates,
            y=iap_rev,
            name='IAP Revenue',
            mode='lines+markers',
            line=dict(color='#e74c3c', width=2)
        ))
    elif metric == 'purchases':
        stash_purch = [int(v) if pd.notna(v) and v is not None else 0 for v in df['stash_purchases'].tolist()]
        iap_purch = [int(v) if pd.notna(v) and v is not None else 0 for v in df['iap_purchases'].tolist()]
        fig.add_trace(go.Scatter(
            x=event_dates,
            y=stash_purch,
            name='Stash Purchases',
            mode='lines+markers',
            line=dict(color='#2ecc71', width=2)
        ))
        fig.add_trace(go.Scatter(
            x=event_dates,
            y=iap_purch,
            name='IAP Purchases',
            mode='lines+markers',
            line=dict(color='#e74c3c', width=2)
        ))
    elif metric == 'conversion':
        # Calculate conversion rates with safe division
        stash_conv = []
        iap_conv = []
        for _, row in df.iterrows():
            stash_continue = float(row['stash_continue']) if pd.notna(row['stash_continue']) else 0
            stash_purchases = float(row['stash_purchases']) if pd.notna(row['stash_purchases']) else 0
            iap_continue = float(row['iap_continue']) if pd.notna(row['iap_continue']) else 0
            iap_purchases = float(row['iap_purchases']) if pd.notna(row['iap_purchases']) else 0

            stash_conv.append((stash_purchases / stash_continue * 100) if stash_continue > 0 else 0.0)
            iap_conv.append((iap_purchases / iap_continue * 100) if iap_continue > 0 else 0.0)

        fig.add_trace(go.Scatter(
            x=event_dates,
            y=stash_conv,
            name='Stash Conversion %',
            mode='lines+markers',
            line=dict(color='#2ecc71', width=2)
        ))
        fig.add_trace(go.Scatter(
            x=event_dates,
            y=iap_conv,
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
    Data is filtered to only include events after test start date.
    """
    start_date = get_effective_start_date(filters)
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
    WITH {FIREBASE_SEGMENT_CTE}
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

    # Convert to native Python types to avoid Plotly issues with numpy types
    event_dates = df['event_date'].tolist()
    first_purchase = [int(v) if pd.notna(v) else 0 for v in df['first_purchase_users']]
    repeat_purchase = [int(v) if pd.notna(v) else 0 for v in df['repeat_purchase_users']]

    fig = go.Figure()

    # First purchase users - blue
    fig.add_trace(go.Scatter(
        x=event_dates,
        y=first_purchase,
        name='First D2C Purchase',
        mode='lines+markers',
        line=dict(color='#3498db', width=2),
        marker=dict(size=8)
    ))

    # Repeat purchase users - orange
    fig.add_trace(go.Scatter(
        x=event_dates,
        y=repeat_purchase,
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
    Data is filtered to only include events after test start date.
    """
    start_date = get_effective_start_date(filters)
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
    WITH {FIREBASE_SEGMENT_CTE}
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
    Data is filtered to only include events after test start date.
    """
    start_date = get_effective_start_date(filters)
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
    WITH {FIREBASE_SEGMENT_CTE}
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

    # Convert to native Python types to avoid Plotly issues with numpy types
    atv_values = [float(v) for v in df['avg_transaction_value']]
    purchase_tiers = df['purchase_tier'].tolist()

    fig = go.Figure()

    fig.add_trace(go.Bar(
        x=purchase_tiers,
        y=atv_values,
        text=[f"${v:.2f}" for v in atv_values],
        textposition='outside',
        marker=dict(color=['#3498db', '#2ecc71', '#f39c12', '#9b59b6'][:len(atv_values)])
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

    # Convert to native Python int to avoid Plotly issues with numpy types
    users_1st = int(row['users_1st_purchase']) if pd.notna(row['users_1st_purchase']) else 0
    users_2nd = int(row['users_2nd_purchase']) if pd.notna(row['users_2nd_purchase']) else 0
    users_3rd = int(row['users_3rd_purchase']) if pd.notna(row['users_3rd_purchase']) else 0
    users_4th_plus = int(row['users_4th_plus_purchase']) if pd.notna(row['users_4th_plus_purchase']) else 0

    # Calculate retention rates (step-by-step)
    retention_1_to_2 = float((users_2nd / users_1st * 100) if users_1st > 0 else 0)
    retention_2_to_3 = float((users_3rd / users_2nd * 100) if users_2nd > 0 else 0)
    retention_3_to_4 = float((users_4th_plus / users_3rd * 100) if users_3rd > 0 else 0)

    # Calculate cumulative retention (from 1st purchase)
    cumulative_2nd = float((users_2nd / users_1st * 100) if users_1st > 0 else 0)
    cumulative_3rd = float((users_3rd / users_1st * 100) if users_1st > 0 else 0)
    cumulative_4th = float((users_4th_plus / users_1st * 100) if users_1st > 0 else 0)

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

    # Build funnel data, filtering out zero values to avoid Plotly Funnel errors
    labels = []
    values = []
    colors = []
    all_labels = ['1st D2C Purchase', '2nd D2C Purchase', '3rd D2C Purchase', '4th+ D2C Purchase']
    all_values = [users_1st, users_2nd, users_3rd, users_4th_plus]
    all_colors = ['#2ecc71', '#27ae60', '#1e8449', '#145a32']

    for i, (label, value, color) in enumerate(zip(all_labels, all_values, all_colors)):
        if value > 0:
            labels.append(label)
            values.append(value)
            colors.append(color)

    # Only create funnel if we have at least one non-zero value
    if values:
        fig.add_trace(go.Funnel(
            name='D2C Adoption',
            y=labels,
            x=values,
            textposition="inside",
            textinfo="value+percent initial",
            marker=dict(color=colors),
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
    Data is filtered to only include events after test start date.
    """
    start_date = get_effective_start_date(filters)
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
    WITH {FIREBASE_SEGMENT_CTE}
    d2c_test_users_with_install AS (
        SELECT
            t.distinct_id,
            DATE(p.first_event_time) as install_date
        FROM d2c_test_users t
        JOIN `yotam-395120.peerplay.dim_player` p ON t.distinct_id = p.distinct_id
    ),
    first_d2c_purchase AS (
        SELECT
            ce.distinct_id,
            MIN(ce.date) as first_purchase_date
        FROM `yotam-395120.peerplay.vmp_master_event_normalized` ce
        INNER JOIN d2c_test_users_with_install t ON ce.distinct_id = t.distinct_id
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
        FROM d2c_test_users_with_install t
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

    # Convert to native Python int to avoid Plotly issues with numpy types
    total_users = int(df['users'].sum())
    users_list = [int(u) for u in df['users']]

    fig = go.Figure()

    fig.add_trace(go.Bar(
        x=df['days_bucket'].tolist(),
        y=users_list,
        text=[f"{u:,} ({u/total_users*100:.1f}%)" if total_users > 0 else "0" for u in users_list],
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

    # Convert to native Python types to avoid issues with numpy types
    total_users = int(df['users'].sum())

    # Calculate weighted average
    weighted_sum = sum(float(avg) * int(users) for avg, users in zip(df['avg_days'], df['users']))
    weighted_avg = float(weighted_sum / total_users) if total_users > 0 else 0.0

    # Calculate users in each bucket
    day_0_users = int(df[df['days_bucket'] == 'Day 0 (Same day)']['users'].sum()) if 'Day 0 (Same day)' in df['days_bucket'].values else 0
    week_1_users = int(df[df['bucket_order'] <= 4]['users'].sum())  # Day 0-7

    return {
        'total_users': total_users,
        'avg_days': weighted_avg,
        'day_0_pct': float(day_0_users / total_users * 100) if total_users > 0 else 0.0,
        'week_1_pct': float(week_1_users / total_users * 100) if total_users > 0 else 0.0
    }


def build_stash_funnel_execution_query(filters: Dict[str, Any]) -> str:
    """
    Build query for Stash funnel executions for D2C Test group only.
    Tracks the full Stash purchase funnel from purchase_click to rewards_store.
    Data is filtered to only include events after test start date.
    """
    start_date = get_effective_start_date(filters)
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
    WITH {FIREBASE_SEGMENT_CTE}
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

    # Helper function to safely convert to int
    def safe_int(val):
        return int(val) if pd.notna(val) else 0

    # Base count (purchase clicks)
    base_count = safe_int(row['funnels_purchase_click'])
    if base_count == 0:
        return go.Figure()

    # Extract all values safely
    funnels_purchase_click = safe_int(row['funnels_purchase_click'])
    funnels_changed_selection = safe_int(row['funnels_changed_selection'])
    funnels_stash_continue = safe_int(row['funnels_stash_continue'])
    funnels_native_popup = safe_int(row['funnels_native_popup'])
    funnels_webform_impression = safe_int(row['funnels_webform_impression'])
    funnels_webform_add_card = safe_int(row['funnels_webform_add_card'])
    funnels_webform_pay_click = safe_int(row['funnels_webform_pay_click'])
    funnels_webform_success = safe_int(row['funnels_webform_success'])
    funnels_client_success = safe_int(row['funnels_client_success'])
    funnels_validation_request = safe_int(row['funnels_validation_request'])
    funnels_validation_approval = safe_int(row['funnels_validation_approval'])
    funnels_rewards_granted = safe_int(row['funnels_rewards_granted'])

    # Build metrics with counts and percentages
    metrics = [
        ("Purchase Click", funnels_purchase_click, 100.0),
        ("Changed Selection", funnels_changed_selection, (funnels_changed_selection / base_count) * 100),
        ("Stash Continue", funnels_stash_continue, (funnels_stash_continue / base_count) * 100),
        ("Native Popup", funnels_native_popup, (funnels_native_popup / base_count) * 100),
        ("Webform Impression", funnels_webform_impression, (funnels_webform_impression / base_count) * 100),
        ("Webform Add Card", funnels_webform_add_card, (funnels_webform_add_card / base_count) * 100),
        ("Webform Pay Click", funnels_webform_pay_click, (funnels_webform_pay_click / base_count) * 100),
        ("Webform Success", funnels_webform_success, (funnels_webform_success / base_count) * 100),
        ("Client Success", funnels_client_success, (funnels_client_success / base_count) * 100),
        ("Validation Request", funnels_validation_request, (funnels_validation_request / base_count) * 100),
        ("Validation Approval", funnels_validation_approval, (funnels_validation_approval / base_count) * 100),
        ("Rewards Granted", funnels_rewards_granted, (funnels_rewards_granted / base_count) * 100),
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

    # Helper function to safely convert to int
    def safe_int(val):
        return int(val) if pd.notna(val) else 0

    base_count = safe_int(row['funnels_purchase_click'])

    if base_count == 0:
        return {}

    # Extract values safely
    funnels_stash_continue = safe_int(row['funnels_stash_continue'])
    funnels_webform_impression = safe_int(row['funnels_webform_impression'])
    funnels_webform_pay_click = safe_int(row['funnels_webform_pay_click'])
    funnels_client_success = safe_int(row['funnels_client_success'])

    # Calculate key conversion rates
    continue_rate = (funnels_stash_continue / base_count * 100) if base_count > 0 else 0
    webform_rate = (funnels_webform_impression / funnels_stash_continue * 100) if funnels_stash_continue > 0 else 0
    pay_click_rate = (funnels_webform_pay_click / funnels_webform_impression * 100) if funnels_webform_impression > 0 else 0
    success_rate = (funnels_client_success / funnels_webform_pay_click * 100) if funnels_webform_pay_click > 0 else 0
    overall_conversion = (funnels_client_success / base_count * 100) if base_count > 0 else 0

    return {
        'purchase_clicks': base_count,
        'stash_continues': funnels_stash_continue,
        'webform_impressions': funnels_webform_impression,
        'purchases': funnels_client_success,
        'continue_rate': float(continue_rate),
        'webform_rate': float(webform_rate),
        'pay_click_rate': float(pay_click_rate),
        'success_rate': float(success_rate),
        'overall_conversion': float(overall_conversion)
    }


def build_test_vs_control_funnel_query(filters: Dict[str, Any]) -> str:
    """
    Build query to compare Test vs Control group funnels.
    Shows purchase clicks and purchase success for both groups.
    """
    start_date = get_effective_start_date(filters)
    end_date = filters.get('end_date')

    query = f"""
    WITH {FIREBASE_SEGMENT_CTE}
    funnel_events AS (
        SELECT
            ce.distinct_id,
            ce.purchase_funnel_id,
            ce.mp_event_name,
            ce.payment_platform,
            ce.price_usd,
            ce.purchase_id,
            ce.google_order_number,
            CASE
                WHEN t.distinct_id IS NOT NULL THEN 'Test'
                WHEN c.distinct_id IS NOT NULL THEN 'Control'
            END as segment
        FROM `yotam-395120.peerplay.vmp_master_event_normalized` ce
        LEFT JOIN d2c_test_users t ON ce.distinct_id = t.distinct_id
        LEFT JOIN d2c_control_users c ON ce.distinct_id = c.distinct_id
        WHERE ce.date >= '{start_date}'
          AND ce.date <= '{end_date}'
          AND ce.mp_event_name IN ('purchase_click', 'purchase_successful')
          AND (t.distinct_id IS NOT NULL OR c.distinct_id IS NOT NULL)
    )
    SELECT
        segment,
        COUNT(DISTINCT CASE WHEN mp_event_name = 'purchase_click' THEN purchase_funnel_id END) as purchase_clicks,
        COUNT(DISTINCT CASE WHEN mp_event_name = 'purchase_successful' THEN purchase_funnel_id END) as total_purchases,
        COUNT(DISTINCT CASE
            WHEN mp_event_name = 'purchase_successful' AND payment_platform = 'stash'
            THEN purchase_funnel_id
        END) as stash_purchases,
        COUNT(DISTINCT CASE
            WHEN mp_event_name = 'purchase_successful' AND payment_platform IN ('apple', 'googleplay')
              AND ((payment_platform = 'apple' AND purchase_id IS NOT NULL AND purchase_id != '')
                OR (payment_platform = 'googleplay' AND google_order_number IS NOT NULL AND google_order_number != ''))
            THEN purchase_funnel_id
        END) as iap_purchases,
        SUM(CASE WHEN mp_event_name = 'purchase_successful' THEN COALESCE(price_usd, 0) ELSE 0 END) as total_revenue
    FROM funnel_events
    GROUP BY segment
    ORDER BY segment DESC
    """
    return query


def get_test_vs_control_funnel_data(filters: Dict[str, Any]) -> pd.DataFrame:
    """Execute Test vs Control comparison query."""
    query = build_test_vs_control_funnel_query(filters)
    return run_query(query)


def create_test_vs_control_funnel_chart(df: pd.DataFrame) -> go.Figure:
    """Create side-by-side bar chart comparing Test vs Control funnels."""
    if df.empty:
        return go.Figure()

    # Extract data for each segment
    test_data = df[df['segment'] == 'Test'].iloc[0] if len(df[df['segment'] == 'Test']) > 0 else None
    control_data = df[df['segment'] == 'Control'].iloc[0] if len(df[df['segment'] == 'Control']) > 0 else None

    if test_data is None and control_data is None:
        return go.Figure()

    def safe_int(val):
        return int(val) if pd.notna(val) else 0

    # Get values
    test_clicks = safe_int(test_data['purchase_clicks']) if test_data is not None else 0
    test_purchases = safe_int(test_data['total_purchases']) if test_data is not None else 0
    control_clicks = safe_int(control_data['purchase_clicks']) if control_data is not None else 0
    control_purchases = safe_int(control_data['total_purchases']) if control_data is not None else 0

    # Calculate conversion rates
    test_conv = (test_purchases / test_clicks * 100) if test_clicks > 0 else 0
    control_conv = (control_purchases / control_clicks * 100) if control_clicks > 0 else 0

    fig = go.Figure()

    # Test group bars
    fig.add_trace(go.Bar(
        name='Test Group',
        x=['Purchase Clicks', 'Purchases'],
        y=[test_clicks, test_purchases],
        text=[f'{test_clicks:,}', f'{test_purchases:,}<br>({test_conv:.1f}%)'],
        textposition='outside',
        marker_color='#2ecc71'
    ))

    # Control group bars
    fig.add_trace(go.Bar(
        name='Control Group',
        x=['Purchase Clicks', 'Purchases'],
        y=[control_clicks, control_purchases],
        text=[f'{control_clicks:,}', f'{control_purchases:,}<br>({control_conv:.1f}%)'],
        textposition='outside',
        marker_color='#3498db'
    ))

    fig.update_layout(
        title="📊 Test vs Control: Purchase Funnel Comparison",
        barmode='group',
        height=400,
        xaxis_title="Funnel Step",
        yaxis_title="Count",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )

    return fig


def build_stash_to_iap_users_query(filters: Dict[str, Any]) -> str:
    """
    Build query to find users who purchased via Stash first and then IAP later.
    These are users who tried D2C and then switched to IAP.
    """
    start_date = get_effective_start_date(filters)
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
    WITH {FIREBASE_SEGMENT_CTE}
    all_purchases AS (
        SELECT
            ce.distinct_id,
            ce.date as purchase_date,
            ce.time as purchase_time,
            ce.payment_platform,
            ce.price_usd,
            ce.purchase_funnel_id
        FROM `yotam-395120.peerplay.vmp_master_event_normalized` ce
        INNER JOIN d2c_test_users t ON ce.distinct_id = t.distinct_id
        WHERE ce.date >= '{start_date}'
          AND ce.date <= '{end_date}'
          AND ce.mp_event_name = 'purchase_successful'
          AND ce.payment_platform IN ('stash', 'apple', 'googleplay')
          {os_filter}
          {version_filter}
    ),
    user_purchase_history AS (
        SELECT
            distinct_id,
            MIN(CASE WHEN payment_platform = 'stash' THEN purchase_date END) as first_stash_date,
            MIN(CASE WHEN payment_platform IN ('apple', 'googleplay') THEN purchase_date END) as first_iap_date,
            COUNT(CASE WHEN payment_platform = 'stash' THEN 1 END) as stash_purchase_count,
            COUNT(CASE WHEN payment_platform IN ('apple', 'googleplay') THEN 1 END) as iap_purchase_count,
            SUM(CASE WHEN payment_platform = 'stash' THEN COALESCE(price_usd, 0) ELSE 0 END) as stash_revenue,
            SUM(CASE WHEN payment_platform IN ('apple', 'googleplay') THEN COALESCE(price_usd, 0) ELSE 0 END) as iap_revenue
        FROM all_purchases
        GROUP BY distinct_id
    )
    SELECT
        distinct_id,
        first_stash_date,
        first_iap_date,
        stash_purchase_count,
        iap_purchase_count,
        stash_revenue,
        iap_revenue,
        DATE_DIFF(first_iap_date, first_stash_date, DAY) as days_between
    FROM user_purchase_history
    WHERE first_stash_date IS NOT NULL
      AND first_iap_date IS NOT NULL
      AND first_iap_date >= first_stash_date  -- IAP purchase came after Stash
    ORDER BY first_stash_date DESC
    LIMIT 200
    """
    return query


def get_stash_to_iap_users(filters: Dict[str, Any]) -> pd.DataFrame:
    """Get users who purchased via Stash and then IAP."""
    query = build_stash_to_iap_users_query(filters)
    return run_query(query)


def get_stash_to_iap_summary(filters: Dict[str, Any]) -> Dict[str, Any]:
    """Get summary statistics for Stash to IAP users."""
    start_date = get_effective_start_date(filters)
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
    WITH {FIREBASE_SEGMENT_CTE}
    all_purchases AS (
        SELECT
            ce.distinct_id,
            ce.date as purchase_date,
            ce.payment_platform
        FROM `yotam-395120.peerplay.vmp_master_event_normalized` ce
        INNER JOIN d2c_test_users t ON ce.distinct_id = t.distinct_id
        WHERE ce.date >= '{start_date}'
          AND ce.date <= '{end_date}'
          AND ce.mp_event_name = 'purchase_successful'
          AND ce.payment_platform IN ('stash', 'apple', 'googleplay')
          {os_filter}
          {version_filter}
    ),
    user_purchase_history AS (
        SELECT
            distinct_id,
            MIN(CASE WHEN payment_platform = 'stash' THEN purchase_date END) as first_stash_date,
            MIN(CASE WHEN payment_platform IN ('apple', 'googleplay') THEN purchase_date END) as first_iap_date
        FROM all_purchases
        GROUP BY distinct_id
    ),
    user_categories AS (
        SELECT
            distinct_id,
            CASE
                WHEN first_stash_date IS NOT NULL AND first_iap_date IS NULL THEN 'stash_only'
                WHEN first_stash_date IS NULL AND first_iap_date IS NOT NULL THEN 'iap_only'
                WHEN first_stash_date IS NOT NULL AND first_iap_date IS NOT NULL AND first_iap_date >= first_stash_date THEN 'stash_then_iap'
                WHEN first_stash_date IS NOT NULL AND first_iap_date IS NOT NULL AND first_iap_date < first_stash_date THEN 'iap_then_stash'
            END as category
        FROM user_purchase_history
    )
    SELECT
        category,
        COUNT(*) as user_count
    FROM user_categories
    WHERE category IS NOT NULL
    GROUP BY category
    """
    df = run_query(query)

    result = {
        'stash_only': 0,
        'iap_only': 0,
        'stash_then_iap': 0,
        'iap_then_stash': 0
    }

    if not df.empty:
        for _, row in df.iterrows():
            if row['category'] in result:
                result[row['category']] = int(row['user_count'])

    return result


def get_stash_then_iap_behavior(filters: Dict[str, Any]) -> pd.DataFrame:
    """
    Get breakdown of stash_then_iap users - did they return to Stash after IAP?
    Returns summary of users who returned vs those who never returned.
    """
    start_date = get_effective_start_date(filters)
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
    WITH {FIREBASE_SEGMENT_CTE}
    all_purchases AS (
        SELECT
            ce.distinct_id,
            ce.date as purchase_date,
            ce.payment_platform
        FROM `yotam-395120.peerplay.vmp_master_event_normalized` ce
        INNER JOIN d2c_test_users t ON ce.distinct_id = t.distinct_id
        WHERE ce.date >= '{start_date}'
          AND ce.date <= '{end_date}'
          AND ce.mp_event_name = 'purchase_successful'
          AND ce.payment_platform IN ('stash', 'apple', 'googleplay')
          {os_filter}
          {version_filter}
    ),
    user_purchase_timeline AS (
        SELECT
            distinct_id,
            MIN(CASE WHEN payment_platform = 'stash' THEN purchase_date END) as first_stash_date,
            MIN(CASE WHEN payment_platform IN ('apple', 'googleplay') THEN purchase_date END) as first_iap_date
        FROM all_purchases
        GROUP BY distinct_id
    ),
    stash_then_iap_users AS (
        SELECT distinct_id, first_stash_date, first_iap_date
        FROM user_purchase_timeline
        WHERE first_stash_date IS NOT NULL
          AND first_iap_date IS NOT NULL
          AND first_iap_date >= first_stash_date
    ),
    stash_after_iap AS (
        SELECT
            s.distinct_id,
            COUNT(CASE WHEN p.payment_platform = 'stash' AND p.purchase_date > s.first_iap_date THEN 1 END) as stash_purchases_after_iap,
            COUNT(CASE WHEN p.payment_platform IN ('apple', 'googleplay') AND p.purchase_date > s.first_iap_date THEN 1 END) as iap_purchases_after_first_iap
        FROM stash_then_iap_users s
        LEFT JOIN all_purchases p ON s.distinct_id = p.distinct_id
        GROUP BY s.distinct_id
    )
    SELECT
        CASE
            WHEN stash_purchases_after_iap > 0 THEN 'Returned to Stash'
            ELSE 'Never returned to Stash'
        END as behavior,
        COUNT(DISTINCT distinct_id) as users,
        SUM(stash_purchases_after_iap) as stash_purchases_after_iap,
        SUM(iap_purchases_after_first_iap) as iap_purchases_after_first_iap
    FROM stash_after_iap
    GROUP BY 1
    ORDER BY 1
    """
    return run_query(query)


def get_stash_then_iap_user_details(filters: Dict[str, Any]) -> pd.DataFrame:
    """
    Get detailed info for stash_then_iap users including whether they returned to Stash.
    """
    start_date = get_effective_start_date(filters)
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
    WITH {FIREBASE_SEGMENT_CTE}
    all_purchases AS (
        SELECT
            ce.distinct_id,
            ce.date as purchase_date,
            ce.payment_platform,
            ce.price_usd
        FROM `yotam-395120.peerplay.vmp_master_event_normalized` ce
        INNER JOIN d2c_test_users t ON ce.distinct_id = t.distinct_id
        WHERE ce.date >= '{start_date}'
          AND ce.date <= '{end_date}'
          AND ce.mp_event_name = 'purchase_successful'
          AND ce.payment_platform IN ('stash', 'apple', 'googleplay')
          {os_filter}
          {version_filter}
    ),
    user_purchase_timeline AS (
        SELECT
            distinct_id,
            MIN(CASE WHEN payment_platform = 'stash' THEN purchase_date END) as first_stash_date,
            MIN(CASE WHEN payment_platform IN ('apple', 'googleplay') THEN purchase_date END) as first_iap_date
        FROM all_purchases
        GROUP BY distinct_id
    ),
    stash_then_iap_users AS (
        SELECT distinct_id, first_stash_date, first_iap_date
        FROM user_purchase_timeline
        WHERE first_stash_date IS NOT NULL
          AND first_iap_date IS NOT NULL
          AND first_iap_date >= first_stash_date
    ),
    user_full_history AS (
        SELECT
            s.distinct_id,
            s.first_stash_date,
            s.first_iap_date,
            COUNT(CASE WHEN p.payment_platform = 'stash' AND p.purchase_date <= s.first_iap_date THEN 1 END) as stash_before_iap,
            COUNT(CASE WHEN p.payment_platform = 'stash' AND p.purchase_date > s.first_iap_date THEN 1 END) as stash_after_iap,
            COUNT(CASE WHEN p.payment_platform IN ('apple', 'googleplay') THEN 1 END) as total_iap,
            ROUND(SUM(CASE WHEN p.payment_platform = 'stash' THEN COALESCE(p.price_usd, 0) ELSE 0 END), 2) as stash_revenue,
            ROUND(SUM(CASE WHEN p.payment_platform IN ('apple', 'googleplay') THEN COALESCE(p.price_usd, 0) ELSE 0 END), 2) as iap_revenue
        FROM stash_then_iap_users s
        LEFT JOIN all_purchases p ON s.distinct_id = p.distinct_id
        GROUP BY s.distinct_id, s.first_stash_date, s.first_iap_date
    )
    SELECT
        distinct_id,
        first_stash_date,
        first_iap_date,
        stash_before_iap,
        stash_after_iap,
        total_iap,
        stash_revenue,
        iap_revenue,
        CASE WHEN stash_after_iap > 0 THEN 'Yes' ELSE 'No' END as returned_to_stash
    FROM user_full_history
    ORDER BY stash_after_iap DESC, first_stash_date
    LIMIT 200
    """
    return run_query(query)
