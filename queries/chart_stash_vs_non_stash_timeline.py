"""Chart: Stash vs Non-Stash Purchasers Timeline - Compare users who purchased via Stash vs IAP only."""

from typing import Dict, Any
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from utils.bigquery_client import run_query


def build_query(filters: Dict[str, Any], test_start_date: str) -> str:
    """
    Build SQL query for daily KPI metrics comparing Stash Purchasers vs Non-Stash Purchasers.
    Only includes users from the Test group (stash_test segment).

    Segments:
    - Stash Purchasers: Users who made at least one purchase via Stash
    - Non-Stash Purchasers: Users who made purchases only via IAP (Apple/Google), never via Stash

    Args:
        filters: Standard dashboard filters
        test_start_date: The date when the test started (YYYY-MM-DD)
    """
    # Validate test_start_date
    if test_start_date is None or test_start_date == 'None' or not test_start_date:
        test_start_date = filters.get('start_date', '2025-01-01')

    # Build filter conditions from sidebar filters
    filter_conditions = []

    if filters.get("mp_os"):
        os_values = ", ".join([f"'{os}'" for os in filters["mp_os"]])
        filter_conditions.append(f"ce.mp_os IN ({os_values})")

    if filters.get("version"):
        if isinstance(filters["version"], list):
            version_values = ", ".join([f"{v}" for v in filters["version"]])
            filter_conditions.append(f"ce.version_float IN ({version_values})")
        else:
            filter_conditions.append(f"ce.version_float = {filters['version']}")

    # Always require version >= 0.3775
    filter_conditions.append("ce.version_float >= 0.3775")

    additional_filters = " AND ".join(filter_conditions) if filter_conditions else "1=1"

    query = f"""
    WITH firebase_segment_events AS (
        -- Get all dynamic_configuration_loaded events with stash_test segment only
        SELECT
            distinct_id,
            date,
            time,
            ROW_NUMBER() OVER (PARTITION BY distinct_id ORDER BY date DESC, time DESC) as rn
        FROM `yotam-395120.peerplay.vmp_master_event_normalized`
        WHERE mp_event_name = 'dynamic_configuration_loaded'
          AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
          AND firebase_segments LIKE '%LiveOpsData.stash_test%'
    ),
    test_users AS (
        -- Get users in Test group only
        SELECT distinct_id
        FROM firebase_segment_events
        WHERE rn = 1
    ),
    d2c_eligible_users AS (
        SELECT
            p.distinct_id
        FROM `yotam-395120.peerplay.dim_player` p
        INNER JOIN test_users tu ON p.distinct_id = tu.distinct_id
        WHERE p.first_country = 'US'
    ),
    -- Identify users who have ever purchased via Stash
    stash_purchasers AS (
        SELECT DISTINCT ce.distinct_id
        FROM `yotam-395120.peerplay.vmp_master_event_normalized` ce
        INNER JOIN d2c_eligible_users d2c ON ce.distinct_id = d2c.distinct_id
        WHERE ce.mp_event_name = 'purchase_successful'
          AND ce.payment_platform = 'stash'
          AND ce.date >= DATE('{test_start_date}')
    ),
    -- Identify users who have purchased via IAP but never via Stash
    iap_purchasers AS (
        SELECT DISTINCT ce.distinct_id
        FROM `yotam-395120.peerplay.vmp_master_event_normalized` ce
        INNER JOIN d2c_eligible_users d2c ON ce.distinct_id = d2c.distinct_id
        WHERE ce.mp_event_name = 'purchase_successful'
          AND ce.payment_platform IN ('apple', 'googleplay')
          AND ((ce.payment_platform = 'apple' AND ce.purchase_id IS NOT NULL AND ce.purchase_id != '')
            OR (ce.payment_platform = 'googleplay' AND ce.google_order_number IS NOT NULL AND ce.google_order_number != ''))
          AND ce.date >= DATE('{test_start_date}')
    ),
    -- Segment users: Stash Purchasers vs Non-Stash Purchasers (IAP only)
    user_purchase_segments AS (
        SELECT
            distinct_id,
            CASE
                WHEN distinct_id IN (SELECT distinct_id FROM stash_purchasers) THEN 'Stash Purchasers'
                WHEN distinct_id IN (SELECT distinct_id FROM iap_purchasers) THEN 'Non-Stash Purchasers'
            END as segment
        FROM d2c_eligible_users
        WHERE distinct_id IN (SELECT distinct_id FROM stash_purchasers)
           OR distinct_id IN (SELECT distinct_id FROM iap_purchasers)
    ),
    -- Get first purchase date for each user (for FTD calculation)
    user_first_purchase AS (
        SELECT
            ce.distinct_id,
            MIN(DATE(TIMESTAMP_MILLIS(CAST(ce.res_timestamp AS INT64)))) as first_purchase_date
        FROM `yotam-395120.peerplay.vmp_master_event_normalized` ce
        WHERE ce.mp_event_name = 'purchase_successful'
          AND ce.date >= '2020-01-01'
          AND (
            (ce.payment_platform = 'stash')
            OR (ce.payment_platform = 'apple' AND ce.purchase_id IS NOT NULL AND ce.purchase_id != '')
            OR (ce.payment_platform = 'googleplay' AND ce.google_order_number IS NOT NULL AND ce.google_order_number != '')
          )
        GROUP BY ce.distinct_id
    ),
    -- Get daily metrics per segment
    daily_metrics AS (
        SELECT
            DATE(TIMESTAMP_MILLIS(CAST(ce.res_timestamp AS INT64))) as event_date,
            ups.segment,

            -- Active users
            COUNT(DISTINCT ce.distinct_id) as active_users,

            -- Total purchases (with validation)
            COUNT(DISTINCT CASE
                WHEN ce.mp_event_name = 'purchase_successful'
                  AND (
                    (ce.payment_platform = 'stash')
                    OR (ce.payment_platform = 'apple' AND ce.purchase_id IS NOT NULL AND ce.purchase_id != '')
                    OR (ce.payment_platform = 'googleplay' AND ce.google_order_number IS NOT NULL AND ce.google_order_number != '')
                  )
                THEN ce.purchase_funnel_id
            END) as total_purchases,

            -- Gross revenue
            SUM(CASE
                WHEN ce.mp_event_name = 'purchase_successful'
                  AND (
                    (ce.payment_platform = 'stash')
                    OR (ce.payment_platform = 'apple' AND ce.purchase_id IS NOT NULL AND ce.purchase_id != '')
                    OR (ce.payment_platform = 'googleplay' AND ce.google_order_number IS NOT NULL AND ce.google_order_number != '')
                  )
                THEN COALESCE(ce.price_usd, 0)
                ELSE 0
            END) as gross_revenue,

            -- Net revenue (after platform fees)
            SUM(CASE
                WHEN ce.mp_event_name = 'purchase_successful' THEN
                  CASE
                    WHEN ce.payment_platform = 'stash' THEN COALESCE(ce.price_usd, 0)
                    WHEN ce.payment_platform IN ('apple', 'googleplay')
                      AND ((ce.payment_platform = 'apple' AND ce.purchase_id IS NOT NULL AND ce.purchase_id != '')
                        OR (ce.payment_platform = 'googleplay' AND ce.google_order_number IS NOT NULL AND ce.google_order_number != ''))
                    THEN COALESCE(ce.price_usd, 0) * 0.7
                    ELSE 0
                  END
                ELSE 0
            END) as net_revenue,

            -- Paying users
            COUNT(DISTINCT CASE
                WHEN ce.mp_event_name = 'purchase_successful'
                  AND (
                    (ce.payment_platform = 'stash')
                    OR (ce.payment_platform = 'apple' AND ce.purchase_id IS NOT NULL AND ce.purchase_id != '')
                    OR (ce.payment_platform = 'googleplay' AND ce.google_order_number IS NOT NULL AND ce.google_order_number != '')
                  )
                THEN ce.distinct_id
            END) as paying_users,

            -- Interrupted purchases
            COUNT(DISTINCT CASE
                WHEN ce.mp_event_name = 'purchase_successful' AND ce.interrupted = 1
                THEN ce.purchase_funnel_id
            END) as interrupted_purchases,

            -- Purchase clicks (for conversion rates)
            COUNT(DISTINCT CASE
                WHEN ce.mp_event_name = 'purchase_click'
                THEN ce.purchase_funnel_id
            END) as purchase_clicks,

            -- PP Continue clicks
            COUNT(DISTINCT CASE
                WHEN ce.mp_event_name = 'click_pre_purchase'
                  AND ce.cta_name = 'continue'
                THEN ce.purchase_funnel_id
            END) as pp_continue_clicks,

            -- FTD users
            COUNT(DISTINCT CASE
                WHEN ce.mp_event_name = 'purchase_successful'
                  AND (
                    (ce.payment_platform = 'stash')
                    OR (ce.payment_platform = 'apple' AND ce.purchase_id IS NOT NULL AND ce.purchase_id != '')
                    OR (ce.payment_platform = 'googleplay' AND ce.google_order_number IS NOT NULL AND ce.google_order_number != '')
                  )
                  AND ufp.first_purchase_date = DATE(TIMESTAMP_MILLIS(CAST(ce.res_timestamp AS INT64)))
                THEN ce.distinct_id
            END) as ftd_users

        FROM `yotam-395120.peerplay.vmp_master_event_normalized` ce
        INNER JOIN user_purchase_segments ups ON ce.distinct_id = ups.distinct_id
        LEFT JOIN user_first_purchase ufp ON ce.distinct_id = ufp.distinct_id
        WHERE ce.date >= DATE('{test_start_date}')
          AND ce.date <= CURRENT_DATE()
          AND {additional_filters}
        GROUP BY event_date, ups.segment
    )
    SELECT
        dm.event_date,
        dm.segment,
        DATE('{test_start_date}') as test_start_date,
        'After' as period,

        -- Raw metrics
        dm.active_users,
        dm.total_purchases,
        dm.gross_revenue,
        dm.net_revenue,
        dm.paying_users,
        dm.interrupted_purchases,
        dm.purchase_clicks,
        dm.pp_continue_clicks,
        dm.ftd_users,

        -- Calculated KPIs
        CASE WHEN dm.active_users > 0
            THEN dm.paying_users * 100.0 / dm.active_users
            ELSE 0
        END as ppu_percent,

        CASE WHEN dm.active_users > 0
            THEN dm.ftd_users * 100.0 / dm.active_users
            ELSE 0
        END as ftd_percent,

        CASE WHEN dm.active_users > 0
            THEN dm.gross_revenue / dm.active_users
            ELSE 0
        END as arpdau,

        CASE WHEN dm.active_users > 0
            THEN dm.net_revenue / dm.active_users
            ELSE 0
        END as arpdau_net,

        CASE WHEN dm.paying_users > 0
            THEN dm.gross_revenue / dm.paying_users
            ELSE 0
        END as arppu,

        CASE WHEN dm.paying_users > 0
            THEN dm.net_revenue / dm.paying_users
            ELSE 0
        END as arppu_net,

        CASE WHEN dm.total_purchases > 0
            THEN dm.gross_revenue / dm.total_purchases
            ELSE 0
        END as atv,

        CASE WHEN dm.total_purchases > 0
            THEN dm.net_revenue / dm.total_purchases
            ELSE 0
        END as atv_net,

        CASE WHEN dm.purchase_clicks > 0
            THEN dm.pp_continue_clicks * 100.0 / dm.purchase_clicks
            ELSE 0
        END as purchase_to_continue_rate,

        CASE WHEN dm.pp_continue_clicks > 0
            THEN dm.total_purchases * 100.0 / dm.pp_continue_clicks
            ELSE 0
        END as continue_to_purchase_rate,

        CASE WHEN dm.total_purchases > 0
            THEN dm.interrupted_purchases * 100.0 / dm.total_purchases
            ELSE 0
        END as interrupted_rate

    FROM daily_metrics dm
    ORDER BY dm.event_date, dm.segment
    """

    return query


def get_data(filters: Dict[str, Any], test_start_date: str) -> pd.DataFrame:
    """Execute query and return results."""
    query = build_query(filters, test_start_date)
    return run_query(query)


def calculate_comparison(df: pd.DataFrame, kpi: str) -> Dict[str, Any]:
    """
    Calculate comparison between Stash Purchasers and Non-Stash Purchasers.

    Args:
        df: DataFrame with timeline data
        kpi: The KPI column to calculate comparison for

    Returns:
        Dictionary with comparison results
    """
    if df.empty:
        return {}

    # Calculate means for each segment (all data is post-test start)
    stash_mean = df[df['segment'] == 'Stash Purchasers'][kpi].mean()
    non_stash_mean = df[df['segment'] == 'Non-Stash Purchasers'][kpi].mean()

    # Handle NaN
    stash_mean = stash_mean if pd.notna(stash_mean) else 0
    non_stash_mean = non_stash_mean if pd.notna(non_stash_mean) else 0

    # Calculate difference
    difference = stash_mean - non_stash_mean
    pct_difference = (difference / non_stash_mean * 100) if non_stash_mean != 0 else 0

    return {
        'stash_purchasers': stash_mean,
        'non_stash_purchasers': non_stash_mean,
        'difference': difference,
        'pct_difference': pct_difference
    }


def create_summary_table(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create a summary table comparing Stash Purchasers vs Non-Stash Purchasers.

    Args:
        df: DataFrame with timeline data

    Returns:
        DataFrame with comparison summary for all KPIs
    """
    kpis = {
        'active_users': 'Active Users',
        'total_purchases': 'Total Purchases',
        'gross_revenue': 'Gross Revenue ($)',
        'net_revenue': 'Net Revenue ($)',
        'paying_users': 'Paying Users',
        'ftd_users': 'FTD Users',
        'ppu_percent': 'PPU %',
        'ftd_percent': 'FTD %',
        'arpdau': 'ARPDAU ($)',
        'arpdau_net': 'ARPDAU Net ($)',
        'arppu': 'ARPPU ($)',
        'arppu_net': 'ARPPU Net ($)',
        'atv': 'ATV ($)',
        'atv_net': 'ATV Net ($)'
    }

    rows = []
    for kpi, label in kpis.items():
        result = calculate_comparison(df, kpi)
        if result:
            rows.append({
                'KPI': label,
                'Stash Purchasers': result['stash_purchasers'],
                'Non-Stash Purchasers': result['non_stash_purchasers'],
                'Difference': result['difference'],
                'Diff %': result['pct_difference']
            })

    return pd.DataFrame(rows)


def create_timeline_visualization(df: pd.DataFrame, selected_kpi: str, kpi_label: str) -> tuple:
    """
    Create timeline chart with comparison bars.

    Args:
        df: DataFrame with daily KPI data
        selected_kpi: Column name of the KPI to display
        kpi_label: Display label for the KPI

    Returns:
        tuple: (figure, summary_dict)
    """
    if df.empty:
        return go.Figure(), {}

    # Separate data by segment
    stash_df = df[df['segment'] == 'Stash Purchasers'].sort_values('event_date').copy()
    non_stash_df = df[df['segment'] == 'Non-Stash Purchasers'].sort_values('event_date').copy()

    has_stash = len(stash_df) > 0
    has_non_stash = len(non_stash_df) > 0

    if not has_stash and not has_non_stash:
        return go.Figure(), {}

    # Calculate averages (all data is post-test start)
    if has_stash:
        stash_avg = df[df['segment'] == 'Stash Purchasers'][selected_kpi].mean()
        stash_avg = stash_avg if pd.notna(stash_avg) else 0
    else:
        stash_avg = 0

    if has_non_stash:
        non_stash_avg = df[df['segment'] == 'Non-Stash Purchasers'][selected_kpi].mean()
        non_stash_avg = non_stash_avg if pd.notna(non_stash_avg) else 0
    else:
        non_stash_avg = 0

    # Calculate difference
    diff = stash_avg - non_stash_avg
    diff_pct = (diff / non_stash_avg * 100) if non_stash_avg != 0 else 0

    # Get test start date
    test_start_date = df['test_start_date'].iloc[0] if 'test_start_date' in df.columns else None

    # Create figure
    fig = make_subplots(
        rows=1, cols=2,
        column_widths=[0.75, 0.25],
        specs=[[{"type": "scatter"}, {"type": "bar"}]],
        horizontal_spacing=0.05
    )

    # Timeline chart (left side)
    if has_stash:
        fig.add_trace(
            go.Scatter(
                x=stash_df['event_date'],
                y=stash_df[selected_kpi],
                name='Stash Purchasers',
                mode='lines+markers',
                line=dict(color='#2ecc71', width=2),
                marker=dict(size=6),
                hovertemplate='%{x}<br>Stash Purchasers: %{y:.2f}<extra></extra>'
            ),
            row=1, col=1
        )

    if has_non_stash:
        fig.add_trace(
            go.Scatter(
                x=non_stash_df['event_date'],
                y=non_stash_df[selected_kpi],
                name='Non-Stash Purchasers',
                mode='lines+markers',
                line=dict(color='#e74c3c', width=2),
                marker=dict(size=6),
                hovertemplate='%{x}<br>Non-Stash Purchasers: %{y:.2f}<extra></extra>'
            ),
            row=1, col=1
        )

    # Note: All data is post-test start, no vertical line needed

    # Bar chart (right side) - Average comparison
    def format_val(v):
        if abs(v) >= 1000000:
            return f'{v/1000000:.1f}M'
        elif abs(v) >= 1000:
            return f'{v/1000:.1f}K'
        elif abs(v) >= 100:
            return f'{v:.0f}'
        else:
            return f'{v:.1f}'

    categories = ['Stash', 'Non-Stash']
    values = [stash_avg, non_stash_avg]
    colors = ['#2ecc71', '#e74c3c']

    fig.add_trace(
        go.Bar(
            x=categories,
            y=values,
            marker_color=colors,
            text=[format_val(v) for v in values],
            textposition='inside',
            textfont=dict(size=10, color='white'),
            insidetextanchor='middle',
            showlegend=False
        ),
        row=1, col=2
    )

    # Update layout
    fig.update_layout(
        title=f"{kpi_label}: Stash vs Non-Stash Purchasers (Test Group)",
        height=400,
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )

    fig.update_xaxes(title_text="Date", row=1, col=1)
    fig.update_xaxes(title_text="Avg (After Test Start)", row=1, col=2)
    fig.update_yaxes(title_text=kpi_label, row=1, col=1)
    fig.update_yaxes(title_text="", row=1, col=2)

    summary = {
        'stash_purchasers': stash_avg,
        'non_stash_purchasers': non_stash_avg,
        'difference': diff,
        'diff_pct': diff_pct,
        'has_stash': has_stash,
        'has_non_stash': has_non_stash
    }

    return fig, summary


# KPI definitions for dropdowns
GRAPH1_KPIS = {
    'active_users': 'Active Users',
    'total_purchases': 'Total Purchases',
    'gross_revenue': 'Gross Revenue ($)',
    'net_revenue': 'Net Revenue ($)',
    'paying_users': 'Paying Users',
    'ftd_users': 'FTD Users',
    'arpdau': 'ARPDAU ($)',
    'arppu': 'ARPPU ($)',
    'interrupted_rate': 'Interrupted Rate (%)'
}

GRAPH2_KPIS = {
    'ppu_percent': 'PPU %',
    'ftd_percent': 'FTD %',
    'atv': 'ATV ($)',
    'arpdau_net': 'ARPDAU Net ($)',
    'arppu_net': 'ARPPU Net ($)',
    'atv_net': 'ATV Net ($)',
    'purchase_to_continue_rate': 'Purchase Click to Continue Rate (%)',
    'continue_to_purchase_rate': 'Continue to Purchase Rate (%)'
}
