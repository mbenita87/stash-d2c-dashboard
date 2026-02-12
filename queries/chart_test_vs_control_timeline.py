"""Chart: Test vs Control Timeline - Daily KPI comparison with Before/After summary and Diff-in-Diff."""

from typing import Dict, Any
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from utils.bigquery_client import run_query


def build_query(filters: Dict[str, Any], test_start_date: str, show_only_test: bool = False) -> str:
    """
    Build SQL query for daily KPI metrics comparing Test vs Control.

    Args:
        filters: Standard dashboard filters
        test_start_date: The date when the test started (YYYY-MM-DD)
        show_only_test: If True, only return Test segment data
    """
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

    # Segment filter - only Test if show_only_test is True
    if show_only_test:
        segment_filter = "MOD(ABS(FARM_FINGERPRINT(p.distinct_id)), 100) < 20"
    else:
        segment_filter = "1=1"  # No segment filter, get both

    # Calculate the symmetric date range based on days since test start
    query = f"""
    WITH d2c_eligible_users AS (
        SELECT
            p.distinct_id,
            CASE
                WHEN MOD(ABS(FARM_FINGERPRINT(p.distinct_id)), 100) < 20 THEN 'Test'
                ELSE 'Control'
            END as segment
        FROM `yotam-395120.peerplay.dim_player` p
        JOIN (
            SELECT distinct_id, MAX(version_float) as latest_version
            FROM `yotam-395120.peerplay.vmp_master_event_normalized`
            WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
              AND version_float IS NOT NULL AND version_float > 0
            GROUP BY distinct_id
        ) v ON p.distinct_id = v.distinct_id
        WHERE p.first_country = 'US'
          AND v.latest_version > 0.378
          AND DATE_DIFF(CURRENT_DATE(), DATE(p.first_event_time), DAY) > 3
          AND {segment_filter}
    ),
    -- Calculate days since test start
    test_info AS (
        SELECT
            DATE('{test_start_date}') as test_start_date,
            DATE_DIFF(CURRENT_DATE(), DATE('{test_start_date}'), DAY) as days_since_start
    ),
    -- Define the date range (symmetric: same days before and after test start)
    date_range AS (
        SELECT
            test_start_date,
            days_since_start,
            DATE_SUB(test_start_date, INTERVAL days_since_start DAY) as range_start,
            CURRENT_DATE() as range_end
        FROM test_info
    ),
    -- Get daily metrics per segment
    daily_metrics AS (
        SELECT
            DATE(TIMESTAMP_MILLIS(CAST(ce.res_timestamp AS INT64))) as event_date,
            d2c.segment,

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
            END) as pp_continue_clicks

        FROM `yotam-395120.peerplay.vmp_master_event_normalized` ce
        INNER JOIN d2c_eligible_users d2c ON ce.distinct_id = d2c.distinct_id
        CROSS JOIN date_range dr
        WHERE ce.date >= dr.range_start
          AND ce.date <= dr.range_end
          AND {additional_filters}
        GROUP BY event_date, d2c.segment
    )
    SELECT
        dm.event_date,
        dm.segment,
        dr.test_start_date,
        CASE WHEN dm.event_date >= dr.test_start_date THEN 'After' ELSE 'Before' END as period,

        -- Raw metrics
        dm.active_users,
        dm.total_purchases,
        dm.gross_revenue,
        dm.net_revenue,
        dm.paying_users,
        dm.interrupted_purchases,
        dm.purchase_clicks,
        dm.pp_continue_clicks,

        -- Calculated KPIs
        CASE WHEN dm.active_users > 0
            THEN dm.paying_users * 100.0 / dm.active_users
            ELSE 0
        END as ppu_percent,

        CASE WHEN dm.active_users > 0
            THEN dm.gross_revenue / dm.active_users
            ELSE 0
        END as arpdau,

        CASE WHEN dm.paying_users > 0
            THEN dm.gross_revenue / dm.paying_users
            ELSE 0
        END as arppu,

        CASE WHEN dm.total_purchases > 0
            THEN dm.gross_revenue / dm.total_purchases
            ELSE 0
        END as atv,

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
    CROSS JOIN date_range dr
    ORDER BY dm.event_date, dm.segment
    """

    return query


def get_data(filters: Dict[str, Any], test_start_date: str, show_only_test: bool = False) -> pd.DataFrame:
    """Execute query and return results."""
    query = build_query(filters, test_start_date, show_only_test=show_only_test)
    return run_query(query)


def calculate_diff_in_diff(df: pd.DataFrame, kpi: str) -> Dict[str, Any]:
    """
    Calculate Diff-in-Diff for a specific KPI.

    Diff-in-Diff = (Test_After - Test_Before) - (Control_After - Control_Before)

    This measures the causal effect of the treatment by comparing the change
    in the Test group to the change in the Control group.

    Args:
        df: DataFrame with timeline data
        kpi: The KPI column to calculate DiD for

    Returns:
        Dictionary with DiD results
    """
    if df.empty:
        return {}

    # Calculate means for each group/period
    test_before = df[(df['segment'] == 'Test') & (df['period'] == 'Before')][kpi].mean()
    test_after = df[(df['segment'] == 'Test') & (df['period'] == 'After')][kpi].mean()
    control_before = df[(df['segment'] == 'Control') & (df['period'] == 'Before')][kpi].mean()
    control_after = df[(df['segment'] == 'Control') & (df['period'] == 'After')][kpi].mean()

    # Handle NaN
    test_before = test_before if pd.notna(test_before) else 0
    test_after = test_after if pd.notna(test_after) else 0
    control_before = control_before if pd.notna(control_before) else 0
    control_after = control_after if pd.notna(control_after) else 0

    # Calculate changes
    test_change = test_after - test_before
    control_change = control_after - control_before

    # Diff-in-Diff
    did = test_change - control_change

    # Percentage changes
    test_pct_change = (test_change / test_before * 100) if test_before != 0 else 0
    control_pct_change = (control_change / control_before * 100) if control_before != 0 else 0

    # DiD as percentage of control baseline
    did_pct = (did / control_before * 100) if control_before != 0 else 0

    return {
        'test_before': test_before,
        'test_after': test_after,
        'control_before': control_before,
        'control_after': control_after,
        'test_change': test_change,
        'control_change': control_change,
        'test_pct_change': test_pct_change,
        'control_pct_change': control_pct_change,
        'diff_in_diff': did,
        'diff_in_diff_pct': did_pct
    }


def create_did_summary_table(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create a summary table with Diff-in-Diff for all key KPIs.

    Args:
        df: DataFrame with timeline data

    Returns:
        DataFrame with DiD summary for all KPIs
    """
    kpis = {
        'active_users': 'Active Users',
        'total_purchases': 'Total Purchases',
        'gross_revenue': 'Gross Revenue ($)',
        'net_revenue': 'Net Revenue ($)',
        'paying_users': 'Paying Users',
        'ppu_percent': 'PPU %',
        'arpdau': 'ARPDAU ($)',
        'arppu': 'ARPPU ($)',
        'atv': 'ATV ($)'
    }

    rows = []
    for kpi, label in kpis.items():
        did_result = calculate_diff_in_diff(df, kpi)
        if did_result:
            rows.append({
                'KPI': label,
                'Test Before': did_result['test_before'],
                'Test After': did_result['test_after'],
                'Test Change %': did_result['test_pct_change'],
                'Control Before': did_result['control_before'],
                'Control After': did_result['control_after'],
                'Control Change %': did_result['control_pct_change'],
                'Diff-in-Diff': did_result['diff_in_diff'],
                'DiD %': did_result['diff_in_diff_pct']
            })

    return pd.DataFrame(rows)


def create_timeline_visualization(df: pd.DataFrame, selected_kpi: str, kpi_label: str) -> tuple:
    """
    Create timeline chart with Before/After summary bars.

    Args:
        df: DataFrame with daily KPI data
        selected_kpi: Column name of the KPI to display
        kpi_label: Display label for the KPI

    Returns:
        tuple: (figure, summary_dict) - The plotly figure and a dictionary with summary statistics
    """
    if df.empty:
        return go.Figure(), {}

    # Separate Test and Control data
    test_df = df[df['segment'] == 'Test'].sort_values('event_date').copy()
    control_df = df[df['segment'] == 'Control'].sort_values('event_date').copy()

    # Check which segments have data
    has_test = len(test_df) > 0
    has_control = len(control_df) > 0

    if not has_test and not has_control:
        return go.Figure(), {}

    # Calculate Before/After averages (original values for summary)
    if has_test:
        test_before_orig = df[(df['segment'] == 'Test') & (df['period'] == 'Before')][selected_kpi].mean()
        test_after_orig = df[(df['segment'] == 'Test') & (df['period'] == 'After')][selected_kpi].mean()
        test_before_orig = test_before_orig if pd.notna(test_before_orig) else 0
        test_after_orig = test_after_orig if pd.notna(test_after_orig) else 0
    else:
        test_before_orig = 0
        test_after_orig = 0

    if has_control:
        control_before_orig = df[(df['segment'] == 'Control') & (df['period'] == 'Before')][selected_kpi].mean()
        control_after_orig = df[(df['segment'] == 'Control') & (df['period'] == 'After')][selected_kpi].mean()
        control_before_orig = control_before_orig if pd.notna(control_before_orig) else 0
        control_after_orig = control_after_orig if pd.notna(control_after_orig) else 0
    else:
        control_before_orig = 0
        control_after_orig = 0

    # Calculate percent changes (using original values - NOT affected by multiplier)
    test_change = ((test_after_orig - test_before_orig) / test_before_orig * 100) if test_before_orig != 0 else 0
    control_change = ((control_after_orig - control_before_orig) / control_before_orig * 100) if control_before_orig != 0 else 0

    # Calculate Diff-in-Diff
    did = (test_after_orig - test_before_orig) - (control_after_orig - control_before_orig)
    did_pct = (did / control_before_orig * 100) if control_before_orig != 0 else 0

    # Set up values for plotting
    test_before = test_before_orig
    test_after = test_after_orig
    control_before = control_before_orig
    control_after = control_after_orig
    y_axis_label = kpi_label
    test_plot_column = selected_kpi
    control_plot_column = selected_kpi

    # Get test start date for vertical line
    test_start_date = df['test_start_date'].iloc[0] if 'test_start_date' in df.columns else None

    # Create figure with secondary x-axis for bar chart
    fig = make_subplots(
        rows=1, cols=2,
        column_widths=[0.75, 0.25],
        specs=[[{"type": "scatter"}, {"type": "bar"}]],
        horizontal_spacing=0.05
    )

    # Timeline chart (left side) - only add traces for segments that have data
    if has_test:
        fig.add_trace(
            go.Scatter(
                x=test_df['event_date'],
                y=test_df[test_plot_column],
                name='Test (20%)',
                mode='lines+markers',
                line=dict(color='#2ecc71', width=2),
                marker=dict(size=6),
                hovertemplate='%{x}<br>Test: %{y:.2f}<extra></extra>'
            ),
            row=1, col=1
        )

    if has_control:
        fig.add_trace(
            go.Scatter(
                x=control_df['event_date'],
                y=control_df[control_plot_column],
                name='Control (80%)',
                mode='lines+markers',
                line=dict(color='#3498db', width=2),
                marker=dict(size=6),
                hovertemplate='%{x}<br>Control: %{y:.2f}<extra></extra>'
            ),
            row=1, col=1
        )

    # Add vertical line for test start date
    if test_start_date:
        fig.add_vline(
            x=pd.to_datetime(test_start_date).timestamp() * 1000,
            line_dash="dash",
            line_color="red",
            annotation_text="Test Start",
            annotation_position="top",
            row=1, col=1
        )

    # Before/After bar chart (right side)
    categories = ['Before', 'After']

    # Format numbers for display (shorter format for large numbers)
    def format_val(v):
        if abs(v) >= 1000000:
            return f'{v/1000000:.1f}M'
        elif abs(v) >= 1000:
            return f'{v/1000:.1f}K'
        elif abs(v) >= 100:
            return f'{v:.0f}'
        else:
            return f'{v:.1f}'

    # Bar chart - only add bars for segments that have data
    if has_test:
        fig.add_trace(
            go.Bar(
                x=categories,
                y=[test_before, test_after],
                name='Test Avg',
                marker_color='#2ecc71',
                text=[format_val(test_before), format_val(test_after)],
                textposition='inside',
                textfont=dict(size=9, color='white'),
                insidetextanchor='middle',
                showlegend=False
            ),
            row=1, col=2
        )

    if has_control:
        fig.add_trace(
            go.Bar(
                x=categories,
                y=[control_before, control_after],
                name='Control Avg',
                marker_color='#3498db',
                text=[format_val(control_before), format_val(control_after)],
                textposition='inside',
                textfont=dict(size=9, color='white'),
                insidetextanchor='middle',
                showlegend=False
            ),
            row=1, col=2
        )

    # Update layout with appropriate title
    if has_test and has_control:
        title = f"{kpi_label}: Test vs Control Timeline"
    elif has_test:
        title = f"{kpi_label}: Test Group Timeline"
    else:
        title = f"{kpi_label}: Control Group Timeline"

    fig.update_layout(
        title=title,
        height=400,
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
        barmode='group'
    )

    fig.update_xaxes(title_text="Date", row=1, col=1)
    fig.update_xaxes(title_text="Period Average", row=1, col=2)
    fig.update_yaxes(title_text=y_axis_label, row=1, col=1)
    fig.update_yaxes(title_text="", row=1, col=2)

    # Create summary dictionary for display outside the chart
    summary = {
        'test_before': test_before_orig,
        'test_after': test_after_orig,
        'control_before': control_before_orig,
        'control_after': control_after_orig,
        'test_change': test_change,
        'control_change': control_change,
        'diff_in_diff': did,
        'diff_in_diff_pct': did_pct,
        'has_test': has_test,
        'has_control': has_control
    }

    return fig, summary


# KPI definitions for dropdowns
GRAPH1_KPIS = {
    'active_users': 'Active Users',
    'total_purchases': 'Total Purchases',
    'gross_revenue': 'Gross Revenue ($)',
    'net_revenue': 'Net Revenue ($)',
    'paying_users': 'Paying Users',
    'arpdau': 'ARPDAU ($)',
    'arppu': 'ARPPU ($)',
    'interrupted_rate': 'Interrupted Rate (%)'
}

GRAPH2_KPIS = {
    'ppu_percent': 'PPU %',
    'atv': 'ATV ($)',
    'purchase_to_continue_rate': 'Purchase Click to Continue Rate (%)',
    'continue_to_purchase_rate': 'Continue to Purchase Rate (%)',
    'interrupted_purchases': 'Interrupted Purchases'
}
