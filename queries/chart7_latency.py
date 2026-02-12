"""Chart 7: Stash Funnel Latency - Median time between funnel steps."""

from typing import Dict, Any
import pandas as pd
import plotly.graph_objects as go
from utils.bigquery_client import run_query, build_date_filter, build_date_filter_seconds, build_filter_conditions, build_test_users_join


def build_query(filters: Dict[str, Any]) -> str:
    """
    Build SQL query for funnel latency analysis.
    Calculates median time between key funnel steps.
    """
    # Build filter conditions
    date_filter_client = build_date_filter(filters["start_date"], filters["end_date"], "res_timestamp")
    date_filter_server = build_date_filter_seconds(filters["start_date"], filters["end_date"], "request_timestamp")
    date_partition_filter = f"ce.date >= '{filters['start_date']}' AND ce.date <= '{filters['end_date']}'"
    
    filter_conditions = build_filter_conditions(filters, "ce")
    test_users_join, _ = build_test_users_join(filters.get("is_stash_test_users", False))
    
    # Hard-coded version filter: only events with version >= 0.3775
    filter_conditions.append("ce.version_float >= 0.3775")
    
    where_clauses = [date_partition_filter, date_filter_client] + filter_conditions
    where_clause = " AND ".join([c.strip() for c in where_clauses if c.strip()])
    
    # Chart 7 always shows all 3 payment platforms for comparison
    payment_platforms = ["stash", "apple", "googleplay"]
    platform_filter = "'stash', 'apple', 'googleplay'"
    
    # Build server event filters (version and country from client metadata)
    server_filter_conditions = []
    if filters.get("version"):
        if isinstance(filters["version"], list):
            version_values = ", ".join([f"{v}" for v in filters["version"]])
            server_filter_conditions.append(f"client_version_float IN ({version_values})")
        else:
            server_filter_conditions.append(f"client_version_float = {filters['version']}")
    server_filter_conditions.append("client_version_float >= 0.3775")
    
    if filters.get("country"):
        country_values = ", ".join([f"'{c}'" for c in filters["country"]])
        server_filter_conditions.append(f"client_country_code IN ({country_values})")
    
    if filters.get("is_low_payers_country"):
        server_filter_conditions.append(f"client_country_code IN (SELECT country_code FROM `yotam-395120.peerplay.dim_country` WHERE is_low_payers_country = true)")
    
    server_where_clause = " AND ".join(server_filter_conditions) if server_filter_conditions else "1=1"
    
    # Build server events with test users filter if enabled
    server_test_users_clause = ""
    if filters.get("is_stash_test_users", False):
        server_test_users_clause = """
        INNER JOIN `yotam-395120.peerplay.stash_test_users_no_google_sheet` test_users
        ON se.distinct_id = test_users.distinct_id
        """
    
    # Build first purchase exclusion logic if enabled (Chart 7 only)
    first_purchase_exclusion = ""
    first_purchase_filter_client = ""
    first_purchase_filter_server = ""
    
    if filters.get("exclude_first_purchase", False):
        first_purchase_exclusion = f"""
    -- Identify first purchase funnel for each user
    first_purchase_funnels AS (
      SELECT DISTINCT
        distinct_id,
        FIRST_VALUE(purchase_funnel_id) OVER (
          PARTITION BY distinct_id
          ORDER BY res_timestamp
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) as first_purchase_funnel_id
      FROM `yotam-395120.peerplay.vmp_master_event_normalized`
      WHERE date >= '{filters['start_date']}' AND date <= '{filters['end_date']}'
        AND payment_platform = 'stash'
        AND purchase_funnel_id IS NOT NULL
        AND mp_event_name = 'click_pre_purchase'
        AND cta_name = 'continue'
    ),
"""
        first_purchase_filter_client = " AND ce.purchase_funnel_id NOT IN (SELECT first_purchase_funnel_id FROM first_purchase_funnels WHERE first_purchase_funnel_id IS NOT NULL)"
        first_purchase_filter_server = " AND purchase_funnel_id NOT IN (SELECT first_purchase_funnel_id FROM first_purchase_funnels WHERE first_purchase_funnel_id IS NOT NULL)"
    
    query = f"""
    WITH 
    -- Identify funnels where user added/edited credit card
    card_edit_events AS (
      SELECT DISTINCT
        transaction_id,
        request_timestamp
      FROM `yotam-395120.peerplay.verification_service_events`
      WHERE event_name = 'stash_form_webhook_click_in_add_new_card'
        AND date >= '{filters['start_date']}' AND date <= '{filters['end_date']}'
        AND transaction_id IS NOT NULL
    ),
    -- Map transaction_id to purchase_funnel_id using the most recent purchase_funnel_id
    card_edit_funnels AS (
      SELECT DISTINCT
        cee.transaction_id,
        LAST_VALUE(se.purchase_funnel_id IGNORE NULLS) OVER (
          PARTITION BY cee.transaction_id
          ORDER BY se.request_timestamp
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) as purchase_funnel_id
      FROM card_edit_events cee
      LEFT JOIN `yotam-395120.peerplay.verification_service_events` se
        ON cee.transaction_id = se.transaction_id
        AND se.purchase_funnel_id IS NOT NULL
      WHERE se.date >= '{filters['start_date']}' AND se.date <= '{filters['end_date']}'
    ),
    {first_purchase_exclusion}
    client_events AS (
      SELECT 
        ce.distinct_id,
        ce.mp_event_name,
        ce.purchase_funnel_id,
        ce.cta_name,
        ce.payment_platform,
        ce.res_timestamp,
        ce.google_order_number,
        ce.purchase_id
      FROM `yotam-395120.peerplay.vmp_master_event_normalized` ce
      {test_users_join.replace('client_events', 'ce') if test_users_join else ''}
      WHERE {where_clause}
        AND ce.purchase_funnel_id IS NOT NULL
        -- Exclude funnels with card edits
        AND ce.purchase_funnel_id NOT IN (SELECT purchase_funnel_id FROM card_edit_funnels WHERE purchase_funnel_id IS NOT NULL)
        {first_purchase_filter_client}
    ),
    -- Get metadata from client events for lookup
    client_events_metadata AS (
      SELECT
        distinct_id,
        res_timestamp / 1000 as res_timestamp_seconds,
        NULLIF(version_float, 0) as version_float,
        mp_country_code
      FROM `yotam-395120.peerplay.vmp_master_event_normalized`
      WHERE date >= '{filters['start_date']}' AND date <= '{filters['end_date']}'
        AND {date_filter_client}
    ),
    -- Join server events with client metadata
    server_events_with_metadata AS (
      SELECT
        se.distinct_id,
        se.event_name,
        se.purchase_funnel_id,
        se.cta_name,
        se.request_timestamp,
        -- Get the last client event metadata before this server event
        LAST_VALUE(cm.version_float IGNORE NULLS) OVER (
          PARTITION BY se.distinct_id
          ORDER BY cm.res_timestamp_seconds
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) as client_version_float,
        LAST_VALUE(cm.mp_country_code IGNORE NULLS) OVER (
          PARTITION BY se.distinct_id
          ORDER BY cm.res_timestamp_seconds
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) as client_country_code
      FROM `yotam-395120.peerplay.verification_service_events` se
      {server_test_users_clause}
      LEFT JOIN client_events_metadata cm
        ON se.distinct_id = cm.distinct_id
        AND cm.res_timestamp_seconds <= se.request_timestamp
      WHERE se.date >= '{filters['start_date']}' AND se.date <= '{filters['end_date']}'
        AND {date_filter_server.replace('request_timestamp', 'se.request_timestamp')}
        AND se.purchase_funnel_id IS NOT NULL
    ),
    server_events AS (
      SELECT DISTINCT
        distinct_id,
        event_name,
        purchase_funnel_id,
        cta_name,
        request_timestamp
      FROM server_events_with_metadata
      WHERE {server_where_clause}
        -- Exclude funnels with card edits
        AND purchase_funnel_id NOT IN (SELECT purchase_funnel_id FROM card_edit_funnels WHERE purchase_funnel_id IS NOT NULL)
        {first_purchase_filter_server}
    ),
    -- Extract relevant events with their timestamps
    funnel_events AS (
      SELECT
        ce.purchase_funnel_id,
        ce.payment_platform,
        MAX(CASE WHEN ce.mp_event_name = 'click_pre_purchase' AND ce.cta_name = 'continue' 
            THEN ce.res_timestamp END) as pp_continue_ts,
        MAX(CASE WHEN ce.mp_event_name = 'purchase_native_popup_impression' 
            THEN ce.res_timestamp END) as native_popup_ts,
        MAX(CASE 
          WHEN ce.mp_event_name = 'purchase_successful' 
            AND (
              (ce.payment_platform = 'stash')
              OR (ce.payment_platform = 'apple' AND ce.purchase_id IS NOT NULL AND ce.purchase_id != '')
              OR (ce.payment_platform = 'googleplay' AND ce.google_order_number IS NOT NULL AND ce.google_order_number != '')
            )
          THEN ce.res_timestamp 
        END) as client_successful_ts,
        MAX(CASE WHEN ce.mp_event_name = 'rewards_store' 
            THEN ce.res_timestamp END) as rewards_store_ts
      FROM client_events ce
      GROUP BY ce.purchase_funnel_id, ce.payment_platform
    ),
    stash_webform_events AS (
      SELECT
        se.purchase_funnel_id,
        MAX(CASE WHEN se.event_name = 'stash_form_webhook_impression_checkout_loading_started' 
            THEN se.request_timestamp END) as webform_impression_ts,
        MAX(CASE WHEN se.event_name = 'stash_form_webhook_click_in_checkout' AND se.cta_name = 'pay' 
            THEN se.request_timestamp END) as webform_click_ts,
        MAX(CASE WHEN se.event_name IN ('stash_form_webhook_purchase_successful', 'stash_webhook_purchase_succeeded') 
            THEN se.request_timestamp END) as webform_successful_ts
      FROM server_events se
      GROUP BY se.purchase_funnel_id
    ),
    -- Calculate time differences
    time_diffs AS (
      SELECT
        fe.payment_platform,
        -- PP continue to rewards store
        CAST(fe.rewards_store_ts - fe.pp_continue_ts AS FLOAT64) / 1000.0 as time_pp_continue_to_rewards,
        -- PP continue to native popup
        CAST(fe.native_popup_ts - fe.pp_continue_ts AS FLOAT64) / 1000.0 as time_pp_continue_to_popup,
        -- Native popup to purchase successful
        CAST(fe.client_successful_ts - fe.native_popup_ts AS FLOAT64) / 1000.0 as time_popup_to_successful,
        -- Purchase successful to rewards store
        CAST(fe.rewards_store_ts - fe.client_successful_ts AS FLOAT64) / 1000.0 as time_successful_to_rewards,
        -- Stash-specific: webform impression to click
        CAST(swe.webform_click_ts - swe.webform_impression_ts AS FLOAT64) / 1000.0 as time_webform_impression_to_click,
        -- Stash-specific: webform click to successful
        CAST(swe.webform_successful_ts - swe.webform_click_ts AS FLOAT64) / 1000.0 as time_webform_click_to_successful,
        -- Stash-specific: webform successful to client successful
        CAST(fe.client_successful_ts - swe.webform_successful_ts AS FLOAT64) / 1000.0 as time_webform_to_client_successful
      FROM funnel_events fe
      LEFT JOIN stash_webform_events swe ON fe.purchase_funnel_id = swe.purchase_funnel_id
      WHERE fe.payment_platform IN ({platform_filter})
        AND fe.pp_continue_ts IS NOT NULL
    )
    SELECT
      payment_platform,
      
      -- Median times (general)
      APPROX_QUANTILES(time_pp_continue_to_rewards, 100)[OFFSET(50)] as median_time_pp_continue_to_rewards_store,
      APPROX_QUANTILES(time_pp_continue_to_popup, 100)[OFFSET(50)] as median_time_pp_continue_to_native_popup,
      APPROX_QUANTILES(time_popup_to_successful, 100)[OFFSET(50)] as median_time_native_popup_to_purchase_successful,
      APPROX_QUANTILES(time_successful_to_rewards, 100)[OFFSET(50)] as median_time_purchase_successful_to_rewards_store,
      
      -- Average times (general)
      AVG(time_pp_continue_to_rewards) as avg_time_pp_continue_to_rewards_store,
      AVG(time_pp_continue_to_popup) as avg_time_pp_continue_to_native_popup,
      AVG(time_popup_to_successful) as avg_time_native_popup_to_purchase_successful,
      AVG(time_successful_to_rewards) as avg_time_purchase_successful_to_rewards_store,
      
      -- Stash-specific medians
      CASE 
        WHEN payment_platform = 'stash' 
        THEN APPROX_QUANTILES(time_webform_impression_to_click, 100)[OFFSET(50)]
        ELSE NULL 
      END as median_time_webform_impression_to_purchase_click,
      
      CASE 
        WHEN payment_platform = 'stash' 
        THEN APPROX_QUANTILES(time_webform_click_to_successful, 100)[OFFSET(50)]
        ELSE NULL 
      END as median_time_purchase_click_to_successful,
      
      CASE 
        WHEN payment_platform = 'stash' 
        THEN APPROX_QUANTILES(time_webform_to_client_successful, 100)[OFFSET(50)]
        ELSE NULL 
      END as median_time_successful_in_webform_to_successful_in_client,
      
      -- Stash-specific averages
      CASE 
        WHEN payment_platform = 'stash' 
        THEN AVG(time_webform_impression_to_click)
        ELSE NULL 
      END as avg_time_webform_impression_to_purchase_click,
      
      CASE 
        WHEN payment_platform = 'stash' 
        THEN AVG(time_webform_click_to_successful)
        ELSE NULL 
      END as avg_time_purchase_click_to_successful,
      
      CASE 
        WHEN payment_platform = 'stash' 
        THEN AVG(time_webform_to_client_successful)
        ELSE NULL 
      END as avg_time_successful_in_webform_to_successful_in_client
      
    FROM time_diffs
    WHERE time_pp_continue_to_rewards > 0  -- Filter out invalid time ranges
    GROUP BY payment_platform
    ORDER BY payment_platform
    """
    
    return query


def get_data(filters: Dict[str, Any]) -> pd.DataFrame:
    """Execute query and return results."""
    query = build_query(filters)
    return run_query(query)


def create_visualization(df: pd.DataFrame) -> go.Figure:
    """Create grouped bar chart for latency comparison."""
    if df.empty:
        return go.Figure()
    
    from plotly.subplots import make_subplots
    
    # Create subplots with 2 rows
    fig = make_subplots(
        rows=2, cols=1,
        subplot_titles=(
            'General Funnel Steps - Median Time (seconds)',
            'General Funnel Steps - Average Time (seconds)'
        ),
        vertical_spacing=0.15,
        specs=[[{"secondary_y": False}],
               [{"secondary_y": False}]]
    )
    
    # Prepare metrics for each subplot
    general_median_metrics = [
        ("PP Continue → Rewards", "median_time_pp_continue_to_rewards_store"),
        ("PP Continue → Native Popup", "median_time_pp_continue_to_native_popup"),
        ("Native Popup → Success", "median_time_native_popup_to_purchase_successful"),
        ("Success → Rewards", "median_time_purchase_successful_to_rewards_store"),
    ]
    
    general_avg_metrics = [
        ("PP Continue → Rewards", "avg_time_pp_continue_to_rewards_store"),
        ("PP Continue → Native Popup", "avg_time_pp_continue_to_native_popup"),
        ("Native Popup → Success", "avg_time_native_popup_to_purchase_successful"),
        ("Success → Rewards", "avg_time_purchase_successful_to_rewards_store"),
    ]
    
    # Row 1: General Median
    for platform in df['payment_platform'].unique():
        platform_data = df[df['payment_platform'] == platform].iloc[0]
        values = []
        labels = []
        for label, col in general_median_metrics:
            if col in df.columns and pd.notna(platform_data.get(col)):
                labels.append(label)
                values.append(platform_data[col])
        
        fig.add_trace(go.Bar(
            name=f'{platform.capitalize()} (Median)',
            x=labels,
            y=values,
            text=[f"{v:.2f}s" if v else "" for v in values],
            textposition='auto',
            showlegend=True
        ), row=1, col=1)
    
    # Row 2: General Average
    for platform in df['payment_platform'].unique():
        platform_data = df[df['payment_platform'] == platform].iloc[0]
        values = []
        labels = []
        for label, col in general_avg_metrics:
            if col in df.columns and pd.notna(platform_data.get(col)):
                labels.append(label)
                values.append(platform_data[col])
        
        fig.add_trace(go.Bar(
            name=f'{platform.capitalize()} (Avg)',
            x=labels,
            y=values,
            text=[f"{v:.2f}s" if v else "" for v in values],
            textposition='auto',
            showlegend=True
        ), row=2, col=1)
    
    fig.update_layout(
        title="Chart 7: Stash Funnel Latency",
        height=800,
        barmode='group',
        showlegend=True
    )
    
    fig.update_xaxes(tickangle=-45, row=1, col=1)
    fig.update_xaxes(tickangle=-45, row=2, col=1)
    
    fig.update_yaxes(title_text="Time (seconds)", row=1, col=1)
    fig.update_yaxes(title_text="Time (seconds)", row=2, col=1)
    
    return fig
