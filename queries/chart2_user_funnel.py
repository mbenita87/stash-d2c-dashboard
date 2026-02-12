"""Chart 2: Distinct Users Funnel - Query and visualization."""

from typing import Dict, Any
import pandas as pd
import plotly.graph_objects as go
from utils.bigquery_client import run_query, build_date_filter, build_date_filter_seconds, build_filter_conditions, build_test_users_join


def build_query(filters: Dict[str, Any]) -> str:
    """
    Build SQL query for distinct users funnel.
    
    Returns count of distinct users at each funnel step.
    """
    # Build filter conditions
    date_filter_client = build_date_filter(filters["start_date"], filters["end_date"], "res_timestamp")
    date_filter_server = build_date_filter_seconds(filters["start_date"], filters["end_date"], "request_timestamp")
    
    # Add date partition filter (required by BigQuery)
    date_partition_filter = f"date >= '{filters['start_date']}' AND date <= '{filters['end_date']}'"
    
    # Build filter conditions WITHOUT table alias for use inside CTE
    filter_conditions = []
    if filters.get("mp_os"):
        os_values = ", ".join([f"'{os}'" for os in filters["mp_os"]])
        filter_conditions.append(f"mp_os IN ({os_values})")
    if filters.get("version"):
        if isinstance(filters["version"], list):
            version_values = ", ".join([f"{v}" for v in filters["version"]])
            filter_conditions.append(f"version_float IN ({version_values})")
        else:
            filter_conditions.append(f"version_float = {filters['version']}")
    if filters.get("country"):
        country_values = ", ".join([f"'{c}'" for c in filters["country"]])
        filter_conditions.append(f"mp_country_code IN ({country_values})")
    if filters.get("is_low_payers_country"):
        filter_conditions.append(f"mp_country_code IN (SELECT country_code FROM `yotam-395120.peerplay.dim_country` WHERE is_low_payers_country = true)")
    
    # Hard-coded version filter: only events with version >= 0.3775
    filter_conditions.append("version_float >= 0.3775")
    
    test_users_join, _ = build_test_users_join(filters.get("is_stash_test_users", False))
    
    where_clauses = [date_partition_filter, date_filter_client] + filter_conditions
    where_clause = " AND ".join([c.strip() for c in where_clauses if c.strip()])
    
    # Build server events with test users filter if enabled
    server_test_users_clause = ""
    if filters.get("is_stash_test_users", False):
        server_test_users_clause = """
        INNER JOIN `yotam-395120.peerplay.stash_test_users_no_google_sheet` test_users
        ON se.distinct_id = test_users.distinct_id
        """
    
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
    
    query = f"""
    WITH client_events AS (
      SELECT 
        ce.distinct_id,
        ce.mp_event_name,
        ce.purchase_funnel_id,
        ce.cta_name,
        ce.payment_platform,
        ce.google_order_number,
        ce.purchase_id
      FROM `yotam-395120.peerplay.vmp_master_event_normalized` ce
      {test_users_join.replace('client_events', 'ce') if test_users_join else ''}
      WHERE {where_clause}
    ),
    -- Get metadata from client events for lookup
    client_events_metadata AS (
      SELECT
        distinct_id,
        res_timestamp / 1000 as res_timestamp_seconds,
        NULLIF(version_float, 0) as version_float,
        mp_country_code
      FROM `yotam-395120.peerplay.vmp_master_event_normalized`
      WHERE {date_partition_filter}
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
      WHERE {date_partition_filter.replace('date', 'se.date')}
        AND {date_filter_server.replace('request_timestamp', 'se.request_timestamp')}
    ),
    server_events AS (
      SELECT DISTINCT
        distinct_id,
        event_name,
        purchase_funnel_id,
        cta_name
      FROM server_events_with_metadata
      WHERE {server_where_clause}
    )
    SELECT
      -- Metric 1: Purchase click users
      (SELECT COUNT(DISTINCT distinct_id) FROM client_events WHERE mp_event_name = 'purchase_click') as purchase_click_users,
      
      -- Metric 2: Pre-purchase changed selection users
      (SELECT COUNT(DISTINCT distinct_id) FROM client_events 
       WHERE mp_event_name = 'click_pre_purchase' AND cta_name IN ('select_stash', 'select_iap')) as pre_purchase_changed_selection_users,
      
      -- Metric 3: Pre-purchase stash continue users
      (SELECT COUNT(DISTINCT distinct_id) FROM client_events 
       WHERE mp_event_name = 'click_pre_purchase' AND cta_name = 'continue' AND payment_platform = 'stash') as pre_purchase_stash_continue_users,
      
      -- Metric 4: Got native popup users
      (SELECT COUNT(DISTINCT distinct_id) FROM client_events 
       WHERE mp_event_name = 'purchase_native_popup_impression' AND payment_platform = 'stash') as got_native_popup_users,
      
      -- Metric 5: Impression stash webform users (from server events)
      (SELECT COUNT(DISTINCT distinct_id) FROM server_events 
       WHERE event_name = 'stash_form_webhook_impression_checkout_loading_started') as impression_stash_webform_users,
      
      -- Metric 6: Webform add new card users (from server events)
      (SELECT COUNT(DISTINCT distinct_id) FROM server_events 
       WHERE event_name = 'stash_form_webhook_click_in_add_new_card') as webform_add_new_card_users,
      
      -- Metric 7: Clicked pay stash webform users (from server events)
      (SELECT COUNT(DISTINCT distinct_id) FROM server_events 
       WHERE event_name = 'stash_form_webhook_click_in_checkout' AND cta_name = 'pay') as clicked_pay_stash_webform_users,
      
      -- Metric 8: Purchase successful stash webform users (from server events)
      (SELECT COUNT(DISTINCT distinct_id) FROM server_events 
       WHERE event_name IN ('stash_form_webhook_purchase_successful', 'stash_webhook_purchase_succeeded')) as purchase_successful_stash_webform_users,
      
      -- Metric 9: Purchase successful client users
      (SELECT COUNT(DISTINCT distinct_id) FROM client_events 
       WHERE mp_event_name = 'purchase_successful' 
         AND payment_platform = 'stash') as purchase_successful_client_users,
      
      -- Metric 10: Purchase validation request users
      (SELECT COUNT(DISTINCT distinct_id) FROM client_events 
       WHERE mp_event_name = 'purchase_verification_request' AND payment_platform = 'stash') as purchase_validation_request_users,
      
      -- Metric 11: Purchase validation approval users
      (SELECT COUNT(DISTINCT distinct_id) FROM client_events 
       WHERE mp_event_name = 'purchase_verification_approval' AND payment_platform = 'stash') as purchase_validation_approval_users,
      
      -- Metric 12: Rewards granted users
      (SELECT COUNT(DISTINCT distinct_id) FROM client_events 
       WHERE mp_event_name = 'rewards_store' AND payment_platform = 'stash') as rewards_granted_users
    """
    
    return query


def get_data(filters: Dict[str, Any]) -> pd.DataFrame:
    """Execute query and return results."""
    query = build_query(filters)
    return run_query(query)


def create_visualization(df: pd.DataFrame) -> go.Figure:
    """Create bar chart visualization for the funnel."""
    if df.empty:
        return go.Figure()
    
    # Extract metrics from the single row
    row = df.iloc[0]
    
    metrics = [
        ("Purchase Click", row["purchase_click_users"]),
        ("Changed Selection", row["pre_purchase_changed_selection_users"]),
        ("Stash Continue", row["pre_purchase_stash_continue_users"]),
        ("Native Popup", row["got_native_popup_users"]),
        ("Webform Impression", row["impression_stash_webform_users"]),
        ("Webform Add New Card", row["webform_add_new_card_users"]),
        ("Webform Pay Click", row["clicked_pay_stash_webform_users"]),
        ("Webform purchase_successful", row["purchase_successful_stash_webform_users"]),
        ("Validation Request", row["purchase_validation_request_users"]),
        ("Validation Approval", row["purchase_validation_approval_users"]),
        ("Client purchase_successful", row["purchase_successful_client_users"]),
        ("Rewards Granted", row["rewards_granted_users"]),
    ]
    
    labels = [m[0] for m in metrics]
    values = [m[1] for m in metrics]
    
    fig = go.Figure(data=[
        go.Bar(
            x=labels,
            y=values,
            text=values,
            textposition='auto',
            marker_color='rgb(55, 83, 109)'
        )
    ])
    
    fig.update_layout(
        title="Chart 2: Distinct Users Funnel",
        xaxis_title="Funnel Step",
        yaxis_title="Distinct Users",
        height=500,
        xaxis_tickangle=-45,
        showlegend=False
    )
    
    return fig
