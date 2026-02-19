"""Chart 4: Total Funnel Executions - Query and visualization."""

from typing import Dict, Any
import pandas as pd
import plotly.graph_objects as go
from utils.bigquery_client import run_query, build_date_filter, build_date_filter_seconds, build_filter_conditions, get_firebase_segment_cte, build_firebase_test_users_join


def build_query(filters: Dict[str, Any]) -> str:
    """
    Build SQL query for total funnel executions.
    Uses Firebase segments (stash_test) for user filtering.

    Returns count of distinct purchase_funnel_id for client events and transaction_id for server events.
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

    where_clauses = [date_partition_filter, date_filter_client] + filter_conditions
    where_clause = " AND ".join([c.strip() for c in where_clauses if c.strip()])

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

    # Use Firebase segments for test users filtering
    firebase_cte = get_firebase_segment_cte()
    firebase_join = build_firebase_test_users_join("ce")
    firebase_join_server = build_firebase_test_users_join("se")

    query = f"""
    WITH {firebase_cte}
    client_events AS (
      SELECT
        ce.distinct_id,
        ce.mp_event_name,
        ce.purchase_funnel_id,
        ce.cta_name,
        ce.payment_platform
      FROM `yotam-395120.peerplay.vmp_master_event_normalized` ce
      {firebase_join}
      WHERE {where_clause}
        AND purchase_funnel_id IS NOT NULL
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
        se.request_id,
        se.transaction_id,
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
      {firebase_join_server}
      LEFT JOIN client_events_metadata cm
        ON se.distinct_id = cm.distinct_id
        AND cm.res_timestamp_seconds <= se.request_timestamp
      WHERE {date_partition_filter.replace('date', 'se.date')}
        AND {date_filter_server.replace('request_timestamp', 'se.request_timestamp')}
        AND se.transaction_id IS NOT NULL
    ),
    server_events AS (
      SELECT DISTINCT
        distinct_id,
        event_name,
        request_id,
        transaction_id,
        cta_name
      FROM server_events_with_metadata
      WHERE {server_where_clause}
    )
    SELECT
      -- Metric 1: Funnels with purchase click
      (SELECT COUNT(DISTINCT purchase_funnel_id) FROM client_events WHERE mp_event_name = 'purchase_click') as funnels_with_purchase_click,
      
      -- Metric 2: Funnels with store selection change
      (SELECT COUNT(DISTINCT purchase_funnel_id) FROM client_events 
       WHERE mp_event_name = 'click_pre_purchase' AND cta_name IN ('select_stash', 'select_iap')) as funnels_with_store_selection_change,
      
      -- Metric 3: Funnels with stash continue
      (SELECT COUNT(DISTINCT purchase_funnel_id) FROM client_events 
       WHERE mp_event_name = 'click_pre_purchase' AND cta_name = 'continue' AND payment_platform = 'stash') as funnels_with_stash_continue,
      
      -- Metric 4: Funnels with native popup
      (SELECT COUNT(DISTINCT purchase_funnel_id) FROM client_events 
       WHERE mp_event_name = 'purchase_native_popup_impression' AND payment_platform = 'stash') as funnels_with_native_popup,
      
      -- Metric 5: Funnels with impression webform (from server events by transaction_id)
      (SELECT COUNT(DISTINCT transaction_id) FROM server_events 
       WHERE event_name = 'stash_form_webhook_impression_checkout_loading_started') as funnels_with_impression_webform,
      
      -- Metric 6: Funnels with add new card webform (from server events by transaction_id)
      (SELECT COUNT(DISTINCT transaction_id) FROM server_events 
       WHERE event_name = 'stash_form_webhook_click_in_add_new_card') as funnels_with_add_new_card_webform,
      
      -- Metric 7: Funnels with click in webform (from server events by transaction_id)
      (SELECT COUNT(DISTINCT transaction_id) FROM server_events 
       WHERE event_name = 'stash_form_webhook_click_in_checkout' AND cta_name = 'pay') as funnels_with_click_in_webform,
      
      -- Metric 8: Funnels with successful in webform (from server events by transaction_id)
      (SELECT COUNT(DISTINCT transaction_id) FROM server_events 
       WHERE event_name IN ('stash_webhook_purchase_succeeded')) as funnels_with_successful_in_webform,
      
      -- Metric 9: Funnels with successful in client
      (SELECT COUNT(DISTINCT purchase_funnel_id) FROM client_events 
       WHERE mp_event_name = 'purchase_successful' AND payment_platform = 'stash') as funnels_with_successful_in_client,
      
      -- Metric 10: Funnels with validation request
      (SELECT COUNT(DISTINCT purchase_funnel_id) FROM client_events 
       WHERE mp_event_name = 'purchase_verification_request' AND payment_platform = 'stash') as funnels_with_validation_request,
      
      -- Metric 11: Funnels with validation approval
      (SELECT COUNT(DISTINCT purchase_funnel_id) FROM client_events 
       WHERE mp_event_name = 'purchase_verification_approval' AND payment_platform = 'stash') as funnels_with_validation_approval,
      
      -- Metric 12: Funnels with rewards store
      (SELECT COUNT(DISTINCT purchase_funnel_id) FROM client_events 
       WHERE mp_event_name = 'rewards_store' AND payment_platform = 'stash') as funnels_with_rewards_store
    """
    
    return query


def get_data(filters: Dict[str, Any]) -> pd.DataFrame:
    """Execute query and return results."""
    query = build_query(filters)
    return run_query(query)


def create_visualization(df: pd.DataFrame) -> go.Figure:
    """Create bar chart visualization for the funnel executions."""
    if df.empty:
        return go.Figure()
    
    # Extract metrics from the single row
    row = df.iloc[0]
    
    metrics = [
        ("Purchase Click", row["funnels_with_purchase_click"]),
        ("Changed Selection", row["funnels_with_store_selection_change"]),
        ("Stash Continue", row["funnels_with_stash_continue"]),
        ("Native Popup", row["funnels_with_native_popup"]),
        ("Webform Impression", row["funnels_with_impression_webform"]),
        ("Webform Add New Card", row["funnels_with_add_new_card_webform"]),
        ("Webform Pay Click", row["funnels_with_click_in_webform"]),
        ("Webform purchase_successful", row["funnels_with_successful_in_webform"]),
        ("Client purchase_successful", row["funnels_with_successful_in_client"]),
        ("Validation Request", row["funnels_with_validation_request"]),
        ("Validation Approval", row["funnels_with_validation_approval"]),
        ("Rewards Granted", row["funnels_with_rewards_store"]),
    ]
    
    labels = [m[0] for m in metrics]
    values = [m[1] for m in metrics]
    
    fig = go.Figure(data=[
        go.Bar(
            x=labels,
            y=values,
            text=values,
            textposition='auto',
            marker_color='rgb(26, 118, 255)'
        )
    ])
    
    fig.update_layout(
        title="Chart 4: Total Funnel Executions",
        xaxis_title="Funnel Step",
        yaxis_title="Number of Funnels",
        height=500,
        xaxis_tickangle=-45,
        showlegend=False
    )
    
    return fig
