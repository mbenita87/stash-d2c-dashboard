"""Chart 6: Stash Adoption Over Time - Daily trend analysis."""

from typing import Dict, Any
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from utils.bigquery_client import run_query, build_date_filter, build_filter_conditions, build_test_users_join


def build_query(filters: Dict[str, Any]) -> str:
    """
    Build SQL query for daily Stash adoption metrics.
    """
    # Build filter conditions
    date_filter = build_date_filter(filters["start_date"], filters["end_date"], "res_timestamp")
    date_partition_filter = f"ce.date >= '{filters['start_date']}' AND ce.date <= '{filters['end_date']}'"
    filter_conditions = build_filter_conditions(filters, "ce")
    test_users_join, _ = build_test_users_join(filters.get("is_stash_test_users", False))
    
    # Hard-coded version filter: only events with version >= 0.3775
    filter_conditions.append("ce.version_float >= 0.3775")
    
    where_clauses = [date_partition_filter, date_filter] + filter_conditions
    where_clause = " AND ".join([c.strip() for c in where_clauses if c.strip()])
    
    query = f"""
    WITH client_events AS (
      SELECT 
        ce.distinct_id,
        ce.mp_event_name,
        ce.purchase_funnel_id,
        ce.cta_name,
        ce.payment_platform,
        ce.price_usd,
        ce.res_timestamp,
        ce.google_order_number,
        ce.purchase_id,
        DATE(TIMESTAMP_MILLIS(CAST(ce.res_timestamp AS INT64))) as event_date
      FROM `yotam-395120.peerplay.vmp_master_event_normalized` ce
      {test_users_join.replace('client_events', 'ce') if test_users_join else ''}
      WHERE {where_clause}
    ),
    daily_metrics AS (
      SELECT
        event_date,
        
        -- Active users per day
        COUNT(DISTINCT distinct_id) as active_users,
        
        -- Purchase clicks per day
        COUNT(DISTINCT CASE 
          WHEN mp_event_name = 'purchase_click' 
          THEN purchase_funnel_id 
        END) as purchase_clicks,
        
        -- Stash purchases
        COUNT(DISTINCT CASE 
          WHEN mp_event_name = 'purchase_successful' 
            AND payment_platform = 'stash'
          THEN purchase_funnel_id 
        END) as stash_purchases,
        
        -- Total purchases (with validation for Apple/GooglePlay)
        COUNT(DISTINCT CASE 
          WHEN mp_event_name = 'purchase_successful' 
            AND (
              (payment_platform = 'stash')
              OR (payment_platform = 'apple' AND purchase_id IS NOT NULL AND purchase_id != '')
              OR (payment_platform = 'googleplay' AND google_order_number IS NOT NULL AND google_order_number != '')
            )
          THEN purchase_funnel_id 
        END) as total_purchases,
        
        -- Stash revenue
        SUM(CASE 
          WHEN mp_event_name = 'purchase_successful' 
            AND payment_platform = 'stash'
          THEN COALESCE(price_usd, 0) 
          ELSE 0 
        END) as stash_revenue,
        
        -- Total revenue (with validation for Apple/GooglePlay)
        SUM(CASE 
          WHEN mp_event_name = 'purchase_successful' 
            AND (
              (payment_platform = 'stash')
              OR (payment_platform = 'apple' AND purchase_id IS NOT NULL AND purchase_id != '')
              OR (payment_platform = 'googleplay' AND google_order_number IS NOT NULL AND google_order_number != '')
            )
          THEN COALESCE(price_usd, 0) 
          ELSE 0 
        END) as total_revenue,
        
        -- Stash payers
        COUNT(DISTINCT CASE 
          WHEN mp_event_name = 'purchase_successful' 
            AND payment_platform = 'stash'
          THEN distinct_id 
        END) as stash_payers,
        
        -- Total payers (with validation for Apple/GooglePlay)
        COUNT(DISTINCT CASE 
          WHEN mp_event_name = 'purchase_successful' 
            AND (
              (payment_platform = 'stash')
              OR (payment_platform = 'apple' AND purchase_id IS NOT NULL AND purchase_id != '')
              OR (payment_platform = 'googleplay' AND google_order_number IS NOT NULL AND google_order_number != '')
            )
          THEN distinct_id 
        END) as total_payers,
        
        -- Stash continues
        COUNT(DISTINCT CASE 
          WHEN mp_event_name = 'click_pre_purchase' 
            AND cta_name = 'continue' 
            AND payment_platform = 'stash'
          THEN purchase_funnel_id 
        END) as stash_continues,
        
        -- Changed to stash
        COUNT(DISTINCT CASE 
          WHEN mp_event_name = 'click_pre_purchase' 
            AND cta_name = 'select_stash'
          THEN purchase_funnel_id 
        END) as changed_to_stash,
        
        -- Total changes
        COUNT(DISTINCT CASE 
          WHEN mp_event_name = 'click_pre_purchase' 
            AND cta_name IN ('select_stash', 'select_iap')
          THEN purchase_funnel_id 
        END) as total_changes
        
      FROM client_events
      GROUP BY event_date
    ),
    time_metrics AS (
      SELECT
        ce1.event_date,
        AVG(CAST(ce2.res_timestamp - ce1.res_timestamp AS FLOAT64) / 1000.0) as avg_time_to_continue_seconds,
        APPROX_QUANTILES(CAST(ce2.res_timestamp - ce1.res_timestamp AS FLOAT64) / 1000.0, 100)[OFFSET(50)] as median_time_to_continue_seconds
      FROM client_events ce1
      INNER JOIN client_events ce2 
        ON ce1.purchase_funnel_id = ce2.purchase_funnel_id
        AND ce1.mp_event_name = 'impression_pre_purchase'
        AND ce2.mp_event_name = 'click_pre_purchase'
        AND ce2.cta_name = 'continue'
        AND ce2.res_timestamp > ce1.res_timestamp
      GROUP BY ce1.event_date
    )
    SELECT
      dm.event_date,
      
      -- Stash purchases share
      CASE 
        WHEN dm.total_purchases > 0 
        THEN (dm.stash_purchases * 100.0 / dm.total_purchases)
        ELSE 0
      END as stash_purchases_share,
      
      -- Stash revenue share
      CASE 
        WHEN dm.total_revenue > 0 
        THEN (dm.stash_revenue * 100.0 / dm.total_revenue)
        ELSE 0
      END as stash_revenue_share,
      
      -- Stash payers share
      CASE 
        WHEN dm.total_payers > 0 
        THEN (dm.stash_payers * 100.0 / dm.total_payers)
        ELSE 0
      END as stash_payers_share,
      
      -- PP continue from purchase clicks rate
      CASE 
        WHEN dm.purchase_clicks > 0 
        THEN (dm.stash_continues * 100.0 / dm.purchase_clicks)
        ELSE 0
      END as pp_continue_from_purchase_clicks_rate,
      
      -- PP purchase continue to successful rate
      CASE 
        WHEN dm.stash_continues > 0 
        THEN (dm.stash_purchases * 100.0 / dm.stash_continues)
        ELSE 0
      END as pp_purchase_continue_to_successful_rate,
      
      -- Time metrics
      COALESCE(tm.avg_time_to_continue_seconds, 0) as avg_time_pre_purchase_entry_to_continue,
      COALESCE(tm.median_time_to_continue_seconds, 0) as median_time_pre_purchase_entry_to_continue
      
    FROM daily_metrics dm
    LEFT JOIN time_metrics tm ON dm.event_date = tm.event_date
    ORDER BY dm.event_date
    """
    
    return query


def get_data(filters: Dict[str, Any]) -> pd.DataFrame:
    """Execute query and return results."""
    query = build_query(filters)
    return run_query(query)


def create_visualization(df: pd.DataFrame) -> go.Figure:
    """Create multi-line chart for daily trends."""
    if df.empty:
        return go.Figure()
    
    # Create subplots with 2 rows
    fig = make_subplots(
        rows=2, cols=1,
        subplot_titles=(
            'Stash Adoption Shares (%)',
            'Conversion Rates (%)'
        ),
        vertical_spacing=0.15,
        specs=[[{"secondary_y": False}],
               [{"secondary_y": False}]]
    )
    
    # Row 1: Stash shares
    fig.add_trace(
        go.Scatter(x=df['event_date'], y=df['stash_purchases_share'], 
                   name='Purchases Share', mode='lines+markers',
                   hovertemplate='%{y:.2f}%<extra></extra>'),
        row=1, col=1
    )
    fig.add_trace(
        go.Scatter(x=df['event_date'], y=df['stash_revenue_share'], 
                   name='Revenue Share', mode='lines+markers',
                   hovertemplate='%{y:.2f}%<extra></extra>'),
        row=1, col=1
    )
    fig.add_trace(
        go.Scatter(x=df['event_date'], y=df['stash_payers_share'], 
                   name='Payers Share', mode='lines+markers',
                   hovertemplate='%{y:.2f}%<extra></extra>'),
        row=1, col=1
    )
    
    # Row 2: Conversion rates
    fig.add_trace(
        go.Scatter(x=df['event_date'], y=df['pp_continue_from_purchase_clicks_rate'], 
                   name='PP Continue Rate', mode='lines+markers',
                   hovertemplate='%{y:.2f}%<extra></extra>'),
        row=2, col=1
    )
    fig.add_trace(
        go.Scatter(x=df['event_date'], y=df['pp_purchase_continue_to_successful_rate'], 
                   name='PP Success Rate', mode='lines+markers',
                   hovertemplate='%{y:.2f}%<extra></extra>'),
        row=2, col=1
    )
    
    # Update layout
    fig.update_layout(
        title="Chart 6: Stash Adoption Over Time",
        height=700,
        showlegend=True,
        hovermode='x unified'
    )
    
    fig.update_xaxes(title_text="Date", row=2, col=1)
    fig.update_yaxes(title_text="Share (%)", row=1, col=1)
    fig.update_yaxes(title_text="Rate (%)", row=2, col=1)
    
    return fig
