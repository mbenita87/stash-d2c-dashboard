"""Chart 1: KPI Compare - Query and visualization by payment platform."""

from typing import Dict, Any
import pandas as pd
import plotly.graph_objects as go
from utils.bigquery_client import run_query, build_date_filter, build_filter_conditions, build_test_users_join


def build_query(filters: Dict[str, Any]) -> str:
    """
    Build SQL query for KPI comparison by payment platform.
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
    
    # Chart 1 always shows all 3 payment platforms
    platform_filter = "'stash', 'apple', 'googleplay'"
    
    query = f"""
    WITH client_events AS (
      SELECT 
        ce.distinct_id,
        ce.mp_event_name,
        ce.purchase_funnel_id,
        ce.cta_name,
        ce.payment_platform,
        ce.price_usd,
        ce.interrupted,
        ce.mp_os,
        ce.google_order_number,
        ce.purchase_id
      FROM `yotam-395120.peerplay.vmp_master_event_normalized` ce
      {test_users_join.replace('client_events', 'ce') if test_users_join else ''}
      WHERE {where_clause}
    ),
    -- Identify funnels where user clicked continue for each platform
    continue_funnels AS (
      SELECT DISTINCT
        purchase_funnel_id,
        payment_platform
      FROM client_events
      WHERE mp_event_name = 'click_pre_purchase' 
        AND cta_name = 'continue'
        AND payment_platform IS NOT NULL
    ),
    stash_metrics AS (
      SELECT
        'stash' as payment_platform,
        
        -- Active users: anyone with Apple OR Android OS
        COUNT(DISTINCT CASE WHEN mp_os IN ('Apple', 'Android') THEN distinct_id END) as active_users,
        
        -- Total purchases: only where payment_platform = 'stash'
        COUNT(DISTINCT CASE 
          WHEN mp_event_name = 'purchase_successful' AND payment_platform = 'stash'
          THEN purchase_funnel_id 
        END) as total_purchases,
        
        -- Total gross revenue: only where payment_platform = 'stash'
        SUM(CASE 
          WHEN mp_event_name = 'purchase_successful' AND payment_platform = 'stash'
          THEN COALESCE(price_usd, 0) 
          ELSE 0 
        END) as total_gross_revenue,
        
        -- Paying users: only where payment_platform = 'stash'
        COUNT(DISTINCT CASE 
          WHEN mp_event_name = 'purchase_successful' AND payment_platform = 'stash'
          THEN distinct_id 
        END) as ppu,
        
        -- Purchase clicks for all platforms
        COUNT(DISTINCT CASE 
          WHEN mp_event_name = 'purchase_click' 
          THEN purchase_funnel_id 
        END) as purchase_clicks,
        
        -- Continue clicks for stash platform
        COUNT(DISTINCT CASE 
          WHEN mp_event_name = 'click_pre_purchase' 
            AND cta_name = 'continue' 
            AND payment_platform = 'stash'
          THEN purchase_funnel_id 
        END) as pp_continue_clicks,
        
        -- Purchases from funnels where user clicked continue with stash
        COUNT(DISTINCT CASE 
          WHEN mp_event_name = 'purchase_successful' 
            AND payment_platform = 'stash'
            AND purchase_funnel_id IN (SELECT purchase_funnel_id FROM continue_funnels WHERE payment_platform = 'stash')
          THEN purchase_funnel_id 
        END) as pp_continue_purchases,
        
        -- Interrupted purchases
        COUNT(DISTINCT CASE 
          WHEN mp_event_name = 'purchase_successful' 
            AND payment_platform = 'stash'
            AND interrupted = 1 
          THEN purchase_funnel_id 
        END) as interrupted_purchases
        
      FROM client_events
    ),
    apple_metrics AS (
      SELECT
        'apple' as payment_platform,
        
        -- Active users: anyone with Apple OS
        COUNT(DISTINCT CASE WHEN mp_os = 'Apple' THEN distinct_id END) as active_users,
        
        -- Total purchases: only where payment_platform = 'apple' AND valid purchase_id
        COUNT(DISTINCT CASE 
          WHEN mp_event_name = 'purchase_successful' 
            AND payment_platform = 'apple'
            AND purchase_id IS NOT NULL 
            AND purchase_id != ''
          THEN purchase_funnel_id 
        END) as total_purchases,
        
        -- Total gross revenue: only where payment_platform = 'apple' AND valid purchase_id
        SUM(CASE 
          WHEN mp_event_name = 'purchase_successful' 
            AND payment_platform = 'apple'
            AND purchase_id IS NOT NULL 
            AND purchase_id != ''
          THEN COALESCE(price_usd, 0) 
          ELSE 0 
        END) as total_gross_revenue,
        
        -- Paying users: only where payment_platform = 'apple' AND valid purchase_id
        COUNT(DISTINCT CASE 
          WHEN mp_event_name = 'purchase_successful' 
            AND payment_platform = 'apple'
            AND purchase_id IS NOT NULL 
            AND purchase_id != ''
          THEN distinct_id 
        END) as ppu,
        
        -- Purchase clicks for Apple OS users
        COUNT(DISTINCT CASE 
          WHEN mp_event_name = 'purchase_click' AND mp_os = 'Apple'
          THEN purchase_funnel_id 
        END) as purchase_clicks,
        
        -- Continue clicks for apple platform
        COUNT(DISTINCT CASE 
          WHEN mp_event_name = 'click_pre_purchase' 
            AND cta_name = 'continue' 
            AND payment_platform = 'apple'
            AND mp_os = 'Apple'
          THEN purchase_funnel_id 
        END) as pp_continue_clicks,
        
        -- Purchases from funnels where user clicked continue with apple
        COUNT(DISTINCT CASE 
          WHEN mp_event_name = 'purchase_successful' 
            AND payment_platform = 'apple'
            AND purchase_id IS NOT NULL 
            AND purchase_id != ''
            AND purchase_funnel_id IN (SELECT purchase_funnel_id FROM continue_funnels WHERE payment_platform = 'apple')
          THEN purchase_funnel_id 
        END) as pp_continue_purchases,
        
        -- Interrupted purchases
        COUNT(DISTINCT CASE 
          WHEN mp_event_name = 'purchase_successful' 
            AND payment_platform = 'apple'
            AND interrupted = 1
            AND purchase_id IS NOT NULL 
            AND purchase_id != ''
          THEN purchase_funnel_id 
        END) as interrupted_purchases
        
      FROM client_events
    ),
    googleplay_metrics AS (
      SELECT
        'googleplay' as payment_platform,
        
        -- Active users: anyone with Android OS
        COUNT(DISTINCT CASE WHEN mp_os = 'Android' THEN distinct_id END) as active_users,
        
        -- Total purchases: only where payment_platform = 'googleplay' AND valid google_order_number
        COUNT(DISTINCT CASE 
          WHEN mp_event_name = 'purchase_successful' 
            AND payment_platform = 'googleplay'
            AND google_order_number IS NOT NULL 
            AND google_order_number != ''
          THEN purchase_funnel_id 
        END) as total_purchases,
        
        -- Total gross revenue: only where payment_platform = 'googleplay' AND valid google_order_number
        SUM(CASE 
          WHEN mp_event_name = 'purchase_successful' 
            AND payment_platform = 'googleplay'
            AND google_order_number IS NOT NULL 
            AND google_order_number != ''
          THEN COALESCE(price_usd, 0) 
          ELSE 0 
        END) as total_gross_revenue,
        
        -- Paying users: only where payment_platform = 'googleplay' AND valid google_order_number
        COUNT(DISTINCT CASE 
          WHEN mp_event_name = 'purchase_successful' 
            AND payment_platform = 'googleplay'
            AND google_order_number IS NOT NULL 
            AND google_order_number != ''
          THEN distinct_id 
        END) as ppu,
        
        -- Purchase clicks for Android OS users
        COUNT(DISTINCT CASE 
          WHEN mp_event_name = 'purchase_click' AND mp_os = 'Android'
          THEN purchase_funnel_id 
        END) as purchase_clicks,
        
        -- Continue clicks for googleplay platform
        COUNT(DISTINCT CASE 
          WHEN mp_event_name = 'click_pre_purchase' 
            AND cta_name = 'continue' 
            AND payment_platform = 'googleplay'
            AND mp_os = 'Android'
          THEN purchase_funnel_id 
        END) as pp_continue_clicks,
        
        -- Purchases from funnels where user clicked continue with googleplay
        COUNT(DISTINCT CASE 
          WHEN mp_event_name = 'purchase_successful' 
            AND payment_platform = 'googleplay'
            AND google_order_number IS NOT NULL 
            AND google_order_number != ''
            AND purchase_funnel_id IN (SELECT purchase_funnel_id FROM continue_funnels WHERE payment_platform = 'googleplay')
          THEN purchase_funnel_id 
        END) as pp_continue_purchases,
        
        -- Interrupted purchases
        COUNT(DISTINCT CASE 
          WHEN mp_event_name = 'purchase_successful' 
            AND payment_platform = 'googleplay'
            AND interrupted = 1
            AND google_order_number IS NOT NULL 
            AND google_order_number != ''
          THEN purchase_funnel_id 
        END) as interrupted_purchases
        
      FROM client_events
    ),
    all_metrics AS (
      SELECT * FROM stash_metrics
      UNION ALL
      SELECT * FROM apple_metrics
      UNION ALL
      SELECT * FROM googleplay_metrics
    )
    SELECT
      payment_platform,
      active_users,
      total_purchases,
      total_gross_revenue,
      -- Net revenue calculation
      CASE 
        WHEN payment_platform = 'stash' THEN total_gross_revenue
        ELSE total_gross_revenue * 0.7
      END as total_net_revenue,
      ppu,
      -- PPU% (Percentage of Paying Users)
      CASE 
        WHEN active_users > 0 THEN (ppu * 100.0 / active_users)
        ELSE 0
      END as ppu_percentage,
      -- ARPDAU
      CASE 
        WHEN active_users > 0 THEN total_gross_revenue / active_users
        ELSE 0
      END as arpdau,
      -- ARPPU
      CASE 
        WHEN ppu > 0 THEN total_gross_revenue / ppu
        ELSE 0
      END as arppu,
      -- ATV (Average Transaction Volume)
      CASE 
        WHEN total_purchases > 0 THEN total_gross_revenue / total_purchases
        ELSE 0
      END as atv,
      -- Purchase Click to PP Continue Rate
      CASE 
        WHEN purchase_clicks > 0 
        THEN (pp_continue_clicks * 100.0 / purchase_clicks)
        ELSE 0
      END as pp_continue_from_purchase_clicks_rate,
      -- PP purchase continue to successful rate (only purchases from continue funnels)
      CASE 
        WHEN pp_continue_clicks > 0 
        THEN (pp_continue_purchases * 100.0 / pp_continue_clicks)
        ELSE 0
      END as pp_purchase_continue_to_successful_rate,
      interrupted_purchases,
      -- Interrupted rate
      CASE 
        WHEN total_purchases > 0 
        THEN (interrupted_purchases * 100.0 / total_purchases)
        ELSE 0
      END as interrupted_purchases_rate
    FROM all_metrics
    WHERE payment_platform IN ({platform_filter})
    ORDER BY payment_platform
    """
    
    return query


def get_data(filters: Dict[str, Any]) -> pd.DataFrame:
    """Execute query and return results."""
    query = build_query(filters)
    return run_query(query)


def create_visualization(df: pd.DataFrame) -> pd.DataFrame:
    """
    Format data as a styled dataframe for display.
    Returns the dataframe for st.dataframe() display.
    """
    if df.empty:
        return pd.DataFrame()
    
    # Format numeric columns
    formatted_df = df.copy()
    
    # Format currency columns
    for col in ['total_gross_revenue', 'total_net_revenue', 'arpdau', 'arppu', 'atv']:
        if col in formatted_df.columns:
            formatted_df[col] = formatted_df[col].apply(lambda x: f"${x:,.2f}")
    
    # Format percentage columns
    for col in ['ppu_percentage', 'pp_continue_from_purchase_clicks_rate', 'pp_purchase_continue_to_successful_rate', 'interrupted_purchases_rate']:
        if col in formatted_df.columns:
            formatted_df[col] = formatted_df[col].apply(lambda x: f"{x:.2f}%")
    
    # Rename columns for better display
    formatted_df.columns = [
        'Payment Platform',
        'Active Users',
        'Total Purchases',
        'Gross Revenue',
        'Net Revenue',
        'Paying Users',
        'PPU%',
        'ARPDAU',
        'ARPPU',
        'ATV',
        'Purchase Click to PP Continue Rate',
        'PP Continue to Purchase Success Rate',
        'Interrupted Purchases',
        'Interrupted Rate'
    ]
    
    return formatted_df


def create_chart(df: pd.DataFrame) -> go.Figure:
    """Create comparison charts for key metrics."""
    if df.empty:
        return go.Figure()
    
    # Create subplots for key metrics
    fig = go.Figure()
    
    # Revenue comparison
    fig.add_trace(go.Bar(
        name='Gross Revenue',
        x=df['payment_platform'],
        y=df['total_gross_revenue'],
        text=df['total_gross_revenue'].apply(lambda x: f"${x:,.0f}"),
        textposition='auto'
    ))
    
    fig.add_trace(go.Bar(
        name='Net Revenue',
        x=df['payment_platform'],
        y=df['total_net_revenue'],
        text=df['total_net_revenue'].apply(lambda x: f"${x:,.0f}"),
        textposition='auto'
    ))
    
    fig.update_layout(
        title="Revenue by Payment Platform",
        xaxis_title="Payment Platform",
        yaxis_title="Revenue ($)",
        barmode='group',
        height=400
    )
    
    return fig
