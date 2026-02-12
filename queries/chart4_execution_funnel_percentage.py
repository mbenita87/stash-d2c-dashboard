"""Chart 2b: Total Funnel Executions (Percentage) - Query and visualization."""

from typing import Dict, Any
import pandas as pd
import plotly.graph_objects as go
from utils.bigquery_client import run_query, build_date_filter, build_date_filter_seconds, build_filter_conditions, build_test_users_join


def build_query(filters: Dict[str, Any]) -> str:
    """
    Build SQL query for total funnel executions.
    
    Returns count of distinct purchase_funnel_id for client events and request_id for server events.
    """
    # Import from chart3 to reuse the exact same query logic
    from queries import chart3_execution_funnel
    return chart3_execution_funnel.build_query(filters)


def get_data(filters: Dict[str, Any]) -> pd.DataFrame:
    """Execute query and return results."""
    query = build_query(filters)
    return run_query(query)


def create_visualization(df: pd.DataFrame) -> go.Figure:
    """Create bar chart visualization for the funnel executions with percentages."""
    if df.empty:
        return go.Figure()
    
    # Extract metrics from the single row
    row = df.iloc[0]
    
    # Get base count (funnels_with_purchase_click)
    base_count = row["funnels_with_purchase_click"]
    
    # Avoid division by zero
    if base_count == 0:
        return go.Figure()
    
    # Calculate percentages
    metrics = [
        ("Purchase Click", 100.0),  # Always 100%
        ("Changed Selection", (row["funnels_with_store_selection_change"] / base_count) * 100),
        ("Stash Continue", (row["funnels_with_stash_continue"] / base_count) * 100),
        ("Native Popup", (row["funnels_with_native_popup"] / base_count) * 100),
        ("Webform Impression", (row["funnels_with_impression_webform"] / base_count) * 100),
        ("Webform Pay Click", (row["funnels_with_click_in_webform"] / base_count) * 100),
        ("Webform purchase_successful", (row["funnels_with_successful_in_webform"] / base_count) * 100),
        ("Client purchase_successful", (row["funnels_with_successful_in_client"] / base_count) * 100),
        ("Validation Request", (row["funnels_with_validation_request"] / base_count) * 100),
        ("Validation Approval", (row["funnels_with_validation_approval"] / base_count) * 100),
        ("Rewards Granted", (row["funnels_with_rewards_store"] / base_count) * 100),
    ]
    
    labels = [m[0] for m in metrics]
    values = [m[1] for m in metrics]
    
    # Format text labels to show percentage with 1 decimal place
    text_labels = [f"{v:.1f}%" for v in values]
    
    fig = go.Figure(data=[
        go.Bar(
            x=labels,
            y=values,
            text=text_labels,
            textposition='auto',
            marker_color='rgb(55, 120, 180)'
        )
    ])
    
    fig.update_layout(
        title="Chart 2b: Total Funnel Executions (% from Purchase Click)",
        xaxis_title="Funnel Step",
        yaxis_title="Percentage (%)",
        height=500,
        xaxis_tickangle=-45,
        showlegend=False
    )
    
    return fig
