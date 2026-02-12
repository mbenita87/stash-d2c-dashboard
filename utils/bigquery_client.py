"""BigQuery client and query utilities for Stash Dashboard."""

from google.cloud import bigquery
from google.oauth2 import service_account
from typing import Optional, Dict, Any, List
import streamlit as st


def get_bigquery_client() -> bigquery.Client:
    """Get or create BigQuery client instance.

    Uses Streamlit secrets for credentials when available (Streamlit Cloud),
    otherwise falls back to Application Default Credentials (local development).
    """
    project_id = "yotam-395120"

    # Check if running on Streamlit Cloud with secrets
    if hasattr(st, 'secrets') and 'gcp_service_account' in st.secrets:
        credentials = service_account.Credentials.from_service_account_info(
            st.secrets["gcp_service_account"]
        )
        return bigquery.Client(project=project_id, credentials=credentials)

    # Fallback to Application Default Credentials (local development)
    return bigquery.Client(project=project_id)


@st.cache_data(ttl=7200)
def run_query(query: str, params: Optional[Dict[str, Any]] = None) -> Any:
    """
    Execute a BigQuery query with optional parameters.
    Results are cached for 120 minutes.
    
    Args:
        query: SQL query string
        params: Optional query parameters
    
    Returns:
        Query results as pandas DataFrame
    """
    client = get_bigquery_client()
    
    # Configure query job with cost limits
    job_config = bigquery.QueryJobConfig()
    job_config.maximum_bytes_billed = 2000000000000  # 2 TB limit
    
    if params:
        job_config.query_parameters = [
            bigquery.ScalarQueryParameter(k, "STRING", v) 
            for k, v in params.items()
        ]
    
    try:
        query_job = client.query(query, job_config=job_config)
        df = query_job.to_dataframe()
        return df
    except Exception as e:
        st.error(f"Query failed: {str(e)}")
        raise


def build_date_filter(start_date: str, end_date: str, timestamp_field: str = "res_timestamp") -> str:
    """
    Build date filter SQL clause for bigint timestamp fields (in milliseconds).
    
    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        timestamp_field: Name of timestamp field (res_timestamp or request_timestamp)
    
    Returns:
        SQL WHERE clause for date filtering
    """
    return f"""
        {timestamp_field} >= UNIX_MILLIS(TIMESTAMP('{start_date}'))
        AND {timestamp_field} < UNIX_MILLIS(TIMESTAMP_ADD(TIMESTAMP('{end_date}'), INTERVAL 1 DAY))
    """


def build_date_filter_seconds(start_date: str, end_date: str, timestamp_field: str = "request_timestamp") -> str:
    """
    Build date filter SQL clause for bigint timestamp fields (in seconds).
    Used for server events which store timestamps in seconds.
    
    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        timestamp_field: Name of timestamp field (request_timestamp)
    
    Returns:
        SQL WHERE clause for date filtering
    """
    return f"""
        {timestamp_field} >= UNIX_SECONDS(TIMESTAMP('{start_date}'))
        AND {timestamp_field} < UNIX_SECONDS(TIMESTAMP_ADD(TIMESTAMP('{end_date}'), INTERVAL 1 DAY))
    """


def build_filter_conditions(
    filters: Dict[str, Any],
    table_alias: str = "ce"
) -> List[str]:
    """
    Build SQL WHERE conditions from filter dictionary.
    
    Args:
        filters: Dictionary of filter values
        table_alias: Table alias for column references
    
    Returns:
        List of SQL condition strings
    """
    conditions = []
    
    if filters.get("mp_os"):
        os_values = ", ".join([f"'{os}'" for os in filters["mp_os"]])
        conditions.append(f"{table_alias}.mp_os IN ({os_values})")
    
    if filters.get("version"):
        if isinstance(filters["version"], list):
            version_values = ", ".join([f"{v}" for v in filters["version"]])
            conditions.append(f"{table_alias}.version_float IN ({version_values})")
        else:
            conditions.append(f"{table_alias}.version_float = {filters['version']}")
    
    if filters.get("country"):
        country_values = ", ".join([f"'{c}'" for c in filters["country"]])
        conditions.append(f"{table_alias}.mp_country_code IN ({country_values})")
    
    if filters.get("exclude_testing_countries"):
        conditions.append(f"{table_alias}.mp_country_code NOT IN ('UA', 'IL', 'AM')")
    
    if filters.get("is_low_payers_country"):
        conditions.append(f"{table_alias}.mp_country_code IN (SELECT country_code FROM `yotam-395120.peerplay.dim_country` WHERE is_low_payers_country = true)")
    
    return conditions


def build_test_users_join(include_test_users: bool) -> tuple[str, str]:
    """
    Build SQL for test users filter.
    
    Args:
        include_test_users: Whether to include only test users
    
    Returns:
        Tuple of (JOIN clause, WHERE clause)
    """
    if include_test_users:
        join_clause = """
        INNER JOIN `yotam-395120.peerplay.stash_test_users_no_google_sheet` test_users
        ON client_events.distinct_id = test_users.distinct_id
        """
        where_clause = ""
    else:
        join_clause = ""
        where_clause = ""
    
    return join_clause, where_clause
