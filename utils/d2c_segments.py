"""D2C Test Segmentation utilities for Test vs Control analysis."""

from typing import Optional, Dict, Any
from utils.bigquery_client import run_query
import pandas as pd


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


def get_d2c_segment_query(segment: Optional[str] = None) -> str:
    """
    Build query for D2C Test Segmentation using Firebase Remote Config segments.

    Segmentation based on Firebase segments:
    - LiveOpsData.stash_test -> test group
    - LiveOpsData.stash_control -> control group

    Conditions:
    - US users only (first_country = 'US')
    - Has firebase segment assignment (stash_test or stash_control)

    Args:
        segment: Optional - 'test', 'control', or None for all

    Returns:
        SQL query string
    """
    # Segment filter
    if segment == 'test':
        segment_filter = "AND segment = 'test'"
    elif segment == 'control':
        segment_filter = "AND segment = 'control'"
    else:
        segment_filter = ""

    query = f"""
    WITH firebase_segment_events AS (
        -- Get all dynamic_configuration_loaded events with stash segments
        SELECT
            distinct_id,
            date,
            time,
            firebase_segments,
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
    user_segments AS (
        -- Get latest segment for each user
        SELECT distinct_id, segment, date as segment_date
        FROM firebase_segment_events
        WHERE rn = 1
    ),
    segmented_users AS (
        SELECT
            p.distinct_id,
            p.first_event_time,
            DATE_DIFF(CURRENT_DATE(), DATE(p.first_event_time), DAY) as days_since_install,
            us.segment,
            us.segment_date
        FROM `yotam-395120.peerplay.dim_player` p
        INNER JOIN user_segments us ON p.distinct_id = us.distinct_id
        WHERE p.first_country = 'US'
    )
    SELECT * FROM segmented_users
    WHERE 1=1
    {segment_filter}
    """

    return query


def get_d2c_users(segment: Optional[str] = None) -> pd.DataFrame:
    """
    Get D2C segmented users.

    Args:
        segment: 'test', 'control', or None for all

    Returns:
        DataFrame with segmented users
    """
    query = get_d2c_segment_query(segment)
    return run_query(query)


def get_d2c_segment_stats(filters: dict) -> pd.DataFrame:
    """
    Get statistics about D2C segments - active users in the date range.
    Uses Firebase Remote Config segments (stash_test / stash_control).
    Data is filtered to only include events after test start date.

    Args:
        filters: Dictionary with filter values (start_date, end_date, mp_os, version, test_start_date)

    Returns:
        DataFrame with segment counts and percentages
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
    WITH firebase_segment_events AS (
        -- Get all dynamic_configuration_loaded events with stash segments
        SELECT
            distinct_id,
            date,
            time,
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
    user_segments AS (
        -- Get latest segment for each user
        SELECT us.distinct_id, us.segment
        FROM firebase_segment_events us
        WHERE us.rn = 1
    ),
    d2c_eligible_users AS (
        -- D2C eligible users (US, has firebase segment)
        SELECT
            p.distinct_id,
            DATE_DIFF(CURRENT_DATE(), DATE(p.first_event_time), DAY) as days_since_install,
            us.segment
        FROM `yotam-395120.peerplay.dim_player` p
        INNER JOIN user_segments us ON p.distinct_id = us.distinct_id
        WHERE p.first_country = 'US'
    ),
    active_users AS (
        -- Users who were active in the date range with the selected filters
        SELECT DISTINCT ce.distinct_id
        FROM `yotam-395120.peerplay.vmp_master_event_normalized` ce
        INNER JOIN d2c_eligible_users d2c ON ce.distinct_id = d2c.distinct_id
        WHERE ce.date >= '{start_date}'
          AND ce.date <= '{end_date}'
          {os_filter}
          {version_filter}
    )
    SELECT
        d2c.segment,
        COUNT(DISTINCT d2c.distinct_id) as users,
        ROUND(AVG(d2c.days_since_install), 1) as avg_days_since_install
    FROM d2c_eligible_users d2c
    INNER JOIN active_users au ON d2c.distinct_id = au.distinct_id
    GROUP BY 1
    ORDER BY 1
    """
    return run_query(query)


def get_d2c_daily_new_users() -> pd.DataFrame:
    """
    Get daily count of new users entering each segment.
    Shows how many users got assigned to each Firebase segment per day.

    Returns:
        DataFrame with daily new user counts per segment
    """
    query = """
    WITH firebase_segment_events AS (
        -- Get first segment assignment for each user
        SELECT
            distinct_id,
            date as segment_date,
            CASE
                WHEN firebase_segments LIKE '%LiveOpsData.stash_test%' THEN 'test'
                WHEN firebase_segments LIKE '%LiveOpsData.stash_control%' THEN 'control'
            END as segment,
            ROW_NUMBER() OVER (PARTITION BY distinct_id ORDER BY date ASC, time ASC) as rn
        FROM `yotam-395120.peerplay.vmp_master_event_normalized`
        WHERE mp_event_name = 'dynamic_configuration_loaded'
          AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
          AND (firebase_segments LIKE '%LiveOpsData.stash_test%'
               OR firebase_segments LIKE '%LiveOpsData.stash_control%')
    ),
    first_segment_assignment AS (
        SELECT distinct_id, segment_date, segment
        FROM firebase_segment_events
        WHERE rn = 1
    )
    SELECT
        segment_date,
        segment,
        COUNT(DISTINCT distinct_id) as new_users
    FROM first_segment_assignment
    WHERE segment_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
    GROUP BY 1, 2
    ORDER BY 1 DESC, 2
    """
    return run_query(query)


def get_d2c_purchase_summary(filters: dict) -> pd.DataFrame:
    """
    Get purchase summary for D2C eligible users.
    Shows total revenue, D2C (Stash) revenue, and IAP revenue.
    Uses Firebase Remote Config segments.
    Data is filtered to only include events after test start date.

    Args:
        filters: Dictionary with filter values (start_date, end_date, mp_os, version, test_start_date)

    Returns:
        DataFrame with purchase summary metrics
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
    WITH firebase_segment_events AS (
        -- Get all dynamic_configuration_loaded events with stash segments
        SELECT
            distinct_id,
            date,
            time,
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
    user_segments AS (
        SELECT distinct_id, segment
        FROM firebase_segment_events
        WHERE rn = 1
    ),
    d2c_eligible_users AS (
        SELECT
            p.distinct_id,
            us.segment
        FROM `yotam-395120.peerplay.dim_player` p
        INNER JOIN user_segments us ON p.distinct_id = us.distinct_id
        WHERE p.first_country = 'US'
    ),
    purchase_data AS (
        SELECT
            ce.distinct_id,
            d2c.segment,
            ce.payment_platform,
            COALESCE(ce.price_usd, 0) as revenue
        FROM `yotam-395120.peerplay.vmp_master_event_normalized` ce
        INNER JOIN d2c_eligible_users d2c ON ce.distinct_id = d2c.distinct_id
        WHERE ce.mp_event_name = 'purchase_successful'
          AND ce.date >= '{start_date}'
          AND ce.date <= '{end_date}'
          {os_filter}
          {version_filter}
          AND (
            (ce.payment_platform = 'stash')
            OR (ce.payment_platform = 'apple' AND ce.purchase_id IS NOT NULL AND ce.purchase_id != '')
            OR (ce.payment_platform = 'googleplay' AND ce.google_order_number IS NOT NULL AND ce.google_order_number != '')
          )
    )
    SELECT
        SUM(revenue) as total_revenue,
        SUM(CASE WHEN payment_platform = 'stash' THEN revenue ELSE 0 END) as d2c_revenue,
        SUM(CASE WHEN payment_platform IN ('apple', 'googleplay') THEN revenue ELSE 0 END) as iap_revenue,
        COUNT(*) as total_purchases,
        COUNT(CASE WHEN payment_platform = 'stash' THEN 1 END) as d2c_purchases,
        COUNT(CASE WHEN payment_platform IN ('apple', 'googleplay') THEN 1 END) as iap_purchases
    FROM purchase_data
    """
    return run_query(query)


def build_d2c_segment_cte(segment: Optional[str] = None) -> tuple[str, str]:
    """
    Build CTE and JOIN clause for D2C segment filtering in other queries.
    Uses Firebase Remote Config segments (stash_test / stash_control).

    Args:
        segment: 'test', 'control', 'all' (both segments), or None (no filter)

    Returns:
        Tuple of (CTE clause, JOIN clause)
    """
    if segment is None or segment == '':
        return "", ""

    # Segment condition
    if segment == 'test':
        segment_condition = "AND segment = 'test'"
    elif segment == 'control':
        segment_condition = "AND segment = 'control'"
    else:  # 'all' - both segments but still D2C eligible
        segment_condition = ""

    cte = f"""
    firebase_segment_events AS (
        SELECT
            distinct_id,
            date,
            time,
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
    d2c_segment_users AS (
        SELECT
            p.distinct_id,
            fs.segment as d2c_segment
        FROM `yotam-395120.peerplay.dim_player` p
        INNER JOIN (
            SELECT distinct_id, segment
            FROM firebase_segment_events
            WHERE rn = 1
        ) fs ON p.distinct_id = fs.distinct_id
        WHERE p.first_country = 'US'
          {segment_condition}
    ),
    """

    join_clause = """
    INNER JOIN d2c_segment_users d2c ON ce.distinct_id = d2c.distinct_id
    """

    return cte, join_clause
