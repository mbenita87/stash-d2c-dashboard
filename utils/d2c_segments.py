"""D2C Test Segmentation utilities for Test vs Control analysis."""

from typing import Optional
from utils.bigquery_client import run_query
import pandas as pd


def get_d2c_segment_query(segment: Optional[str] = None) -> str:
    """
    Build query for D2C Test Segmentation.

    Conditions:
    - US users (first_country = 'US')
    - Version > 0.378 (latest version in last 30 days)
    - Days since install > 3
    - 20% test / 80% control (based on distinct_id hash)

    Args:
        segment: Optional - 'test', 'control', or None for all

    Returns:
        SQL query string
    """
    # Segment filter
    if segment == 'test':
        segment_filter = "AND hash_bucket < 20"
    elif segment == 'control':
        segment_filter = "AND hash_bucket >= 20"
    else:
        segment_filter = ""

    query = f"""
    WITH user_latest_version AS (
        -- Get latest version for each user (last 30 days for better coverage)
        SELECT
            distinct_id,
            MAX(version_float) as latest_version,
            MAX(date) as last_active_date
        FROM `yotam-395120.peerplay.vmp_master_event_normalized`
        WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
          AND version_float IS NOT NULL
          AND version_float > 0
        GROUP BY distinct_id
    ),
    eligible_users AS (
        SELECT
            p.distinct_id,
            p.first_event_time,
            DATE_DIFF(CURRENT_DATE(), DATE(p.first_event_time), DAY) as days_since_install,
            v.latest_version,
            v.last_active_date,
            MOD(ABS(FARM_FINGERPRINT(p.distinct_id)), 100) as hash_bucket
        FROM `yotam-395120.peerplay.dim_player` p
        JOIN user_latest_version v ON p.distinct_id = v.distinct_id
        WHERE p.first_country = 'US'
          AND v.latest_version > 0.378
    ),
    segmented_users AS (
        SELECT
            distinct_id,
            first_event_time,
            days_since_install,
            latest_version,
            last_active_date,
            hash_bucket,
            CASE
                WHEN hash_bucket < 20 THEN 'test'
                ELSE 'control'
            END as segment
        FROM eligible_users
        WHERE days_since_install > 3
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

    Args:
        filters: Dictionary with filter values (start_date, end_date, mp_os, version)

    Returns:
        DataFrame with segment counts and percentages
    """
    start_date = filters.get('start_date')
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
    WITH d2c_eligible_users AS (
        -- D2C eligible users (US, version > 0.378, days > 3)
        SELECT
            p.distinct_id,
            DATE_DIFF(CURRENT_DATE(), DATE(p.first_event_time), DAY) as days_since_install,
            MOD(ABS(FARM_FINGERPRINT(p.distinct_id)), 100) as hash_bucket
        FROM `yotam-395120.peerplay.dim_player` p
        JOIN (
            SELECT distinct_id, MAX(version_float) as latest_version
            FROM `yotam-395120.peerplay.vmp_master_event_normalized`
            WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
              AND version_float IS NOT NULL AND version_float > 0
            GROUP BY distinct_id
        ) v ON p.distinct_id = v.distinct_id
        WHERE p.first_country = 'US'
          AND v.latest_version > 0.378
          AND DATE_DIFF(CURRENT_DATE(), DATE(p.first_event_time), DAY) > 3
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
        CASE WHEN d2c.hash_bucket < 20 THEN 'test' ELSE 'control' END as segment,
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
    Shows how many users become eligible each day (after 3 days from install).

    Returns:
        DataFrame with daily new user counts per segment
    """
    query = """
    WITH user_latest_version AS (
        SELECT
            distinct_id,
            MAX(version_float) as latest_version
        FROM `yotam-395120.peerplay.vmp_master_event_normalized`
        WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
          AND version_float IS NOT NULL
          AND version_float > 0
        GROUP BY distinct_id
    ),
    eligible_users AS (
        SELECT
            p.distinct_id,
            DATE(p.first_event_time) as install_date,
            DATE_ADD(DATE(p.first_event_time), INTERVAL 4 DAY) as eligible_date,
            v.latest_version,
            MOD(ABS(FARM_FINGERPRINT(p.distinct_id)), 100) as hash_bucket
        FROM `yotam-395120.peerplay.dim_player` p
        JOIN user_latest_version v ON p.distinct_id = v.distinct_id
        WHERE p.first_country = 'US'
          AND v.latest_version > 0.378
          AND DATE_DIFF(CURRENT_DATE(), DATE(p.first_event_time), DAY) > 3
    )
    SELECT
        eligible_date,
        CASE WHEN hash_bucket < 20 THEN 'test' ELSE 'control' END as segment,
        COUNT(DISTINCT distinct_id) as new_users
    FROM eligible_users
    WHERE eligible_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
    GROUP BY 1, 2
    ORDER BY 1 DESC, 2
    """
    return run_query(query)


def get_d2c_purchase_summary(filters: dict) -> pd.DataFrame:
    """
    Get purchase summary for D2C eligible users.
    Shows total revenue, D2C (Stash) revenue, and IAP revenue.

    Args:
        filters: Dictionary with filter values (start_date, end_date, mp_os, version)

    Returns:
        DataFrame with purchase summary metrics
    """
    start_date = filters.get('start_date')
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
    WITH d2c_eligible_users AS (
        SELECT
            p.distinct_id,
            CASE
                WHEN MOD(ABS(FARM_FINGERPRINT(p.distinct_id)), 100) < 20 THEN 'test'
                ELSE 'control'
            END as segment
        FROM `yotam-395120.peerplay.dim_player` p
        JOIN (
            SELECT distinct_id, MAX(version_float) as latest_version
            FROM `yotam-395120.peerplay.vmp_master_event_normalized`
            WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
              AND version_float IS NOT NULL AND version_float > 0
            GROUP BY distinct_id
        ) v ON p.distinct_id = v.distinct_id
        WHERE p.first_country = 'US'
          AND v.latest_version > 0.378
          AND DATE_DIFF(CURRENT_DATE(), DATE(p.first_event_time), DAY) > 3
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

    Args:
        segment: 'test', 'control', 'all' (both segments), or None (no filter)

    Returns:
        Tuple of (CTE clause, JOIN clause)
    """
    if segment is None or segment == '':
        return "", ""

    # Segment condition
    if segment == 'test':
        segment_condition = "MOD(ABS(FARM_FINGERPRINT(p.distinct_id)), 100) < 20"
    elif segment == 'control':
        segment_condition = "MOD(ABS(FARM_FINGERPRINT(p.distinct_id)), 100) >= 20"
    else:  # 'all' - both segments but still D2C eligible
        segment_condition = "1=1"

    cte = f"""
    d2c_segment_users AS (
        SELECT
            p.distinct_id,
            CASE
                WHEN MOD(ABS(FARM_FINGERPRINT(p.distinct_id)), 100) < 20 THEN 'test'
                ELSE 'control'
            END as d2c_segment
        FROM `yotam-395120.peerplay.dim_player` p
        JOIN (
            SELECT distinct_id, MAX(version_float) as latest_version
            FROM `yotam-395120.peerplay.vmp_master_event_normalized`
            WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
              AND version_float IS NOT NULL AND version_float > 0
            GROUP BY distinct_id
        ) v ON p.distinct_id = v.distinct_id
        WHERE p.first_country = 'US'
          AND v.latest_version > 0.378
          AND DATE_DIFF(CURRENT_DATE(), DATE(p.first_event_time), DAY) > 3
          AND {segment_condition}
    ),
    """

    join_clause = """
    INNER JOIN d2c_segment_users d2c ON ce.distinct_id = d2c.distinct_id
    """

    return cte, join_clause
