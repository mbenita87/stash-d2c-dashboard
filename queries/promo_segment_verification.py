"""Promo Segment Verification - Verify which segment receives a specific promo config."""

from typing import Dict, Any, Optional
import pandas as pd
from utils.bigquery_client import run_query


def get_promo_segment_data(config_id: int, days_back: int = 7, test_start_date: Optional[str] = None) -> pd.DataFrame:
    """
    Get users who received a specific promo and their Firebase segment.
    All times are in UTC.

    Args:
        config_id: The config_id from promo_snapshot to filter by
        days_back: Number of days to look back (default 7)
        test_start_date: Optional test start date (YYYY-MM-DD) - only count impressions after this

    Returns:
        DataFrame with segment breakdown of users who received the promo
    """
    # Build date filter based on test start or days_back (all times in UTC)
    if test_start_date:
        date_filter = f"AND ce.date >= '{test_start_date}'"
    else:
        date_filter = f"AND ce.date >= DATE_SUB(CURRENT_DATE(), INTERVAL {days_back} DAY)"

    query = f"""
    WITH firebase_segment_events AS (
        -- Get all dynamic_configuration_loaded events with stash segments
        SELECT
            distinct_id,
            date,
            time,
            CASE
                WHEN firebase_segments LIKE '%LiveOpsData.stash_test%' THEN 'Test'
                WHEN firebase_segments LIKE '%LiveOpsData.stash_control%' THEN 'Control'
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
        SELECT distinct_id, segment
        FROM firebase_segment_events
        WHERE rn = 1
    ),
    promo_impressions AS (
        -- Get users who received the specific promo
        SELECT DISTINCT
            ce.distinct_id,
            MIN(ce.date) as first_impression_date,
            COUNT(*) as impression_count
        FROM `yotam-395120.peerplay.vmp_master_event_normalized` ce
        WHERE ce.mp_event_name = 'impression_promo_popup'
          {date_filter}
          AND ce.promo_snapshot IS NOT NULL
          AND SAFE_CAST(JSON_EXTRACT_SCALAR(ce.promo_snapshot, '$.config_id') AS INT64) = {config_id}
        GROUP BY ce.distinct_id
    )
    SELECT
        COALESCE(us.segment, 'Not in Test/Control') as segment,
        COUNT(DISTINCT pi.distinct_id) as users,
        SUM(pi.impression_count) as total_impressions
    FROM promo_impressions pi
    LEFT JOIN user_segments us ON pi.distinct_id = us.distinct_id
    GROUP BY 1
    ORDER BY 1
    """
    return run_query(query)


def get_promo_user_details(config_id: int, days_back: int = 7, limit: int = 100, test_start_date: Optional[str] = None) -> pd.DataFrame:
    """
    Get detailed list of users who received a specific promo with their segment.
    All times are in UTC.

    Args:
        config_id: The config_id from promo_snapshot to filter by
        days_back: Number of days to look back (default 7)
        limit: Maximum number of users to return
        test_start_date: Optional test start date (YYYY-MM-DD) - only count impressions after this

    Returns:
        DataFrame with user details
    """
    # Build date filter based on test start or days_back (all times in UTC)
    if test_start_date:
        date_filter = f"AND ce.date >= '{test_start_date}'"
    else:
        date_filter = f"AND ce.date >= DATE_SUB(CURRENT_DATE(), INTERVAL {days_back} DAY)"

    query = f"""
    WITH firebase_segment_events AS (
        SELECT
            distinct_id,
            date,
            time,
            CASE
                WHEN firebase_segments LIKE '%LiveOpsData.stash_test%' THEN 'Test'
                WHEN firebase_segments LIKE '%LiveOpsData.stash_control%' THEN 'Control'
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
    promo_impressions AS (
        SELECT
            ce.distinct_id,
            MIN(ce.date) as first_impression_date,
            MAX(ce.date) as last_impression_date,
            COUNT(*) as impression_count
        FROM `yotam-395120.peerplay.vmp_master_event_normalized` ce
        WHERE ce.mp_event_name = 'impression_promo_popup'
          {date_filter}
          AND ce.promo_snapshot IS NOT NULL
          AND SAFE_CAST(JSON_EXTRACT_SCALAR(ce.promo_snapshot, '$.config_id') AS INT64) = {config_id}
        GROUP BY ce.distinct_id
    )
    SELECT
        pi.distinct_id,
        COALESCE(us.segment, 'Not in Test/Control') as segment,
        pi.first_impression_date,
        pi.last_impression_date,
        pi.impression_count
    FROM promo_impressions pi
    LEFT JOIN user_segments us ON pi.distinct_id = us.distinct_id
    ORDER BY pi.first_impression_date DESC
    LIMIT {limit}
    """
    return run_query(query)


def get_users_outside_test(config_id: int, days_back: int = 7, limit: int = 100, test_start_date: Optional[str] = None) -> pd.DataFrame:
    """
    Get users who received the promo popup but are NOT in the Test/Control segments.
    These are users who shouldn't be seeing the test popup.

    Args:
        config_id: The config_id from promo_snapshot to filter by
        days_back: Number of days to look back (default 7)
        limit: Maximum number of users to return
        test_start_date: Optional test start date (YYYY-MM-DD)

    Returns:
        DataFrame with user details for users outside test/control
    """
    if test_start_date:
        date_filter = f"AND ce.date >= '{test_start_date}'"
    else:
        date_filter = f"AND ce.date >= DATE_SUB(CURRENT_DATE(), INTERVAL {days_back} DAY)"

    query = f"""
    WITH firebase_segment_events AS (
        SELECT
            distinct_id,
            date,
            time,
            CASE
                WHEN firebase_segments LIKE '%LiveOpsData.stash_test%' THEN 'Test'
                WHEN firebase_segments LIKE '%LiveOpsData.stash_control%' THEN 'Control'
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
    promo_impressions AS (
        SELECT
            ce.distinct_id,
            ce.mp_country_code as country,
            ce.mp_os as os,
            MIN(ce.date) as first_impression_date,
            MAX(ce.date) as last_impression_date,
            COUNT(*) as impression_count
        FROM `yotam-395120.peerplay.vmp_master_event_normalized` ce
        WHERE ce.mp_event_name = 'impression_promo_popup'
          {date_filter}
          AND ce.promo_snapshot IS NOT NULL
          AND SAFE_CAST(JSON_EXTRACT_SCALAR(ce.promo_snapshot, '$.config_id') AS INT64) = {config_id}
        GROUP BY ce.distinct_id, ce.mp_country_code, ce.mp_os
    )
    SELECT
        pi.distinct_id,
        'Not in Test/Control' as segment,
        pi.country,
        pi.os,
        pi.first_impression_date,
        pi.last_impression_date,
        pi.impression_count
    FROM promo_impressions pi
    LEFT JOIN user_segments us ON pi.distinct_id = us.distinct_id
    WHERE us.segment IS NULL  -- Users NOT in Test or Control
    ORDER BY pi.first_impression_date DESC
    LIMIT {limit}
    """
    return run_query(query)
