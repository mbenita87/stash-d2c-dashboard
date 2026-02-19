"""Filter UI components for Stash Dashboard."""

import streamlit as st
from datetime import datetime, timedelta
from typing import Dict, Any, List
from utils.bigquery_client import run_query


@st.cache_data(ttl=3600)  # Cache for 1 hour
def get_available_versions() -> List[float]:
    """
    Fetch list of available app versions from BigQuery.
    Returns versions >= 0.3775 sorted in descending order.
    """
    query = """
    SELECT DISTINCT version_float
    FROM `yotam-395120.peerplay.vmp_master_event_normalized`
    WHERE version_float IS NOT NULL
      AND version_float >= 0.3775
      AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
    ORDER BY version_float DESC
    """
    try:
        df = run_query(query)
        versions = df['version_float'].tolist()
        return versions
    except Exception as e:
        st.error(f"Error fetching versions: {str(e)}")
        return []


@st.cache_data(ttl=3600)  # Cache for 1 hour
def get_available_countries() -> List[str]:
    """
    Fetch list of available countries from BigQuery.
    Returns countries sorted alphabetically.
    """
    query = """
    SELECT DISTINCT mp_country_code
    FROM `yotam-395120.peerplay.vmp_master_event_normalized`
    WHERE mp_country_code IS NOT NULL
      AND mp_country_code != ''
      AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
    ORDER BY mp_country_code ASC
    """
    try:
        df = run_query(query)
        countries = df['mp_country_code'].tolist()
        return countries
    except Exception as e:
        st.error(f"Error fetching countries: {str(e)}")
        return []


def init_filter_defaults():
    """Initialize default filter values in session state."""
    default_end_date = datetime.now().date()
    # Default start date is the 15th of the current month (test start date)
    default_start_date = datetime(2026, 2, 15).date()

    if 'filter_start_date' not in st.session_state:
        st.session_state.filter_start_date = default_start_date
    if 'filter_end_date' not in st.session_state:
        st.session_state.filter_end_date = default_end_date
    if 'filter_mp_os' not in st.session_state:
        st.session_state.filter_mp_os = ["Apple", "Android"]
    if 'filter_versions' not in st.session_state:
        st.session_state.filter_versions = None  # Will be set to all versions
    if 'filter_countries' not in st.session_state:
        st.session_state.filter_countries = None  # Will be set to all countries
    if 'filter_is_low_payers' not in st.session_state:
        st.session_state.filter_is_low_payers = False
    if 'filter_exclude_testing' not in st.session_state:
        st.session_state.filter_exclude_testing = True
    if 'filter_is_stash_test_users' not in st.session_state:
        st.session_state.filter_is_stash_test_users = True
    if 'filter_test_start_date' not in st.session_state:
        # D2C Test started on 2026-02-15
        st.session_state.filter_test_start_date = datetime(2026, 2, 15).date()


def render_filters(tab: str = "stash_analytics") -> Dict[str, Any]:
    """
    Render filter sidebar and return selected filter values.

    Args:
        tab: Current tab - "business_analytics", "d2c_test_funnel", or "stash_analytics"

    Returns:
        Dictionary of filter values
    """
    # Initialize defaults
    init_filter_defaults()

    # Pre-fetch options before form (can't have queries inside form)
    available_versions = get_available_versions()
    is_business_tab = tab in ["business_analytics", "d2c_test_funnel"]
    available_countries = get_available_countries() if not is_business_tab else []

    # Set default versions if not set
    if st.session_state.filter_versions is None and available_versions:
        st.session_state.filter_versions = available_versions

    # Set default countries if not set
    if st.session_state.filter_countries is None and available_countries:
        st.session_state.filter_countries = available_countries

    st.sidebar.header("📊 Dashboard Filters")

    with st.sidebar.form(key="filters_form"):
        # Date range filters
        st.subheader("Date Range")
        default_end_date = datetime.now().date()

        start_date = st.date_input(
            "Start Date",
            value=st.session_state.filter_start_date,
            max_value=default_end_date
        )

        end_date = st.date_input(
            "End Date",
            value=st.session_state.filter_end_date,
            max_value=default_end_date
        )

        # Platform filter
        st.subheader("Platform")
        mp_os = st.multiselect(
            "Operating System",
            options=["Apple", "Android"],
            default=st.session_state.filter_mp_os
        )

        # Version filter
        st.subheader("Version")
        if available_versions:
            # Use saved versions or all versions as default
            default_versions = st.session_state.filter_versions if st.session_state.filter_versions else available_versions
            # Filter to only include versions that still exist
            default_versions = [v for v in default_versions if v in available_versions]
            if not default_versions:
                default_versions = available_versions

            selected_versions = st.multiselect(
                "App Versions",
                options=available_versions,
                default=default_versions,
                help="Select one or more versions. Default: all versions >= 0.3775"
            )
            version = selected_versions if selected_versions else None
        else:
            version = None
            st.info("No versions available")

        # Country filter - only show for Stash Analytics tab
        if not is_business_tab:
            st.subheader("Geography")
            if available_countries:
                default_countries = st.session_state.filter_countries if st.session_state.filter_countries else available_countries
                default_countries = [c for c in default_countries if c in available_countries]
                if not default_countries:
                    default_countries = available_countries

                selected_countries = st.multiselect(
                    "Countries",
                    options=available_countries,
                    default=default_countries,
                    help="Select one or more countries. Default: all countries"
                )
                countries = selected_countries if selected_countries else None
            else:
                countries = None
                st.info("No countries available")

            is_low_payers_country = st.checkbox(
                "Low Payers Countries Only",
                value=st.session_state.filter_is_low_payers
            )
        else:
            countries = None
            is_low_payers_country = False

        exclude_testing_countries = st.checkbox(
            "Exclude Testing Countries (UA, IL, AM)",
            value=st.session_state.filter_exclude_testing,
            help="Exclude Ukraine, Israel, and Armenia from analysis"
        )

        # Test users filter - only show for Stash Analytics tab
        if not is_business_tab:
            st.subheader("Test Users")
            is_stash_test_users = st.checkbox(
                "Stash Test Users Only",
                value=st.session_state.filter_is_stash_test_users,
                help="Filter to only users in the stash_test_users table"
            )
        else:
            is_stash_test_users = False

        # Test Start Date - only show for Business Analytics tabs
        if is_business_tab:
            st.subheader("D2C Test")
            test_start_date = st.date_input(
                "Test Start Date (UTC)",
                value=st.session_state.filter_test_start_date,
                help="Select the date when the D2C test started (all times are in UTC)"
            )
        else:
            test_start_date = None

        # Submit button
        st.markdown("---")
        submitted = st.form_submit_button("🚀 Submit", use_container_width=True, type="primary")

        if submitted:
            # Save all values to session state
            st.session_state.filter_start_date = start_date
            st.session_state.filter_end_date = end_date
            st.session_state.filter_mp_os = mp_os
            st.session_state.filter_versions = version
            if not is_business_tab:
                st.session_state.filter_countries = countries
                st.session_state.filter_is_low_payers = is_low_payers_country
                st.session_state.filter_is_stash_test_users = is_stash_test_users
            st.session_state.filter_exclude_testing = exclude_testing_countries
            if is_business_tab:
                st.session_state.filter_test_start_date = test_start_date

    # Validate date range
    if start_date > end_date:
        st.sidebar.error("Start date must be before end date")
        start_date = end_date

    # Check max 61 days
    days_diff = (end_date - start_date).days
    if days_diff > 60:
        st.sidebar.warning(f"Date range is {days_diff + 1} days. Maximum is 61 days.")
        start_date = end_date - timedelta(days=60)

    filters = {
        "start_date": start_date.strftime("%Y-%m-%d"),
        "end_date": end_date.strftime("%Y-%m-%d"),
        "mp_os": mp_os,
        "version": version,
        "country": countries,
        "is_low_payers_country": is_low_payers_country,
        "exclude_testing_countries": exclude_testing_countries,
        "is_stash_test_users": is_stash_test_users,
        "test_start_date": test_start_date.strftime("%Y-%m-%d") if test_start_date else None,
        "tab": tab
    }

    # Display applied filters summary
    st.sidebar.markdown("---")
    st.sidebar.caption(f"📅 Date Range: {days_diff + 1} days")
    if not is_business_tab and is_stash_test_users:
        st.sidebar.caption("👥 Test Users Only")
    if not is_business_tab and is_low_payers_country:
        st.sidebar.caption("🌍 Low Payers Countries Only")
    if exclude_testing_countries:
        st.sidebar.caption("🚫 Excluding UA, IL, AM")
    if is_business_tab:
        st.sidebar.caption("🇺🇸 US Users Only (D2C)")
    if tab == "d2c_test_funnel":
        st.sidebar.caption("🧪 Test Group Only (20%)")

    return filters


def display_filter_summary(filters: Dict[str, Any]):
    """Display a summary of applied filters in the main area."""
    with st.expander("🔍 Applied Filters", expanded=False):
        cols = st.columns(3)

        with cols[0]:
            st.metric("Date Range", f"{filters['start_date']} to {filters['end_date']}")
            st.caption(f"Platforms: {', '.join(filters['mp_os']) if filters['mp_os'] else 'All'}")

        with cols[1]:
            if filters['version']:
                if len(filters['version']) <= 3:
                    version_text = ', '.join([str(v) for v in filters['version']])
                else:
                    version_text = f"{len(filters['version'])} versions"
                st.caption(f"Versions: {version_text}")
            else:
                st.caption("Versions: All")

            if filters['country']:
                if len(filters['country']) <= 3:
                    country_text = ', '.join(filters['country'])
                else:
                    country_text = f"{len(filters['country'])} countries"
                st.caption(f"Countries: {country_text}")
            else:
                st.caption("Countries: All")

        with cols[2]:
            flags = []
            if filters['is_low_payers_country']:
                flags.append("Low Payers Only")
            if filters['is_stash_test_users']:
                flags.append("Test Users Only")
            if flags:
                st.caption(" | ".join(flags))
