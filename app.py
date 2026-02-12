"""
Stash Analytics Dashboard
Main Streamlit application integrating all charts with OAuth authentication.
"""

import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# Import authentication
from auth_code import authenticate_user, is_oauth_configured, show_user_sidebar

# Import utilities
from utils.filters import render_filters, display_filter_summary

# Import chart modules
from queries import chart1_kpi_compare, chart2_user_funnel, chart3_user_funnel_percentage, chart4_execution_funnel, chart5_execution_funnel_percentage
from queries import chart6_adoption_over_time, chart7_latency
from queries import chart_test_vs_control_timeline
from queries import chart_d2c_test_funnel

# Import D2C utilities
from utils.d2c_segments import get_d2c_segment_stats, get_d2c_purchase_summary


def get_elapsed_time_str(last_fetch_time: datetime) -> str:
    """Format elapsed time since last fetch."""
    elapsed = datetime.now() - last_fetch_time
    total_seconds = int(elapsed.total_seconds())

    if total_seconds < 60:
        return f"{total_seconds} seconds"
    elif total_seconds < 3600:
        minutes = total_seconds // 60
        return f"{minutes} minutes"
    else:
        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60
        if minutes > 0:
            return f"{hours} hours and {minutes} minutes"
        return f"{hours} hours"


def render_business_analytics_tab(filters):
    """Render the Business Analytics tab for D2C Test vs Control analysis."""

    # Initialize last fetch time if not exists
    if 'last_fetch_time_business' not in st.session_state:
        st.session_state.last_fetch_time_business = datetime.now()

    # Header with refresh button and last fetch info
    col_header, col_time, col_refresh = st.columns([3, 2, 1])
    with col_header:
        st.header("📊 D2C Business Analytics")
    with col_time:
        last_fetch = st.session_state.last_fetch_time_business
        elapsed_str = get_elapsed_time_str(last_fetch)
        st.caption(f"🕐 Last fetch: {last_fetch.strftime('%H:%M:%S')}")
        st.caption(f"⏱️ {elapsed_str} ago")
    with col_refresh:
        if st.button("🔄 Refresh", help="Clear cache and reload all data"):
            st.cache_data.clear()
            st.session_state.last_fetch_time_business = datetime.now()
            st.rerun()

    st.info("""
    **D2C Test Segmentation:**
    - US users only
    - App version > 0.378
    - Days since install > 3
    - **Test (20%)** vs **Control (80%)** - deterministic split by user ID
    """)

    # Display sample sizes (active users in date range)
    with st.spinner("Loading sample sizes..."):
        try:
            segment_stats = get_d2c_segment_stats(filters)
            if not segment_stats.empty:
                test_users = segment_stats[segment_stats['segment'] == 'test']['users'].values[0] if len(segment_stats[segment_stats['segment'] == 'test']) > 0 else 0
                control_users = segment_stats[segment_stats['segment'] == 'control']['users'].values[0] if len(segment_stats[segment_stats['segment'] == 'control']) > 0 else 0
                total_users = test_users + control_users

                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Test Group", f"{test_users:,}", help="20% of eligible users")
                with col2:
                    st.metric("Control Group", f"{control_users:,}", help="80% of eligible users")
                with col3:
                    st.metric("Total Eligible Users", f"{total_users:,}")
        except Exception as e:
            st.warning(f"Could not load sample sizes: {str(e)}")

    # Display purchase summary
    with st.spinner("Loading purchase summary..."):
        try:
            purchase_summary = get_d2c_purchase_summary(filters)
            if not purchase_summary.empty:
                total_revenue = purchase_summary['total_revenue'].values[0] or 0
                d2c_revenue = purchase_summary['d2c_revenue'].values[0] or 0
                iap_revenue = purchase_summary['iap_revenue'].values[0] or 0

                d2c_pct = (d2c_revenue / total_revenue * 100) if total_revenue > 0 else 0
                iap_pct = (iap_revenue / total_revenue * 100) if total_revenue > 0 else 0

                st.markdown(f"##### 💰 Purchase Summary ({filters['start_date']} to {filters['end_date']})")
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric(
                        "Total Revenue",
                        f"${total_revenue:,.0f}",
                        help="Total revenue from all payment platforms"
                    )
                with col2:
                    st.metric(
                        "D2C (Stash)",
                        f"${d2c_revenue:,.0f}",
                        f"{d2c_pct:.1f}%",
                        help="Revenue from Stash payments (0% platform fee)"
                    )
                with col3:
                    st.metric(
                        "IAP (Apple/Google)",
                        f"${iap_revenue:,.0f}",
                        f"{iap_pct:.1f}%",
                        help="Revenue from Apple App Store and Google Play (30% platform fee)"
                    )

                # D2C Savings calculation
                d2c_savings = d2c_revenue * 0.30  # 30% that would have been paid to Apple/Google
                iap_fees = iap_revenue * 0.30  # 30% paid to Apple/Google
                net_d2c = d2c_revenue  # D2C keeps 100%
                net_iap = iap_revenue * 0.70  # IAP keeps 70%
                total_net = net_d2c + net_iap

                st.markdown("##### 💵 D2C Margin Analysis")
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric(
                        "D2C Savings",
                        f"${d2c_savings:,.0f}",
                        help="Money saved by using D2C instead of IAP (30% of D2C revenue)"
                    )
                with col2:
                    st.metric(
                        "IAP Fees Paid",
                        f"${iap_fees:,.0f}",
                        help="30% commission paid to Apple/Google on IAP revenue"
                    )
                with col3:
                    st.metric(
                        "Total Net Revenue",
                        f"${total_net:,.0f}",
                        help="Revenue after platform fees (D2C: 100%, IAP: 70%)"
                    )
                with col4:
                    effective_margin = (total_net / total_revenue * 100) if total_revenue > 0 else 0
                    st.metric(
                        "Effective Margin",
                        f"{effective_margin:.1f}%",
                        help="Net revenue as % of gross (higher is better, max 100%)"
                    )
        except Exception as e:
            st.warning(f"Could not load purchase summary: {str(e)}")

    st.markdown("---")

    # Get test start date from filters
    test_start_date = filters.get('test_start_date')

    # Fetch timeline data
    with st.spinner("Loading Test vs Control data..."):
        try:
            timeline_df = chart_test_vs_control_timeline.get_data(filters, str(test_start_date))

            if not timeline_df.empty:
                # Diff-in-Diff Summary Section
                col_did_header, col_did_help = st.columns([6, 1])
                with col_did_header:
                    st.subheader("📈 Diff-in-Diff Summary")
                    st.caption("Diff-in-Diff = (Test_After - Test_Before) - (Control_After - Control_Before)")
                with col_did_help:
                    with st.popover("❓ KPI Definitions"):
                        st.markdown("""
                        **Active Users** - Unique active users

                        **Total Purchases** - Total verified purchases

                        **Gross Revenue** - Gross revenue before fees

                        **Net Revenue** - Net revenue after fees (30% Apple/Google)

                        **Paying Users** - Unique paying users

                        **PPU %** - Percentage of paying users (Paying Users / Active Users)

                        **ARPDAU** - Average revenue per daily active user (Revenue / Active Users)

                        **ARPPU** - Average revenue per paying user (Revenue / Paying Users)

                        **ATV** - Average transaction value (Revenue / Purchases)
                        """)

                did_summary = chart_test_vs_control_timeline.create_did_summary_table(timeline_df)

                if not did_summary.empty:
                    # Format the summary table
                    formatted_did = did_summary.copy()

                    # Format numeric columns
                    for col in ['Test Before', 'Test After', 'Control Before', 'Control After', 'Diff-in-Diff']:
                        formatted_did[col] = formatted_did[col].apply(lambda x: f"{x:,.2f}" if abs(x) < 1000 else f"{x:,.0f}")

                    for col in ['Test Change %', 'Control Change %', 'DiD %']:
                        formatted_did[col] = formatted_did[col].apply(lambda x: f"{x:+.1f}%")

                    st.dataframe(formatted_did, use_container_width=True, hide_index=True)

                    with st.expander("📥 Download Diff-in-Diff Summary"):
                        csv_did = did_summary.to_csv(index=False).encode('utf-8')
                        st.download_button(
                            label="Download CSV",
                            data=csv_did,
                            file_name=f"did_summary_{filters['start_date']}_to_{filters['end_date']}.csv",
                            mime="text/csv",
                            key="download_did"
                        )

                st.markdown("---")

                # Timeline Section
                st.subheader("📊 Test vs Control Timeline")

                # Graph 1: Revenue & Users KPIs
                st.markdown("#### Revenue & Users Metrics")
                selected_kpi1 = st.selectbox(
                    "Select KPI",
                    options=list(chart_test_vs_control_timeline.GRAPH1_KPIS.keys()),
                    format_func=lambda x: chart_test_vs_control_timeline.GRAPH1_KPIS[x],
                    key="kpi_selector_1"
                )

                fig1, summary1 = chart_test_vs_control_timeline.create_timeline_visualization(
                    timeline_df,
                    selected_kpi1,
                    chart_test_vs_control_timeline.GRAPH1_KPIS[selected_kpi1]
                )
                st.plotly_chart(fig1, use_container_width=True, key="timeline_graph_1")

                # Summary below graph 1
                if summary1:
                    has_test = summary1.get('has_test', True)
                    has_control = summary1.get('has_control', True)

                    if has_test and has_control:
                        col_s1, col_s2, col_s3 = st.columns(3)
                        with col_s1:
                            test_color = "green" if summary1['test_change'] >= 0 else "red"
                            st.markdown(f"**Test Change:** :{test_color}[{summary1['test_change']:+.1f}%]")
                        with col_s2:
                            ctrl_color = "green" if summary1['control_change'] >= 0 else "red"
                            st.markdown(f"**Control Change:** :{ctrl_color}[{summary1['control_change']:+.1f}%]")
                        with col_s3:
                            did_color = "green" if summary1['diff_in_diff_pct'] >= 0 else "red"
                            st.markdown(f"**Diff-in-Diff:** :{did_color}[{summary1['diff_in_diff_pct']:+.1f}%]")

                st.markdown("---")

                # Graph 2: Conversion & Other KPIs
                st.markdown("#### Conversion & Other Metrics")
                selected_kpi2 = st.selectbox(
                    "Select KPI",
                    options=list(chart_test_vs_control_timeline.GRAPH2_KPIS.keys()),
                    format_func=lambda x: chart_test_vs_control_timeline.GRAPH2_KPIS[x],
                    key="kpi_selector_2"
                )

                fig2, summary2 = chart_test_vs_control_timeline.create_timeline_visualization(
                    timeline_df,
                    selected_kpi2,
                    chart_test_vs_control_timeline.GRAPH2_KPIS[selected_kpi2]
                )
                st.plotly_chart(fig2, use_container_width=True, key="timeline_graph_2")

                # Summary below graph 2
                if summary2:
                    has_test = summary2.get('has_test', True)
                    has_control = summary2.get('has_control', True)

                    if has_test and has_control:
                        col_s1, col_s2, col_s3 = st.columns(3)
                        with col_s1:
                            test_color = "green" if summary2['test_change'] >= 0 else "red"
                            st.markdown(f"**Test Change:** :{test_color}[{summary2['test_change']:+.1f}%]")
                        with col_s2:
                            ctrl_color = "green" if summary2['control_change'] >= 0 else "red"
                            st.markdown(f"**Control Change:** :{ctrl_color}[{summary2['control_change']:+.1f}%]")
                        with col_s3:
                            did_color = "green" if summary2['diff_in_diff_pct'] >= 0 else "red"
                            st.markdown(f"**Diff-in-Diff:** :{did_color}[{summary2['diff_in_diff_pct']:+.1f}%]")

                st.markdown("---")

                # Graph 3: Conversion Rate to Purchase
                st.markdown("#### 🎯 Conversion Rate to Purchase (PPU%)")
                st.caption("Percentage of active users who made at least one purchase")

                fig3, summary3 = chart_test_vs_control_timeline.create_timeline_visualization(
                    timeline_df,
                    'ppu_percent',
                    'Conversion Rate (PPU%)'
                )
                st.plotly_chart(fig3, use_container_width=True, key="timeline_graph_3")

                # Summary below graph 3
                if summary3:
                    has_test = summary3.get('has_test', True)
                    has_control = summary3.get('has_control', True)

                    if has_test and has_control:
                        col_s1, col_s2, col_s3 = st.columns(3)
                        with col_s1:
                            test_color = "green" if summary3['test_change'] >= 0 else "red"
                            st.markdown(f"**Test Change:** :{test_color}[{summary3['test_change']:+.1f}%]")
                        with col_s2:
                            ctrl_color = "green" if summary3['control_change'] >= 0 else "red"
                            st.markdown(f"**Control Change:** :{ctrl_color}[{summary3['control_change']:+.1f}%]")
                        with col_s3:
                            did_color = "green" if summary3['diff_in_diff_pct'] >= 0 else "red"
                            st.markdown(f"**Diff-in-Diff:** :{did_color}[{summary3['diff_in_diff_pct']:+.1f}%]")

                st.markdown("---")

                # Download Timeline Data and Show Query
                with st.expander("📥 Download Timeline Data"):
                    st.dataframe(timeline_df)
                    csv_timeline = timeline_df.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        label="Download CSV",
                        data=csv_timeline,
                        file_name=f"timeline_data_{filters['start_date']}_to_{filters['end_date']}.csv",
                        mime="text/csv",
                        key="download_timeline"
                    )

                with st.expander("📋 View SQL Query"):
                    st.caption("Select and copy the query below:")
                    sql_query = chart_test_vs_control_timeline.build_query(filters, str(test_start_date))
                    st.code(sql_query, language="sql")

            else:
                st.warning("No data available for the selected date range.")

        except Exception as e:
            st.error(f"Error loading data: {str(e)}")


def render_d2c_test_funnel_tab(filters):
    """Render the D2C Test Funnel tab - focusing on Test group only."""

    # Initialize last fetch time if not exists
    if 'last_fetch_time_funnel' not in st.session_state:
        st.session_state.last_fetch_time_funnel = datetime.now()

    # Header with refresh button and last fetch info
    col_header, col_time, col_refresh = st.columns([3, 2, 1])
    with col_header:
        st.header("🧪 D2C Test Group Funnel")
    with col_time:
        last_fetch = st.session_state.last_fetch_time_funnel
        elapsed_str = get_elapsed_time_str(last_fetch)
        st.caption(f"🕐 Last fetch: {last_fetch.strftime('%H:%M:%S')}")
        st.caption(f"⏱️ {elapsed_str} ago")
    with col_refresh:
        if st.button("🔄 Refresh", key="refresh_funnel", help="Clear cache and reload all data"):
            st.cache_data.clear()
            st.session_state.last_fetch_time_funnel = datetime.now()
            st.rerun()

    st.info("""
    **Test Group Only (20%)** - D2C eligible US users with hash < 20
    - Comparing **Stash (D2C)** vs **IAP (Apple/Google)** funnels
    """)

    # Display sample sizes (active users in date range)
    with st.spinner("Loading sample sizes..."):
        try:
            segment_stats = get_d2c_segment_stats(filters)
            if not segment_stats.empty:
                test_users = segment_stats[segment_stats['segment'] == 'test']['users'].values[0] if len(segment_stats[segment_stats['segment'] == 'test']) > 0 else 0
                control_users = segment_stats[segment_stats['segment'] == 'control']['users'].values[0] if len(segment_stats[segment_stats['segment'] == 'control']) > 0 else 0
                total_users = test_users + control_users

                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Test Group (Active)", f"{test_users:,}", help="Active users in Test group (20%) for selected date range")
                with col2:
                    st.metric("Control Group (Active)", f"{control_users:,}", help="Active users in Control group (80%) for selected date range")
                with col3:
                    st.metric("Total Active Users", f"{total_users:,}")
        except Exception as e:
            st.warning(f"Could not load sample sizes: {str(e)}")

    st.markdown("---")

    # Fetch funnel data
    with st.spinner("Loading funnel data..."):
        try:
            funnel_df = chart_d2c_test_funnel.get_funnel_data(filters)
            daily_df = chart_d2c_test_funnel.get_daily_funnel_data(filters)

            if not funnel_df.empty:
                row = funnel_df.iloc[0]

                # Summary metrics
                st.subheader("📊 Funnel Summary")

                col1, col2, col3 = st.columns(3)

                # Total metrics
                total_purchases = row['stash_purchases'] + row['apple_purchases'] + row['google_purchases']
                total_revenue = row['stash_revenue'] + row['apple_revenue'] + row['google_revenue']

                with col1:
                    st.metric("Purchase Clicks", f"{int(row['purchase_clicks']):,}")
                with col2:
                    st.metric("Total Purchases", f"{int(total_purchases):,}")
                with col3:
                    st.metric("Total Revenue", f"${total_revenue:,.0f}")

                st.markdown("---")

                # Stash vs IAP comparison
                st.subheader("🔄 Stash vs IAP Comparison")

                col1, col2 = st.columns(2)

                with col1:
                    st.markdown("##### 💚 Stash (D2C)")
                    stash_conv = (row['stash_purchases'] / row['stash_continue'] * 100) if row['stash_continue'] > 0 else 0
                    st.metric("Continue Clicks", f"{int(row['stash_continue']):,}")
                    st.metric("Purchases", f"{int(row['stash_purchases']):,}")
                    st.metric("Revenue", f"${row['stash_revenue']:,.0f}")
                    st.metric("Conversion Rate", f"{stash_conv:.1f}%")

                with col2:
                    st.markdown("##### 🔴 IAP (Apple/Google)")
                    iap_continue = row['apple_continue'] + row['google_continue']
                    iap_purchases = row['apple_purchases'] + row['google_purchases']
                    iap_revenue = row['apple_revenue'] + row['google_revenue']
                    iap_conv = (iap_purchases / iap_continue * 100) if iap_continue > 0 else 0
                    st.metric("Continue Clicks", f"{int(iap_continue):,}")
                    st.metric("Purchases", f"{int(iap_purchases):,}")
                    st.metric("Revenue", f"${iap_revenue:,.0f}")
                    st.metric("Conversion Rate", f"{iap_conv:.1f}%")

                # D2C Margin Analysis
                st.markdown("---")
                st.subheader("💵 D2C Margin Analysis")

                stash_revenue = row['stash_revenue']
                d2c_savings = stash_revenue * 0.30  # 30% saved
                iap_fees = iap_revenue * 0.30  # 30% paid to platforms
                net_stash = stash_revenue  # 100% kept
                net_iap = iap_revenue * 0.70  # 70% kept
                total_net = net_stash + net_iap
                total_gross = stash_revenue + iap_revenue
                effective_margin = (total_net / total_gross * 100) if total_gross > 0 else 0

                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric(
                        "D2C Savings",
                        f"${d2c_savings:,.0f}",
                        help="30% saved by using Stash instead of IAP"
                    )
                with col2:
                    st.metric(
                        "IAP Fees Paid",
                        f"${iap_fees:,.0f}",
                        help="30% commission paid to Apple/Google"
                    )
                with col3:
                    st.metric(
                        "Total Net Revenue",
                        f"${total_net:,.0f}",
                        help="Revenue after platform fees"
                    )
                with col4:
                    st.metric(
                        "Effective Margin",
                        f"{effective_margin:.1f}%",
                        help="Net/Gross ratio (100% = all D2C, 70% = all IAP)"
                    )

                st.markdown("---")

                # Funnel charts - side by side
                st.subheader("📈 Funnel Visualization")
                fig_stash, fig_iap = chart_d2c_test_funnel.create_funnel_charts(funnel_df)

                col_funnel1, col_funnel2 = st.columns(2)
                with col_funnel1:
                    st.plotly_chart(fig_stash, use_container_width=True)
                with col_funnel2:
                    st.plotly_chart(fig_iap, use_container_width=True)

                with st.expander("📥 Download Funnel Data"):
                    st.dataframe(funnel_df)
                    csv_funnel = funnel_df.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        label="Download CSV",
                        data=csv_funnel,
                        file_name=f"d2c_funnel_{filters['start_date']}_to_{filters['end_date']}.csv",
                        mime="text/csv",
                        key="download_funnel"
                    )

                st.markdown("---")

                # Pre-purchase choice pie chart
                st.subheader("🥧 Pre-Purchase Choice Distribution")
                st.caption("Shows how many users stayed with Stash (default) vs switched to IAP")
                fig_pie = chart_d2c_test_funnel.create_prepurchase_choice_pie(funnel_df)
                st.plotly_chart(fig_pie, use_container_width=True)

                # Create pie chart data for download
                pie_data = pd.DataFrame({
                    'Choice': ['Stash (Default)', 'IAP (Switched)'],
                    'Continue Clicks': [row['stash_continue'], row['apple_continue'] + row['google_continue']]
                })
                with st.expander("📥 Download Pre-Purchase Choice Data"):
                    st.dataframe(pie_data)
                    csv_pie = pie_data.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        label="Download CSV",
                        data=csv_pie,
                        file_name=f"prepurchase_choice_{filters['start_date']}_to_{filters['end_date']}.csv",
                        mime="text/csv",
                        key="download_pie"
                    )

                st.markdown("---")

                # Daily charts
                if not daily_df.empty:
                    st.subheader("📅 Daily Trends")

                    # Revenue chart
                    fig_revenue = chart_d2c_test_funnel.create_daily_chart(daily_df, 'revenue', 'Daily Revenue: Stash vs IAP')
                    st.plotly_chart(fig_revenue, use_container_width=True)

                    # Purchases chart
                    fig_purchases = chart_d2c_test_funnel.create_daily_chart(daily_df, 'purchases', 'Daily Purchases: Stash vs IAP')
                    st.plotly_chart(fig_purchases, use_container_width=True)

                    # Conversion chart
                    fig_conversion = chart_d2c_test_funnel.create_daily_chart(daily_df, 'conversion', 'Daily Conversion Rate: Stash vs IAP')
                    st.plotly_chart(fig_conversion, use_container_width=True)

                    with st.expander("📥 Download Daily Trends Data"):
                        st.dataframe(daily_df)
                        csv_daily = daily_df.to_csv(index=False).encode('utf-8')
                        st.download_button(
                            label="Download CSV",
                            data=csv_daily,
                            file_name=f"daily_trends_{filters['start_date']}_to_{filters['end_date']}.csv",
                            mime="text/csv",
                            key="download_daily"
                        )

                # First vs Repeat D2C Purchasers chart
                st.markdown("---")
                st.subheader("🔄 D2C Purchase Behavior: First vs Repeat")
                st.caption("Users who made their first D2C purchase vs users making their 2nd+ D2C purchase")
                with st.spinner("Loading first vs repeat purchase data..."):
                    first_repeat_df = chart_d2c_test_funnel.get_d2c_first_vs_repeat_data(filters)
                    if not first_repeat_df.empty:
                        fig_first_repeat = chart_d2c_test_funnel.create_first_vs_repeat_chart(first_repeat_df)
                        st.plotly_chart(fig_first_repeat, use_container_width=True)

                        with st.expander("📥 Download First vs Repeat Data"):
                            st.dataframe(first_repeat_df)
                            csv_first_repeat = first_repeat_df.to_csv(index=False).encode('utf-8')
                            st.download_button(
                                label="Download CSV",
                                data=csv_first_repeat,
                                file_name=f"first_vs_repeat_{filters['start_date']}_to_{filters['end_date']}.csv",
                                mime="text/csv",
                                key="download_first_repeat"
                            )
                    else:
                        st.info("No D2C purchase data available for the selected date range.")

                # D2C Adoption Funnel - 1st to 2nd to 3rd to 4th+ purchase
                st.markdown("---")
                st.subheader("📊 D2C Adoption Funnel")
                st.caption("Shows retention from 1st D2C purchase to 2nd, 3rd, and 4th+ purchases")
                with st.spinner("Loading D2C adoption funnel data..."):
                    adoption_df = chart_d2c_test_funnel.get_d2c_adoption_funnel_data(filters)
                    if not adoption_df.empty:
                        fig_adoption, adoption_metrics = chart_d2c_test_funnel.create_d2c_adoption_funnel_chart(adoption_df)
                        st.plotly_chart(fig_adoption, use_container_width=True)

                        # Display retention metrics
                        if adoption_metrics:
                            st.markdown("##### 📈 Retention Rates")
                            col1, col2, col3 = st.columns(3)
                            with col1:
                                st.metric(
                                    "1st → 2nd Purchase",
                                    f"{adoption_metrics['retention_1_to_2']:.1f}%",
                                    help=f"{adoption_metrics['users_2nd']:,} of {adoption_metrics['users_1st']:,} users"
                                )
                            with col2:
                                st.metric(
                                    "2nd → 3rd Purchase",
                                    f"{adoption_metrics['retention_2_to_3']:.1f}%",
                                    help=f"{adoption_metrics['users_3rd']:,} of {adoption_metrics['users_2nd']:,} users"
                                )
                            with col3:
                                st.metric(
                                    "3rd → 4th+ Purchase",
                                    f"{adoption_metrics['retention_3_to_4']:.1f}%",
                                    help=f"{adoption_metrics['users_4th_plus']:,} of {adoption_metrics['users_3rd']:,} users"
                                )

                        with st.expander("📥 Download Adoption Funnel Data"):
                            st.dataframe(adoption_df)
                            csv_adoption = adoption_df.to_csv(index=False).encode('utf-8')
                            st.download_button(
                                label="Download CSV",
                                data=csv_adoption,
                                file_name=f"adoption_funnel_{filters['start_date']}_to_{filters['end_date']}.csv",
                                mime="text/csv",
                                key="download_adoption"
                            )
                    else:
                        st.info("No D2C adoption data available for the selected date range.")

                # Average Transaction Value by Purchase Number
                st.markdown("---")
                st.subheader("💰 Average Transaction Value by Purchase Number")
                st.caption("Shows if users spend more or less on subsequent D2C purchases")
                with st.spinner("Loading ATV data..."):
                    atv_df = chart_d2c_test_funnel.get_d2c_atv_by_purchase_number(filters)
                    if not atv_df.empty:
                        fig_atv = chart_d2c_test_funnel.create_atv_by_purchase_chart(atv_df)
                        st.plotly_chart(fig_atv, use_container_width=True)

                        # Display ATV metrics (max 4 columns)
                        num_cols = min(len(atv_df), 4)
                        cols = st.columns(num_cols)
                        for i, (_, atv_row) in enumerate(atv_df.head(4).iterrows()):
                            with cols[i]:
                                st.metric(
                                    atv_row['purchase_tier'],
                                    f"${atv_row['avg_transaction_value']:.2f}",
                                    help=f"{int(atv_row['num_purchases']):,} purchases, ${atv_row['total_revenue']:,.0f} total"
                                )

                        with st.expander("📥 Download ATV Data"):
                            st.dataframe(atv_df)
                            csv_atv = atv_df.to_csv(index=False).encode('utf-8')
                            st.download_button(
                                label="Download CSV",
                                data=csv_atv,
                                file_name=f"atv_by_purchase_{filters['start_date']}_to_{filters['end_date']}.csv",
                                mime="text/csv",
                                key="download_atv"
                            )
                    else:
                        st.info("No ATV data available for the selected date range.")

                # Time to First D2C Purchase
                st.markdown("---")
                st.subheader("⏱️ Time to First D2C Purchase")
                st.caption("Distribution of days from app install to first D2C purchase")
                with st.spinner("Loading time to first purchase data..."):
                    time_df = chart_d2c_test_funnel.get_time_to_first_d2c_purchase(filters)
                    if not time_df.empty:
                        fig_time = chart_d2c_test_funnel.create_time_to_first_purchase_chart(time_df)
                        st.plotly_chart(fig_time, use_container_width=True)

                        # Display summary stats
                        stats = chart_d2c_test_funnel.get_time_to_first_purchase_stats(time_df)
                        if stats:
                            col1, col2, col3 = st.columns(3)
                            with col1:
                                st.metric(
                                    "Average Days to First Purchase",
                                    f"{stats['avg_days']:.1f} days"
                                )
                            with col2:
                                st.metric(
                                    "Same Day Conversion",
                                    f"{stats['day_0_pct']:.1f}%",
                                    help="Users who made first D2C purchase on install day"
                                )
                            with col3:
                                st.metric(
                                    "First Week Conversion",
                                    f"{stats['week_1_pct']:.1f}%",
                                    help="Users who made first D2C purchase within 7 days"
                                )

                        with st.expander("📥 Download Time to First Purchase Data"):
                            st.dataframe(time_df)
                            csv_time = time_df.to_csv(index=False).encode('utf-8')
                            st.download_button(
                                label="Download CSV",
                                data=csv_time,
                                file_name=f"time_to_first_purchase_{filters['start_date']}_to_{filters['end_date']}.csv",
                                mime="text/csv",
                                key="download_time"
                            )
                    else:
                        st.info("No time to first purchase data available.")

                # Stash Funnel Execution Chart (Chart 5 equivalent for Test group)
                st.markdown("---")
                st.subheader("🔄 Stash Funnel Executions (% from Purchase Click)")
                st.caption("Full Stash purchase funnel from initial click to rewards - Test Group Only")
                with st.spinner("Loading Stash funnel execution data..."):
                    execution_df = chart_d2c_test_funnel.get_stash_funnel_execution_data(filters)
                    if not execution_df.empty:
                        fig_execution = chart_d2c_test_funnel.create_stash_funnel_execution_chart(execution_df)
                        st.plotly_chart(fig_execution, use_container_width=True)

                        # Display key conversion metrics
                        funnel_metrics = chart_d2c_test_funnel.get_stash_funnel_metrics(execution_df)
                        if funnel_metrics:
                            st.markdown("##### 📈 Key Conversion Rates")
                            col1, col2, col3, col4, col5 = st.columns(5)
                            with col1:
                                st.metric(
                                    "Purchase Clicks",
                                    f"{funnel_metrics['purchase_clicks']:,}",
                                    help="Total purchase click events"
                                )
                            with col2:
                                st.metric(
                                    "Stash Continue Rate",
                                    f"{funnel_metrics['continue_rate']:.1f}%",
                                    help="Purchase Clicks → Stash Continue"
                                )
                            with col3:
                                st.metric(
                                    "Webform Rate",
                                    f"{funnel_metrics['webform_rate']:.1f}%",
                                    help="Stash Continue → Webform Impression"
                                )
                            with col4:
                                st.metric(
                                    "Pay Click Rate",
                                    f"{funnel_metrics['pay_click_rate']:.1f}%",
                                    help="Webform Impression → Pay Click"
                                )
                            with col5:
                                st.metric(
                                    "Overall Conversion",
                                    f"{funnel_metrics['overall_conversion']:.1f}%",
                                    help="Purchase Click → Successful Purchase"
                                )

                        # Download option
                        with st.expander("📥 Download Funnel Execution Data"):
                            st.dataframe(execution_df)
                            csv_execution = execution_df.to_csv(index=False).encode('utf-8')
                            st.download_button(
                                label="Download CSV",
                                data=csv_execution,
                                file_name=f"stash_funnel_execution_{filters['start_date']}_to_{filters['end_date']}.csv",
                                mime="text/csv"
                            )
                    else:
                        st.info("No Stash funnel execution data available for the selected date range.")

            else:
                st.warning("No funnel data available for the selected date range.")

        except Exception as e:
            st.error(f"Error loading funnel data: {str(e)}")


def render_stash_analytics_tab(filters):
    """Render the original Stash Analytics tab."""

    # Initialize last fetch time if not exists
    if 'last_fetch_time_stash' not in st.session_state:
        st.session_state.last_fetch_time_stash = datetime.now()

    # Header with refresh button and last fetch info
    col_header, col_time, col_refresh = st.columns([3, 2, 1])
    with col_header:
        st.header("📈 Stash Analytics")
    with col_time:
        last_fetch = st.session_state.last_fetch_time_stash
        elapsed_str = get_elapsed_time_str(last_fetch)
        st.caption(f"🕐 Last fetch: {last_fetch.strftime('%H:%M:%S')}")
        st.caption(f"⏱️ {elapsed_str} ago")
    with col_refresh:
        if st.button("🔄 Refresh", key="refresh_stash", help="Clear cache and reload all data"):
            st.cache_data.clear()
            st.session_state.last_fetch_time_stash = datetime.now()
            st.rerun()

    # Fetch chart data in parallel (except Chart 7 which has its own filter)
    with st.spinner("⚡ Loading charts 1-6 in parallel..."):
        with ThreadPoolExecutor(max_workers=6) as executor:
            # Submit all queries to run concurrently (except Chart 7)
            future1 = executor.submit(chart1_kpi_compare.get_data, filters)
            future2 = executor.submit(chart2_user_funnel.get_data, filters)
            future3 = executor.submit(chart3_user_funnel_percentage.get_data, filters)
            future4 = executor.submit(chart4_execution_funnel.get_data, filters)
            future5 = executor.submit(chart5_execution_funnel_percentage.get_data, filters)
            future6 = executor.submit(chart6_adoption_over_time.get_data, filters)

            # Wait for all queries to complete
            try:
                df1 = future1.result()
                df2 = future2.result()
                df3 = future3.result()
                df4 = future4.result()
                df5 = future5.result()
                df6 = future6.result()
            except Exception as e:
                st.error(f"Error loading data: {str(e)}")
                return

    st.success("✅ Charts 1-6 loaded successfully!")
    st.markdown("---")

    # Chart 1: KPI Compare
    st.header("📈 Chart 1: KPI Compare by Payment Platform")

    try:
        # Show formatted table
        formatted_df1 = chart1_kpi_compare.create_visualization(df1)
        st.dataframe(formatted_df1, use_container_width=True, hide_index=True)

        # Show revenue comparison chart
        fig1 = chart1_kpi_compare.create_chart(df1)
        st.plotly_chart(fig1, use_container_width=True, key="chart1_kpi_compare")

        with st.expander("📥 Download Chart 1 Data"):
            csv1 = df1.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="Download CSV",
                data=csv1,
                file_name=f"chart1_kpi_compare_{filters['start_date']}_to_{filters['end_date']}.csv",
                mime="text/csv"
            )

        # Metrics Definitions
        st.info("📖 **Metrics Definitions:**")
        st.markdown("""
        - **Active Users**: Total distinct users with any event (platform-specific based on OS)
        - **Total Purchases**: Count of successful purchase transactions (excluding invalid order numbers)
        - **Gross Revenue**: Total revenue before platform fees
        - **Net Revenue**: Revenue after platform fees (30% for Apple/GooglePlay, 0% for Stash)
        - **Paying Users (PPU)**: Total distinct users who completed at least one purchase
        - **PPU%**: Percentage of active users who are paying users (Paying Users / Active Users × 100)
        - **ARPDAU**: Average Revenue Per Daily Active User (Gross Revenue / Active Users)
        - **ARPPU**: Average Revenue Per Paying User (Gross Revenue / Paying Users)
        - **ATV**: Average Transaction Volume - average revenue per purchase (Gross Revenue / Total Purchases)
        - **Purchase Click to PP Continue Rate**: Percentage of purchase clicks that continued to each payment platform (Continue Clicks / Purchase Clicks × 100)
        - **PP Continue to Purchase Success Rate**: Percentage of continues that completed purchases for each payment platform - only counting purchases from funnels where user clicked continue (Purchases from Continue Funnels / Continue Clicks × 100)
        - **Interrupted Purchases**: Count of purchases marked as interrupted
        - **Interrupted Rate**: Percentage of purchases that were interrupted

        **Note:** Apple purchases require valid `purchase_id`, GooglePlay purchases require valid `google_order_number`
        """)
    except Exception as e:
        st.error(f"Error loading Chart 1: {str(e)}")

    st.markdown("---")

    # Chart 2: Distinct Users Funnel
    st.header("📊 Chart 2: Distinct Users Funnel")

    try:
        fig2 = chart2_user_funnel.create_visualization(df2)
        st.plotly_chart(fig2, use_container_width=True, key="chart2_user_funnel")

        with st.expander("📥 Download Chart 2 Data"):
            st.dataframe(df2)
            csv2 = df2.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="Download CSV",
                data=csv2,
                file_name=f"chart2_user_funnel_{filters['start_date']}_to_{filters['end_date']}.csv",
                mime="text/csv"
            )
    except Exception as e:
        st.error(f"Error loading Chart 2: {str(e)}")

    st.markdown("---")

    # Chart 3: Distinct Users Funnel (Percentage)
    st.header("📊 Chart 3: Distinct Users Funnel (% from Purchase Click)")

    try:
        fig3 = chart3_user_funnel_percentage.create_visualization(df3)
        st.plotly_chart(fig3, use_container_width=True, key="chart3_user_funnel_percentage")

        with st.expander("📥 Download Chart 3 Data"):
            st.dataframe(df3)
            csv3 = df3.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="Download CSV",
                data=csv3,
                file_name=f"chart3_user_funnel_percentage_{filters['start_date']}_to_{filters['end_date']}.csv",
                mime="text/csv"
            )
    except Exception as e:
        st.error(f"Error loading Chart 3: {str(e)}")

    st.markdown("---")

    # Chart 4: Total Funnel Executions
    st.header("🔄 Chart 4: Total Funnel Executions")

    try:
        fig4 = chart4_execution_funnel.create_visualization(df4)
        st.plotly_chart(fig4, use_container_width=True, key="chart4_execution_funnel")

        with st.expander("📥 Download Chart 4 Data"):
            st.dataframe(df4)
            csv4 = df4.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="Download CSV",
                data=csv4,
                file_name=f"chart4_execution_funnel_{filters['start_date']}_to_{filters['end_date']}.csv",
                mime="text/csv"
            )
    except Exception as e:
        st.error(f"Error loading Chart 4: {str(e)}")

    st.markdown("---")

    # Chart 5: Total Funnel Executions (Percentage)
    st.header("🔄 Chart 5: Total Funnel Executions (% from Purchase Click)")

    try:
        fig5 = chart5_execution_funnel_percentage.create_visualization(df5)
        st.plotly_chart(fig5, use_container_width=True, key="chart5_execution_funnel_percentage")

        with st.expander("📥 Download Chart 5 Data"):
            st.dataframe(df5)
            csv5 = df5.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="Download CSV",
                data=csv5,
                file_name=f"chart5_execution_funnel_percentage_{filters['start_date']}_to_{filters['end_date']}.csv",
                mime="text/csv"
            )
    except Exception as e:
        st.error(f"Error loading Chart 5: {str(e)}")

    st.markdown("---")

    # Chart 6: Stash Adoption Over Time
    st.header("📅 Chart 6: Stash Adoption Over Time")

    try:
        fig6 = chart6_adoption_over_time.create_visualization(df6)
        st.plotly_chart(fig6, use_container_width=True, key="chart6_adoption_over_time")

        with st.expander("📥 Download Chart 6 Data"):
            st.dataframe(df6)
            csv6 = df6.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="Download CSV",
                data=csv6,
                file_name=f"chart6_adoption_over_time_{filters['start_date']}_to_{filters['end_date']}.csv",
                mime="text/csv"
            )
    except Exception as e:
        st.error(f"Error loading Chart 6: {str(e)}")

    st.markdown("---")

    # Chart 7: Stash Funnel Latency
    st.header("⏱️ Chart 7: Stash Funnel Latency")

    # Chart 7-specific filter (local to this chart only)
    exclude_first_purchase_chart7 = st.checkbox(
        "Exclude First Ever Stash Purchase",
        value=False,
        key="chart7_exclude_first_purchase",
        help="Exclude each user's first Stash purchase funnel to focus on experienced user latency"
    )

    try:
        # Create a copy of filters with the Chart 7-specific setting
        filters_chart7 = filters.copy()
        filters_chart7['exclude_first_purchase'] = exclude_first_purchase_chart7

        # Fetch Chart 7 data with its own filters
        with st.spinner("⚡ Loading Chart 7 data..."):
            df7 = chart7_latency.get_data(filters_chart7)

        if not df7.empty:
            fig7 = chart7_latency.create_visualization(df7)
            st.plotly_chart(fig7, use_container_width=True, key="chart7_latency")

            with st.expander("📥 Download Chart 7 Data"):
                st.dataframe(df7)
                csv7 = df7.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="Download CSV",
                    data=csv7,
                    file_name=f"chart7_latency_{filters['start_date']}_to_{filters['end_date']}.csv",
                    mime="text/csv"
                )
        else:
            st.info("No data available for the selected filters.")
    except Exception as e:
        st.error(f"Error loading Chart 7: {str(e)}")


def main():
    """Main application function."""

    # Page configuration
    st.set_page_config(
        page_title="Stash Analytics Dashboard",
        page_icon="💰",
        layout="wide",
        initial_sidebar_state="expanded"
    )

    # Authentication check
    if is_oauth_configured():
        user = authenticate_user()
        if not user:
            return
        show_user_sidebar()

    # Main title
    st.title("💰 Stash Analytics Dashboard")

    # Tab selection in sidebar
    st.sidebar.markdown("### 📑 Select Dashboard")
    current_tab = st.sidebar.radio(
        "Dashboard",
        options=["business_analytics", "d2c_test_funnel", "stash_analytics"],
        format_func=lambda x: {
            "business_analytics": "📊 Business Analytics",
            "d2c_test_funnel": "🧪 D2C Test Funnel",
            "stash_analytics": "📈 Stash Analytics"
        }.get(x, x),
        label_visibility="collapsed"
    )

    st.sidebar.markdown("---")

    # Render filters based on current tab
    filters = render_filters(tab=current_tab)

    st.markdown("---")

    # Render content based on selected tab
    if current_tab == "business_analytics":
        render_business_analytics_tab(filters)
    elif current_tab == "d2c_test_funnel":
        render_d2c_test_funnel_tab(filters)
    else:
        render_stash_analytics_tab(filters)

    # Footer
    st.markdown("---")
    st.caption(f"Dashboard last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    st.caption("Data refreshes every 120 minutes (cached)")


if __name__ == "__main__":
    main()
