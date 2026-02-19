# Stash Analytics Dashboard

Comprehensive analytics dashboard for monitoring Stash payment integration performance in Merge Cruise.

## Features

- **Chart 1: KPI Compare** - Payment platform comparison (revenue, users, conversion rates)
- **Chart 2: Distinct Users Funnel** - User counts at each funnel step
- **Chart 3: Distinct Users Funnel (Percentage)** - User funnel as percentage from purchase click
- **Chart 4: Total Funnel Executions** - Purchase funnel counts by purchase_funnel_id
- **Chart 5: Total Funnel Executions (Percentage)** - Execution funnel as percentage from purchase click
- **Chart 6: Stash Adoption Over Time** - Daily trend analysis
- **Chart 7: Stash Funnel Latency** - Median time between funnel steps

## Filters

- Date range (default: last 60 days + today)
- Platform (iOS, Android)
- App version
- Countries
- Low payers countries
- Stash test users
- Payment platform (for Chart 3)

## Data Sources

- **Client Events**: `yotam-395120.peerplay.vmp_master_event_normalized`
- **Server Events**: `yotam-395120.peerplay.verification_service_events`
- **Test Users**: `yotam-395120.peerplay.stash_test_users_no_google_sheet`

## Local Development

### Prerequisites

```bash
pip install -r requirements.txt
```

### Run Locally

```bash
# Make sure you're authenticated with gcloud
gcloud auth application-default login

# Run the dashboard
streamlit run app.py
```

The dashboard will be available at `http://localhost:8501`

**Note**: OAuth authentication is optional for local development. If OAuth secrets are not configured, the dashboard will run without authentication checks.

## Deployment to GCP Cloud Run

### First Time Deployment

1. Make sure you're logged in to gcloud:
```bash
gcloud auth login
```

2. Deploy the dashboard:
```bash
./deploy.sh
```

3. After first deployment, configure the OAuth redirect URI:
   - Go to: https://console.cloud.google.com/apis/credentials?project=yotam-395120
   - Click on your OAuth 2.0 Client ID
   - Add the redirect URI (the deploy script will show you the exact URI)
   - Click "Save"
   - Wait 1-2 minutes for changes to propagate

### Subsequent Deployments

Just run:
```bash
./deploy.sh
```

## Authentication

The dashboard uses Google OAuth and restricts access to:
- `@peerplay.com` email addresses
- `@peerplay.io` email addresses

## Architecture

```
stash_dashboard/
├── app.py                          # Main Streamlit app
├── auth_code.py                    # OAuth authentication
├── queries/                        # Chart query modules
│   ├── chart1_kpi_compare.py
│   ├── chart2_user_funnel.py
│   ├── chart3_user_funnel_percentage.py
│   ├── chart4_execution_funnel.py
│   ├── chart5_execution_funnel_percentage.py
│   ├── chart6_adoption_over_time.py
│   └── chart7_latency.py
├── utils/                          # Utility modules
│   ├── bigquery_client.py          # BigQuery connection
│   ├── filters.py                  # Filter UI components
│   └── metrics.py                  # Metric calculations
├── requirements.txt                # Python dependencies
├── Dockerfile                      # Container config
└── deploy.sh                       # Deployment script
```

## Query Optimization

- Queries are cached for 120 minutes using `@st.cache_data(ttl=7200)`
- Date filters applied at the BigQuery level for efficiency
- Maximum query cost limited to 10 GB
- Partitioned table scans where available

## Support

For issues or questions, contact the Data Analytics team.