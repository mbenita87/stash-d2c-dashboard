# OAuth Configuration for Stash Dashboard

## Current Configuration

**OAuth Client ID:** `57935720907-vlsiuorpvmnql8h0g6qn8rsosem8bfv1.apps.googleusercontent.com`

**Cloud Run URL:** `https://stash-dashboard-aqglgkkvdq-uc.a.run.app`

## Required Redirect URI

Add this to your OAuth client's "Authorized redirect URIs":

```
https://stash-dashboard-aqglgkkvdq-uc.a.run.app/
```

⚠️ **Important:** The trailing slash `/` is required!

## Quick Configuration

### Method 1: Direct Link (Fastest)

1. Open: https://console.cloud.google.com/apis/credentials/oauthclient/57935720907-vlsiuorpvmnql8h0g6qn8rsosem8bfv1?project=yotam-395120

2. Scroll to "Authorized redirect URIs"

3. Click "+ ADD URI"

4. Paste: `https://stash-dashboard-aqglgkkvdq-uc.a.run.app/`

5. Click "SAVE"

6. Wait 1-2 minutes for propagation

### Method 2: Via Console Navigation

1. Go to: https://console.cloud.google.com/apis/credentials?project=yotam-395120

2. Find OAuth client starting with: `57935720907-vlsiuorpvmnql8h0g6qn8rsosem8bfv1`

3. Click the pencil icon (✏️) to edit

4. Add redirect URI as above

## Why Can't This Be Automated?

Google does not provide `gcloud` CLI commands or APIs for managing OAuth client redirect URIs without additional OAuth setup (chicken-and-egg problem). This is by design for security reasons.

## Troubleshooting

### Still getting "redirect_uri_mismatch"?

1. Verify the URI has a trailing slash: `/`
2. Check for typos in the URL
3. Wait 2-3 minutes after saving
4. Clear browser cache and try again
5. Verify you're using the correct OAuth client ID

### Check Current Cloud Run URL

```bash
gcloud run services describe stash-dashboard --region=us-central1 --format="value(status.url)"
```

### Check OAuth Client ID

```bash
gcloud secrets versions access latest --secret="STREAMLIT_OAUTH_CLIENT_ID" --project=yotam-395120
```

## For Future Deployments

When deploying to a new URL, update the redirect URI in the OAuth client configuration:

1. Get new Cloud Run URL: `gcloud run services describe stash-dashboard --region=us-central1 --format="value(status.url)"`
2. Add new URL with trailing `/` to OAuth client
3. Keep old URLs if you want them to remain accessible
