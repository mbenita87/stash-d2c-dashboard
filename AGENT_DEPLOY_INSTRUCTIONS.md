# Streamlit Dashboard Deployment - Agent Instructions

> ⚠️ **IMPORTANT:** This is a template repository. Do NOT deploy dashboard code to this repo.
> Copy the template files to the user's dashboard folder/repository for deployment.

This file contains all instructions for deploying a Streamlit dashboard to GCP Cloud Run.
An AI agent can follow these steps. Steps requiring user action are clearly marked.

---

## Overview

| Step | Actor | Description |
|------|-------|-------------|
| 1 | Agent | Create required files |
| 2 | Agent | Add auth code to app.py |
| 3 | Agent | Add auth dependencies |
| 4 | Agent | Run deploy command |
| 5 | **User** | Add redirect URI (manual, one-time) |
| 6 | Agent | Verify deployment |

---

## Step 1: Create Required Files

### 1.1 Create Dockerfile

Create a file named `Dockerfile` in the dashboard folder with this exact content:

```dockerfile
FROM python:3.11-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1
ENV STREAMLIT_SERVER_PORT=8080
ENV STREAMLIT_SERVER_ADDRESS=0.0.0.0
ENV STREAMLIT_SERVER_HEADLESS=true
ENV STREAMLIT_BROWSER_GATHER_USAGE_STATS=false
ENV CLOUD_RUN=true

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD curl --fail http://localhost:8080/_stcore/health || exit 1

CMD ["streamlit", "run", "app.py", "--server.port=8080", "--server.address=0.0.0.0"]
```

### 1.2 Create deploy.sh

Create a file named `deploy.sh` in the dashboard folder.

**Important:** Replace `YOUR_DASHBOARD_NAME` with the actual dashboard name (lowercase, hyphens only).

```bash
#!/bin/bash
set -e

DASHBOARD_NAME="YOUR_DASHBOARD_NAME"
REGION="us-central1"
PROJECT="yotam-395120"

echo "🚀 Deploying: $DASHBOARD_NAME"

CURRENT_URL=$(gcloud run services describe $DASHBOARD_NAME --region=$REGION --project=$PROJECT --format="value(status.url)" 2>/dev/null || echo "")

if [ -z "$CURRENT_URL" ]; then
    gcloud run deploy $DASHBOARD_NAME \
      --source . \
      --region $REGION \
      --project $PROJECT \
      --allow-unauthenticated \
      --memory 1Gi \
      --cpu 1 \
      --timeout 300 \
      --max-instances 3 \
      --set-secrets "GOOGLE_OAUTH_CLIENT_ID=STREAMLIT_OAUTH_CLIENT_ID:latest,GOOGLE_OAUTH_CLIENT_SECRET=STREAMLIT_OAUTH_CLIENT_SECRET:latest" \
      --set-env-vars "CLOUD_RUN=true" \
      --quiet
    
    SERVICE_URL=$(gcloud run services describe $DASHBOARD_NAME --region=$REGION --project=$PROJECT --format="value(status.url)")
    
    gcloud run services update $DASHBOARD_NAME \
      --region $REGION \
      --project $PROJECT \
      --update-env-vars "STREAMLIT_REDIRECT_URI=${SERVICE_URL}/" \
      --quiet
else
    SERVICE_URL=$CURRENT_URL
    
    gcloud run deploy $DASHBOARD_NAME \
      --source . \
      --region $REGION \
      --project $PROJECT \
      --allow-unauthenticated \
      --memory 1Gi \
      --cpu 1 \
      --timeout 300 \
      --max-instances 3 \
      --set-secrets "GOOGLE_OAUTH_CLIENT_ID=STREAMLIT_OAUTH_CLIENT_ID:latest,GOOGLE_OAUTH_CLIENT_SECRET=STREAMLIT_OAUTH_CLIENT_SECRET:latest" \
      --set-env-vars "CLOUD_RUN=true,STREAMLIT_REDIRECT_URI=${SERVICE_URL}/" \
      --quiet
fi

SERVICE_URL=$(gcloud run services describe $DASHBOARD_NAME --region=$REGION --project=$PROJECT --format="value(status.url)")
echo "✅ Deployed: $SERVICE_URL"
```

After creating, make it executable:
```bash
chmod +x deploy.sh
```

---

## Step 2: Add Authentication Code to app.py

Add this code to the **TOP** of the user's `app.py` file (after their imports):

```python
# =============================================================================
# AUTHENTICATION CODE - DO NOT MODIFY
# =============================================================================
import os
import requests
from google_auth_oauthlib.flow import Flow

ALLOWED_DOMAINS = ['peerplay.com', 'peerplay.io']

def get_secret(key):
    try:
        if hasattr(st, 'secrets') and key in st.secrets:
            return st.secrets[key]
    except:
        pass
    return os.environ.get(key)

def check_authorization(email):
    if not email:
        return False
    domain = email.split('@')[-1].lower() if '@' in email else ''
    return domain in [d.lower() for d in ALLOWED_DOMAINS]

def get_google_oauth_url():
    client_id = get_secret('GOOGLE_OAUTH_CLIENT_ID')
    client_secret = get_secret('GOOGLE_OAUTH_CLIENT_SECRET')
    redirect_uri = get_secret('STREAMLIT_REDIRECT_URI') or "http://localhost:8501/"
    if not client_id or not client_secret:
        return None
    flow = Flow.from_client_config(
        {"web": {"client_id": client_id, "client_secret": client_secret,
                 "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                 "token_uri": "https://oauth2.googleapis.com/token",
                 "redirect_uris": [redirect_uri]}},
        scopes=["openid", "https://www.googleapis.com/auth/userinfo.email",
                "https://www.googleapis.com/auth/userinfo.profile"],
        redirect_uri=redirect_uri)
    authorization_url, _ = flow.authorization_url(access_type='offline', prompt='consent')
    return authorization_url

def authenticate_user():
    if st.session_state.get('authenticated'):
        return st.session_state.get('user_email')
    code = st.query_params.get('code')
    if code:
        try:
            client_id = get_secret('GOOGLE_OAUTH_CLIENT_ID')
            client_secret = get_secret('GOOGLE_OAUTH_CLIENT_SECRET')
            redirect_uri = get_secret('STREAMLIT_REDIRECT_URI') or "http://localhost:8501/"
            flow = Flow.from_client_config(
                {"web": {"client_id": client_id, "client_secret": client_secret,
                         "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                         "token_uri": "https://oauth2.googleapis.com/token",
                         "redirect_uris": [redirect_uri]}},
                scopes=["openid", "https://www.googleapis.com/auth/userinfo.email",
                        "https://www.googleapis.com/auth/userinfo.profile"],
                redirect_uri=redirect_uri)
            flow.fetch_token(code=code)
            user_info = requests.get('https://www.googleapis.com/oauth2/v2/userinfo',
                                     headers={'Authorization': f'Bearer {flow.credentials.token}'}).json()
            if check_authorization(user_info.get('email', '')):
                st.session_state.authenticated = True
                st.session_state.user_email = user_info.get('email', '')
                st.session_state.user_name = user_info.get('name', '')
                st.query_params.clear()
                st.rerun()
            else:
                st.error("❌ Access Denied - Peerplay employees only")
                st.stop()
        except Exception as e:
            st.error(f"Auth error: {e}")
            st.stop()
    auth_url = get_google_oauth_url()
    if auth_url:
        st.markdown(f'''<div style="text-align:center;padding:100px;">
            <h1>🔒 Login Required</h1>
            <p>Restricted to Peerplay employees</p><br>
            <a href="{auth_url}" style="background:#4285f4;color:white;padding:15px 30px;
               border-radius:10px;text-decoration:none;font-weight:bold;">Sign in with Google</a>
        </div>''', unsafe_allow_html=True)
        st.stop()
    return None

def is_oauth_configured():
    return get_secret('GOOGLE_OAUTH_CLIENT_ID') is not None

def show_user_sidebar():
    if st.session_state.get('authenticated'):
        st.sidebar.markdown(f"**👤 {st.session_state.get('user_name', 'User')}**")
        if st.sidebar.button("🚪 Logout"):
            st.session_state.clear()
            st.rerun()
        st.sidebar.markdown("---")
# =============================================================================
# END AUTHENTICATION CODE
# =============================================================================
```

Then, add this at the START of the user's `main()` function:

```python
def main():
    # Authentication check - add these 5 lines at the start
    if is_oauth_configured():
        user = authenticate_user()
        if not user:
            return
        show_user_sidebar()
    
    # ... rest of the user's existing code ...
```

---

## Step 3: Add Authentication Dependencies

Add these lines to the user's `requirements.txt`:

```
google-auth>=2.23.0
google-auth-oauthlib>=1.0.0
requests>=2.31.0
```

---

## Step 4: Deploy

Run these commands from the dashboard folder:

```bash
chmod +x deploy.sh
./deploy.sh
```

**Expected output:** The command will output the deployed URL, like:
```
✅ Deployed: https://dashboard-name-aqglgkkvdq-uc.a.run.app
```

**Save this URL** - it's needed for the next step.

---

## Step 5: User Action Required - Add Redirect URI

⚠️ **THIS STEP REQUIRES USER ACTION** ⚠️

The agent cannot complete this step. Ask the user to do it with the following instructions:

---

### MESSAGE TO USER:

```
Your dashboard is deployed! 🎉

Before login will work, you need to do ONE manual configuration step (takes 1 minute):

**Add the redirect URI to Google Cloud Console:**

1. Open this link:
   https://console.cloud.google.com/apis/credentials?project=yotam-395120

2. In the "OAuth 2.0 Client IDs" section, click on the client name (not the download icon)

3. Scroll down to "Authorized redirect URIs"

4. Click "+ ADD URI"

5. Paste this URL (copy exactly, including the trailing slash):
   
   [PASTE_DEPLOYED_URL_HERE]/
   
   Example: https://my-dashboard-aqglgkkvdq-uc.a.run.app/

6. Click "SAVE"

7. Wait 1-2 minutes for changes to take effect

Let me know when you've done this, and I'll verify the deployment is working!
```

**Replace `[PASTE_DEPLOYED_URL_HERE]` with the actual URL from Step 4, and add a trailing slash.**

---

## Step 6: Verify Deployment

After the user confirms they've added the redirect URI:

1. Ask the user to open the dashboard URL in their browser
2. They should see the "Sign in with Google" button
3. After clicking and logging in with a @peerplay.com account, the dashboard should load

**If there are issues:**

| Error | Solution |
|-------|----------|
| "redirect_uri_mismatch" | Redirect URI not added correctly. Must include trailing `/` and match exactly. |
| "Access Denied" after login | User is not using @peerplay.com email |
| Blank page | Clear browser cookies and try again |

---

## Summary Checklist

For the agent to complete:

- [ ] Create `Dockerfile`
- [ ] Create `deploy.sh` (with correct DASHBOARD_NAME)
- [ ] Add auth code to `app.py`
- [ ] Add auth dependencies to `requirements.txt`
- [ ] Run `./deploy.sh`
- [ ] Ask user to add redirect URI (provide clear instructions with the URL)
- [ ] Verify deployment works

---

## Quick Reference

| Item | Value |
|------|-------|
| GCP Project | `yotam-395120` |
| Region | `us-central1` |
| OAuth Credentials URL | https://console.cloud.google.com/apis/credentials?project=yotam-395120 |
| Cloud Run Console | https://console.cloud.google.com/run?project=yotam-395120 |
| Allowed Email Domains | @peerplay.com, @peerplay.io |

