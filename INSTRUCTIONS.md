# 🚀 Dashboard Deployment Instructions

Deploy your Streamlit dashboard to Cloud Run with Google login.

---

## Quick Overview

| Step | Action | Time |
|------|--------|------|
| 1 | Copy template files | 1 min |
| 2 | Update deploy.sh | 30 sec |
| 3 | Add auth dependencies | 30 sec |
| 4 | Add auth code to app.py | 2 min |
| 5 | Deploy | 2-3 min |
| 6 | **Configure redirect URI** | 1 min |

---

## Prerequisites

- Your dashboard code (`app.py`)
- `requirements.txt` with your dependencies
- `gcloud` CLI installed
- Logged in: `gcloud auth login`

---

## Step 1: Copy Template Files

Copy these files to your dashboard folder:

```bash
cp /Users/yotam/dashboard-template/Dockerfile /path/to/your-dashboard/
cp /Users/yotam/dashboard-template/deploy.sh /path/to/your-dashboard/
```

---

## Step 2: Update deploy.sh

Open `deploy.sh` and change **line 14**:

```bash
DASHBOARD_NAME="your-dashboard-name"
```

**Rules for dashboard name:**
- Lowercase only
- Use hyphens (not underscores)
- Examples: `sales-dashboard`, `user-analytics`, `revenue-tracker`

---

## Step 3: Add Auth Dependencies

Add these lines to your `requirements.txt`:

```
google-auth>=2.23.0
google-auth-oauthlib>=1.0.0
requests>=2.31.0
```

---

## Step 4: Add Auth Code to app.py

1. Open `/Users/yotam/dashboard-template/auth_code.py`
2. Copy the **entire file contents**
3. Paste at the **TOP** of your `app.py` (after your imports)

Then modify your `main()` function:

```python
def main():
    # ═══ ADD THESE 5 LINES AT THE START ═══
    if is_oauth_configured():
        user = authenticate_user()
        if not user:
            return
        show_user_sidebar()
    # ═══ END OF AUTH CODE ═══
    
    # Your existing dashboard code below...
    st.title("My Dashboard")
```

---

## Step 5: Deploy

```bash
cd /path/to/your-dashboard
chmod +x deploy.sh
./deploy.sh
```

Wait 2-3 minutes. The script will output your dashboard URL.

---

## Step 6: Configure Redirect URI ⚠️ REQUIRED

> **This step is required for login to work.** Without it, users will see an error after trying to sign in.

After deployment, you'll see output like this:

```
✅ DEPLOYED SUCCESSFULLY!

📍 Dashboard URL:
   https://sales-dashboard-aqglgkkvdq-uc.a.run.app

⚠️  FIRST TIME SETUP REQUIRED:
   Add this redirect URI to Google Cloud Console:
   https://sales-dashboard-aqglgkkvdq-uc.a.run.app/
```

### How to Add the Redirect URI:

**1. Open Google Cloud Console Credentials page:**

   👉 https://console.cloud.google.com/apis/credentials?project=yotam-395120

**2. Find and click the OAuth 2.0 Client ID:**
   
   Look for a row in the "OAuth 2.0 Client IDs" section. Click on the name (not the download button).

**3. Scroll down to "Authorized redirect URIs"**

**4. Click "+ ADD URI"**

**5. Paste your dashboard URL** (with trailing slash!)
   
   ```
   https://your-dashboard-name-aqglgkkvdq-uc.a.run.app/
   ```
   
   ⚠️ **Important:** Include the `/` at the end!

**6. Click "SAVE" at the bottom**

**7. Wait 1-2 minutes** for changes to propagate

**8. Test your dashboard** - login should now work!

---

## ✅ Done!

Your dashboard is live. Share the URL with your team.

They'll:
1. Go to the URL
2. Click "Sign in with Google"
3. Log in with their @peerplay.com account
4. See your dashboard

---

## Updating Your Dashboard

After making code changes:

```bash
./deploy.sh
```

No need to add redirect URI again - it's already configured.

---

## Troubleshooting

| Problem | Cause | Solution |
|---------|-------|----------|
| **"redirect_uri_mismatch" error** | Redirect URI not added | Do Step 6 above |
| **"redirect_uri_mismatch" error** | Missing trailing slash | Make sure URI ends with `/` |
| **"Access Denied" after login** | Email not @peerplay.com | Use your @peerplay.com account |
| **Blank page after login** | Browser cached old state | Clear cookies or use incognito |
| **"Error 403" on dashboard** | Redirect URI typo | Check URL matches exactly |
| **Works on 2nd try but not 1st** | Propagation delay | Wait 1-2 minutes after adding URI |

---

## Files Checklist

Your dashboard folder should have:

```
your-dashboard/
├── app.py              ✅ Your code + auth code from template
├── requirements.txt    ✅ Your deps + auth deps  
├── Dockerfile          ✅ Copied from template (unchanged)
└── deploy.sh           ✅ Copied + changed DASHBOARD_NAME
```

---

## Reference Links

| Resource | URL |
|----------|-----|
| OAuth Credentials | https://console.cloud.google.com/apis/credentials?project=yotam-395120 |
| Cloud Run Console | https://console.cloud.google.com/run?project=yotam-395120 |
| Template Folder | `/Users/yotam/dashboard-template/` |
