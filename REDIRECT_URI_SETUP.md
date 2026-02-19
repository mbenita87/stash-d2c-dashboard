# 🔐 Redirect URI Setup

> **Do this once after your first deployment. Without this step, login will not work.**

---

## What You Need

After running `./deploy.sh`, you received a URL like:

```
https://your-dashboard-name-aqglgkkvdq-uc.a.run.app/
```

You need to register this URL with Google OAuth.

---

## Step-by-Step Instructions

### 1️⃣ Open Google Cloud Console

Click this link:

👉 **https://console.cloud.google.com/apis/credentials?project=yotam-395120**

(Or copy and paste into your browser)

---

### 2️⃣ Click on the OAuth Client ID

On the credentials page, find the **"OAuth 2.0 Client IDs"** section.

Click on the **name** of the client (something like "Streamlit Dashboards" or "Web client").

**Don't click the download icon** - click the name itself.

---

### 3️⃣ Find "Authorized redirect URIs"

Scroll down the page until you see a section called:

**"Authorized redirect URIs"**

---

### 4️⃣ Add Your Dashboard URL

1. Click the **"+ ADD URI"** button

2. Paste your dashboard URL:
   ```
   https://your-dashboard-name-aqglgkkvdq-uc.a.run.app/
   ```

3. ⚠️ **Make sure to include the trailing slash** `/` at the end!

---

### 5️⃣ Save

Click the blue **"SAVE"** button at the bottom of the page.

---

### 6️⃣ Wait and Test

- Wait **1-2 minutes** for changes to take effect
- Go to your dashboard URL
- Click "Sign in with Google"
- Login should work now!

---

## Common Mistakes

| ❌ Wrong | ✅ Correct |
|----------|-----------|
| `https://my-dashboard-xxx.run.app` | `https://my-dashboard-xxx.run.app/` |
| Missing the trailing `/` | Include the trailing `/` |
| HTTP instead of HTTPS | Always use `https://` |
| Typo in URL | Copy exactly from deploy output |

---

## Still Not Working?

1. **Double-check the URL** - must match exactly (including `https://` and trailing `/`)
2. **Clear browser cookies** - old sessions can cause issues
3. **Try incognito window** - rules out cache issues
4. **Wait 5 minutes** - sometimes Google takes time to propagate

---

## Example

If your deploy output showed:
```
📍 Dashboard URL:
   https://sales-dashboard-aqglgkkvdq-uc.a.run.app
```

Then add this as the redirect URI:
```
https://sales-dashboard-aqglgkkvdq-uc.a.run.app/
```
                                                 ↑
                                          Don't forget this!


