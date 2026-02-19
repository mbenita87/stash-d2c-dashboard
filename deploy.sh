#!/bin/bash
# =============================================================================
# STREAMLIT DASHBOARD DEPLOY SCRIPT
# =============================================================================
# Copy this file to your dashboard folder and change DASHBOARD_NAME below.
# Then run: chmod +x deploy.sh && ./deploy.sh
# =============================================================================

set -e

# ╔═══════════════════════════════════════════════════════════════════════════╗
# ║  CHANGE THIS TO YOUR DASHBOARD NAME (lowercase, hyphens only)             ║
# ╚═══════════════════════════════════════════════════════════════════════════╝
DASHBOARD_NAME="stash-dashboard"

# ═══════════════════════════════════════════════════════════════════════════
# DO NOT MODIFY BELOW THIS LINE
# ═══════════════════════════════════════════════════════════════════════════
REGION="us-central1"
PROJECT="yotam-395120"

echo ""
echo "🚀 Deploying: $DASHBOARD_NAME"
echo ""

# Check gcloud
if ! command -v gcloud &> /dev/null; then
    echo "❌ gcloud CLI not found. Install: https://cloud.google.com/sdk/docs/install"
    exit 1
fi

# Check login
ACCOUNT=$(gcloud auth list --filter=status:ACTIVE --format="value(account)" 2>/dev/null || true)
if [ -z "$ACCOUNT" ]; then
    echo "❌ Not logged in. Run: gcloud auth login"
    exit 1
fi

echo "👤 Deploying as: $ACCOUNT"

# Get current URL if service exists
CURRENT_URL=$(gcloud run services describe $DASHBOARD_NAME --region=$REGION --project=$PROJECT --format="value(status.url)" 2>/dev/null || echo "")
IS_FIRST_DEPLOY=false

if [ -z "$CURRENT_URL" ]; then
    IS_FIRST_DEPLOY=true
    echo "📦 First deployment (this takes 2-3 minutes)..."
    
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
    
    echo "🔧 Setting redirect URI..."
    gcloud run services update $DASHBOARD_NAME \
      --region $REGION \
      --project $PROJECT \
      --update-env-vars "STREAMLIT_REDIRECT_URI=${SERVICE_URL}/" \
      --quiet
else
    SERVICE_URL=$CURRENT_URL
    echo "📦 Updating deployment..."
    
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

echo ""
echo "════════════════════════════════════════════════════════════════════════════"
echo "✅ DEPLOYMENT SUCCESSFUL!"
echo "════════════════════════════════════════════════════════════════════════════"
echo ""
echo "📍 Your Dashboard URL:"
echo ""
echo "   $SERVICE_URL"
echo ""

if [ "$IS_FIRST_DEPLOY" = true ]; then
    echo "════════════════════════════════════════════════════════════════════════════"
    echo "⚠️  ACTION REQUIRED: Configure Redirect URI"
    echo "════════════════════════════════════════════════════════════════════════════"
    echo ""
    echo "   Login will NOT work until you complete this step!"
    echo ""
    echo "   ┌─────────────────────────────────────────────────────────────────────┐"
    echo "   │  REDIRECT URI TO ADD (copy this exactly, including the slash):     │"
    echo "   │                                                                     │"
    echo "   │  ${SERVICE_URL}/                                                    │"
    echo "   └─────────────────────────────────────────────────────────────────────┘"
    echo ""
    echo "   HOW TO ADD IT:"
    echo ""
    echo "   1. Open this link in your browser:"
    echo "      https://console.cloud.google.com/apis/credentials?project=yotam-395120"
    echo ""
    echo "   2. Click on the OAuth 2.0 Client ID (click the name, not download icon)"
    echo ""
    echo "   3. Scroll to 'Authorized redirect URIs'"
    echo ""
    echo "   4. Click '+ ADD URI'"
    echo ""
    echo "   5. Paste: ${SERVICE_URL}/"
    echo ""
    echo "   6. Click 'SAVE'"
    echo ""
    echo "   7. Wait 1-2 minutes, then test your dashboard"
    echo ""
    echo "════════════════════════════════════════════════════════════════════════════"
else
    echo "   (Redirect URI already configured from previous deployment)"
    echo ""
fi

echo "🔄 To update after changes: ./deploy.sh"
echo ""
