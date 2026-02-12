# =============================================================================
# GOOGLE OAUTH AUTHENTICATION FOR STREAMLIT DASHBOARDS
# =============================================================================
# 
# HOW TO USE:
# 1. Copy this entire file's content
# 2. Paste at the TOP of your app.py (after your imports)
# 3. In your main() function, add the authentication check (see bottom of file)
#
# This code contains NO secrets - they are loaded from environment variables
# that Cloud Run injects from GCP Secret Manager.
# =============================================================================

import streamlit as st
import os
import requests
from google_auth_oauthlib.flow import Flow

# =============================================================================
# CONFIGURATION
# =============================================================================

# Email domains allowed to access (add more if needed)
ALLOWED_DOMAINS = ['peerplay.com', 'peerplay.io']

# =============================================================================
# AUTHENTICATION FUNCTIONS (do not modify)
# =============================================================================

def get_secret(key):
    """Get secret from Streamlit secrets or environment variables.
    Cloud Run injects secrets from Secret Manager as env vars.
    """
    try:
        if hasattr(st, 'secrets') and key in st.secrets:
            return st.secrets[key]
    except:
        pass
    return os.environ.get(key)


def check_authorization(email):
    """Check if user's email domain is allowed."""
    if not email:
        return False
    domain = email.split('@')[-1].lower() if '@' in email else ''
    return domain in [d.lower() for d in ALLOWED_DOMAINS]


def get_google_oauth_url():
    """Generate Google OAuth authorization URL."""
    client_id = get_secret('GOOGLE_OAUTH_CLIENT_ID')
    client_secret = get_secret('GOOGLE_OAUTH_CLIENT_SECRET')
    redirect_uri = get_secret('STREAMLIT_REDIRECT_URI') or "http://localhost:8501/"
    
    if not client_id or not client_secret:
        return None
    
    flow = Flow.from_client_config(
        {
            "web": {
                "client_id": client_id,
                "client_secret": client_secret,
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
                "redirect_uris": [redirect_uri]
            }
        },
        scopes=[
            "openid",
            "https://www.googleapis.com/auth/userinfo.email",
            "https://www.googleapis.com/auth/userinfo.profile"
        ],
        redirect_uri=redirect_uri
    )
    
    authorization_url, _ = flow.authorization_url(
        access_type='offline',
        prompt='consent'
    )
    return authorization_url


def authenticate_user():
    """Handle the full OAuth authentication flow.
    Returns user email if authenticated, None otherwise.
    """
    # Check if already authenticated
    if st.session_state.get('authenticated'):
        return st.session_state.get('user_email')
    
    # Check for OAuth callback (user returning from Google login)
    code = st.query_params.get('code')
    
    if code:
        try:
            client_id = get_secret('GOOGLE_OAUTH_CLIENT_ID')
            client_secret = get_secret('GOOGLE_OAUTH_CLIENT_SECRET')
            redirect_uri = get_secret('STREAMLIT_REDIRECT_URI') or "http://localhost:8501/"
            
            flow = Flow.from_client_config(
                {
                    "web": {
                        "client_id": client_id,
                        "client_secret": client_secret,
                        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                        "token_uri": "https://oauth2.googleapis.com/token",
                        "redirect_uris": [redirect_uri]
                    }
                },
                scopes=[
                    "openid",
                    "https://www.googleapis.com/auth/userinfo.email",
                    "https://www.googleapis.com/auth/userinfo.profile"
                ],
                redirect_uri=redirect_uri
            )
            
            # Exchange code for token
            flow.fetch_token(code=code)
            
            # Get user info from Google
            user_info = requests.get(
                'https://www.googleapis.com/oauth2/v2/userinfo',
                headers={'Authorization': f'Bearer {flow.credentials.token}'}
            ).json()
            
            user_email = user_info.get('email', '')
            
            # Check if user is authorized
            if check_authorization(user_email):
                st.session_state.authenticated = True
                st.session_state.user_email = user_email
                st.session_state.user_name = user_info.get('name', '')
                st.query_params.clear()
                st.rerun()
            else:
                st.error(f"❌ Access Denied: {user_email}")
                st.info("This dashboard is restricted to Peerplay employees only.")
                st.stop()
                
        except Exception as e:
            st.error(f"Authentication error: {e}")
            st.stop()
    
    # Show login page
    auth_url = get_google_oauth_url()
    if auth_url:
        st.markdown("""
        <style>
            .login-container {
                display: flex;
                flex-direction: column;
                align-items: center;
                justify-content: center;
                min-height: 60vh;
                text-align: center;
            }
            .login-btn {
                background: linear-gradient(135deg, #4285f4 0%, #357abd 100%);
                color: white !important;
                padding: 15px 30px;
                border-radius: 10px;
                text-decoration: none;
                font-weight: bold;
                font-size: 1.1rem;
                display: inline-block;
                margin-top: 20px;
                transition: transform 0.2s, box-shadow 0.2s;
            }
            .login-btn:hover {
                transform: translateY(-2px);
                box-shadow: 0 5px 20px rgba(66, 133, 244, 0.4);
                color: white !important;
            }
        </style>
        """, unsafe_allow_html=True)
        
        st.markdown(f"""
        <div class="login-container">
            <h1>🔒 Login Required</h1>
            <p style="font-size: 1.2rem; color: #666;">
                This dashboard is restricted to <strong>Peerplay employees</strong>.
            </p>
            <a href="{auth_url}" class="login-btn">
                🔐 Sign in with Google
            </a>
        </div>
        """, unsafe_allow_html=True)
        
        st.stop()
    
    # No OAuth configured (local development without secrets)
    return None


def is_oauth_configured():
    """Check if OAuth credentials are available."""
    return get_secret('GOOGLE_OAUTH_CLIENT_ID') is not None


def show_user_sidebar():
    """Show logged-in user info and logout button in sidebar."""
    if st.session_state.get('authenticated'):
        st.sidebar.markdown(f"**👤 {st.session_state.get('user_name', 'User')}**")
        st.sidebar.caption(st.session_state.get('user_email', ''))
        if st.sidebar.button("🚪 Logout"):
            for key in ['authenticated', 'user_email', 'user_name']:
                st.session_state.pop(key, None)
            st.rerun()
        st.sidebar.markdown("---")


# =============================================================================
# HOW TO USE IN YOUR APP
# =============================================================================
# 
# Add this at the START of your main() function:
#
#     def main():
#         # Authentication check
#         if is_oauth_configured():
#             user = authenticate_user()
#             if not user:
#                 return
#             show_user_sidebar()
#         
#         # Your dashboard code starts here
#         st.title("My Dashboard")
#         ...
#
# =============================================================================


