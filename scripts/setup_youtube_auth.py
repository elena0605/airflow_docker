from google_auth_oauthlib.flow import Flow
import pickle
import os
import json
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Disable OAuthlib's HTTPS verification when running locally.
os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'

def setup_oauth():
    # Define OAuth 2.0 scopes
    SCOPES = ['https://www.googleapis.com/auth/youtube.force-ssl', 'https://www.googleapis.com/auth/youtube.readonly']
    
    # Paths
    client_secrets_file = "config/credentials/client_secrets.json"
    token_path = "config/credentials/token.pickle"

    # Create config directory if it doesn't exist
    os.makedirs(os.path.dirname(token_path), exist_ok=True)
    
    try:
        flow = Flow.from_client_secrets_file(
            client_secrets_file,
            scopes=SCOPES,
            redirect_uri='http://localhost:8080/oauth2callback'
        )
        
        # Generate authorization URL
        auth_url, _ = flow.authorization_url(
            access_type='offline',
            include_granted_scopes='true',
            prompt='consent'
        )
        
        print("\nPlease follow these steps:")
        print("1. Go to this URL in your browser:")
        print(auth_url)
        print("\n2. After authorization, you'll see an error page (Airflow 404 Page cannot be found) - THIS IS EXPECTED!")
        print("3. Copy the FULL URL from your browser's address bar.")
    
        
        # Get authorization response from user
        redirect_url = input("\nEnter the redirect URL: ").strip()
        
        # Fetch tokens
        flow.fetch_token(authorization_response=redirect_url)
        
        # Save credentials
        with open(token_path, 'wb') as token:
            pickle.dump(flow.credentials, token)
        
        print("\nAuthentication successful!")
        print(f"Credentials saved to: {token_path}")

         # Print token expiration info
        creds = flow.credentials

        print("\nToken Information:")
        print(f"Access Token Expiry: {creds.expiry}")
        print(f"Has Refresh Token: {'Yes' if creds.refresh_token else 'No'}")
        
    except Exception as e:
        print(f"\nError during authentication: {e}")
        raise

if __name__ == "__main__":
    setup_oauth()