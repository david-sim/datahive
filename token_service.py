"""
Token Service for Microsoft Azure AD authentication.
Manages access tokens and refresh tokens for DataHive API authentication.
"""

import json
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple
import requests


class TokenService:
    """Service for managing Microsoft Azure AD access and refresh tokens."""
    
    def __init__(self, config_path: str = "config.json"):
        """Initialize the TokenService with configuration."""
        self.config = self._load_config(config_path)
        self.azure_config = self.config.get("azure_ad", {})
        self.cache_config = self.config.get("cache", {})
        self.logger = self._setup_logging()
        
        # Token storage
        self.access_token: Optional[str] = None
        self.refresh_token: Optional[str] = None
        self.token_expires_at: Optional[datetime] = None
        
    def _load_config(self, config_path: str) -> Dict:
        """Load configuration from JSON file."""
        try:
            with open(config_path, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in configuration file: {e}")
    
    def _setup_logging(self) -> logging.Logger:
        """Setup logging configuration."""
        logging_config = self.config.get("logging", {})
        level = getattr(logging, logging_config.get("level", "INFO").upper())
        format_str = logging_config.get("format", "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        
        logging.basicConfig(level=level, format=format_str)
        return logging.getLogger(__name__)
    
    def get_access_token(self) -> str:
        """Get a valid access token, refreshing if necessary."""
        if self._is_token_valid():
            self.logger.debug("Using existing valid access token")
            return self.access_token
        
        self.logger.info("Access token expired or not available, acquiring new token")
        return self._acquire_new_token()
    
    def _is_token_valid(self) -> bool:
        """Check if the current access token is valid and not expired."""
        if not self.access_token or not self.token_expires_at:
            return False
        
        # Add buffer time before expiry
        buffer_seconds = self.cache_config.get("cache_expiry_buffer", 300)
        expiry_with_buffer = self.token_expires_at - timedelta(seconds=buffer_seconds)
        
        return datetime.now() < expiry_with_buffer
    
    def _acquire_new_token(self) -> str:
        """Acquire a new access token from Azure AD."""
        token_url = f"{self.azure_config['authority']}/{self.azure_config['tenant_id']}/oauth2/v2.0/token"
        
        data = {
            'grant_type': 'client_credentials',
            'client_id': self.azure_config['client_id'],
            'client_secret': self.azure_config['client_secret'],
            'scope': self.azure_config['scope']
        }
        
        try:
            self.logger.debug(f"Requesting new token from: {token_url}")
            response = requests.post(token_url, data=data)
            response.raise_for_status()
            
            token_data = response.json()
            self.access_token = token_data['access_token']
            
            # Calculate expiry time
            expires_in = token_data.get('expires_in', 3600)
            self.token_expires_at = datetime.now() + timedelta(seconds=expires_in)
            
            self.logger.info(f"Successfully acquired new access token, expires at: {self.token_expires_at}")
            return self.access_token
            
        except requests.RequestException as e:
            self.logger.error(f"Failed to acquire access token: {e}")
            raise Exception(f"Token acquisition failed: {e}")
        except KeyError as e:
            self.logger.error(f"Unexpected token response format: {e}")
            raise Exception(f"Invalid token response: {e}")
    
    def refresh_access_token(self) -> str:
        """Refresh the access token using refresh token (if available)."""
        if not self.refresh_token:
            self.logger.warning("No refresh token available, acquiring new token")
            return self._acquire_new_token()
        
        # Placeholder for refresh token logic
        # In practice, this would use the refresh_token grant type
        self.logger.info("Refresh token functionality not implemented, acquiring new token")
        return self._acquire_new_token()
    
    def revoke_token(self) -> bool:
        """Revoke the current access token."""
        if not self.access_token:
            self.logger.warning("No access token to revoke")
            return True
        
        try:
            # Placeholder for token revocation logic
            self.logger.info("Token revocation functionality - placeholder")
            self.access_token = None
            self.refresh_token = None
            self.token_expires_at = None
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to revoke token: {e}")
            return False
    
    def get_token_info(self) -> Dict:
        """Get information about the current token status."""
        return {
            "has_access_token": bool(self.access_token),
            "has_refresh_token": bool(self.refresh_token),
            "expires_at": self.token_expires_at.isoformat() if self.token_expires_at else None,
            "is_valid": self._is_token_valid(),
            "expires_in_seconds": (
                int((self.token_expires_at - datetime.now()).total_seconds())
                if self.token_expires_at else None
            )
        }


# Example usage and testing functions
def main():
    """Example usage of the TokenService."""
    try:
        # Initialize token service
        token_service = TokenService()
        
        # Get token info
        info = token_service.get_token_info()
        print(f"Token info: {json.dumps(info, indent=2)}")
        
        # Get access token (this would normally work with proper config)
        print("Note: This is a placeholder implementation.")
        print("Configure azure_ad settings in config.json for actual token acquisition.")
        
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()