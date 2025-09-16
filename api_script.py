"""
API Script for DataHive warehouse integration.
Provides functionality to request datasets from DataHive API with Snowflake backend.
"""

import json
import logging
import time
from typing import Dict, List, Optional, Any
import requests
from datetime import datetime

from token_service import TokenService


class DataHiveAPI:
    """Main API client for DataHive data warehouse operations."""
    
    def __init__(self, config_path: str = "config.json"):
        """Initialize the DataHive API client."""
        self.config = self._load_config(config_path)
        self.datahive_config = self.config.get("datahive", {})
        self.snowflake_config = self.config.get("snowflake", {})
        self.logger = self._setup_logging()
        
        # Initialize token service for authentication
        self.token_service = TokenService(config_path)
        
        # API configuration
        self.base_url = self.datahive_config.get("base_url", "")
        self.api_version = self.datahive_config.get("api_version", "v1")
        self.timeout = self.datahive_config.get("timeout", 30)
        self.max_retries = self.datahive_config.get("max_retries", 3)
        
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
    
    def _get_headers(self) -> Dict[str, str]:
        """Get HTTP headers including authentication token."""
        try:
            access_token = self.token_service.get_access_token()
            return {
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
                "Accept": "application/json",
                "User-Agent": "DataHive-API-Client/1.0"
            }
        except Exception as e:
            self.logger.error(f"Failed to get authentication headers: {e}")
            return {
                "Content-Type": "application/json",
                "Accept": "application/json",
                "User-Agent": "DataHive-API-Client/1.0"
            }
    
    def _make_request(self, method: str, endpoint: str, **kwargs) -> requests.Response:
        """Make HTTP request with retry logic and error handling."""
        url = f"{self.base_url}/{self.api_version}/{endpoint.lstrip('/')}"
        headers = self._get_headers()
        
        # Merge with any additional headers
        if 'headers' in kwargs:
            headers.update(kwargs.pop('headers'))
        
        for attempt in range(self.max_retries + 1):
            try:
                self.logger.debug(f"Making {method} request to {url} (attempt {attempt + 1})")
                
                response = requests.request(
                    method=method,
                    url=url,
                    headers=headers,
                    timeout=self.timeout,
                    **kwargs
                )
                
                # Handle authentication errors by refreshing token
                if response.status_code == 401 and attempt < self.max_retries:
                    self.logger.warning("Authentication failed, refreshing token")
                    self.token_service.refresh_access_token()
                    headers = self._get_headers()
                    continue
                
                response.raise_for_status()
                return response
                
            except requests.RequestException as e:
                self.logger.warning(f"Request attempt {attempt + 1} failed: {e}")
                if attempt == self.max_retries:
                    raise
                time.sleep(2 ** attempt)  # Exponential backoff
        
        raise Exception("Max retries exceeded")
    
    def get_datasets(self, filters: Optional[Dict] = None) -> List[Dict]:
        """Get list of available datasets from DataHive."""
        self.logger.info("Fetching available datasets")
        
        params = {}
        if filters:
            params.update(filters)
        
        try:
            response = self._make_request("GET", "datasets", params=params)
            datasets = response.json()
            
            self.logger.info(f"Retrieved {len(datasets)} datasets")
            return datasets
            
        except Exception as e:
            self.logger.error(f"Failed to fetch datasets: {e}")
            raise Exception(f"Dataset retrieval failed: {e}")
    
    def get_dataset_info(self, dataset_id: str) -> Dict:
        """Get detailed information about a specific dataset."""
        self.logger.info(f"Fetching info for dataset: {dataset_id}")
        
        try:
            response = self._make_request("GET", f"datasets/{dataset_id}")
            dataset_info = response.json()
            
            self.logger.info(f"Retrieved info for dataset: {dataset_id}")
            return dataset_info
            
        except Exception as e:
            self.logger.error(f"Failed to fetch dataset info for {dataset_id}: {e}")
            raise Exception(f"Dataset info retrieval failed: {e}")
    
    def request_dataset(self, dataset_id: str, query_params: Optional[Dict] = None) -> Dict:
        """Request data from a specific dataset."""
        self.logger.info(f"Requesting data from dataset: {dataset_id}")
        
        payload = {
            "dataset_id": dataset_id,
            "timestamp": datetime.now().isoformat(),
            "query_params": query_params or {}
        }
        
        try:
            response = self._make_request("POST", "datasets/request", json=payload)
            result = response.json()
            
            self.logger.info(f"Successfully requested data from dataset: {dataset_id}")
            return result
            
        except Exception as e:
            self.logger.error(f"Failed to request dataset {dataset_id}: {e}")
            raise Exception(f"Dataset request failed: {e}")
    
    def execute_query(self, query: str, parameters: Optional[Dict] = None) -> Dict:
        """Execute a custom SQL query against the Snowflake warehouse."""
        self.logger.info("Executing custom query")
        
        payload = {
            "query": query,
            "parameters": parameters or {},
            "warehouse": self.snowflake_config.get("warehouse"),
            "database": self.snowflake_config.get("database"),
            "schema": self.snowflake_config.get("schema"),
            "timestamp": datetime.now().isoformat()
        }
        
        try:
            response = self._make_request("POST", "query/execute", json=payload)
            result = response.json()
            
            self.logger.info("Successfully executed custom query")
            return result
            
        except Exception as e:
            self.logger.error(f"Failed to execute query: {e}")
            raise Exception(f"Query execution failed: {e}")
    
    def get_query_status(self, query_id: str) -> Dict:
        """Get status of a previously executed query."""
        self.logger.info(f"Checking status for query: {query_id}")
        
        try:
            response = self._make_request("GET", f"query/{query_id}/status")
            status = response.json()
            
            self.logger.info(f"Retrieved status for query: {query_id}")
            return status
            
        except Exception as e:
            self.logger.error(f"Failed to get query status for {query_id}: {e}")
            raise Exception(f"Query status retrieval failed: {e}")
    
    def download_results(self, query_id: str, format: str = "json") -> Any:
        """Download query results in specified format."""
        self.logger.info(f"Downloading results for query: {query_id} in format: {format}")
        
        params = {"format": format}
        
        try:
            response = self._make_request("GET", f"query/{query_id}/results", params=params)
            
            if format.lower() == "json":
                return response.json()
            else:
                return response.content
            
        except Exception as e:
            self.logger.error(f"Failed to download results for {query_id}: {e}")
            raise Exception(f"Results download failed: {e}")
    
    def get_connection_status(self) -> Dict:
        """Check the connection status to DataHive and Snowflake."""
        self.logger.info("Checking connection status")
        
        try:
            response = self._make_request("GET", "health/status")
            status = response.json()
            
            self.logger.info("Successfully retrieved connection status")
            return status
            
        except Exception as e:
            self.logger.error(f"Failed to get connection status: {e}")
            raise Exception(f"Connection status check failed: {e}")


class DataHiveClient:
    """High-level client wrapper for common DataHive operations."""
    
    def __init__(self, config_path: str = "config.json"):
        """Initialize the DataHive client."""
        self.api = DataHiveAPI(config_path)
        self.logger = logging.getLogger(__name__)
    
    def list_available_datasets(self, category: Optional[str] = None) -> List[str]:
        """Get a simple list of available dataset names."""
        filters = {"category": category} if category else None
        datasets = self.api.get_datasets(filters)
        return [dataset.get("name", dataset.get("id", "Unknown")) for dataset in datasets]
    
    def fetch_dataset(self, dataset_name: str, limit: Optional[int] = None) -> Dict:
        """Fetch data from a dataset with optional row limit."""
        query_params = {"limit": limit} if limit else None
        return self.api.request_dataset(dataset_name, query_params)
    
    def run_sql_query(self, sql: str, async_execution: bool = False) -> Dict:
        """Run a SQL query with optional asynchronous execution."""
        result = self.api.execute_query(sql)
        
        if async_execution and "query_id" in result:
            # For async queries, return query ID for status checking
            return {"query_id": result["query_id"], "status": "submitted"}
        
        return result
    
    def wait_for_query_completion(self, query_id: str, timeout: int = 300) -> Dict:
        """Wait for an asynchronous query to complete."""
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            status = self.api.get_query_status(query_id)
            
            if status.get("state") in ["COMPLETED", "SUCCEEDED"]:
                return self.api.download_results(query_id)
            elif status.get("state") in ["FAILED", "CANCELLED"]:
                raise Exception(f"Query failed: {status.get('error', 'Unknown error')}")
            
            time.sleep(5)  # Poll every 5 seconds
        
        raise TimeoutError(f"Query {query_id} did not complete within {timeout} seconds")


# Example usage and testing functions
def main():
    """Example usage of the DataHive API client."""
    try:
        # Initialize the client
        client = DataHiveClient()
        
        print("DataHive API Client - Example Usage")
        print("=" * 40)
        
        # Check connection status
        print("Checking connection status...")
        try:
            status = client.api.get_connection_status()
            print(f"Connection status: {json.dumps(status, indent=2)}")
        except Exception as e:
            print(f"Connection check failed: {e}")
        
        # List available datasets
        print("\nListing available datasets...")
        try:
            datasets = client.list_available_datasets()
            print(f"Available datasets: {datasets}")
        except Exception as e:
            print(f"Failed to list datasets: {e}")
        
        print("\nNote: This is a placeholder implementation.")
        print("Configure datahive settings in config.json for actual API calls.")
        
    except Exception as e:
        print(f"Error initializing client: {e}")


if __name__ == "__main__":
    main()