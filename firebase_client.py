"""
Firebase Client Singleton for the Metabolic Organism.
Provides centralized, resilient access to Firestore with comprehensive error handling,
logging, and automatic reconnection. All system state flows through this client.
"""
import json
import logging
from datetime import datetime
from typing import Any, Dict, Optional, List
from dataclasses import asdict, dataclass
import threading

import firebase_admin
from firebase_admin import credentials, firestore
from google.cloud.firestore_v1.client import Client as FirestoreClient
from google.cloud.firestore_v1.document import DocumentReference
from google.cloud.firestore_v1.collection import CollectionReference

# Initialize logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class MarketState:
    """Unified market state data structure published by Sensorium."""
    timestamp: datetime
    prices: Dict[str, float]  # token_address -> price in USD
    liquidity: Dict[str, Dict[str, float]]  # token_address -> {pool_address: liquidity_usd}
    social_sentiment: Dict[str, float]  # token_address -> sentiment_score (-1 to 1)
    whale_movements: List[Dict[str, Any]]  # List of whale transaction events
    gas_price_gwei: float
    risk_flags: Dict[str, List[str]]  # token_address -> list of risk flags
    volatility_index: float  # Aggregate market volatility (0-1)


class FirebaseClient:
    """Singleton Firebase client with thread-safe initialization and comprehensive error handling."""
    
    _instance: Optional['FirebaseClient'] = None
    _lock: threading.Lock = threading.Lock()
    
    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(FirebaseClient, cls).__new__(cls)
                cls._instance._initialized = False
            return cls._instance
    
    def __init__(self):
        """Initialize Firebase Admin SDK with credentials from environment."""
        if self._initialized:
            return
            
        try:
            import os
            from dotenv import load_dotenv
            load_dotenv()
            
            credentials_path = os.getenv('FIREBASE_CREDENTIALS_PATH')
            if not credentials_path:
                raise ValueError("FIREBASE_CREDENTIALS_PATH environment variable not set")
            
            if not os.path.exists(credentials_path):
                raise FileNotFoundError(f"Firebase credentials file not found at {credentials_path}")
            
            # Initialize Firebase app if not already initialized
            if not firebase_admin._apps:
                cred = credentials.Certificate(credentials_path)
                firebase_admin.initialize_app(cred)
                logger.info("Firebase Admin SDK initialized successfully")
            
            self.db: FirestoreClient = firestore.client()
            self._initialized = True
            logger.info("FirebaseClient initialized and ready")
            
            # Test connection
            test_ref = self.db.collection('system_health').document('connection_test')
            test_ref.set({'timestamp': firestore.SERVER_TIMESTAMP, 'status': 'connected'})
            logger.info("Firestore connection test successful")
            
        except Exception as e:
            logger.error(f"Failed to initialize FirebaseClient: {e}", exc_info=True)
            raise
    
    def publish_market_state(self, market_state: MarketState) -> bool:
        """
        Publish a MarketState object to Firestore.
        
        Args:
            market_state: The MarketState object to publish
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            state_dict = asdict(market_state)
            state_dict['timestamp'] = market_state.timestamp.isoformat()
            
            # Store in market_states collection with timestamp as document ID
            doc_ref = self.db.collection('market_states').document(market_state.timestamp.isoformat())
            doc_ref.set(state_dict)
            
            # Also update the latest market state
            latest_ref = self.db.collection('system_state').document('latest_market_state')
            latest_ref.set(state_dict)
            
            logger.info(f"Published MarketState for {market_state.timestamp}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to publish MarketState: {e}")
            return False
    
    def get_latest_market_state(self) -> Optional[Dict[str, Any]]:
        """
        Retrieve the latest market state from Firestore.
        
        Returns:
            Optional[Dict]: Latest market state or None if not available
        """
        try:
            doc_ref = self.db.collection('system_state').document('latest_market_state')
            doc = doc_ref.get()
            
            if doc.exists:
                return doc.to_dict()
            else:
                logger.warning("No latest market state found in Firestore")
                return None
                
        except Exception as e:
            logger.error(f"Failed to get latest market state: {e}")
            return None
    
    def publish_trade_intent(self, pod_id: str, intent_data: Dict[str, Any]) -> bool:
        """
        Publish a trade intent from a Cortex pod.
        
        Args:
            pod_id: Identifier of the strategy pod
            intent_data: Dictionary containing trade intent
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            intent_data['timestamp'] = firestore.SERVER_TIMESTAMP
            intent_data['pod_id'] = pod_id
            
            doc_ref = self.db.collection('trade_intents').document()
            doc_ref.set(intent_data)
            
            logger.info(f"Published trade intent from {pod_id}: {intent_data.get('action', 'unknown')}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to publish trade intent from {pod_id}: {e}")
            return False
    
    def get_pending_intents(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Retrieve pending trade intents sorted by confidence score.
        
        Args:
            limit: Maximum number of intents to retrieve
            
        Returns:
            List of intent dictionaries
        """
        try:
            intents_ref = self.db.collection('trade_intents')
            query = intents_ref.order_by('confidence_score', direction=firestore.Query.DESCENDING).limit(limit)
            docs = query.stream()
            
            return [doc.to_dict() for doc in docs]
            
        except Exception as e:
            logger.error(f"Failed to get pending intents: {e}")
            return []
    
    def update_system_health(self, component: str, status: str, details: Dict[str, Any] = None) -> bool:
        """
        Update system health status for monitoring.
        
        Args:
            component: Name of the component
            status: Current status (running, warning, error, stopped)
            details: Additional details about the component state
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            health_ref = self.db.collection('system_health').document(component)
            update_data = {
                'status': status,
                'last_updated': firestore.SERVER_TIMESTAMP,
                'details': details or {}
            }
            health_ref.set(update_data)
            
            logger.debug(f"Updated health for {component}: {status}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to update system health for {component}: {e}")
            return False
    
    def log_trade_execution(self, trade_data: Dict[str, Any]) -> bool:
        """
        Log executed trades for analysis and audit.
        
        Args:
            trade_data: Complete trade execution data
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            trade_data['executed_at'] = firestore.SERVER_TIMESTAMP
            
            # Store in trades collection
            trades_ref = self.db.collection('trades').document()
            trades_ref.set(trade_data)
            
            # Update performance metrics
            perf_ref = self.db.collection('performance_metrics').document('current')
            perf_ref.set({
                'last_trade': trade_data,
                'last_updated': firestore.SERVER_TIMESTAMP
            }, merge=True)
            
            logger.info(f"Logged trade execution: {trade_data.get('trade_id', 'unknown')}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to log trade execution: {e}")
            return False
    
    def set_kill_switch(self, reason: str, level: str = 'emergency') -> bool:
        """
        Activate the system kill switch.
        
        Args:
            reason: Reason for activation
            level: Severity level (warning, emergency, critical)
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            kill_switch_ref = self.db.collection('system_state').document('kill_switch')
            kill_switch_ref.set({
                'activated': True,
                'timestamp': firestore.SERVER_TIMESTAMP,
                'reason': reason,
                'level': level,
                'acknowledged': False
            })
            
            logger.critical(f"Kill switch activated: {reason} (level: {level})")
            return True
            
        except Exception as e:
            logger.error(f"Failed to activate kill switch: {e}")
            return False
    
    def is_kill_switch_active(self) -> bool:
        """
        Check if kill switch is active.
        
        Returns:
            bool: True if kill switch is active
        """
        try:
            kill_switch_ref = self.db.collection('system_state').document('kill_switch')
            doc = kill_switch_ref.get()
            
            if doc.exists:
                data = doc.to_dict()
                return data.get('activated', False) and not data.get('acknowledged', False)
            return False
            
        except Exception as e:
            logger.error(f"Failed to check kill switch: {e}")
            # Default to safe mode if we can't check
            return True
    
    def cleanup_old_data(self, collection_name: str, days_to_keep: int = 7) -> int:
        """
        Clean up old data from Firestore collections.
        
        Args:
            collection_name: Name of collection to clean
            days_to_keep: Number of days of data to keep
            
        Returns:
            int: Number of documents deleted
        """
        try:
            cutoff_date = datetime.now() - timedelta(days=days_to_keep)
            
            collection_ref = self.db.collection(collection_name)
            query = collection_ref.where('timestamp', '<', cutoff_date)
            
            deleted_count = 0
            for doc in query.stream():
                doc.reference.delete()
                deleted_count += 1
            
            logger.info(f"Cleaned up {deleted_count} old documents from {collection_name}")
            return deleted_count
            
        except Exception as e:
            logger.error(f"Failed to clean up old data from {collection_name}: {e}")
            return 0


# Global instance for easy import
firebase_client = FirebaseClient()