"""
Centralized configuration management for the Metabolic Organism.
All configuration is loaded from environment variables with sensible defaults.
"""
import os
from typing import List, Dict, Any
from dataclasses import dataclass
from decimal import Decimal

from dotenv import load_dotenv

# Load environment variables
load_dotenv()


@dataclass
class RPCSettings:
    """RPC endpoint configuration with failover support."""
    alchemy_url: str = os.getenv('ALCHEMY_BASE_RPC_URL', '')
    quicknode_url: str = os.getenv('QUICKNODE_BASE_RPC_URL', '')
    public_url: str = os.getenv('PUBLIC_BASE_RPC_URL', 'https://mainnet.base.org')
    
    def get_endpoints(self) -> List[str]:
        """Return RPC endpoints in order of priority."""
        endpoints = []
        if self.alchemy_url:
            endpoints.append(self.alchemy_url)
        if self.quicknode_url:
            endpoints.append(self.quicknode_url)
        endpoints.append(self.public_url)
        return endpoints


@dataclass
class WalletSettings:
    """Wallet configuration and security settings."""
    private_key: str = os.getenv('WALLET_PRIVATE_KEY', '')
    address: str = os.getenv('WALLET_ADDRESS', '')
    initial_capital_usd: Decimal = Decimal(os.getenv('INITIAL_CAPITAL_USD', '27.69'))
    max_position_size_usd: Decimal = Decimal(os.getenv('MAX_POSITION_SIZE_USD', '10.00'))
    
    def validate(self) -> bool:
        """Validate wallet configuration."""
        if not self.private_key:
            raise ValueError("WALLET_PRIVATE_KEY environment variable not set")
        if not self.address:
            raise ValueError("WALLET_ADDRESS environment variable not set")
        if self.initial_capital_usd <= 0:
            raise ValueError("INITIAL_CAPITAL_USD must be positive")
        if self.max_position_size_usd <= 0:
            raise ValueError("MAX_POSITION_SIZE_USD must be positive")
        return True


@dataclass
class GasSettings:
    """Gas price management settings."""
    max_gas_price_gwei: float = float(os.getenv('MAX_GAS_PRICE_GWEI', '50'))
    gas_buffer_percent: float = float(os.getenv('GAS_BUFFER_PERCENT', '10'))
    check_interval_seconds: int = 30
    
    def validate(self) -> bool:
        """Validate gas settings."""
        if self.max_gas_price_gwei <= 0