"""
Execution Configuration Module.

HARD SAFETY RULES:
- PAPER MODE ONLY
- NO AUTO-EXECUTION
- ALL TRADES REQUIRE MANUAL CONFIRMATION

Any violation terminates the system.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional


@dataclass
class ExecutionConfig:
    """
    Global execution configuration.
    
    CRITICAL: These defaults are SAFETY BOUNDS.
    - mode MUST be "paper"
    - auto_execute MUST be False
    - require_manual_confirm MUST be True
    """
    enabled: bool = True
    mode: str = "paper"  # paper | live (live is BLOCKED)
    auto_execute: bool = False  # NEVER True
    require_manual_confirm: bool = True  # ALWAYS True
    max_orders_per_day: int = 2
    
    # Paper trading connection
    ibkr_host: str = "127.0.0.1"
    ibkr_port: int = 7497  # Paper trading port (7496 = live, 7497 = paper)
    ibkr_client_id: int = 1
    
    def __post_init__(self):
        """Validate config on creation."""
        validate_execution_config(self)


class ExecutionBlocked(Exception):
    """Raised when execution is blocked due to safety controls."""
    pass


class LiveTradingAttempted(SystemExit):
    """Raised when live trading is attempted - terminates system."""
    def __init__(self, reason: str = "LIVE TRADING DISABLED - PAPER MODE ONLY"):
        super().__init__(reason)


def validate_execution_config(config: ExecutionConfig) -> None:
    """
    KILL SWITCH - Validates execution config safety.
    
    If any assertion fails → TERMINATE SYSTEM.
    
    HARD RULES (NON-NEGOTIABLE):
    1. PAPER MODE ONLY
    2. NO AUTO-EXECUTION  
    3. ALL TRADES REQUIRE MANUAL CONFIRMATION
    """
    
    # KILL SWITCH 1: Paper mode only
    if config.mode != "paper":
        raise LiveTradingAttempted(
            f"LIVE TRADING DISABLED - mode='{config.mode}' is not allowed. "
            "Only mode='paper' is permitted."
        )
    
    # KILL SWITCH 2: No auto-execution
    if config.auto_execute is True:
        raise LiveTradingAttempted(
            "AUTO-EXECUTION DISABLED - auto_execute=True is not allowed. "
            "All trades require manual confirmation."
        )
    
    # KILL SWITCH 3: Manual confirmation required
    if config.require_manual_confirm is False:
        raise LiveTradingAttempted(
            "MANUAL CONFIRMATION REQUIRED - require_manual_confirm=False is not allowed. "
            "Human-in-the-loop control is mandatory."
        )
    
    # KILL SWITCH 4: Port sanity check (7497 = paper, 7496 = live)
    if config.ibkr_port == 7496:
        raise LiveTradingAttempted(
            f"LIVE TRADING PORT DETECTED - port={config.ibkr_port}. "
            "Only port 7497 (paper) is permitted."
        )


def get_execution_config() -> ExecutionConfig:
    """
    Get the global execution config.
    
    Always returns PAPER mode config.
    Kill switch is validated on creation.
    """
    return ExecutionConfig()


# Validate on module load - fail fast if config is unsafe
_GLOBAL_CONFIG = ExecutionConfig()
