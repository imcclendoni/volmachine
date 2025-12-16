"""
Structured JSON Logging.

Provides structured logging for all engine operations.
"""

import json
import logging
import os
from datetime import date, datetime
from pathlib import Path
from typing import Any, Optional


class StructuredLogger:
    """
    Structured JSON logger for the engine.
    
    Logs to both file and console with structured JSON format.
    """
    
    def __init__(
        self,
        log_dir: str = "./logs",
        level: int = logging.INFO,
        console: bool = True,
    ):
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        
        self.level = level
        self.console = console
        
        # Set up logger
        self.logger = logging.getLogger("volmachine")
        self.logger.setLevel(level)
        self.logger.handlers = []  # Clear existing handlers
        
        # File handler for today
        self._setup_file_handler()
        
        # Console handler
        if console:
            console_handler = logging.StreamHandler()
            console_handler.setLevel(level)
            console_handler.setFormatter(logging.Formatter(
                '%(asctime)s - %(levelname)s - %(message)s'
            ))
            self.logger.addHandler(console_handler)
    
    def _setup_file_handler(self):
        """Set up file handler for today's log."""
        today = date.today()
        log_file = self.log_dir / f"engine_{today.isoformat()}.jsonl"
        
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(self.level)
        file_handler.setFormatter(logging.Formatter('%(message)s'))
        self.logger.addHandler(file_handler)
    
    def _serialize(self, obj: Any) -> Any:
        """Serialize object for JSON."""
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, date):
            return obj.isoformat()
        elif hasattr(obj, 'model_dump'):
            return obj.model_dump()
        elif hasattr(obj, '__dict__'):
            return {k: self._serialize(v) for k, v in obj.__dict__.items() if not k.startswith('_')}
        elif isinstance(obj, dict):
            return {k: self._serialize(v) for k, v in obj.items()}
        elif isinstance(obj, (list, tuple)):
            return [self._serialize(v) for v in obj]
        else:
            return obj
    
    def _log(self, level: str, event: str, data: Optional[dict] = None):
        """Internal log method."""
        record = {
            'timestamp': datetime.now().isoformat(),
            'level': level,
            'event': event,
        }
        
        if data:
            record['data'] = self._serialize(data)
        
        json_str = json.dumps(record, default=str)
        
        if level == 'DEBUG':
            self.logger.debug(json_str)
        elif level == 'INFO':
            self.logger.info(json_str)
        elif level == 'WARNING':
            self.logger.warning(json_str)
        elif level == 'ERROR':
            self.logger.error(json_str)
        elif level == 'CRITICAL':
            self.logger.critical(json_str)
    
    # Public logging methods
    
    def debug(self, event: str, **data):
        """Log debug message."""
        self._log('DEBUG', event, data if data else None)
    
    def info(self, event: str, **data):
        """Log info message."""
        self._log('INFO', event, data if data else None)
    
    def warning(self, event: str, **data):
        """Log warning message."""
        self._log('WARNING', event, data if data else None)
    
    def error(self, event: str, **data):
        """Log error message."""
        self._log('ERROR', event, data if data else None)
    
    def critical(self, event: str, **data):
        """Log critical message."""
        self._log('CRITICAL', event, data if data else None)
    
    # Specialized log methods
    
    def log_regime(self, regime_data: dict):
        """Log regime classification."""
        self.info('regime_classified', **regime_data)
    
    def log_edge(self, edge_data: dict):
        """Log edge detection."""
        self.info('edge_detected', **edge_data)
    
    def log_candidate(self, candidate_data: dict):
        """Log trade candidate."""
        self.info('trade_candidate', **candidate_data)
    
    def log_trade(self, trade_data: dict):
        """Log trade execution."""
        self.info('trade_executed', **trade_data)
    
    def log_position_update(self, position_data: dict):
        """Log position update."""
        self.info('position_updated', **position_data)
    
    def log_risk_check(self, risk_data: dict):
        """Log risk check."""
        self.info('risk_checked', **risk_data)
    
    def log_kill_switch(self, reason: str, **data):
        """Log kill switch activation."""
        self.critical('kill_switch_activated', reason=reason, **data)


# Global logger instance
_logger: Optional[StructuredLogger] = None


def get_logger(
    log_dir: str = "./logs",
    level: int = logging.INFO,
) -> StructuredLogger:
    """Get or create the global logger instance."""
    global _logger
    
    if _logger is None:
        _logger = StructuredLogger(log_dir=log_dir, level=level)
    
    return _logger
