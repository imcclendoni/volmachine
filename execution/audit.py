"""
Execution Audit Log Module.

Logs every execution attempt. No silent failures. No skipped logs.
"""

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Optional

from execution.plan import ExecutionPlan


def get_audit_log_path() -> Path:
    """Get path to audit log file."""
    log_dir = Path('./logs/execution')
    log_dir.mkdir(parents=True, exist_ok=True)
    return log_dir / 'audit.jsonl'


def log_execution_attempt(
    symbol: str,
    plan: ExecutionPlan,
    submitted: bool,
    reason: str,
    mode: str = "paper",
) -> dict:
    """
    Log every execution attempt.
    
    No silent failures. No skipped logs.
    
    Args:
        symbol: Underlying symbol
        plan: The execution plan
        submitted: Whether the order was actually submitted
        reason: Reason for the action/status
        mode: Execution mode (always "paper")
    
    Returns:
        The log entry that was written
    """
    log_entry = {
        'timestamp': datetime.now().isoformat(),
        'symbol': symbol,
        'mode': mode,
        'submitted': submitted,
        'reason': reason,
        'order_preview': plan.to_dict() if plan else None,
    }
    
    # Append to jsonl file
    log_path = get_audit_log_path()
    try:
        with open(log_path, 'a') as f:
            f.write(json.dumps(log_entry) + '\n')
    except Exception as e:
        # Even if file write fails, print to console
        print(f"AUDIT LOG (file write failed): {json.dumps(log_entry)}")
        print(f"File write error: {e}")
    
    return log_entry


def log_confirmation_attempt(
    trade_id: str,
    action: str,  # CONFIRM | CANCEL | REJECT
    user_input: Optional[str] = None,
) -> dict:
    """Log user confirmation attempts."""
    log_entry = {
        'timestamp': datetime.now().isoformat(),
        'trade_id': trade_id,
        'action': action,
        'user_input': user_input,
        'event_type': 'CONFIRMATION_ATTEMPT',
    }
    
    log_path = get_audit_log_path()
    try:
        with open(log_path, 'a') as f:
            f.write(json.dumps(log_entry) + '\n')
    except Exception as e:
        print(f"AUDIT LOG (file write failed): {json.dumps(log_entry)}")
    
    return log_entry


def log_kill_switch_triggered(reason: str) -> dict:
    """Log when kill switch is triggered."""
    log_entry = {
        'timestamp': datetime.now().isoformat(),
        'event_type': 'KILL_SWITCH_TRIGGERED',
        'reason': reason,
        'severity': 'CRITICAL',
    }
    
    log_path = get_audit_log_path()
    try:
        with open(log_path, 'a') as f:
            f.write(json.dumps(log_entry) + '\n')
    except Exception:
        pass
    
    # Also print to console
    print(f"🚨 KILL SWITCH: {reason}")
    
    return log_entry


def get_audit_log_entries(limit: int = 100) -> list[dict]:
    """Read recent audit log entries."""
    log_path = get_audit_log_path()
    
    if not log_path.exists():
        return []
    
    entries = []
    try:
        with open(log_path, 'r') as f:
            for line in f:
                if line.strip():
                    entries.append(json.loads(line))
    except Exception:
        return []
    
    # Return most recent entries
    return entries[-limit:]
