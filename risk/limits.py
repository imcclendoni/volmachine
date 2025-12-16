"""
Risk Limits and Kill Switches.

Enforces daily/weekly loss limits and drawdown kill switches.
"""

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Optional

from data.schemas import PortfolioState


@dataclass
class LimitConfig:
    """Configuration for risk limits."""
    
    # Account
    account_equity: float = 100000
    
    # Daily limits
    daily_loss_limit_pct: float = 3.0  # Stop trading after 3% daily loss
    daily_loss_warning_pct: float = 2.0  # Warn at 2%
    
    # Weekly limits
    weekly_loss_limit_pct: float = 5.0
    weekly_loss_warning_pct: float = 3.0
    
    # Drawdown kill switch
    max_drawdown_pct: float = 15.0  # Stop ALL trading
    drawdown_warning_pct: float = 10.0
    
    # Recovery
    recovery_after_days: int = 1  # Days to wait after kill switch


@dataclass
class LimitStatus:
    """Status of risk limits."""
    
    # Current levels
    daily_loss_pct: float
    weekly_loss_pct: float
    max_drawdown_pct: float
    
    # Status
    trading_allowed: bool = True
    kill_switch_active: bool = False
    
    # Warnings
    warnings: list[str] = None
    
    # Reasons
    blocked_reason: Optional[str] = None
    
    def __post_init__(self):
        if self.warnings is None:
            self.warnings = []


class LimitTracker:
    """Tracks and enforces trading limits."""
    
    def __init__(self, config: Optional[LimitConfig] = None):
        self.config = config or LimitConfig()
        
        # State
        self._daily_pnl: dict[date, float] = {}
        self._equity_high: float = self.config.account_equity
        self._current_equity: float = self.config.account_equity
        self._kill_switch_active: bool = False
        self._kill_switch_date: Optional[date] = None
    
    def update_pnl(self, pnl: float, trade_date: Optional[date] = None):
        """
        Update P&L for a date.
        
        Args:
            pnl: P&L amount (positive or negative)
            trade_date: Date of P&L (default today)
        """
        if trade_date is None:
            trade_date = date.today()
        
        if trade_date not in self._daily_pnl:
            self._daily_pnl[trade_date] = 0
        
        self._daily_pnl[trade_date] += pnl
        self._current_equity += pnl
        
        # Update high water mark
        if self._current_equity > self._equity_high:
            self._equity_high = self._current_equity
    
    def set_equity(self, equity: float):
        """Set current equity (for reconciliation)."""
        self._current_equity = equity
        if equity > self._equity_high:
            self._equity_high = equity
    
    def get_daily_pnl(self, for_date: Optional[date] = None) -> float:
        """Get P&L for a specific date."""
        if for_date is None:
            for_date = date.today()
        return self._daily_pnl.get(for_date, 0)
    
    def get_weekly_pnl(self, for_date: Optional[date] = None) -> float:
        """Get P&L for the week containing the date."""
        if for_date is None:
            for_date = date.today()
        
        # Find week start (Monday)
        week_start = for_date - timedelta(days=for_date.weekday())
        
        weekly_pnl = 0
        for d, pnl in self._daily_pnl.items():
            if week_start <= d <= for_date:
                weekly_pnl += pnl
        
        return weekly_pnl
    
    def get_drawdown(self) -> float:
        """Get current drawdown from high water mark."""
        if self._equity_high <= 0:
            return 0
        return (self._current_equity - self._equity_high) / self._equity_high * 100
    
    def check_limits(self) -> LimitStatus:
        """
        Check all limits and return status.
        
        Returns:
            LimitStatus with current state and any violations
        """
        today = date.today()
        warnings = []
        
        # Calculate metrics
        daily_loss_pct = -self.get_daily_pnl(today) / self.config.account_equity * 100
        weekly_loss_pct = -self.get_weekly_pnl(today) / self.config.account_equity * 100
        drawdown_pct = -self.get_drawdown()  # Make positive for loss
        
        trading_allowed = True
        blocked_reason = None
        
        # Check kill switch recovery
        if self._kill_switch_active and self._kill_switch_date:
            recovery_date = self._kill_switch_date + timedelta(days=self.config.recovery_after_days)
            if today < recovery_date:
                return LimitStatus(
                    daily_loss_pct=daily_loss_pct,
                    weekly_loss_pct=weekly_loss_pct,
                    max_drawdown_pct=drawdown_pct,
                    trading_allowed=False,
                    kill_switch_active=True,
                    blocked_reason=f"Kill switch active until {recovery_date}",
                    warnings=[f"Kill switch triggered on {self._kill_switch_date}"],
                )
            else:
                # Recovery period passed
                self._kill_switch_active = False
                self._kill_switch_date = None
        
        # Check drawdown kill switch
        if drawdown_pct >= self.config.max_drawdown_pct:
            self._kill_switch_active = True
            self._kill_switch_date = today
            return LimitStatus(
                daily_loss_pct=daily_loss_pct,
                weekly_loss_pct=weekly_loss_pct,
                max_drawdown_pct=drawdown_pct,
                trading_allowed=False,
                kill_switch_active=True,
                blocked_reason=f"Max drawdown {drawdown_pct:.1f}% >= {self.config.max_drawdown_pct}%",
                warnings=[f"KILL SWITCH: Max drawdown exceeded"],
            )
        elif drawdown_pct >= self.config.drawdown_warning_pct:
            warnings.append(f"Drawdown warning: {drawdown_pct:.1f}%")
        
        # Check daily limit
        if daily_loss_pct >= self.config.daily_loss_limit_pct:
            trading_allowed = False
            blocked_reason = f"Daily loss {daily_loss_pct:.1f}% >= limit {self.config.daily_loss_limit_pct}%"
        elif daily_loss_pct >= self.config.daily_loss_warning_pct:
            warnings.append(f"Daily loss warning: {daily_loss_pct:.1f}%")
        
        # Check weekly limit
        if weekly_loss_pct >= self.config.weekly_loss_limit_pct:
            trading_allowed = False
            blocked_reason = f"Weekly loss {weekly_loss_pct:.1f}% >= limit {self.config.weekly_loss_limit_pct}%"
        elif weekly_loss_pct >= self.config.weekly_loss_warning_pct:
            warnings.append(f"Weekly loss warning: {weekly_loss_pct:.1f}%")
        
        return LimitStatus(
            daily_loss_pct=daily_loss_pct,
            weekly_loss_pct=weekly_loss_pct,
            max_drawdown_pct=drawdown_pct,
            trading_allowed=trading_allowed,
            kill_switch_active=self._kill_switch_active,
            warnings=warnings,
            blocked_reason=blocked_reason,
        )
    
    def is_trading_allowed(self) -> bool:
        """Quick check if trading is currently allowed."""
        status = self.check_limits()
        return status.trading_allowed
    
    def get_do_not_trade_reasons(self) -> list[str]:
        """Get list of reasons not to trade."""
        status = self.check_limits()
        
        reasons = []
        if status.kill_switch_active:
            reasons.append("Kill switch active")
        if status.blocked_reason:
            reasons.append(status.blocked_reason)
        reasons.extend(status.warnings)
        
        return reasons
    
    def reset_daily(self):
        """Reset daily tracking (call at start of new day)."""
        # Keep history but clear today
        pass  # Daily data auto-accumulates
    
    def reset_all(self):
        """Reset all tracking (use carefully)."""
        self._daily_pnl = {}
        self._kill_switch_active = False
        self._kill_switch_date = None
        self._equity_high = self.config.account_equity
        self._current_equity = self.config.account_equity
    
    def to_dict(self) -> dict:
        """Export state as dictionary."""
        status = self.check_limits()
        return {
            'current_equity': self._current_equity,
            'equity_high': self._equity_high,
            'daily_loss_pct': status.daily_loss_pct,
            'weekly_loss_pct': status.weekly_loss_pct,
            'drawdown_pct': status.max_drawdown_pct,
            'trading_allowed': status.trading_allowed,
            'kill_switch_active': status.kill_switch_active,
            'warnings': status.warnings,
            'blocked_reason': status.blocked_reason,
        }
