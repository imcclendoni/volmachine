"""
IBKR Order Client for VolMachine.

Handles order execution via Interactive Brokers TWS/Gateway using ib_insync.

HARD RULES:
1. PAPER MODE ONLY (port 7497)
2. NO AUTO-EXECUTION - all orders require manual confirmation
3. KILL SWITCH if live port (7496) detected
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Callable
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class OrderStatus(str, Enum):
    """Order status states."""
    PREVIEW = "preview"
    PENDING = "pending"
    SUBMITTED = "submitted"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELLED = "cancelled"
    ERROR = "error"


@dataclass
class ResolvedLeg:
    """Resolved option contract from IBKR."""
    symbol: str
    expiration: str
    strike: float
    option_type: str  # 'C' or 'P'
    action: str  # 'BUY' or 'SELL'
    quantity: int
    
    # Resolved from IBKR
    con_id: Optional[int] = None
    local_symbol: Optional[str] = None
    trading_class: Optional[str] = None
    exchange: str = "SMART"
    currency: str = "USD"
    
    # Validation
    is_resolved: bool = False
    error: Optional[str] = None


@dataclass
class OrderTicket:
    """Order ticket for multi-leg spread."""
    candidate_id: str
    symbol: str
    legs: list[ResolvedLeg]
    total_quantity: int
    
    # Pricing
    limit_price: Optional[float] = None
    order_type: str = "LMT"
    
    # Status
    status: OrderStatus = OrderStatus.PREVIEW
    order_id: Optional[int] = None
    perm_id: Optional[int] = None
    
    # Fill info
    filled_quantity: int = 0
    avg_fill_price: float = 0.0
    
    # Timestamps
    created_at: datetime = field(default_factory=datetime.now)
    submitted_at: Optional[datetime] = None
    filled_at: Optional[datetime] = None
    
    # Errors
    error: Optional[str] = None


class IBKROrderClient:
    """
    IBKR order execution client.
    
    Uses ib_insync for connection to TWS/Gateway.
    PAPER MODE ONLY - enforced at connection.
    """
    
    # Paper trading ports
    PAPER_PORTS = {7497, 4002}  # TWS paper, Gateway paper
    LIVE_PORTS = {7496, 4001}   # TWS live, Gateway live (BLOCKED)
    
    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 7497,
        client_id: int = 1,
    ):
        self.host = host
        self.port = port
        self.client_id = client_id
        self._ib = None
        self._connected = False
        self._order_status_callbacks: list[Callable] = []
        
        # SAFETY: Block live ports
        if port in self.LIVE_PORTS:
            raise LiveTradingBlocked(
                f"KILL SWITCH: Live trading port {port} detected. "
                f"Only paper ports {self.PAPER_PORTS} allowed."
            )
    
    def connect(self) -> bool:
        """
        Connect to TWS/Gateway.
        
        Returns True if connected successfully.
        Raises LiveTradingBlocked if live account detected.
        """
        try:
            from ib_insync import IB
        except ImportError:
            raise ImportError(
                "ib_insync not installed. Run: pip install ib_insync"
            )
        
        self._ib = IB()
        
        try:
            self._ib.connect(
                self.host,
                self.port,
                clientId=self.client_id,
                readonly=False,
            )
        except Exception as e:
            logger.error(f"Failed to connect to IBKR: {e}")
            raise ConnectionError(f"Failed to connect to IBKR at {self.host}:{self.port}: {e}")
        
        # Verify paper account
        account_type = self._get_account_type()
        if account_type == "LIVE":
            self._ib.disconnect()
            raise LiveTradingBlocked(
                "KILL SWITCH: Live trading account detected. "
                "Only paper accounts allowed."
            )
        
        self._connected = True
        logger.info(f"Connected to IBKR (paper) at {self.host}:{self.port}")
        
        # Subscribe to order updates
        self._ib.orderStatusEvent += self._on_order_status
        self._ib.execDetailsEvent += self._on_exec_details
        
        return True
    
    def disconnect(self):
        """Disconnect from TWS/Gateway."""
        if self._ib:
            self._ib.disconnect()
            self._ib = None
        self._connected = False
    
    def _get_account_type(self) -> str:
        """Get account type (PAPER or LIVE)."""
        if not self._ib:
            return "UNKNOWN"
        
        accounts = self._ib.managedAccounts()
        if accounts:
            # Paper accounts typically start with 'D' or contain 'PAPER'
            first_account = accounts[0]
            if first_account.startswith('D') or 'PAPER' in first_account.upper():
                return "PAPER"
        
        # Check account summary for additional verification
        # This is a secondary check
        return "LIVE"  # Assume live if unsure (safer to block)
    
    def resolve_contracts(self, legs: list[dict]) -> list[ResolvedLeg]:
        """
        Resolve option contracts via reqContractDetails.
        
        Args:
            legs: List of leg dicts from candidate structure
            
        Returns:
            List of ResolvedLeg with conId and localSymbol populated
        """
        if not self._connected:
            raise ConnectionError("Not connected to IBKR")
        
        from ib_insync import Option
        
        resolved = []
        
        for leg in legs:
            # Parse leg data
            symbol = leg.get('symbol', '')
            expiration = leg.get('expiration', '')
            strike = float(leg.get('strike', 0))
            option_type = leg.get('option_type', 'C')[0].upper()
            action = leg.get('action', 'BUY')
            quantity = int(leg.get('quantity', 1))
            
            # Convert expiration to IBKR format (YYYYMMDD)
            exp_str = expiration.replace('-', '')
            
            # Create option contract
            contract = Option(
                symbol=symbol,
                lastTradeDateOrContractMonth=exp_str,
                strike=strike,
                right=option_type,
                exchange='SMART',
                currency='USD',
            )
            
            resolved_leg = ResolvedLeg(
                symbol=symbol,
                expiration=expiration,
                strike=strike,
                option_type=option_type,
                action=action,
                quantity=quantity,
            )
            
            try:
                # Qualify contract to get conId
                qualified = self._ib.qualifyContracts(contract)
                
                if qualified:
                    qc = qualified[0]
                    resolved_leg.con_id = qc.conId
                    resolved_leg.local_symbol = qc.localSymbol
                    resolved_leg.trading_class = qc.tradingClass
                    resolved_leg.is_resolved = True
                    logger.info(f"Resolved: {qc.localSymbol} -> conId={qc.conId}")
                else:
                    resolved_leg.error = "Contract not found"
                    logger.warning(f"Failed to resolve: {symbol} {expiration} {strike} {option_type}")
                    
            except Exception as e:
                resolved_leg.error = str(e)
                logger.error(f"Error resolving contract: {e}")
            
            resolved.append(resolved_leg)
        
        return resolved
    
    def create_order_ticket(
        self,
        candidate_id: str,
        symbol: str,
        resolved_legs: list[ResolvedLeg],
        quantity: int,
        limit_price: Optional[float] = None,
    ) -> OrderTicket:
        """
        Create an order ticket for submission.
        
        Args:
            candidate_id: TradeCandidate ID
            symbol: Underlying symbol
            resolved_legs: Resolved legs from resolve_contracts()
            quantity: Number of spreads to trade
            limit_price: Limit price (debit for debit spreads, credit for credit spreads)
            
        Returns:
            OrderTicket ready for submission
        """
        # Validate all legs resolved
        for leg in resolved_legs:
            if not leg.is_resolved:
                raise ValueError(f"Leg not resolved: {leg.symbol} {leg.strike} {leg.option_type}")
        
        ticket = OrderTicket(
            candidate_id=candidate_id,
            symbol=symbol,
            legs=resolved_legs,
            total_quantity=quantity,
            limit_price=limit_price,
            status=OrderStatus.PREVIEW,
        )
        
        return ticket
    
    def submit_order(self, ticket: OrderTicket, transmit: bool = False) -> OrderTicket:
        """
        Submit BAG order to IBKR.
        
        Args:
            ticket: OrderTicket to submit
            transmit: If False, order is created but not sent (preview).
                      If True, order is sent to exchange.
            
        Returns:
            Updated OrderTicket with order_id
        """
        if not self._connected:
            raise ConnectionError("Not connected to IBKR")
        
        from ib_insync import Contract, ComboLeg, Order
        
        # Create BAG contract
        bag = Contract()
        bag.symbol = ticket.symbol
        bag.secType = 'BAG'
        bag.currency = 'USD'
        bag.exchange = 'SMART'
        
        # Add combo legs
        combo_legs = []
        for leg in ticket.legs:
            cl = ComboLeg()
            cl.conId = leg.con_id
            cl.ratio = abs(leg.quantity)
            cl.action = leg.action
            cl.exchange = 'SMART'
            combo_legs.append(cl)
        
        bag.comboLegs = combo_legs
        
        # Create order
        order = Order()
        order.action = 'BUY'  # For spreads, BUY means enter position
        order.totalQuantity = ticket.total_quantity
        order.orderType = ticket.order_type
        
        if ticket.limit_price is not None:
            order.lmtPrice = ticket.limit_price
        
        # TRANSMIT CONTROL:
        # - False = order created in TWS but NOT sent to exchange (preview)
        # - True = order sent to exchange for execution
        order.transmit = transmit
        
        # Submit order
        trade = self._ib.placeOrder(bag, order)
        
        ticket.order_id = trade.order.orderId
        ticket.perm_id = trade.order.permId
        
        if transmit:
            ticket.status = OrderStatus.SUBMITTED
            ticket.submitted_at = datetime.now()
            logger.info(f"Order TRANSMITTED to exchange: orderId={ticket.order_id}, permId={ticket.perm_id}")
        else:
            ticket.status = OrderStatus.PENDING
            logger.info(f"Order created (preview, not transmitted): orderId={ticket.order_id}")
        
        return ticket
    
    def transmit_order(self, ticket: OrderTicket) -> OrderTicket:
        """
        Transmit a previously created order to the exchange.
        
        Use this after submit_order(transmit=False) to actually send the order.
        """
        if not self._connected:
            raise ConnectionError("Not connected to IBKR")
        
        if not ticket.order_id:
            raise ValueError("Order has no order_id - must call submit_order first")
        
        from ib_insync import Order
        
        # Find the pending order and transmit it
        for trade in self._ib.openTrades():
            if trade.order.orderId == ticket.order_id:
                # Modify order to transmit
                trade.order.transmit = True
                self._ib.placeOrder(trade.contract, trade.order)
                
                ticket.status = OrderStatus.SUBMITTED
                ticket.submitted_at = datetime.now()
                logger.info(f"Order TRANSMITTED: orderId={ticket.order_id}")
                return ticket
        
        raise ValueError(f"Order {ticket.order_id} not found in open trades")
    
    def get_account_id(self) -> str:
        """Get the connected account ID."""
        if not self._connected or not self._ib:
            return "NOT_CONNECTED"
        
        accounts = self._ib.managedAccounts()
        return accounts[0] if accounts else "UNKNOWN"
    
    def is_connected(self) -> bool:
        """Check if connected to IBKR."""
        return self._connected and self._ib is not None
    
    def cancel_order(self, ticket: OrderTicket) -> OrderTicket:
        """Cancel a pending order."""
        if not self._connected:
            raise ConnectionError("Not connected to IBKR")
        
        if ticket.order_id:
            # Find the order
            for order in self._ib.orders():
                if order.orderId == ticket.order_id:
                    self._ib.cancelOrder(order)
                    ticket.status = OrderStatus.CANCELLED
                    logger.info(f"Order cancelled: orderId={ticket.order_id}")
                    break
        
        return ticket
    
    def subscribe_order_status(self, callback: Callable[[OrderTicket], None]):
        """Subscribe to order status updates."""
        self._order_status_callbacks.append(callback)
    
    def _on_order_status(self, trade):
        """Handle order status updates from IBKR."""
        status = trade.orderStatus.status
        
        # Map IBKR status to our status
        status_map = {
            'Submitted': OrderStatus.SUBMITTED,
            'Filled': OrderStatus.FILLED,
            'Cancelled': OrderStatus.CANCELLED,
            'ApiCancelled': OrderStatus.CANCELLED,
            'PendingSubmit': OrderStatus.PENDING,
            'PreSubmitted': OrderStatus.PENDING,
        }
        
        our_status = status_map.get(status, OrderStatus.SUBMITTED)
        
        logger.info(f"Order status update: orderId={trade.order.orderId}, status={status}")
        
        # Notify callbacks
        for callback in self._order_status_callbacks:
            try:
                callback(trade)
            except Exception as e:
                logger.error(f"Error in order status callback: {e}")
    
    def _on_exec_details(self, trade, fill):
        """Handle execution details from IBKR."""
        logger.info(
            f"Execution: orderId={trade.order.orderId}, "
            f"filled={fill.execution.shares}, "
            f"price={fill.execution.price}"
        )


class LiveTradingBlocked(Exception):
    """Raised when live trading is attempted."""
    pass


# Singleton for connection management
_ibkr_client: Optional[IBKROrderClient] = None


def get_ibkr_client(
    host: str = "127.0.0.1",
    port: int = 7497,
    client_id: int = 1,
) -> IBKROrderClient:
    """Get or create IBKR client singleton."""
    global _ibkr_client
    
    if _ibkr_client is None:
        _ibkr_client = IBKROrderClient(host, port, client_id)
    
    return _ibkr_client


def reset_ibkr_client():
    """Reset the IBKR client (for testing)."""
    global _ibkr_client
    if _ibkr_client:
        _ibkr_client.disconnect()
    _ibkr_client = None
