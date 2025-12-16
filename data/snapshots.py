"""
Option Chain Snapshots.

Persists option chain data with timestamps for:
- Reproducibility (run on historical snapshots)
- Quote validation (staleness detection)
- Data quality auditing
"""

import json
import gzip
from datetime import datetime, date
from pathlib import Path
from typing import Optional, List
from dataclasses import dataclass

from data.schemas import OptionChain, OptionContract, OptionType


@dataclass
class SnapshotMetadata:
    """Metadata for a chain snapshot."""
    symbol: str
    snapshot_time: datetime
    source: str  # Provider name
    contract_count: int
    expiration_count: int
    underlying_price: float
    
    # Quality metrics
    pct_valid_quotes: float
    pct_stale_quotes: float
    avg_bid_ask_spread_pct: float


class ChainSnapshotStore:
    """
    Stores and retrieves option chain snapshots.
    
    Storage format: gzipped JSONL by date and symbol.
    Path: {base_dir}/{date}/{symbol}.jsonl.gz
    """
    
    # Quote staleness threshold (5 minutes)
    STALENESS_THRESHOLD_SECONDS = 300
    
    def __init__(self, base_dir: str = "./cache/snapshots"):
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)
    
    def save_snapshot(
        self, 
        chain: OptionChain, 
        source: str = "unknown",
    ) -> SnapshotMetadata:
        """
        Save an option chain snapshot.
        
        Args:
            chain: The option chain to save
            source: Provider name (polygon, tradier, etc)
            
        Returns:
            Metadata about the saved snapshot
        """
        snapshot_time = chain.timestamp
        snapshot_date = snapshot_time.date()
        
        # Create directory
        date_dir = self.base_dir / snapshot_date.isoformat()
        date_dir.mkdir(parents=True, exist_ok=True)
        
        # Calculate quality metrics
        valid_count = 0
        stale_count = 0
        total_spread = 0
        
        for contract in chain.contracts:
            if self._is_valid_quote(contract):
                valid_count += 1
            if self._is_stale_quote(contract, snapshot_time):
                stale_count += 1
            if contract.mid and contract.mid > 0:
                total_spread += (contract.ask - contract.bid) / contract.mid
        
        total = len(chain.contracts)
        pct_valid = valid_count / total if total > 0 else 0
        pct_stale = stale_count / total if total > 0 else 0
        avg_spread = total_spread / total * 100 if total > 0 else 0
        
        metadata = SnapshotMetadata(
            symbol=chain.symbol,
            snapshot_time=snapshot_time,
            source=source,
            contract_count=total,
            expiration_count=len(chain.expirations),
            underlying_price=chain.underlying_price,
            pct_valid_quotes=pct_valid,
            pct_stale_quotes=pct_stale,
            avg_bid_ask_spread_pct=avg_spread,
        )
        
        # Serialize chain
        chain_data = {
            "metadata": {
                "symbol": metadata.symbol,
                "snapshot_time": metadata.snapshot_time.isoformat(),
                "source": metadata.source,
                "contract_count": metadata.contract_count,
                "expiration_count": metadata.expiration_count,
                "underlying_price": metadata.underlying_price,
                "pct_valid_quotes": metadata.pct_valid_quotes,
                "pct_stale_quotes": metadata.pct_stale_quotes,
                "avg_bid_ask_spread_pct": metadata.avg_bid_ask_spread_pct,
            },
            "chain": self._chain_to_dict(chain),
        }
        
        # Append to file (JSONL allows multiple snapshots per day)
        file_path = date_dir / f"{chain.symbol}.jsonl.gz"
        
        with gzip.open(file_path, "at", encoding="utf-8") as f:
            f.write(json.dumps(chain_data) + "\n")
        
        return metadata
    
    def load_snapshots(
        self, 
        symbol: str, 
        snapshot_date: date,
    ) -> List[tuple[SnapshotMetadata, OptionChain]]:
        """
        Load all snapshots for a symbol on a given date.
        
        Args:
            symbol: Underlying symbol
            snapshot_date: Date to load
            
        Returns:
            List of (metadata, chain) tuples
        """
        file_path = self.base_dir / snapshot_date.isoformat() / f"{symbol}.jsonl.gz"
        
        if not file_path.exists():
            return []
        
        results = []
        with gzip.open(file_path, "rt", encoding="utf-8") as f:
            for line in f:
                data = json.loads(line)
                meta = data["metadata"]
                
                metadata = SnapshotMetadata(
                    symbol=meta["symbol"],
                    snapshot_time=datetime.fromisoformat(meta["snapshot_time"]),
                    source=meta["source"],
                    contract_count=meta["contract_count"],
                    expiration_count=meta["expiration_count"],
                    underlying_price=meta["underlying_price"],
                    pct_valid_quotes=meta["pct_valid_quotes"],
                    pct_stale_quotes=meta["pct_stale_quotes"],
                    avg_bid_ask_spread_pct=meta["avg_bid_ask_spread_pct"],
                )
                
                chain = self._dict_to_chain(data["chain"])
                results.append((metadata, chain))
        
        return results
    
    def get_latest_snapshot(
        self, 
        symbol: str, 
        as_of: Optional[datetime] = None,
    ) -> Optional[tuple[SnapshotMetadata, OptionChain]]:
        """
        Get the most recent snapshot for a symbol.
        
        Args:
            symbol: Underlying symbol
            as_of: Get snapshot as of this time (default: now)
            
        Returns:
            (metadata, chain) or None
        """
        as_of = as_of or datetime.now()
        target_date = as_of.date()
        
        # Try today first, then go back up to 5 days
        for delta in range(6):
            check_date = target_date
            if delta > 0:
                from datetime import timedelta
                check_date = target_date - timedelta(days=delta)
            
            snapshots = self.load_snapshots(symbol, check_date)
            
            if snapshots:
                # Filter to snapshots before as_of time
                valid = [
                    s for s in snapshots 
                    if s[0].snapshot_time <= as_of
                ]
                if valid:
                    # Return most recent
                    return max(valid, key=lambda x: x[0].snapshot_time)
        
        return None
    
    def _is_valid_quote(self, contract: OptionContract) -> bool:
        """Check if quote is valid (bid/ask sanity)."""
        if contract.bid <= 0 or contract.ask <= 0:
            return False
        if contract.bid > contract.ask:
            return False
        # Bid-ask spread sanity check (< 100% of mid)
        if contract.mid and contract.mid > 0:
            spread_pct = (contract.ask - contract.bid) / contract.mid
            if spread_pct > 1.0:
                return False
        return True
    
    def _is_stale_quote(
        self, 
        contract: OptionContract, 
        reference_time: datetime
    ) -> bool:
        """Check if quote is stale (too old)."""
        if not contract.quote_time:
            return True  # No quote time = assume stale
        
        age_seconds = (reference_time - contract.quote_time).total_seconds()
        return age_seconds > self.STALENESS_THRESHOLD_SECONDS
    
    def _chain_to_dict(self, chain: OptionChain) -> dict:
        """Convert chain to serializable dict."""
        return {
            "symbol": chain.symbol,
            "underlying_price": chain.underlying_price,
            "timestamp": chain.timestamp.isoformat(),
            "expirations": [e.isoformat() for e in chain.expirations],
            "contracts": [
                {
                    "symbol": c.symbol,
                    "contract_symbol": c.contract_symbol,
                    "option_type": c.option_type.value,
                    "strike": c.strike,
                    "expiration": c.expiration.isoformat(),
                    "bid": c.bid,
                    "ask": c.ask,
                    "last": c.last,
                    "mid": c.mid,
                    "iv": c.iv,
                    "volume": c.volume,
                    "open_interest": c.open_interest,
                    "quote_time": c.quote_time.isoformat() if c.quote_time else None,
                }
                for c in chain.contracts
            ],
        }
    
    def _dict_to_chain(self, data: dict) -> OptionChain:
        """Convert dict back to OptionChain."""
        contracts = []
        for c in data["contracts"]:
            contracts.append(OptionContract(
                symbol=c["symbol"],
                contract_symbol=c["contract_symbol"],
                option_type=OptionType(c["option_type"]),
                strike=c["strike"],
                expiration=date.fromisoformat(c["expiration"]),
                bid=c["bid"],
                ask=c["ask"],
                last=c.get("last"),
                mid=c.get("mid"),
                iv=c.get("iv"),
                volume=c.get("volume", 0),
                open_interest=c.get("open_interest", 0),
                quote_time=datetime.fromisoformat(c["quote_time"]) if c.get("quote_time") else None,
            ))
        
        return OptionChain(
            symbol=data["symbol"],
            underlying_price=data["underlying_price"],
            timestamp=datetime.fromisoformat(data["timestamp"]),
            expirations=[date.fromisoformat(e) for e in data["expirations"]],
            contracts=contracts,
        )


def get_quote_quality_summary(chain: OptionChain, reference_time: datetime) -> dict:
    """
    Get a summary of quote quality for a chain.
    
    Returns:
        Dict with quality metrics
    """
    store = ChainSnapshotStore()
    
    valid_count = 0
    stale_count = 0
    total_spread = 0
    total_oi = 0
    total_volume = 0
    
    for c in chain.contracts:
        if store._is_valid_quote(c):
            valid_count += 1
        if store._is_stale_quote(c, reference_time):
            stale_count += 1
        if c.mid and c.mid > 0:
            total_spread += (c.ask - c.bid) / c.mid
        total_oi += c.open_interest
        total_volume += c.volume
    
    total = len(chain.contracts)
    
    return {
        "contract_count": total,
        "valid_quote_pct": valid_count / total if total > 0 else 0,
        "stale_quote_pct": stale_count / total if total > 0 else 0,
        "avg_spread_pct": total_spread / total * 100 if total > 0 else 0,
        "total_oi": total_oi,
        "total_volume": total_volume,
        "quality_grade": _calculate_quality_grade(valid_count / total if total > 0 else 0),
    }


def _calculate_quality_grade(valid_pct: float) -> str:
    """Calculate letter grade for quote quality."""
    if valid_pct >= 0.95:
        return "A"
    elif valid_pct >= 0.85:
        return "B"
    elif valid_pct >= 0.70:
        return "C"
    elif valid_pct >= 0.50:
        return "D"
    else:
        return "F"
