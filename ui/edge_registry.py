"""
Edge Registry - Dynamic Edge Discovery for Dashboard

Discovers and loads edge artifacts without hardcoding edge names.
Reads from:
- docs/edges/EDGE_*_v1.md (edge documentation)
- logs/edges/<edge_id>/latest_signals.json (today's signals)
- logs/edges/<edge_id>/latest_snapshot.json (performance snapshot)
"""

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime


@dataclass
class EdgeData:
    """Container for all data related to a single edge."""
    edge_id: str
    version: str = "v1.0"
    status: str = "LOCKED"  # LIVE, LOCKED, RESEARCH
    
    # Documentation (from docs/edges/)
    doc_path: Optional[Path] = None
    doc_content: Optional[str] = None
    
    # Today's signals (from logs/edges/<id>/latest_signals.json)
    signals_path: Optional[Path] = None
    signals: Optional[Dict[str, Any]] = None
    candidate_count: int = 0
    
    # Performance snapshot (from logs/edges/<id>/latest_snapshot.json)
    snapshot_path: Optional[Path] = None
    snapshot: Optional[Dict[str, Any]] = None
    
    # Derived fields
    universe: List[str] = field(default_factory=list)
    regime_gate: Dict[str, Any] = field(default_factory=dict)
    
    def load_documentation(self):
        """Load markdown documentation if available."""
        if self.doc_path and self.doc_path.exists():
            self.doc_content = self.doc_path.read_text()
    
    def load_signals(self):
        """Load latest signals JSON if available."""
        if self.signals_path and self.signals_path.exists():
            try:
                with open(self.signals_path) as f:
                    self.signals = json.load(f)
                self.candidate_count = self.signals.get('candidate_count', 0)
                self.universe = self.signals.get('universe', [])
                self.regime_gate = self.signals.get('regime_gate', {})
            except Exception as e:
                print(f"Warning: Failed to load signals for {self.edge_id}: {e}")
    
    def load_snapshot(self):
        """Load performance snapshot JSON if available."""
        if self.snapshot_path and self.snapshot_path.exists():
            try:
                with open(self.snapshot_path) as f:
                    self.snapshot = json.load(f)
            except Exception as e:
                print(f"Warning: Failed to load snapshot for {self.edge_id}: {e}")
    
    def load_all(self):
        """Load all available data for this edge."""
        self.load_documentation()
        self.load_signals()
        self.load_snapshot()


class EdgeRegistry:
    """
    Registry for discovering and loading edge artifacts.
    
    Discovers edges by scanning:
    1. docs/edges/EDGE_*_v1.md files
    2. logs/edges/*/latest_signals.json files
    
    Usage:
        registry = EdgeRegistry(project_root)
        edges = registry.discover_edges()
        for edge in edges:
            print(f"{edge.edge_id}: {edge.candidate_count} candidates")
    """
    
    def __init__(self, project_root: Path = None):
        if project_root is None:
            # Default to two levels up from this file
            project_root = Path(__file__).parent.parent
        self.project_root = Path(project_root)
        self.docs_dir = self.project_root / "docs" / "edges"
        self.logs_dir = self.project_root / "logs" / "edges"
        self._edges: Dict[str, EdgeData] = {}
    
    def discover_edges(self) -> List[EdgeData]:
        """
        Discover all available edges and load their data.
        
        Returns list of EdgeData sorted by edge_id.
        """
        self._edges = {}
        
        # 1. Discover from docs/edges/EDGE_*_v1.md
        if self.docs_dir.exists():
            for doc_file in self.docs_dir.glob("EDGE_*_v1.md"):
                # Extract edge_id from filename: EDGE_FLAT_v1.md -> flat
                match = re.match(r'EDGE_([A-Z]+)_v\d+\.md', doc_file.name)
                if match:
                    edge_id = match.group(1).lower()
                    if edge_id not in self._edges:
                        self._edges[edge_id] = EdgeData(edge_id=edge_id)
                    self._edges[edge_id].doc_path = doc_file
        
        # 2. Discover from logs/edges/*/latest_signals.json
        if self.logs_dir.exists():
            for signals_file in self.logs_dir.glob("*/latest_signals.json"):
                edge_id = signals_file.parent.name.lower()
                if edge_id not in self._edges:
                    self._edges[edge_id] = EdgeData(edge_id=edge_id)
                self._edges[edge_id].signals_path = signals_file
                
                # Also check for snapshot
                snapshot_file = signals_file.parent / "latest_snapshot.json"
                if snapshot_file.exists():
                    self._edges[edge_id].snapshot_path = snapshot_file
        
        # 3. Load all data
        for edge in self._edges.values():
            edge.load_all()
        
        return sorted(self._edges.values(), key=lambda e: e.edge_id)
    
    def get_edge(self, edge_id: str) -> Optional[EdgeData]:
        """Get a specific edge by ID."""
        return self._edges.get(edge_id.lower())
    
    def get_total_candidates(self) -> int:
        """Get total candidate count across all edges."""
        return sum(e.candidate_count for e in self._edges.values())
    
    def get_active_edges(self) -> List[EdgeData]:
        """Get edges that have candidates today."""
        return [e for e in self._edges.values() if e.candidate_count > 0]


def get_edge_registry(project_root: Path = None) -> EdgeRegistry:
    """Factory function to create and initialize edge registry."""
    registry = EdgeRegistry(project_root)
    registry.discover_edges()
    return registry


# For quick testing
if __name__ == '__main__':
    registry = get_edge_registry()
    edges = registry.discover_edges()
    
    print(f"Discovered {len(edges)} edges:")
    for edge in edges:
        print(f"\n  {edge.edge_id.upper()} ({edge.version})")
        print(f"    Doc: {'✓' if edge.doc_content else '✗'}")
        print(f"    Signals: {edge.candidate_count} candidates")
        print(f"    Snapshot: {'✓' if edge.snapshot else '✗'}")
        if edge.snapshot:
            p1 = edge.snapshot.get('phase1', {})
            print(f"    Phase1: {p1.get('trades')} trades, {p1.get('wr', 0)*100:.1f}% WR")
