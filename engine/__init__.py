"""
Engine package - Main orchestration and reporting.
"""

from engine.engine import VolMachineEngine
from engine.decision import (
    create_trade_candidate,
    format_candidate_summary,
    candidate_to_dict,
)
from engine.logger import StructuredLogger, get_logger
from engine.report import (
    generate_markdown_report,
    generate_html_report,
    save_report,
    create_daily_report,
)


__all__ = [
    'VolMachineEngine',
    'create_trade_candidate',
    'format_candidate_summary',
    'candidate_to_dict',
    'StructuredLogger',
    'get_logger',
    'generate_markdown_report',
    'generate_html_report',
    'save_report',
    'create_daily_report',
]
