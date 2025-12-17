#!/bin/bash
#
# VolMachine Scheduled Scans
# 
# This script sets up cron jobs for twice-daily scans.
# Run: ./scripts/schedule_setup.sh to install cron jobs.
#
# Schedule:
#   - OPEN:  09:45 ET (market open + 15 min)
#   - CLOSE: 15:30 ET (30 min before close)

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

echo "VolMachine Scheduler Setup"
echo "=========================="
echo "Project: $PROJECT_DIR"
echo ""

# Show existing cron jobs
echo "Current crontab:"
crontab -l 2>/dev/null || echo "  (none)"
echo ""

# Generate cron entries
cat << EOF

Add these lines to your crontab (run: crontab -e):

# VolMachine: OPEN session (09:45 ET = 14:45 UTC)
45 9 * * 1-5 cd $PROJECT_DIR && /usr/bin/python3 scripts/run_daily.py --session open >> logs/cron.log 2>&1

# VolMachine: CLOSE session (15:30 ET = 20:30 UTC)  
30 15 * * 1-5 cd $PROJECT_DIR && /usr/bin/python3 scripts/run_daily.py --session close >> logs/cron.log 2>&1

EOF

echo ""
echo "Or run manually:"
echo "  python3 scripts/run_daily.py --session open"
echo "  python3 scripts/run_daily.py --session close"
