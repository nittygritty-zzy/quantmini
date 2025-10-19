# QuantMini Automation

Automated data pipelines using macOS launchd.

## Active Automations ✅

### Daily Data Updates
**Schedule**: Every day at 6:00 PM (18:00)
**Script**: `scripts/daily_update.sh`
**Purpose**: Download and process daily market data

### Weekly Delisted Stocks Updates
**Schedule**: Every Sunday at 2:00 AM
**Script**: `scripts/weekly_update.sh`
**Purpose**: Download delisted stocks to fix survivorship bias
**Documentation**: `docs/DELISTED_STOCKS.md`

## What It Does

### Daily Automation
The daily automation (`scripts/daily_update.sh`) will:
1. Download latest market data (landing layer)
2. Ingest to bronze layer (validated Parquet)
3. Enrich to silver layer (calculated features)
4. Convert stocks_daily to gold layer (Qlib binary format, incremental)
5. Log all output to dated log files in `logs/`

### Weekly Automation
The weekly automation (`scripts/weekly_update.sh`) will:
1. Query Polygon API for stocks delisted in the last 90 days
2. Download historical OHLCV data to bronze layer
3. Enrich to silver layer
4. Convert to gold layer (Qlib binary format, incremental)
5. Log all output to `logs/weekly_*.log`

This fixes **survivorship bias** by ensuring delisted stocks are included in backtests.

## Management Commands

### Check Status
```bash
# Check both automations
launchctl list | grep quantmini

# View recent daily logs
tail -f logs/daily_$(date +%Y%m%d)*.log

# View recent weekly logs
tail -f logs/weekly_*.log
```

### Test Manually
```bash
# Daily update
./scripts/daily_update.sh

# Weekly update
./scripts/weekly_update.sh
```

### Stop/Start/Restart

**Daily automation**:
```bash
# Stop
launchctl unload ~/Library/LaunchAgents/com.quantmini.daily.plist

# Start
launchctl load ~/Library/LaunchAgents/com.quantmini.daily.plist

# Restart
launchctl unload ~/Library/LaunchAgents/com.quantmini.daily.plist
launchctl load ~/Library/LaunchAgents/com.quantmini.daily.plist
```

**Weekly automation**:
```bash
# Stop
launchctl unload ~/Library/LaunchAgents/com.quantmini.weekly.plist

# Start
launchctl load ~/Library/LaunchAgents/com.quantmini.weekly.plist

# Restart
launchctl unload ~/Library/LaunchAgents/com.quantmini.weekly.plist
launchctl load ~/Library/LaunchAgents/com.quantmini.weekly.plist
```

### Change Schedule

**Daily automation** - Edit `~/Library/LaunchAgents/com.quantmini.daily.plist`:
```xml
<key>StartCalendarInterval</key>
<dict>
    <key>Hour</key>
    <integer>18</integer>  <!-- Change this (0-23) -->
    <key>Minute</key>
    <integer>0</integer>   <!-- Change this (0-59) -->
</dict>
```

**Weekly automation** - Edit `~/Library/LaunchAgents/com.quantmini.weekly.plist`:
```xml
<key>StartCalendarInterval</key>
<dict>
    <key>Weekday</key>
    <integer>0</integer>   <!-- 0=Sunday, 1=Monday, ..., 6=Saturday -->
    <key>Hour</key>
    <integer>2</integer>   <!-- Change this (0-23) -->
    <key>Minute</key>
    <integer>0</integer>   <!-- Change this (0-59) -->
</dict>
```

Then reload the modified agent:
```bash
# For daily
launchctl unload ~/Library/LaunchAgents/com.quantmini.daily.plist
launchctl load ~/Library/LaunchAgents/com.quantmini.daily.plist

# For weekly
launchctl unload ~/Library/LaunchAgents/com.quantmini.weekly.plist
launchctl load ~/Library/LaunchAgents/com.quantmini.weekly.plist
```

### Run Multiple Times Per Day
Edit the plist file to use an array:
```xml
<key>StartCalendarInterval</key>
<array>
    <dict>
        <key>Hour</key>
        <integer>6</integer>
        <key>Minute</key>
        <integer>0</integer>
    </dict>
    <dict>
        <key>Hour</key>
        <integer>18</integer>
        <key>Minute</key>
        <integer>0</integer>
    </dict>
</array>
```

### View Logs
```bash
# Today's logs
ls -lh logs/daily_$(date +%Y%m%d)*.log

# All recent logs
ls -lht logs/daily_*.log | head -10

# Watch live
tail -f logs/daily_*.log
```

## Setup

### Daily Automation (Already Configured)
The daily automation is already set up. See management commands above.

### Weekly Automation (One-Time Setup)
Run the setup script to install the weekly automation:
```bash
./scripts/setup_weekly_automation.sh
```

This will:
1. Create `~/Library/LaunchAgents/com.quantmini.weekly.plist`
2. Load the LaunchAgent
3. Verify it's running

## Files

### Daily Automation
- **Script**: `scripts/daily_update.sh`
- **LaunchAgent**: `~/Library/LaunchAgents/com.quantmini.daily.plist`
- **Logs**: `logs/daily_YYYYMMDD_HHMMSS.log`
- **Stdout/stderr**: `logs/daily_stdout.log`, `logs/daily_stderr.log`

### Weekly Automation
- **Script**: `scripts/weekly_update.sh`
- **LaunchAgent**: `~/Library/LaunchAgents/com.quantmini.weekly.plist`
- **Setup script**: `scripts/setup_weekly_automation.sh`
- **Logs**: `logs/weekly_YYYYMMDD_HHMMSS.log`
- **Stdout/stderr**: `logs/weekly_stdout.log`, `logs/weekly_stderr.log`
- **Downloaded data**: `data/bronze/stocks_daily/`, `data/delisted_stocks.csv`

## Troubleshooting

### Jobs not running?
```bash
# Check which jobs are loaded
launchctl list | grep quantmini

# Check system log for errors
log show --predicate 'subsystem == "com.apple.launchd"' --last 1h | grep quantmini
```

### Test scripts manually
```bash
# Test daily update
./scripts/daily_update.sh

# Test weekly update
./scripts/weekly_update.sh
```

### Check permissions
```bash
# Make sure scripts are executable
chmod +x scripts/daily_update.sh
chmod +x scripts/weekly_update.sh

# Check LaunchAgent permissions
ls -l ~/Library/LaunchAgents/com.quantmini.*.plist
```

### Weekly automation not finding delisted stocks?
```bash
# Check POLYGON_API_KEY is set
echo $POLYGON_API_KEY

# Or check in the plist file
grep POLYGON_API_KEY ~/Library/LaunchAgents/com.quantmini.weekly.plist

# Test the download script directly
python scripts/download_delisted_stocks.py --start-date 2024-01-01 --end-date $(date +%Y-%m-%d) --skip-download
```

### Disable temporarily
```bash
# Daily automation
launchctl unload ~/Library/LaunchAgents/com.quantmini.daily.plist

# Weekly automation
launchctl unload ~/Library/LaunchAgents/com.quantmini.weekly.plist
```

## Alternative: Cron (Simpler but Less Reliable)

If you prefer cron over LaunchAgent:
```bash
# Edit crontab
crontab -e

# Add these lines
0 18 * * * /Users/zheyuanzhao/workspace/quantmini/scripts/daily_update.sh    # Daily at 6 PM
0 2 * * 0 /Users/zheyuanzhao/workspace/quantmini/scripts/weekly_update.sh    # Sunday at 2 AM
```

**Note**: LaunchAgent is recommended on macOS as it's more reliable, handles sleep/wake better, and integrates with system logging.

## Log Retention

Logs older than 30 days are automatically cleaned up by the script.
