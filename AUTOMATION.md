# QuantMini Daily Automation

Automated daily data pipeline using macOS launchd.

## Setup Complete ✅

The daily automation is already configured and running! It will execute **every day at 6:00 PM** (18:00).

## What It Does

The daily automation (`scripts/daily_update.sh`) will:
1. Download latest market data for stocks_daily, stocks_minute, and options_daily
2. Enrich the data with calculated features
3. Convert stocks_daily to Qlib binary format (incremental)
4. Log all output to dated log files in `logs/`

## Management Commands

### Check Status
```bash
# Check if the job is loaded
launchctl list | grep quantmini

# View recent logs
tail -f logs/daily_$(date +%Y%m%d)*.log
```

### Test Manually
```bash
# Run the script manually to test
./scripts/daily_update.sh

# Or just run the pipeline directly
source .venv/bin/activate
python -m src.cli.main pipeline daily -t stocks_daily -d 1
```

### Stop/Start/Restart
```bash
# Stop (unload)
launchctl unload ~/Library/LaunchAgents/com.quantmini.daily.plist

# Start (load)
launchctl load ~/Library/LaunchAgents/com.quantmini.daily.plist

# Restart
launchctl unload ~/Library/LaunchAgents/com.quantmini.daily.plist
launchctl load ~/Library/LaunchAgents/com.quantmini.daily.plist
```

### Change Schedule
Edit `~/Library/LaunchAgents/com.quantmini.daily.plist`:
```xml
<key>StartCalendarInterval</key>
<dict>
    <key>Hour</key>
    <integer>18</integer>  <!-- Change this (0-23) -->
    <key>Minute</key>
    <integer>0</integer>   <!-- Change this (0-59) -->
</dict>
```

Then reload:
```bash
launchctl unload ~/Library/LaunchAgents/com.quantmini.daily.plist
launchctl load ~/Library/LaunchAgents/com.quantmini.daily.plist
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

## Files

- **Automation script**: `scripts/daily_update.sh`
- **Launch agent**: `~/Library/LaunchAgents/com.quantmini.daily.plist`
- **Logs**: `logs/daily_YYYYMMDD_HHMMSS.log`
- **Stdout/stderr**: `logs/daily_stdout.log`, `logs/daily_stderr.log`

## Troubleshooting

### Job not running?
```bash
# Check if loaded
launchctl list | grep quantmini

# Check system log for errors
log show --predicate 'subsystem == "com.apple.launchd"' --last 1h | grep quantmini
```

### Test the script manually
```bash
./scripts/daily_update.sh
```

### Check permissions
```bash
# Make sure script is executable
chmod +x scripts/daily_update.sh

# Check file permissions
ls -l ~/Library/LaunchAgents/com.quantmini.daily.plist
```

### Disable temporarily
```bash
launchctl unload ~/Library/LaunchAgents/com.quantmini.daily.plist
```

## Alternative: Cron (Simpler but Less Reliable)

If you prefer cron:
```bash
# Edit crontab
crontab -e

# Add this line (runs at 6 PM daily)
0 18 * * * /Users/zheyuanzhao/workspace/quantmini/scripts/daily_update.sh
```

**Note**: launchd is recommended on macOS as it's more reliable and handles sleep/wake better.

## Log Retention

Logs older than 30 days are automatically cleaned up by the script.
