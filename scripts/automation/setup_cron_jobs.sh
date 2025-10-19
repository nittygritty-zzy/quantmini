#!/bin/bash
# Setup Weekly Automation for QuantMini
# Installs macOS LaunchAgent for weekly delisted stocks updates

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
PLIST_DEST="$HOME/Library/LaunchAgents/com.quantmini.weekly.plist"

echo "========================================="
echo "QuantMini Weekly Automation Setup"
echo "========================================="
echo ""

# Check if weekly_update.sh exists and is executable
if [ ! -f "$PROJECT_DIR/scripts/weekly_update.sh" ]; then
    echo "❌ Error: weekly_update.sh not found"
    exit 1
fi

if [ ! -x "$PROJECT_DIR/scripts/weekly_update.sh" ]; then
    echo "Making weekly_update.sh executable..."
    chmod +x "$PROJECT_DIR/scripts/weekly_update.sh"
fi

# Create LaunchAgents directory if it doesn't exist
mkdir -p "$HOME/Library/LaunchAgents"

# Unload existing agent if loaded
if launchctl list | grep -q "com.quantmini.weekly"; then
    echo "Unloading existing weekly agent..."
    launchctl unload "$PLIST_DEST" 2>/dev/null || true
fi

# Create plist file
echo "Creating LaunchAgent configuration..."
cat > "$PLIST_DEST" << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.quantmini.weekly</string>

    <key>ProgramArguments</key>
    <array>
        <string>PROJECT_DIR_PLACEHOLDER/scripts/weekly_update.sh</string>
    </array>

    <key>StartCalendarInterval</key>
    <dict>
        <key>Weekday</key>
        <integer>0</integer>  <!-- 0 = Sunday -->
        <key>Hour</key>
        <integer>2</integer>   <!-- 2 AM -->
        <key>Minute</key>
        <integer>0</integer>
    </dict>

    <key>StandardOutPath</key>
    <string>PROJECT_DIR_PLACEHOLDER/logs/weekly_stdout.log</string>

    <key>StandardErrorPath</key>
    <string>PROJECT_DIR_PLACEHOLDER/logs/weekly_stderr.log</string>

    <key>RunAtLoad</key>
    <false/>

    <key>EnvironmentVariables</key>
    <dict>
        <key>PATH</key>
        <string>/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin</string>
        <key>POLYGON_API_KEY</key>
        <string>POLYGON_KEY_PLACEHOLDER</string>
    </dict>
</dict>
</plist>
EOF

# Replace placeholders
sed -i '' "s|PROJECT_DIR_PLACEHOLDER|$PROJECT_DIR|g" "$PLIST_DEST"

# Get POLYGON_API_KEY from environment or .env
if [ -n "$POLYGON_API_KEY" ]; then
    sed -i '' "s|POLYGON_KEY_PLACEHOLDER|$POLYGON_API_KEY|g" "$PLIST_DEST"
elif [ -f "$PROJECT_DIR/.env" ] && grep -q "POLYGON_API_KEY" "$PROJECT_DIR/.env"; then
    API_KEY=$(grep "POLYGON_API_KEY" "$PROJECT_DIR/.env" | cut -d '=' -f2 | tr -d ' "' | tr -d "'")
    sed -i '' "s|POLYGON_KEY_PLACEHOLDER|$API_KEY|g" "$PLIST_DEST"
else
    echo "⚠️  Warning: POLYGON_API_KEY not found. You'll need to add it manually to $PLIST_DEST"
    sed -i '' "s|POLYGON_KEY_PLACEHOLDER|YOUR_KEY_HERE|g" "$PLIST_DEST"
fi

# Load the agent
echo "Loading LaunchAgent..."
launchctl load "$PLIST_DEST"

# Verify it's loaded
sleep 1
if launchctl list | grep -q "com.quantmini.weekly"; then
    echo ""
    echo "✅ Weekly automation setup complete!"
    echo ""
    echo "Schedule: Every Sunday at 2:00 AM"
    echo "Script: $PROJECT_DIR/scripts/weekly_update.sh"
    echo "Logs: $PROJECT_DIR/logs/weekly_*.log"
    echo ""
    echo "Management Commands:"
    echo "  Check status:  launchctl list | grep quantmini.weekly"
    echo "  Stop:          launchctl unload ~/Library/LaunchAgents/com.quantmini.weekly.plist"
    echo "  Start:         launchctl load ~/Library/LaunchAgents/com.quantmini.weekly.plist"
    echo "  Test manually: $PROJECT_DIR/scripts/weekly_update.sh"
    echo ""
else
    echo ""
    echo "⚠️  Warning: Agent loaded but not showing in list"
    echo "This is normal on first install. It will run on schedule."
    echo ""
fi

echo "========================================="
