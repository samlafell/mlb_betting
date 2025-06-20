#!/bin/bash

# 🛡️ AUTOMATED DATABASE BACKUP SYSTEM
# Prevents catastrophic data loss by creating timestamped backups

set -e  # Exit on any error

# Configuration
DB_PATH="data/raw/mlb_betting.duckdb"
BACKUP_DIR="backups/database"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
BACKUP_FILE="${BACKUP_DIR}/mlb_betting_${TIMESTAMP}.duckdb"

# Create backup directory if it doesn't exist
mkdir -p "$BACKUP_DIR"

# Check if database exists
if [ ! -f "$DB_PATH" ]; then
    echo "❌ ERROR: Database file not found at $DB_PATH"
    exit 1
fi

# Get database size
DB_SIZE=$(stat -f%z "$DB_PATH" 2>/dev/null || stat -c%s "$DB_PATH" 2>/dev/null)
DB_SIZE_MB=$((DB_SIZE / 1024 / 1024))

# Only backup if database has meaningful data (> 1MB)
if [ "$DB_SIZE" -lt 1048576 ]; then
    echo "⚠️  WARNING: Database is only ${DB_SIZE} bytes - skipping backup of potentially empty database"
    exit 1
fi

echo "�� Creating database backup..."
echo "   Source: $DB_PATH (${DB_SIZE_MB}MB)"
echo "   Backup: $BACKUP_FILE"

# Create backup
cp "$DB_PATH" "$BACKUP_FILE"

# Verify backup
if [ -f "$BACKUP_FILE" ]; then
    BACKUP_SIZE=$(stat -f%z "$BACKUP_FILE" 2>/dev/null || stat -c%s "$BACKUP_FILE" 2>/dev/null)
    if [ "$BACKUP_SIZE" -eq "$DB_SIZE" ]; then
        echo "✅ Backup created successfully: $BACKUP_FILE"
    else
        echo "❌ ERROR: Backup size mismatch!"
        exit 1
    fi
else
    echo "❌ ERROR: Backup failed!"
    exit 1
fi

# Clean up old backups (keep last 10)
echo "🧹 Cleaning up old backups (keeping last 10)..."
ls -t "${BACKUP_DIR}"/mlb_betting_*.duckdb | tail -n +11 | xargs rm -f 2>/dev/null || true

# Show remaining backups
BACKUP_COUNT=$(ls "${BACKUP_DIR}"/mlb_betting_*.duckdb 2>/dev/null | wc -l)
echo "📊 Total backups: $BACKUP_COUNT"

echo "🎉 Database backup completed successfully!"
