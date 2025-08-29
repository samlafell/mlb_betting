#!/bin/bash
# 💾 MLB Betting System - Automated Backup & Recovery System
# Docker-based production backup with multiple retention policies and disaster recovery
# Designed for 24/7 operation with zero data loss tolerance

set -e

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
BACKUP_DIR="$PROJECT_ROOT/backups"
LOG_FILE="$PROJECT_ROOT/logs/backup-system.log"

# Load environment variables
if [ -f "$PROJECT_ROOT/.env.production" ]; then
    source "$PROJECT_ROOT/.env.production"
fi

# Default configuration
POSTGRES_CONTAINER="${POSTGRES_CONTAINER:-mlb_postgres_prod}"
REDIS_CONTAINER="${REDIS_CONTAINER:-mlb_redis_prod}"
MLFLOW_CONTAINER="${MLFLOW_CONTAINER:-mlb_mlflow_prod}"

# S3 Configuration (optional)
S3_BUCKET="${S3_BACKUP_BUCKET:-}"
AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-}"
AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-}"

# Retention policies (days)
DAILY_RETENTION=7
WEEKLY_RETENTION=30
MONTHLY_RETENTION=365
CRITICAL_RETENTION=2555  # 7 years for regulatory compliance

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
NC='\033[0m'

# Logging functions
log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

success() {
    echo -e "${GREEN}✅ $1${NC}"
    log "SUCCESS: $1"
}

error() {
    echo -e "${RED}❌ $1${NC}"
    log "ERROR: $1"
}

warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
    log "WARNING: $1"
}

info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
    log "INFO: $1"
}

# Create backup directories
init_backup_dirs() {
    local dirs=(
        "postgres/daily"
        "postgres/weekly" 
        "postgres/monthly"
        "postgres/critical"
        "redis/daily"
        "redis/weekly"
        "redis/monthly"
        "mlflow/daily"
        "mlflow/weekly"
        "mlflow/monthly"
        "system/daily"
        "system/weekly"
        "system/monthly"
    )
    
    for dir in "${dirs[@]}"; do
        mkdir -p "$BACKUP_DIR/$dir"
    done
}

# PostgreSQL backup functions
backup_postgres() {
    local backup_type=$1
    local backup_date=$(date +'%Y%m%d_%H%M%S')
    local backup_file="$BACKUP_DIR/postgres/$backup_type/mlb_betting_${backup_type}_${backup_date}.sql"
    local compressed_file="${backup_file}.gz"
    
    info "Starting PostgreSQL $backup_type backup..."
    
    # Check if container is running
    if ! docker ps --filter "name=$POSTGRES_CONTAINER" --filter "status=running" --quiet | head -n 1; then
        error "PostgreSQL container $POSTGRES_CONTAINER is not running"
        return 1
    fi
    
    # Create backup with transaction-consistent snapshot
    docker exec "$POSTGRES_CONTAINER" pg_dump \
        -U "$POSTGRES_USER" \
        -d "$POSTGRES_DB" \
        --verbose \
        --format=custom \
        --compress=9 \
        --no-owner \
        --no-privileges \
        --single-transaction \
        --create \
        --clean > "$backup_file.custom"
    
    # Also create SQL dump for easier recovery
    docker exec "$POSTGRES_CONTAINER" pg_dump \
        -U "$POSTGRES_USER" \
        -d "$POSTGRES_DB" \
        --verbose \
        --no-owner \
        --no-privileges \
        --single-transaction \
        --create \
        --clean > "$backup_file"
    
    # Compress SQL backup
    gzip -9 "$backup_file"
    
    # Verify backup integrity
    if docker exec "$POSTGRES_CONTAINER" pg_restore --list "$backup_file.custom" > /dev/null 2>&1; then
        success "PostgreSQL $backup_type backup completed: $(basename "$compressed_file")"
        
        # Store backup metadata
        cat > "${compressed_file}.meta" << EOF
backup_type: $backup_type
backup_date: $backup_date
database: $POSTGRES_DB
size: $(stat -f%z "$compressed_file" 2>/dev/null || stat -c%s "$compressed_file")
container: $POSTGRES_CONTAINER
format: sql_compressed
checksum: $(sha256sum "$compressed_file" | cut -d' ' -f1)
EOF
        
        return 0
    else
        error "PostgreSQL backup verification failed"
        rm -f "$backup_file.custom" "$compressed_file"
        return 1
    fi
}

# Redis backup functions
backup_redis() {
    local backup_type=$1
    local backup_date=$(date +'%Y%m%d_%H%M%S')
    local backup_file="$BACKUP_DIR/redis/$backup_type/redis_${backup_type}_${backup_date}.rdb"
    
    info "Starting Redis $backup_type backup..."
    
    # Check if container is running
    if ! docker ps --filter "name=$REDIS_CONTAINER" --filter "status=running" --quiet | head -n 1; then
        error "Redis container $REDIS_CONTAINER is not running"
        return 1
    fi
    
    # Trigger Redis save
    if [ -n "$REDIS_PASSWORD" ]; then
        docker exec "$REDIS_CONTAINER" redis-cli -a "$REDIS_PASSWORD" BGSAVE
    else
        docker exec "$REDIS_CONTAINER" redis-cli BGSAVE
    fi
    
    # Wait for background save to complete
    local save_status=""
    local attempts=0
    while [ "$save_status" != "OK" ] && [ $attempts -lt 60 ]; do
        if [ -n "$REDIS_PASSWORD" ]; then
            save_status=$(docker exec "$REDIS_CONTAINER" redis-cli -a "$REDIS_PASSWORD" LASTSAVE 2>/dev/null || echo "ERROR")
        else
            save_status=$(docker exec "$REDIS_CONTAINER" redis-cli LASTSAVE 2>/dev/null || echo "ERROR")
        fi
        
        if [ "$save_status" != "ERROR" ]; then
            save_status="OK"
        else
            sleep 1
            attempts=$((attempts + 1))
        fi
    done
    
    if [ $attempts -ge 60 ]; then
        error "Redis backup timeout after 60 seconds"
        return 1
    fi
    
    # Copy RDB file from container
    docker cp "$REDIS_CONTAINER:/data/dump.rdb" "$backup_file"
    
    # Compress backup
    gzip -9 "$backup_file"
    local compressed_file="${backup_file}.gz"
    
    if [ -f "$compressed_file" ]; then
        success "Redis $backup_type backup completed: $(basename "$compressed_file")"
        
        # Store backup metadata
        cat > "${compressed_file}.meta" << EOF
backup_type: $backup_type
backup_date: $backup_date
size: $(stat -f%z "$compressed_file" 2>/dev/null || stat -c%s "$compressed_file")
container: $REDIS_CONTAINER
format: rdb_compressed
checksum: $(sha256sum "$compressed_file" | cut -d' ' -f1)
EOF
        
        return 0
    else
        error "Redis backup file not created"
        return 1
    fi
}

# MLflow backup functions
backup_mlflow() {
    local backup_type=$1
    local backup_date=$(date +'%Y%m%d_%H%M%S')
    local backup_file="$BACKUP_DIR/mlflow/$backup_type/mlflow_${backup_type}_${backup_date}.tar.gz"
    
    info "Starting MLflow $backup_type backup..."
    
    # Check if container is running
    if ! docker ps --filter "name=$MLFLOW_CONTAINER" --filter "status=running" --quiet | head -n 1; then
        warning "MLflow container $MLFLOW_CONTAINER is not running, backing up volume only"
    fi
    
    # Backup MLflow artifacts and tracking data
    docker run --rm \
        -v mlflow_artifacts_prod:/source:ro \
        -v "$BACKUP_DIR/mlflow/$backup_type":/backup \
        alpine:latest \
        tar -czf "/backup/mlflow_${backup_type}_${backup_date}.tar.gz" -C /source .
    
    if [ -f "$backup_file" ]; then
        success "MLflow $backup_type backup completed: $(basename "$backup_file")"
        
        # Store backup metadata
        cat > "${backup_file}.meta" << EOF
backup_type: $backup_type
backup_date: $backup_date
size: $(stat -f%z "$backup_file" 2>/dev/null || stat -c%s "$backup_file")
container: $MLFLOW_CONTAINER
format: tar_compressed
checksum: $(sha256sum "$backup_file" | cut -d' ' -f1)
EOF
        
        return 0
    else
        error "MLflow backup file not created"
        return 1
    fi
}

# System configuration backup
backup_system() {
    local backup_type=$1
    local backup_date=$(date +'%Y%m%d_%H%M%S')
    local backup_file="$BACKUP_DIR/system/$backup_type/system_${backup_type}_${backup_date}.tar.gz"
    
    info "Starting system configuration $backup_type backup..."
    
    # Backup system configuration files
    tar -czf "$backup_file" \
        -C "$PROJECT_ROOT" \
        --exclude='logs/*' \
        --exclude='backups/*' \
        --exclude='data/cache/*' \
        --exclude='mlruns/*' \
        --exclude='.git/*' \
        --exclude='__pycache__/*' \
        --exclude='*.pyc' \
        .env.production \
        config.toml \
        docker-compose.production.yml \
        docker/ \
        src/ \
        sql/ \
        scripts/ \
        2>/dev/null || true
    
    if [ -f "$backup_file" ]; then
        success "System configuration $backup_type backup completed: $(basename "$backup_file")"
        
        # Store backup metadata
        cat > "${backup_file}.meta" << EOF
backup_type: $backup_type
backup_date: $backup_date
size: $(stat -f%z "$backup_file" 2>/dev/null || stat -c%s "$backup_file")
format: tar_compressed
checksum: $(sha256sum "$backup_file" | cut -d' ' -f1)
EOF
        
        return 0
    else
        error "System configuration backup failed"
        return 1
    fi
}

# Cloud backup to S3
upload_to_s3() {
    local local_file=$1
    local s3_key=$2
    
    if [ -z "$S3_BUCKET" ] || [ -z "$AWS_ACCESS_KEY_ID" ] || [ -z "$AWS_SECRET_ACCESS_KEY" ]; then
        warning "S3 configuration not complete, skipping cloud backup"
        return 0
    fi
    
    info "Uploading $(basename "$local_file") to S3..."
    
    if command -v aws >/dev/null 2>&1; then
        aws s3 cp "$local_file" "s3://$S3_BUCKET/$s3_key" \
            --storage-class STANDARD_IA \
            --server-side-encryption AES256
        
        if [ $? -eq 0 ]; then
            success "Successfully uploaded to S3: $s3_key"
            # Also upload metadata
            aws s3 cp "${local_file}.meta" "s3://$S3_BUCKET/${s3_key}.meta"
        else
            error "Failed to upload to S3: $s3_key"
            return 1
        fi
    else
        warning "AWS CLI not installed, skipping S3 upload"
    fi
}

# Cleanup old backups based on retention policy
cleanup_old_backups() {
    local backup_path=$1
    local retention_days=$2
    
    info "Cleaning up backups older than $retention_days days in $backup_path"
    
    find "$backup_path" -name "*.gz" -type f -mtime +$retention_days -delete
    find "$backup_path" -name "*.meta" -type f -mtime +$retention_days -delete
    find "$backup_path" -name "*.custom" -type f -mtime +$retention_days -delete
    
    local deleted_count=$(find "$backup_path" -name "*.gz" -type f -mtime +$retention_days 2>/dev/null | wc -l)
    if [ $deleted_count -gt 0 ]; then
        info "Deleted $deleted_count old backup files from $backup_path"
    fi
}

# Restore functions
restore_postgres() {
    local backup_file=$1
    
    if [ ! -f "$backup_file" ]; then
        error "Backup file not found: $backup_file"
        return 1
    fi
    
    warning "This will replace the current database. Are you sure? (y/N)"
    read -r confirmation
    if [[ ! $confirmation =~ ^[Yy]$ ]]; then
        info "Restore cancelled"
        return 0
    fi
    
    info "Restoring PostgreSQL from: $(basename "$backup_file")"
    
    # Stop application services to prevent connections
    docker compose -f "$PROJECT_ROOT/docker-compose.production.yml" stop fastapi data_collector
    
    # Drop and recreate database
    docker exec "$POSTGRES_CONTAINER" psql -U "$POSTGRES_USER" -c "DROP DATABASE IF EXISTS $POSTGRES_DB;"
    docker exec "$POSTGRES_CONTAINER" psql -U "$POSTGRES_USER" -c "CREATE DATABASE $POSTGRES_DB;"
    
    # Restore from backup
    if [[ "$backup_file" == *.custom ]]; then
        # Custom format restore
        docker exec -i "$POSTGRES_CONTAINER" pg_restore \
            -U "$POSTGRES_USER" \
            -d "$POSTGRES_DB" \
            --verbose \
            --clean \
            --no-owner \
            --no-privileges < "$backup_file"
    else
        # SQL format restore
        if [[ "$backup_file" == *.gz ]]; then
            gunzip -c "$backup_file" | docker exec -i "$POSTGRES_CONTAINER" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"
        else
            docker exec -i "$POSTGRES_CONTAINER" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" < "$backup_file"
        fi
    fi
    
    if [ $? -eq 0 ]; then
        success "PostgreSQL restore completed successfully"
        
        # Restart application services
        docker compose -f "$PROJECT_ROOT/docker-compose.production.yml" start fastapi data_collector
        
        # Wait for services to be ready
        sleep 30
        
        # Verify restore
        if curl -f http://localhost:8000/health >/dev/null 2>&1; then
            success "Application services restarted successfully"
        else
            warning "Application services may need manual restart"
        fi
    else
        error "PostgreSQL restore failed"
        return 1
    fi
}

# Main backup execution
perform_backup() {
    local backup_type=$1
    
    echo -e "${PURPLE}🎯 Starting $backup_type backup at $(date)${NC}"
    
    init_backup_dirs
    
    local failed_backups=()
    
    # PostgreSQL backup
    if ! backup_postgres "$backup_type"; then
        failed_backups+=("PostgreSQL")
    fi
    
    # Redis backup
    if ! backup_redis "$backup_type"; then
        failed_backups+=("Redis")
    fi
    
    # MLflow backup
    if ! backup_mlflow "$backup_type"; then
        failed_backups+=("MLflow")
    fi
    
    # System configuration backup
    if ! backup_system "$backup_type"; then
        failed_backups+=("System")
    fi
    
    # Upload to S3 if configured
    if [ -n "$S3_BUCKET" ]; then
        info "Uploading backups to S3..."
        for backup_dir in "$BACKUP_DIR"/*/"$backup_type"; do
            if [ -d "$backup_dir" ]; then
                for backup_file in "$backup_dir"/*.gz; do
                    if [ -f "$backup_file" ]; then
                        local s3_key="mlb-betting/$(date +'%Y/%m/%d')/$(basename "$backup_file")"
                        upload_to_s3 "$backup_file" "$s3_key"
                    fi
                done
            fi
        done
    fi
    
    # Cleanup old backups based on retention policy
    case $backup_type in
        "daily")
            cleanup_old_backups "$BACKUP_DIR/postgres/daily" $DAILY_RETENTION
            cleanup_old_backups "$BACKUP_DIR/redis/daily" $DAILY_RETENTION
            cleanup_old_backups "$BACKUP_DIR/mlflow/daily" $DAILY_RETENTION
            cleanup_old_backups "$BACKUP_DIR/system/daily" $DAILY_RETENTION
            ;;
        "weekly")
            cleanup_old_backups "$BACKUP_DIR/postgres/weekly" $WEEKLY_RETENTION
            cleanup_old_backups "$BACKUP_DIR/redis/weekly" $WEEKLY_RETENTION
            cleanup_old_backups "$BACKUP_DIR/mlflow/weekly" $WEEKLY_RETENTION
            cleanup_old_backups "$BACKUP_DIR/system/weekly" $WEEKLY_RETENTION
            ;;
        "monthly")
            cleanup_old_backups "$BACKUP_DIR/postgres/monthly" $MONTHLY_RETENTION
            cleanup_old_backups "$BACKUP_DIR/redis/monthly" $MONTHLY_RETENTION
            cleanup_old_backups "$BACKUP_DIR/mlflow/monthly" $MONTHLY_RETENTION
            cleanup_old_backups "$BACKUP_DIR/system/monthly" $MONTHLY_RETENTION
            ;;
    esac
    
    # Summary
    if [ ${#failed_backups[@]} -eq 0 ]; then
        success "All $backup_type backups completed successfully"
        echo -e "${GREEN}🎉 Backup operation completed successfully${NC}"
    else
        error "Failed backups: ${failed_backups[*]}"
        echo -e "${RED}⚠️  Some backups failed. Check logs for details.${NC}"
        return 1
    fi
}

# Command line interface
case "${1:-}" in
    "daily")
        perform_backup "daily"
        ;;
    "weekly") 
        perform_backup "weekly"
        ;;
    "monthly")
        perform_backup "monthly"
        ;;
    "restore")
        if [ -z "${2:-}" ]; then
            error "Usage: $0 restore <backup_file>"
            exit 1
        fi
        restore_postgres "$2"
        ;;
    "list")
        echo -e "${BLUE}📋 Available backups:${NC}"
        find "$BACKUP_DIR" -name "*.gz" -type f -exec ls -lh {} \; | sort -k9
        ;;
    "verify")
        if [ -z "${2:-}" ]; then
            error "Usage: $0 verify <backup_file>"
            exit 1
        fi
        # Verify backup integrity
        backup_file="$2"
        if [ -f "${backup_file}.meta" ]; then
            stored_checksum=$(grep "checksum:" "${backup_file}.meta" | cut -d' ' -f2)
            current_checksum=$(sha256sum "$backup_file" | cut -d' ' -f1)
            if [ "$stored_checksum" = "$current_checksum" ]; then
                success "Backup integrity verified: $(basename "$backup_file")"
            else
                error "Backup integrity check failed: $(basename "$backup_file")"
                exit 1
            fi
        else
            warning "No metadata file found, performing basic file check"
            if [ -f "$backup_file" ] && [ -s "$backup_file" ]; then
                success "Backup file exists and is not empty"
            else
                error "Backup file is missing or empty"
                exit 1
            fi
        fi
        ;;
    "status")
        echo -e "${BLUE}📊 Backup System Status:${NC}"
        echo -e "Backup Directory: $BACKUP_DIR"
        echo -e "Total Backup Size: $(du -sh "$BACKUP_DIR" 2>/dev/null | cut -f1 || echo "N/A")"
        echo
        echo -e "${BLUE}Recent Backups:${NC}"
        find "$BACKUP_DIR" -name "*.gz" -type f -mtime -7 -exec ls -lh {} \; | sort -k9 | tail -10
        ;;
    *)
        echo -e "${PURPLE}💾 MLB Betting System - Backup & Recovery${NC}"
        echo
        echo "Usage: $0 {daily|weekly|monthly|restore|list|verify|status}"
        echo
        echo "Commands:"
        echo "  daily     - Perform daily backup (7 day retention)"
        echo "  weekly    - Perform weekly backup (30 day retention)"
        echo "  monthly   - Perform monthly backup (365 day retention)"
        echo "  restore   - Restore from backup file"
        echo "  list      - List available backups"
        echo "  verify    - Verify backup integrity"
        echo "  status    - Show backup system status"
        echo
        echo "Examples:"
        echo "  $0 daily"
        echo "  $0 restore /path/to/backup.sql.gz"
        echo "  $0 verify /path/to/backup.sql.gz"
        echo
        echo "Automated schedule (add to cron):"
        echo "  0 2 * * * $0 daily      # Daily at 2 AM"
        echo "  0 3 * * 0 $0 weekly     # Weekly on Sunday at 3 AM"
        echo "  0 4 1 * * $0 monthly    # Monthly on 1st at 4 AM"
        exit 1
        ;;
esac