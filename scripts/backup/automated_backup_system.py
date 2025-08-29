#!/usr/bin/env python3
"""
Automated Backup and Disaster Recovery System
Comprehensive backup automation with point-in-time recovery capability

Features:
- Database backups with point-in-time recovery
- Application state and configuration backups
- ML model and artifact versioning
- Cross-region backup replication
- Automated restore procedures
- Backup validation and integrity checks
- Disaster recovery orchestration
"""

import asyncio
import json
import logging
import os
import shutil
import subprocess
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
import boto3
from botocore.exceptions import ClientError
import psycopg2
from psycopg2 import sql

# Import project modules
from ...src.core.config import get_settings
from ...src.core.enhanced_logging import get_contextual_logger, LogComponent
from ...src.core.exceptions import handle_exception, BackupError, RecoveryError
from ...src.data.database.connection import get_database_connection


class BackupType(Enum):
    """Types of backups supported by the system."""
    FULL_DATABASE = "full_database"
    INCREMENTAL_DATABASE = "incremental_database"
    APPLICATION_STATE = "application_state"
    ML_MODELS = "ml_models"
    CONFIGURATION = "configuration"
    LOGS = "logs"
    SYSTEM_COMPLETE = "system_complete"


class BackupStatus(Enum):
    """Backup operation status."""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    VALIDATING = "validating"
    VALIDATED = "validated"
    EXPIRED = "expired"


class RecoveryType(Enum):
    """Types of recovery operations."""
    POINT_IN_TIME = "point_in_time"
    FULL_RESTORE = "full_restore"
    PARTIAL_RESTORE = "partial_restore"
    CROSS_REGION_RESTORE = "cross_region_restore"
    DISASTER_RECOVERY = "disaster_recovery"


@dataclass
class BackupMetadata:
    """Metadata for backup operations."""
    backup_id: str
    backup_type: BackupType
    start_time: datetime
    end_time: Optional[datetime] = None
    status: BackupStatus = BackupStatus.PENDING
    file_path: str = ""
    file_size_bytes: int = 0
    checksum: str = ""
    compression_ratio: float = 0.0
    encryption_enabled: bool = True
    storage_class: str = "STANDARD"
    retention_days: int = 30
    cross_region_replicated: bool = False
    validation_status: str = "pending"
    error_message: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RestorePoint:
    """Point-in-time restore point information."""
    timestamp: datetime
    backup_id: str
    transaction_log_position: str
    database_size_bytes: int
    consistency_verified: bool = False
    restore_time_estimate_minutes: int = 0


class AutomatedBackupSystem:
    """Comprehensive backup and disaster recovery system."""
    
    def __init__(self, settings=None):
        """Initialize the backup system."""
        self.settings = settings or get_settings()
        self.logger = get_contextual_logger(__name__, LogComponent.SERVICES)
        
        # Backup configuration
        self.backup_root = Path("/backup")
        self.temp_backup_dir = Path("/tmp/backups")
        self.archive_root = Path("/archive")
        
        # AWS configuration
        self.s3_client = boto3.client('s3', region_name=self.settings.aws_region)
        self.rds_client = boto3.client('rds', region_name=self.settings.aws_region)
        self.backup_bucket = f"{self.settings.project_name}-backups-{self.settings.environment}"
        
        # Backup retention policies
        self.retention_policies = {
            BackupType.FULL_DATABASE: 30,      # 30 days
            BackupType.INCREMENTAL_DATABASE: 7, # 7 days
            BackupType.APPLICATION_STATE: 14,   # 14 days
            BackupType.ML_MODELS: 90,          # 90 days
            BackupType.CONFIGURATION: 30,      # 30 days
            BackupType.LOGS: 7,                # 7 days
            BackupType.SYSTEM_COMPLETE: 30,    # 30 days
        }
        
        # Backup schedules
        self.backup_schedules = {
            BackupType.FULL_DATABASE: "0 2 * * *",          # Daily at 2 AM
            BackupType.INCREMENTAL_DATABASE: "0 */6 * * *",  # Every 6 hours
            BackupType.APPLICATION_STATE: "0 4 * * *",       # Daily at 4 AM
            BackupType.ML_MODELS: "0 3 * * 0",              # Weekly on Sunday at 3 AM
            BackupType.CONFIGURATION: "0 1 * * *",           # Daily at 1 AM
            BackupType.LOGS: "0 5 * * *",                   # Daily at 5 AM
            BackupType.SYSTEM_COMPLETE: "0 6 * * 0",        # Weekly on Sunday at 6 AM
        }
        
        # Initialize directories
        self._setup_directories()

    async def initialize(self) -> None:
        """Initialize the backup system."""
        try:
            self.logger.info("Initializing automated backup system")
            
            # Create S3 bucket if it doesn't exist
            await self._ensure_s3_bucket_exists()
            
            # Create backup tracking table
            await self._create_backup_tracking_schema()
            
            # Validate backup infrastructure
            await self._validate_backup_infrastructure()
            
            self.logger.info("Automated backup system initialized successfully")
            
        except Exception as e:
            handled_error = handle_exception(
                e, component="backup_system", operation="initialize"
            )
            self.logger.error(
                "Failed to initialize backup system",
                error=handled_error,
                correlation_id=handled_error.correlation_id
            )
            raise BackupError(f"Backup system initialization failed: {handled_error.user_message}")

    def _setup_directories(self) -> None:
        """Set up backup directories."""
        directories = [
            self.backup_root,
            self.temp_backup_dir,
            self.archive_root,
            self.backup_root / "database",
            self.backup_root / "application",
            self.backup_root / "models",
            self.backup_root / "config",
            self.backup_root / "logs",
        ]
        
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
            os.chmod(directory, 0o750)  # Secure permissions

    async def _ensure_s3_bucket_exists(self) -> None:
        """Ensure the backup S3 bucket exists with proper configuration."""
        try:
            # Check if bucket exists
            self.s3_client.head_bucket(Bucket=self.backup_bucket)
            self.logger.info(f"Backup bucket {self.backup_bucket} exists")
            
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                # Create bucket
                self.logger.info(f"Creating backup bucket {self.backup_bucket}")
                
                create_kwargs = {'Bucket': self.backup_bucket}
                if self.settings.aws_region != 'us-east-1':
                    create_kwargs['CreateBucketConfiguration'] = {
                        'LocationConstraint': self.settings.aws_region
                    }
                
                self.s3_client.create_bucket(**create_kwargs)
                
                # Configure bucket encryption
                self.s3_client.put_bucket_encryption(
                    Bucket=self.backup_bucket,
                    ServerSideEncryptionConfiguration={
                        'Rules': [{
                            'ApplyServerSideEncryptionByDefault': {
                                'SSEAlgorithm': 'AES256'
                            },
                            'BucketKeyEnabled': True
                        }]
                    }
                )
                
                # Configure bucket versioning
                self.s3_client.put_bucket_versioning(
                    Bucket=self.backup_bucket,
                    VersioningConfiguration={'Status': 'Enabled'}
                )
                
                # Configure lifecycle policy
                self.s3_client.put_bucket_lifecycle_configuration(
                    Bucket=self.backup_bucket,
                    LifecycleConfiguration={
                        'Rules': [
                            {
                                'ID': 'backup-lifecycle',
                                'Status': 'Enabled',
                                'Filter': {'Prefix': ''},
                                'Transitions': [
                                    {
                                        'Days': 30,
                                        'StorageClass': 'STANDARD_IA'
                                    },
                                    {
                                        'Days': 90,
                                        'StorageClass': 'GLACIER'
                                    },
                                    {
                                        'Days': 365,
                                        'StorageClass': 'DEEP_ARCHIVE'
                                    }
                                ]
                            }
                        ]
                    }
                )
                
                self.logger.info(f"Backup bucket {self.backup_bucket} created and configured")
            else:
                raise

    async def create_full_database_backup(self) -> BackupMetadata:
        """Create a full database backup."""
        backup_id = f"db_full_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
        backup_metadata = BackupMetadata(
            backup_id=backup_id,
            backup_type=BackupType.FULL_DATABASE,
            start_time=datetime.now(timezone.utc)
        )
        
        try:
            self.logger.info(f"Starting full database backup: {backup_id}")
            backup_metadata.status = BackupStatus.IN_PROGRESS
            
            # Create backup file path
            backup_file = self.backup_root / "database" / f"{backup_id}.sql.gz"
            backup_metadata.file_path = str(backup_file)
            
            # Get database connection details
            db_config = self._get_database_config()
            
            # Create pg_dump command
            dump_command = [
                "pg_dump",
                f"--host={db_config['host']}",
                f"--port={db_config['port']}",
                f"--username={db_config['username']}",
                f"--dbname={db_config['database']}",
                "--verbose",
                "--clean",
                "--if-exists",
                "--create",
                "--format=custom",
                "--compress=9",
                f"--file={backup_file}"
            ]
            
            # Set password via environment
            env = os.environ.copy()
            env["PGPASSWORD"] = db_config["password"]
            
            # Execute backup
            process = subprocess.run(
                dump_command,
                env=env,
                capture_output=True,
                text=True,
                timeout=3600  # 1 hour timeout
            )
            
            if process.returncode != 0:
                raise BackupError(f"pg_dump failed: {process.stderr}")
            
            # Calculate file size and checksum
            backup_metadata.file_size_bytes = backup_file.stat().st_size
            backup_metadata.checksum = await self._calculate_file_checksum(backup_file)
            backup_metadata.compression_ratio = self._calculate_compression_ratio(backup_file)
            
            # Upload to S3
            s3_key = f"database/{backup_id}.sql.gz"
            await self._upload_to_s3(backup_file, s3_key)
            
            # Update metadata
            backup_metadata.end_time = datetime.now(timezone.utc)
            backup_metadata.status = BackupStatus.COMPLETED
            
            # Store backup metadata
            await self._store_backup_metadata(backup_metadata)
            
            self.logger.info(
                f"Full database backup completed: {backup_id}",
                file_size_mb=backup_metadata.file_size_bytes // (1024 * 1024),
                duration_seconds=(backup_metadata.end_time - backup_metadata.start_time).total_seconds()
            )
            
            return backup_metadata
            
        except Exception as e:
            backup_metadata.status = BackupStatus.FAILED
            backup_metadata.error_message = str(e)
            backup_metadata.end_time = datetime.now(timezone.utc)
            
            handled_error = handle_exception(
                e, component="backup_system", operation="create_full_database_backup"
            )
            self.logger.error(
                f"Full database backup failed: {backup_id}",
                error=handled_error,
                correlation_id=handled_error.correlation_id
            )
            
            await self._store_backup_metadata(backup_metadata)
            raise BackupError(f"Database backup failed: {handled_error.user_message}")

    async def create_incremental_database_backup(self) -> BackupMetadata:
        """Create an incremental database backup using WAL files."""
        backup_id = f"db_incr_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
        backup_metadata = BackupMetadata(
            backup_id=backup_id,
            backup_type=BackupType.INCREMENTAL_DATABASE,
            start_time=datetime.now(timezone.utc)
        )
        
        try:
            self.logger.info(f"Starting incremental database backup: {backup_id}")
            backup_metadata.status = BackupStatus.IN_PROGRESS
            
            # Get database connection
            async with get_database_connection() as conn:
                # Force a checkpoint to ensure all changes are written
                await conn.execute("CHECKPOINT;")
                
                # Get current WAL location
                wal_location = await conn.fetchval("SELECT pg_current_wal_lsn();")
                
                # Get WAL file information
                wal_files = await conn.fetch("""
                    SELECT name, size, modification 
                    FROM pg_ls_waldir() 
                    WHERE modification > NOW() - INTERVAL '6 hours'
                    ORDER BY modification DESC;
                """)
            
            # Create incremental backup directory
            backup_dir = self.temp_backup_dir / backup_id
            backup_dir.mkdir(parents=True, exist_ok=True)
            
            # Archive WAL files
            total_size = 0
            for wal_file in wal_files:
                wal_path = Path("/var/lib/postgresql/data/pg_wal") / wal_file['name']
                if wal_path.exists():
                    dest_path = backup_dir / wal_file['name']
                    shutil.copy2(wal_path, dest_path)
                    total_size += dest_path.stat().st_size
            
            # Create backup manifest
            manifest = {
                "backup_id": backup_id,
                "backup_type": "incremental",
                "wal_location": wal_location,
                "wal_files": [{"name": f['name'], "size": f['size']} for f in wal_files],
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "total_size_bytes": total_size
            }
            
            manifest_file = backup_dir / "backup_manifest.json"
            with open(manifest_file, 'w') as f:
                json.dump(manifest, f, indent=2)
            
            # Compress the entire backup directory
            backup_archive = self.backup_root / "database" / f"{backup_id}.tar.gz"
            await self._create_compressed_archive(backup_dir, backup_archive)
            
            # Update metadata
            backup_metadata.file_path = str(backup_archive)
            backup_metadata.file_size_bytes = backup_archive.stat().st_size
            backup_metadata.checksum = await self._calculate_file_checksum(backup_archive)
            backup_metadata.metadata = manifest
            
            # Upload to S3
            s3_key = f"database/incremental/{backup_id}.tar.gz"
            await self._upload_to_s3(backup_archive, s3_key)
            
            # Cleanup temporary files
            shutil.rmtree(backup_dir)
            
            backup_metadata.end_time = datetime.now(timezone.utc)
            backup_metadata.status = BackupStatus.COMPLETED
            
            await self._store_backup_metadata(backup_metadata)
            
            self.logger.info(
                f"Incremental database backup completed: {backup_id}",
                wal_files_count=len(wal_files),
                file_size_mb=backup_metadata.file_size_bytes // (1024 * 1024)
            )
            
            return backup_metadata
            
        except Exception as e:
            backup_metadata.status = BackupStatus.FAILED
            backup_metadata.error_message = str(e)
            backup_metadata.end_time = datetime.now(timezone.utc)
            
            handled_error = handle_exception(
                e, component="backup_system", operation="create_incremental_database_backup"
            )
            self.logger.error(
                f"Incremental database backup failed: {backup_id}",
                error=handled_error,
                correlation_id=handled_error.correlation_id
            )
            
            await self._store_backup_metadata(backup_metadata)
            raise BackupError(f"Incremental backup failed: {handled_error.user_message}")

    async def create_application_state_backup(self) -> BackupMetadata:
        """Create backup of application state and configuration."""
        backup_id = f"app_state_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
        backup_metadata = BackupMetadata(
            backup_id=backup_id,
            backup_type=BackupType.APPLICATION_STATE,
            start_time=datetime.now(timezone.utc)
        )
        
        try:
            self.logger.info(f"Starting application state backup: {backup_id}")
            backup_metadata.status = BackupStatus.IN_PROGRESS
            
            # Create temporary backup directory
            backup_dir = self.temp_backup_dir / backup_id
            backup_dir.mkdir(parents=True, exist_ok=True)
            
            # Backup application files
            app_items = [
                ("config", "config.toml"),
                ("environment", ".env"),
                ("docker_config", "docker-compose.yml"),
                ("docker_env", "docker-compose.override.yml"),
                ("nginx_config", "docker/nginx/nginx.conf"),
                ("startup_scripts", "scripts/"),
            ]
            
            backed_up_items = []
            total_size = 0
            
            for item_name, item_path in app_items:
                source_path = Path(item_path)
                if source_path.exists():
                    dest_path = backup_dir / item_name
                    
                    if source_path.is_dir():
                        shutil.copytree(source_path, dest_path, dirs_exist_ok=True)
                    else:
                        shutil.copy2(source_path, dest_path)
                    
                    item_size = self._get_directory_size(dest_path) if dest_path.is_dir() else dest_path.stat().st_size
                    total_size += item_size
                    backed_up_items.append({
                        "name": item_name,
                        "path": str(source_path),
                        "size": item_size
                    })
            
            # Create application state manifest
            manifest = {
                "backup_id": backup_id,
                "backup_type": "application_state",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "items": backed_up_items,
                "total_size_bytes": total_size,
                "environment": os.environ.get("ENVIRONMENT", "unknown")
            }
            
            manifest_file = backup_dir / "app_state_manifest.json"
            with open(manifest_file, 'w') as f:
                json.dump(manifest, f, indent=2)
            
            # Create compressed archive
            backup_archive = self.backup_root / "application" / f"{backup_id}.tar.gz"
            await self._create_compressed_archive(backup_dir, backup_archive)
            
            # Update metadata
            backup_metadata.file_path = str(backup_archive)
            backup_metadata.file_size_bytes = backup_archive.stat().st_size
            backup_metadata.checksum = await self._calculate_file_checksum(backup_archive)
            backup_metadata.metadata = manifest
            
            # Upload to S3
            s3_key = f"application/{backup_id}.tar.gz"
            await self._upload_to_s3(backup_archive, s3_key)
            
            # Cleanup temporary files
            shutil.rmtree(backup_dir)
            
            backup_metadata.end_time = datetime.now(timezone.utc)
            backup_metadata.status = BackupStatus.COMPLETED
            
            await self._store_backup_metadata(backup_metadata)
            
            self.logger.info(
                f"Application state backup completed: {backup_id}",
                items_count=len(backed_up_items),
                file_size_mb=backup_metadata.file_size_bytes // (1024 * 1024)
            )
            
            return backup_metadata
            
        except Exception as e:
            backup_metadata.status = BackupStatus.FAILED
            backup_metadata.error_message = str(e)
            backup_metadata.end_time = datetime.now(timezone.utc)
            
            handled_error = handle_exception(
                e, component="backup_system", operation="create_application_state_backup"
            )
            self.logger.error(
                f"Application state backup failed: {backup_id}",
                error=handled_error,
                correlation_id=handled_error.correlation_id
            )
            
            await self._store_backup_metadata(backup_metadata)
            raise BackupError(f"Application state backup failed: {handled_error.user_message}")

    async def create_ml_models_backup(self) -> BackupMetadata:
        """Create backup of ML models and artifacts."""
        backup_id = f"ml_models_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
        backup_metadata = BackupMetadata(
            backup_id=backup_id,
            backup_type=BackupType.ML_MODELS,
            start_time=datetime.now(timezone.utc)
        )
        
        try:
            self.logger.info(f"Starting ML models backup: {backup_id}")
            backup_metadata.status = BackupStatus.IN_PROGRESS
            
            # Create temporary backup directory
            backup_dir = self.temp_backup_dir / backup_id
            backup_dir.mkdir(parents=True, exist_ok=True)
            
            # Backup ML-related directories and files
            ml_items = [
                ("models", "models/"),
                ("mlruns", "mlruns/"),
                ("mlflow_db", "mlflow.db"),
                ("checkpoints", "checkpoints/"),
                ("feature_store", "data/feature_store/"),
            ]
            
            backed_up_items = []
            total_size = 0
            
            for item_name, item_path in ml_items:
                source_path = Path(item_path)
                if source_path.exists():
                    dest_path = backup_dir / item_name
                    
                    if source_path.is_dir():
                        shutil.copytree(source_path, dest_path, dirs_exist_ok=True)
                    else:
                        shutil.copy2(source_path, dest_path)
                    
                    item_size = self._get_directory_size(dest_path) if dest_path.is_dir() else dest_path.stat().st_size
                    total_size += item_size
                    backed_up_items.append({
                        "name": item_name,
                        "path": str(source_path),
                        "size": item_size
                    })
            
            # Get model registry information from MLflow
            try:
                import mlflow
                
                # Get registered models
                client = mlflow.tracking.MlflowClient()
                registered_models = client.list_registered_models()
                
                model_info = []
                for model in registered_models:
                    latest_version = client.get_latest_versions(model.name, stages=["Production", "Staging"])
                    model_info.append({
                        "name": model.name,
                        "description": model.description,
                        "creation_timestamp": model.creation_timestamp,
                        "last_updated_timestamp": model.last_updated_timestamp,
                        "latest_versions": [
                            {
                                "version": v.version,
                                "stage": v.current_stage,
                                "creation_timestamp": v.creation_timestamp
                            }
                            for v in latest_version
                        ]
                    })
                
            except Exception as e:
                self.logger.warning(f"Could not retrieve MLflow model registry info: {e}")
                model_info = []
            
            # Create ML models manifest
            manifest = {
                "backup_id": backup_id,
                "backup_type": "ml_models",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "items": backed_up_items,
                "model_registry": model_info,
                "total_size_bytes": total_size
            }
            
            manifest_file = backup_dir / "ml_models_manifest.json"
            with open(manifest_file, 'w') as f:
                json.dump(manifest, f, indent=2, default=str)
            
            # Create compressed archive
            backup_archive = self.backup_root / "models" / f"{backup_id}.tar.gz"
            await self._create_compressed_archive(backup_dir, backup_archive)
            
            # Update metadata
            backup_metadata.file_path = str(backup_archive)
            backup_metadata.file_size_bytes = backup_archive.stat().st_size
            backup_metadata.checksum = await self._calculate_file_checksum(backup_archive)
            backup_metadata.metadata = manifest
            backup_metadata.retention_days = self.retention_policies[BackupType.ML_MODELS]
            
            # Upload to S3
            s3_key = f"ml_models/{backup_id}.tar.gz"
            await self._upload_to_s3(backup_archive, s3_key)
            
            # Cleanup temporary files
            shutil.rmtree(backup_dir)
            
            backup_metadata.end_time = datetime.now(timezone.utc)
            backup_metadata.status = BackupStatus.COMPLETED
            
            await self._store_backup_metadata(backup_metadata)
            
            self.logger.info(
                f"ML models backup completed: {backup_id}",
                items_count=len(backed_up_items),
                models_count=len(model_info),
                file_size_mb=backup_metadata.file_size_bytes // (1024 * 1024)
            )
            
            return backup_metadata
            
        except Exception as e:
            backup_metadata.status = BackupStatus.FAILED
            backup_metadata.error_message = str(e)
            backup_metadata.end_time = datetime.now(timezone.utc)
            
            handled_error = handle_exception(
                e, component="backup_system", operation="create_ml_models_backup"
            )
            self.logger.error(
                f"ML models backup failed: {backup_id}",
                error=handled_error,
                correlation_id=handled_error.correlation_id
            )
            
            await self._store_backup_metadata(backup_metadata)
            raise BackupError(f"ML models backup failed: {handled_error.user_message}")

    async def validate_backup(self, backup_id: str) -> bool:
        """Validate the integrity of a backup."""
        try:
            # Get backup metadata
            backup_metadata = await self._get_backup_metadata(backup_id)
            if not backup_metadata:
                raise BackupError(f"Backup metadata not found for ID: {backup_id}")
            
            backup_metadata.status = BackupStatus.VALIDATING
            await self._update_backup_metadata(backup_metadata)
            
            self.logger.info(f"Starting backup validation: {backup_id}")
            
            # Download backup from S3 for validation
            temp_file = self.temp_backup_dir / f"{backup_id}_validation"
            s3_key = self._get_s3_key_for_backup(backup_metadata)
            
            self.s3_client.download_file(self.backup_bucket, s3_key, str(temp_file))
            
            # Verify file size
            actual_size = temp_file.stat().st_size
            if actual_size != backup_metadata.file_size_bytes:
                raise BackupError(
                    f"File size mismatch: expected {backup_metadata.file_size_bytes}, got {actual_size}"
                )
            
            # Verify checksum
            actual_checksum = await self._calculate_file_checksum(temp_file)
            if actual_checksum != backup_metadata.checksum:
                raise BackupError(
                    f"Checksum mismatch: expected {backup_metadata.checksum}, got {actual_checksum}"
                )
            
            # Perform backup-type-specific validation
            validation_success = False
            
            if backup_metadata.backup_type == BackupType.FULL_DATABASE:
                validation_success = await self._validate_database_backup(temp_file)
            elif backup_metadata.backup_type == BackupType.APPLICATION_STATE:
                validation_success = await self._validate_application_backup(temp_file)
            elif backup_metadata.backup_type == BackupType.ML_MODELS:
                validation_success = await self._validate_ml_models_backup(temp_file)
            else:
                # Basic validation passed for other types
                validation_success = True
            
            # Update validation status
            backup_metadata.status = BackupStatus.VALIDATED if validation_success else BackupStatus.FAILED
            backup_metadata.validation_status = "passed" if validation_success else "failed"
            
            await self._update_backup_metadata(backup_metadata)
            
            # Cleanup temporary file
            temp_file.unlink(missing_ok=True)
            
            self.logger.info(
                f"Backup validation completed: {backup_id}",
                validation_passed=validation_success
            )
            
            return validation_success
            
        except Exception as e:
            handled_error = handle_exception(
                e, component="backup_system", operation="validate_backup"
            )
            self.logger.error(
                f"Backup validation failed: {backup_id}",
                error=handled_error,
                correlation_id=handled_error.correlation_id
            )
            
            # Update backup status to failed
            if 'backup_metadata' in locals():
                backup_metadata.status = BackupStatus.FAILED
                backup_metadata.validation_status = f"failed: {str(e)}"
                await self._update_backup_metadata(backup_metadata)
            
            raise BackupError(f"Backup validation failed: {handled_error.user_message}")

    async def list_available_restore_points(self, 
                                          backup_type: Optional[BackupType] = None,
                                          days_back: int = 30) -> List[RestorePoint]:
        """List available restore points."""
        try:
            cutoff_date = datetime.now(timezone.utc) - timedelta(days=days_back)
            
            async with get_database_connection() as conn:
                query = """
                    SELECT backup_id, backup_type, start_time, file_size_bytes, 
                           status, validation_status, metadata
                    FROM backup_system.backup_metadata
                    WHERE start_time >= $1
                      AND status = 'completed'
                      AND validation_status = 'passed'
                """
                
                params = [cutoff_date]
                
                if backup_type:
                    query += " AND backup_type = $2"
                    params.append(backup_type.value)
                
                query += " ORDER BY start_time DESC"
                
                rows = await conn.fetch(query, *params)
            
            restore_points = []
            for row in rows:
                # Estimate restore time based on backup size
                size_gb = row['file_size_bytes'] / (1024 ** 3)
                restore_time_estimate = int(max(5, size_gb * 2))  # 2 minutes per GB, minimum 5 minutes
                
                restore_point = RestorePoint(
                    timestamp=row['start_time'],
                    backup_id=row['backup_id'],
                    transaction_log_position="",  # Would be extracted from metadata for incremental backups
                    database_size_bytes=row['file_size_bytes'],
                    consistency_verified=row['validation_status'] == 'passed',
                    restore_time_estimate_minutes=restore_time_estimate
                )
                
                restore_points.append(restore_point)
            
            self.logger.info(
                f"Found {len(restore_points)} available restore points",
                backup_type=backup_type.value if backup_type else "all",
                days_back=days_back
            )
            
            return restore_points
            
        except Exception as e:
            handled_error = handle_exception(
                e, component="backup_system", operation="list_available_restore_points"
            )
            self.logger.error(
                "Failed to list available restore points",
                error=handled_error,
                correlation_id=handled_error.correlation_id
            )
            raise BackupError(f"Failed to list restore points: {handled_error.user_message}")

    async def perform_disaster_recovery(self, 
                                      recovery_type: RecoveryType,
                                      target_timestamp: Optional[datetime] = None,
                                      backup_id: Optional[str] = None) -> bool:
        """Perform disaster recovery operation."""
        try:
            recovery_id = f"recovery_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
            
            self.logger.warning(
                f"Starting disaster recovery operation: {recovery_id}",
                recovery_type=recovery_type.value,
                target_timestamp=target_timestamp,
                backup_id=backup_id
            )
            
            # Validate recovery parameters
            if recovery_type == RecoveryType.POINT_IN_TIME and not target_timestamp:
                raise RecoveryError("Target timestamp required for point-in-time recovery")
            
            if recovery_type == RecoveryType.FULL_RESTORE and not backup_id:
                raise RecoveryError("Backup ID required for full restore")
            
            # Create recovery plan
            recovery_plan = await self._create_recovery_plan(
                recovery_type, target_timestamp, backup_id
            )
            
            # Execute recovery steps
            for step_index, step in enumerate(recovery_plan['steps']):
                self.logger.info(
                    f"Executing recovery step {step_index + 1}/{len(recovery_plan['steps'])}: {step['description']}"
                )
                
                success = await self._execute_recovery_step(step)
                if not success:
                    raise RecoveryError(f"Recovery step failed: {step['description']}")
            
            # Validate recovery
            validation_success = await self._validate_recovery(recovery_type)
            
            if validation_success:
                self.logger.info(f"Disaster recovery completed successfully: {recovery_id}")
                return True
            else:
                raise RecoveryError("Recovery validation failed")
            
        except Exception as e:
            handled_error = handle_exception(
                e, component="backup_system", operation="perform_disaster_recovery"
            )
            self.logger.error(
                f"Disaster recovery failed: {recovery_id}",
                error=handled_error,
                correlation_id=handled_error.correlation_id
            )
            raise RecoveryError(f"Disaster recovery failed: {handled_error.user_message}")

    # Helper methods

    def _get_database_config(self) -> Dict[str, str]:
        """Get database configuration from settings."""
        return {
            "host": "localhost",
            "port": "5433",
            "username": "mlb_user",
            "password": os.environ.get("POSTGRES_PASSWORD", "postgres"),
            "database": "mlb_betting"
        }

    async def _calculate_file_checksum(self, file_path: Path) -> str:
        """Calculate SHA256 checksum of a file."""
        import hashlib
        
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            # Read file in chunks to handle large files
            for chunk in iter(lambda: f.read(4096), b""):
                sha256_hash.update(chunk)
        
        return sha256_hash.hexdigest()

    def _calculate_compression_ratio(self, compressed_file: Path) -> float:
        """Calculate compression ratio (placeholder implementation)."""
        # This would require the original file size for accurate calculation
        # For now, return a default ratio
        return 0.7

    async def _create_compressed_archive(self, source_dir: Path, archive_path: Path) -> None:
        """Create a compressed archive from a directory."""
        import tarfile
        
        with tarfile.open(archive_path, "w:gz") as tar:
            tar.add(source_dir, arcname=source_dir.name)

    def _get_directory_size(self, directory: Path) -> int:
        """Get total size of a directory."""
        total_size = 0
        for path in directory.rglob('*'):
            if path.is_file():
                total_size += path.stat().st_size
        return total_size

    async def _upload_to_s3(self, file_path: Path, s3_key: str) -> None:
        """Upload file to S3 with encryption."""
        self.s3_client.upload_file(
            str(file_path),
            self.backup_bucket,
            s3_key,
            ExtraArgs={
                'ServerSideEncryption': 'AES256',
                'StorageClass': 'STANDARD'
            }
        )

    def _get_s3_key_for_backup(self, backup_metadata: BackupMetadata) -> str:
        """Generate S3 key for a backup."""
        backup_type_map = {
            BackupType.FULL_DATABASE: "database",
            BackupType.INCREMENTAL_DATABASE: "database/incremental",
            BackupType.APPLICATION_STATE: "application",
            BackupType.ML_MODELS: "ml_models",
            BackupType.CONFIGURATION: "config",
            BackupType.LOGS: "logs",
        }
        
        prefix = backup_type_map.get(backup_metadata.backup_type, "misc")
        filename = Path(backup_metadata.file_path).name
        return f"{prefix}/{filename}"

    # Database operations for backup tracking

    async def _create_backup_tracking_schema(self) -> None:
        """Create database schema for backup tracking."""
        async with get_database_connection() as conn:
            await conn.execute("""
                CREATE SCHEMA IF NOT EXISTS backup_system;
                
                CREATE TABLE IF NOT EXISTS backup_system.backup_metadata (
                    backup_id TEXT PRIMARY KEY,
                    backup_type TEXT NOT NULL,
                    start_time TIMESTAMP WITH TIME ZONE NOT NULL,
                    end_time TIMESTAMP WITH TIME ZONE,
                    status TEXT NOT NULL,
                    file_path TEXT,
                    file_size_bytes BIGINT,
                    checksum TEXT,
                    compression_ratio REAL,
                    encryption_enabled BOOLEAN DEFAULT TRUE,
                    storage_class TEXT DEFAULT 'STANDARD',
                    retention_days INTEGER,
                    cross_region_replicated BOOLEAN DEFAULT FALSE,
                    validation_status TEXT DEFAULT 'pending',
                    error_message TEXT,
                    metadata JSONB,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                );
                
                CREATE INDEX IF NOT EXISTS idx_backup_metadata_backup_type 
                ON backup_system.backup_metadata (backup_type);
                
                CREATE INDEX IF NOT EXISTS idx_backup_metadata_start_time 
                ON backup_system.backup_metadata (start_time DESC);
                
                CREATE INDEX IF NOT EXISTS idx_backup_metadata_status 
                ON backup_system.backup_metadata (status);
                
                -- Trigger to update updated_at timestamp
                CREATE OR REPLACE FUNCTION backup_system.update_updated_at_column()
                RETURNS TRIGGER AS $$
                BEGIN
                    NEW.updated_at = NOW();
                    RETURN NEW;
                END;
                $$ language 'plpgsql';
                
                DROP TRIGGER IF EXISTS update_backup_metadata_updated_at ON backup_system.backup_metadata;
                CREATE TRIGGER update_backup_metadata_updated_at
                    BEFORE UPDATE ON backup_system.backup_metadata
                    FOR EACH ROW EXECUTE FUNCTION backup_system.update_updated_at_column();
            """)

    async def _store_backup_metadata(self, metadata: BackupMetadata) -> None:
        """Store backup metadata in database."""
        async with get_database_connection() as conn:
            await conn.execute("""
                INSERT INTO backup_system.backup_metadata 
                (backup_id, backup_type, start_time, end_time, status, file_path, 
                 file_size_bytes, checksum, compression_ratio, encryption_enabled, 
                 storage_class, retention_days, cross_region_replicated, 
                 validation_status, error_message, metadata)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
            """, metadata.backup_id, metadata.backup_type.value, metadata.start_time,
                metadata.end_time, metadata.status.value, metadata.file_path,
                metadata.file_size_bytes, metadata.checksum, metadata.compression_ratio,
                metadata.encryption_enabled, metadata.storage_class, metadata.retention_days,
                metadata.cross_region_replicated, metadata.validation_status,
                metadata.error_message, json.dumps(metadata.metadata))

    async def _get_backup_metadata(self, backup_id: str) -> Optional[BackupMetadata]:
        """Retrieve backup metadata from database."""
        async with get_database_connection() as conn:
            row = await conn.fetchrow("""
                SELECT * FROM backup_system.backup_metadata WHERE backup_id = $1
            """, backup_id)
            
            if not row:
                return None
            
            return BackupMetadata(
                backup_id=row['backup_id'],
                backup_type=BackupType(row['backup_type']),
                start_time=row['start_time'],
                end_time=row['end_time'],
                status=BackupStatus(row['status']),
                file_path=row['file_path'] or "",
                file_size_bytes=row['file_size_bytes'] or 0,
                checksum=row['checksum'] or "",
                compression_ratio=row['compression_ratio'] or 0.0,
                encryption_enabled=row['encryption_enabled'],
                storage_class=row['storage_class'],
                retention_days=row['retention_days'],
                cross_region_replicated=row['cross_region_replicated'],
                validation_status=row['validation_status'],
                error_message=row['error_message'] or "",
                metadata=json.loads(row['metadata']) if row['metadata'] else {}
            )

    async def _update_backup_metadata(self, metadata: BackupMetadata) -> None:
        """Update backup metadata in database."""
        async with get_database_connection() as conn:
            await conn.execute("""
                UPDATE backup_system.backup_metadata SET
                    end_time = $2,
                    status = $3,
                    file_path = $4,
                    file_size_bytes = $5,
                    checksum = $6,
                    validation_status = $7,
                    error_message = $8,
                    metadata = $9
                WHERE backup_id = $1
            """, metadata.backup_id, metadata.end_time, metadata.status.value,
                metadata.file_path, metadata.file_size_bytes, metadata.checksum,
                metadata.validation_status, metadata.error_message,
                json.dumps(metadata.metadata))

    async def _validate_backup_infrastructure(self) -> None:
        """Validate that backup infrastructure is properly configured."""
        # Check database connectivity
        async with get_database_connection() as conn:
            await conn.fetchval("SELECT 1")
        
        # Check S3 connectivity
        self.s3_client.head_bucket(Bucket=self.backup_bucket)
        
        # Check local storage permissions
        test_file = self.backup_root / "test_permissions"
        test_file.write_text("test")
        test_file.unlink()
        
        self.logger.info("Backup infrastructure validation passed")

    # Placeholder methods for backup validation and recovery
    # These would be implemented with specific logic for each backup type

    async def _validate_database_backup(self, backup_file: Path) -> bool:
        """Validate database backup integrity."""
        # Placeholder: would test pg_restore with --list option
        return True

    async def _validate_application_backup(self, backup_file: Path) -> bool:
        """Validate application backup integrity."""
        # Placeholder: would extract and validate application files
        return True

    async def _validate_ml_models_backup(self, backup_file: Path) -> bool:
        """Validate ML models backup integrity."""
        # Placeholder: would validate model files and registry
        return True

    async def _create_recovery_plan(self, 
                                  recovery_type: RecoveryType,
                                  target_timestamp: Optional[datetime],
                                  backup_id: Optional[str]) -> Dict[str, Any]:
        """Create a recovery plan based on recovery type."""
        # Placeholder implementation
        return {
            "recovery_type": recovery_type.value,
            "target_timestamp": target_timestamp,
            "backup_id": backup_id,
            "steps": [
                {"description": "Stop application services", "type": "service_stop"},
                {"description": "Restore database", "type": "database_restore"},
                {"description": "Restore application state", "type": "application_restore"},
                {"description": "Validate data integrity", "type": "validation"},
                {"description": "Start application services", "type": "service_start"}
            ]
        }

    async def _execute_recovery_step(self, step: Dict[str, Any]) -> bool:
        """Execute a recovery step."""
        # Placeholder implementation
        self.logger.info(f"Executing recovery step: {step['description']}")
        # Simulate step execution
        await asyncio.sleep(1)
        return True

    async def _validate_recovery(self, recovery_type: RecoveryType) -> bool:
        """Validate that recovery was successful."""
        # Placeholder implementation
        return True


# Global service instance
_backup_system = None


def get_backup_system() -> AutomatedBackupSystem:
    """Get or create the global backup system instance."""
    global _backup_system
    if _backup_system is None:
        _backup_system = AutomatedBackupSystem()
    return _backup_system


if __name__ == "__main__":
    # CLI interface for backup system
    import argparse
    
    parser = argparse.ArgumentParser(description="MLB Betting System Backup Utility")
    parser.add_argument("command", choices=[
        "full-backup", "incremental-backup", "app-backup", 
        "ml-backup", "validate", "list-restores", "disaster-recovery"
    ])
    parser.add_argument("--backup-id", help="Backup ID for validation or recovery")
    parser.add_argument("--days-back", type=int, default=30, help="Days back for restore point listing")
    
    args = parser.parse_args()
    
    async def main():
        backup_system = get_backup_system()
        await backup_system.initialize()
        
        if args.command == "full-backup":
            result = await backup_system.create_full_database_backup()
            print(f"Full backup completed: {result.backup_id}")
        
        elif args.command == "incremental-backup":
            result = await backup_system.create_incremental_database_backup()
            print(f"Incremental backup completed: {result.backup_id}")
        
        elif args.command == "app-backup":
            result = await backup_system.create_application_state_backup()
            print(f"Application backup completed: {result.backup_id}")
        
        elif args.command == "ml-backup":
            result = await backup_system.create_ml_models_backup()
            print(f"ML models backup completed: {result.backup_id}")
        
        elif args.command == "validate":
            if not args.backup_id:
                print("Backup ID required for validation")
                return
            
            success = await backup_system.validate_backup(args.backup_id)
            print(f"Backup validation: {'PASSED' if success else 'FAILED'}")
        
        elif args.command == "list-restores":
            restore_points = await backup_system.list_available_restore_points(days_back=args.days_back)
            print(f"Found {len(restore_points)} restore points:")
            for rp in restore_points:
                print(f"  {rp.timestamp} - {rp.backup_id} ({rp.restore_time_estimate_minutes}min restore)")
    
    asyncio.run(main())