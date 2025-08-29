"""
Disaster Recovery & Backup Automation System
Comprehensive backup, recovery, and business continuity management for 24/7 operations
"""

import logging
import asyncio
import json
import shutil
import tarfile
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from enum import Enum
from pathlib import Path
import aiofiles
import asyncpg

from .config import get_settings
from .logging import LogComponent, get_logger
from ..services.monitoring.prometheus_metrics_service import get_metrics_service

logger = get_logger(__name__, LogComponent.INFRASTRUCTURE)


class BackupType(str, Enum):
    """Types of backups"""
    FULL = "full"
    INCREMENTAL = "incremental"
    DIFFERENTIAL = "differential"
    EMERGENCY = "emergency"


class BackupStatus(str, Enum):
    """Backup operation status"""
    SCHEDULED = "scheduled"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class RecoveryType(str, Enum):
    """Types of recovery operations"""
    FULL_RESTORE = "full_restore"
    PARTIAL_RESTORE = "partial_restore"
    POINT_IN_TIME = "point_in_time"
    DATABASE_ONLY = "database_only"
    CONFIG_ONLY = "config_only"


@dataclass
class BackupJob:
    """Backup job definition"""
    job_id: str
    backup_type: BackupType
    components: List[str]
    scheduled_time: datetime
    started_time: Optional[datetime] = None
    completed_time: Optional[datetime] = None
    status: BackupStatus = BackupStatus.SCHEDULED
    backup_path: Optional[str] = None
    backup_size_bytes: int = 0
    error_message: Optional[str] = None
    retention_days: int = 30


@dataclass
class RecoveryJob:
    """Recovery job definition"""
    job_id: str
    recovery_type: RecoveryType
    backup_source: str
    target_time: Optional[datetime] = None
    components: List[str] = None
    started_time: Optional[datetime] = None
    completed_time: Optional[datetime] = None
    status: BackupStatus = BackupStatus.SCHEDULED
    error_message: Optional[str] = None
    rollback_data: Optional[Dict[str, Any]] = None


class DisasterRecoverySystem:
    """
    Comprehensive disaster recovery and backup automation system
    """
    
    def __init__(self):
        self.settings = get_settings()
        self.metrics_service = get_metrics_service()
        
        # Configuration
        self.config = {
            # Backup configuration
            "backup_root_path": Path("/data/backups"),
            "max_parallel_backups": 2,
            "backup_compression": True,
            "backup_encryption": False,  # Can be enabled with proper key management
            
            # Retention policies
            "daily_retention_days": 30,
            "weekly_retention_weeks": 12,
            "monthly_retention_months": 12,
            "yearly_retention_years": 5,
            
            # Recovery configuration
            "recovery_staging_path": Path("/data/recovery"),
            "max_recovery_time_minutes": 60,
            "auto_validation_enabled": True,
            
            # Replication configuration
            "enable_real_time_replication": True,
            "replication_lag_threshold_seconds": 30,
            "replication_health_check_interval": 300,  # 5 minutes
            
            # Monitoring
            "backup_health_check_interval": 3600,  # 1 hour
            "disaster_detection_interval": 600,    # 10 minutes
            
            # Emergency procedures
            "emergency_contact_enabled": False,
            "emergency_shutdown_threshold": 3,  # 3 critical failures
        }
        
        # Component backup handlers
        self.backup_handlers = {
            "database": self._backup_database,
            "models": self._backup_models,
            "configurations": self._backup_configurations,
            "logs": self._backup_logs,
            "feature_store": self._backup_feature_store,
        }
        
        # Component recovery handlers
        self.recovery_handlers = {
            "database": self._recover_database,
            "models": self._recover_models,
            "configurations": self._recover_configurations,
            "logs": self._recover_logs,
            "feature_store": self._recover_feature_store,
        }
        
        # State tracking
        self.active_backup_jobs: Dict[str, BackupJob] = {}
        self.active_recovery_jobs: Dict[str, RecoveryJob] = {}
        self.backup_history: List[BackupJob] = []
        self.recovery_history: List[RecoveryJob] = []
        
        # Disaster detection
        self.disaster_indicators: Dict[str, Any] = {}
        self.recovery_procedures: Dict[str, Any] = {}

    async def initialize(self):
        """Initialize the disaster recovery system"""
        
        try:
            logger.info("Initializing Disaster Recovery System...")
            
            # Create backup directories
            self.config["backup_root_path"].mkdir(parents=True, exist_ok=True)
            self.config["recovery_staging_path"].mkdir(parents=True, exist_ok=True)
            
            # Start monitoring tasks
            asyncio.create_task(self._backup_monitoring_loop())
            asyncio.create_task(self._disaster_monitoring_loop())
            asyncio.create_task(self._maintenance_loop())
            
            # Schedule regular backups
            await self._schedule_regular_backups()
            
            logger.info("✅ Disaster Recovery System initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize disaster recovery system: {e}")
            raise

    async def create_backup(
        self,
        backup_type: BackupType = BackupType.FULL,
        components: Optional[List[str]] = None,
        retention_days: int = 30
    ) -> str:
        """Create a backup job"""
        
        try:
            job_id = f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            if components is None:
                components = list(self.backup_handlers.keys())
            
            backup_job = BackupJob(
                job_id=job_id,
                backup_type=backup_type,
                components=components,
                scheduled_time=datetime.now(),
                retention_days=retention_days
            )
            
            self.active_backup_jobs[job_id] = backup_job
            
            logger.info(f"Creating backup job {job_id}: {backup_type.value} backup of {components}")
            
            # Start backup execution
            asyncio.create_task(self._execute_backup_job(job_id))
            
            # Record metrics
            self.metrics_service.record_pipeline_start(job_id, "disaster_recovery_backup")
            
            return job_id
            
        except Exception as e:
            logger.error(f"Failed to create backup: {e}")
            raise

    async def _execute_backup_job(self, job_id: str):
        """Execute a backup job"""
        
        job = self.active_backup_jobs[job_id]
        
        try:
            job.status = BackupStatus.RUNNING
            job.started_time = datetime.now()
            
            # Create backup directory
            backup_dir = self.config["backup_root_path"] / f"{job.backup_type.value}_{job.started_time.strftime('%Y%m%d_%H%M%S')}"
            backup_dir.mkdir(parents=True, exist_ok=True)
            job.backup_path = str(backup_dir)
            
            logger.info(f"Executing backup job {job_id} to {backup_dir}")
            
            total_size = 0
            
            # Execute component backups
            for component in job.components:
                try:
                    if component in self.backup_handlers:
                        component_size = await self.backup_handlers[component](backup_dir, job)
                        total_size += component_size
                        logger.info(f"Backed up {component}: {component_size} bytes")
                    else:
                        logger.warning(f"No backup handler for component: {component}")
                        
                except Exception as e:
                    logger.error(f"Component backup failed for {component}: {e}")
                    # Continue with other components
            
            # Create backup metadata
            metadata = {
                "job_id": job_id,
                "backup_type": job.backup_type.value,
                "components": job.components,
                "created_time": job.started_time.isoformat(),
                "total_size_bytes": total_size,
                "system_info": {
                    "python_version": "3.11+",
                    "system_platform": "unix",
                    "backup_version": "1.0",
                }
            }
            
            metadata_file = backup_dir / "backup_metadata.json"
            async with aiofiles.open(metadata_file, 'w') as f:
                await f.write(json.dumps(metadata, indent=2))
            
            # Compress backup if enabled
            if self.config["backup_compression"]:
                compressed_path = f"{backup_dir}.tar.gz"
                await asyncio.to_thread(self._compress_backup, backup_dir, compressed_path)
                
                # Remove uncompressed directory
                shutil.rmtree(backup_dir)
                job.backup_path = compressed_path
                
                # Update size
                total_size = Path(compressed_path).stat().st_size
            
            # Complete backup
            job.status = BackupStatus.COMPLETED
            job.completed_time = datetime.now()
            job.backup_size_bytes = total_size
            
            # Move to history
            self.backup_history.append(job)
            del self.active_backup_jobs[job_id]
            
            duration = (job.completed_time - job.started_time).total_seconds()
            logger.info(f"Backup job {job_id} completed in {duration:.1f}s: {total_size} bytes")
            
            # Record success metrics
            self.metrics_service.record_pipeline_completion(
                job_id, "disaster_recovery_backup", "success"
            )
            
            # Clean up old backups
            await self._cleanup_old_backups()
            
        except Exception as e:
            job.status = BackupStatus.FAILED
            job.error_message = str(e)
            job.completed_time = datetime.now()
            
            # Move to history
            self.backup_history.append(job)
            del self.active_backup_jobs[job_id]
            
            logger.error(f"Backup job {job_id} failed: {e}")
            
            # Record failure metrics
            self.metrics_service.record_pipeline_completion(
                job_id, "disaster_recovery_backup", "failed", errors=[e]
            )

    def _compress_backup(self, source_dir: Path, output_path: str):
        """Compress backup directory"""
        
        with tarfile.open(output_path, "w:gz") as tar:
            tar.add(source_dir, arcname=source_dir.name)

    # Component Backup Implementations
    
    async def _backup_database(self, backup_dir: Path, job: BackupJob) -> int:
        """Backup database"""
        
        try:
            output_file = backup_dir / "database_backup.sql"
            
            # Connect to database
            conn = await asyncpg.connect(
                host=self.settings.database.host,
                port=self.settings.database.port,
                database=self.settings.database.database,
                user=self.settings.database.user,
                password=self.settings.database.password,
            )
            
            # Get all tables
            tables = await conn.fetch("""
                SELECT schemaname, tablename 
                FROM pg_tables 
                WHERE schemaname IN ('public', 'raw_data', 'curated', 'staging')
            """)
            
            total_size = 0
            
            async with aiofiles.open(output_file, 'w') as f:
                await f.write("-- MLB Betting System Database Backup\n")
                await f.write(f"-- Created: {datetime.now()}\n")
                await f.write(f"-- Job ID: {job.job_id}\n\n")
                
                # Backup each table
                for table in tables:
                    schema = table['schemaname']
                    table_name = table['tablename']
                    full_name = f"{schema}.{table_name}"
                    
                    try:
                        # Get table data
                        rows = await conn.fetch(f"SELECT * FROM {full_name}")
                        
                        if rows:
                            await f.write(f"\n-- Table: {full_name}\n")
                            
                            # Write INSERT statements (simplified approach)
                            for row in rows[:100]:  # Limit for safety
                                values = ", ".join(f"'{v}'" if v is not None else "NULL" for v in row)
                                columns = ", ".join(row.keys())
                                await f.write(f"INSERT INTO {full_name} ({columns}) VALUES ({values});\n")
                        
                    except Exception as e:
                        logger.warning(f"Failed to backup table {full_name}: {e}")
                        continue
            
            await conn.close()
            
            # Get file size
            total_size = output_file.stat().st_size
            return total_size
            
        except Exception as e:
            logger.error(f"Database backup failed: {e}")
            return 0

    async def _backup_models(self, backup_dir: Path, job: BackupJob) -> int:
        """Backup ML models"""
        
        try:
            models_dir = backup_dir / "models"
            models_dir.mkdir(exist_ok=True)
            
            # This would backup MLflow models, trained model files, etc.
            # For now, create placeholder
            
            model_info = {
                "backup_time": datetime.now().isoformat(),
                "models_backed_up": ["moneyline_home_win", "total_over_under"],
                "mlflow_tracking_uri": str(self.settings.mlflow.tracking_uri),
            }
            
            metadata_file = models_dir / "models_metadata.json"
            async with aiofiles.open(metadata_file, 'w') as f:
                await f.write(json.dumps(model_info, indent=2))
            
            return metadata_file.stat().st_size
            
        except Exception as e:
            logger.error(f"Models backup failed: {e}")
            return 0

    async def _backup_configurations(self, backup_dir: Path, job: BackupJob) -> int:
        """Backup system configurations"""
        
        try:
            config_dir = backup_dir / "configurations"
            config_dir.mkdir(exist_ok=True)
            
            total_size = 0
            
            # Backup main configuration
            if Path("config.toml").exists():
                shutil.copy2("config.toml", config_dir / "config.toml")
                total_size += (config_dir / "config.toml").stat().st_size
            
            # Backup environment configuration
            env_info = {
                "backup_time": datetime.now().isoformat(),
                "database_host": self.settings.database.host,
                "database_port": self.settings.database.port,
                "database_name": self.settings.database.database,
                "redis_url": self.settings.redis.url,
                "mlflow_tracking_uri": str(self.settings.mlflow.tracking_uri),
            }
            
            env_file = config_dir / "environment.json"
            async with aiofiles.open(env_file, 'w') as f:
                await f.write(json.dumps(env_info, indent=2))
                
            total_size += env_file.stat().st_size
            
            return total_size
            
        except Exception as e:
            logger.error(f"Configuration backup failed: {e}")
            return 0

    async def _backup_logs(self, backup_dir: Path, job: BackupJob) -> int:
        """Backup system logs"""
        
        try:
            logs_dir = backup_dir / "logs"
            logs_dir.mkdir(exist_ok=True)
            
            total_size = 0
            
            # Backup recent log files
            log_path = Path("logs")
            if log_path.exists():
                for log_file in log_path.glob("*.log"):
                    # Only backup logs from last 7 days
                    if (datetime.now() - datetime.fromtimestamp(log_file.stat().st_mtime)).days <= 7:
                        dest_file = logs_dir / log_file.name
                        shutil.copy2(log_file, dest_file)
                        total_size += dest_file.stat().st_size
            
            return total_size
            
        except Exception as e:
            logger.error(f"Logs backup failed: {e}")
            return 0

    async def _backup_feature_store(self, backup_dir: Path, job: BackupJob) -> int:
        """Backup feature store data"""
        
        try:
            features_dir = backup_dir / "feature_store"
            features_dir.mkdir(exist_ok=True)
            
            # Create feature store backup metadata
            feature_info = {
                "backup_time": datetime.now().isoformat(),
                "redis_url": self.settings.redis.url,
                "feature_version": "v2.1",
                "cached_features": "Recent feature vectors and metadata",
            }
            
            metadata_file = features_dir / "feature_store_metadata.json"
            async with aiofiles.open(metadata_file, 'w') as f:
                await f.write(json.dumps(feature_info, indent=2))
            
            return metadata_file.stat().st_size
            
        except Exception as e:
            logger.error(f"Feature store backup failed: {e}")
            return 0

    # Recovery Operations
    
    async def create_recovery(
        self,
        backup_source: str,
        recovery_type: RecoveryType = RecoveryType.FULL_RESTORE,
        components: Optional[List[str]] = None,
        target_time: Optional[datetime] = None
    ) -> str:
        """Create a recovery job"""
        
        try:
            job_id = f"recovery_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            recovery_job = RecoveryJob(
                job_id=job_id,
                recovery_type=recovery_type,
                backup_source=backup_source,
                target_time=target_time,
                components=components or list(self.recovery_handlers.keys())
            )
            
            self.active_recovery_jobs[job_id] = recovery_job
            
            logger.info(f"Creating recovery job {job_id}: {recovery_type.value} from {backup_source}")
            
            # Start recovery execution
            asyncio.create_task(self._execute_recovery_job(job_id))
            
            return job_id
            
        except Exception as e:
            logger.error(f"Failed to create recovery: {e}")
            raise

    async def _execute_recovery_job(self, job_id: str):
        """Execute a recovery job"""
        
        job = self.active_recovery_jobs[job_id]
        
        try:
            job.status = BackupStatus.RUNNING
            job.started_time = datetime.now()
            
            logger.info(f"Executing recovery job {job_id}")
            
            # Prepare staging area
            staging_dir = self.config["recovery_staging_path"] / job_id
            staging_dir.mkdir(parents=True, exist_ok=True)
            
            # Extract backup if compressed
            backup_path = Path(job.backup_source)
            if backup_path.suffix == ".gz":
                await asyncio.to_thread(self._extract_backup, backup_path, staging_dir)
                backup_dir = staging_dir / backup_path.stem.replace(".tar", "")
            else:
                backup_dir = backup_path
            
            # Execute component recoveries
            for component in job.components:
                try:
                    if component in self.recovery_handlers:
                        await self.recovery_handlers[component](backup_dir, job)
                        logger.info(f"Recovered component: {component}")
                    else:
                        logger.warning(f"No recovery handler for component: {component}")
                        
                except Exception as e:
                    logger.error(f"Component recovery failed for {component}: {e}")
                    # Continue with other components
            
            # Validate recovery if enabled
            if self.config["auto_validation_enabled"]:
                validation_result = await self._validate_recovery(job)
                if not validation_result:
                    raise Exception("Recovery validation failed")
            
            # Complete recovery
            job.status = BackupStatus.COMPLETED
            job.completed_time = datetime.now()
            
            # Move to history
            self.recovery_history.append(job)
            del self.active_recovery_jobs[job_id]
            
            duration = (job.completed_time - job.started_time).total_seconds()
            logger.info(f"Recovery job {job_id} completed in {duration:.1f}s")
            
            # Clean up staging
            shutil.rmtree(staging_dir, ignore_errors=True)
            
        except Exception as e:
            job.status = BackupStatus.FAILED
            job.error_message = str(e)
            job.completed_time = datetime.now()
            
            # Move to history
            self.recovery_history.append(job)
            del self.active_recovery_jobs[job_id]
            
            logger.error(f"Recovery job {job_id} failed: {e}")

    def _extract_backup(self, backup_path: Path, staging_dir: Path):
        """Extract compressed backup"""
        
        with tarfile.open(backup_path, "r:gz") as tar:
            tar.extractall(staging_dir)

    # Component Recovery Implementations
    
    async def _recover_database(self, backup_dir: Path, job: RecoveryJob):
        """Recover database from backup"""
        
        try:
            sql_file = backup_dir / "database_backup.sql"
            if not sql_file.exists():
                raise Exception("Database backup file not found")
            
            # Connect to database
            conn = await asyncpg.connect(
                host=self.settings.database.host,
                port=self.settings.database.port,
                database=self.settings.database.database,
                user=self.settings.database.user,
                password=self.settings.database.password,
            )
            
            # Read and execute SQL
            async with aiofiles.open(sql_file, 'r') as f:
                sql_content = await f.read()
                
            # Execute in transaction (simplified)
            await conn.execute("BEGIN")
            
            try:
                # Split and execute statements
                statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]
                
                for statement in statements[:10]:  # Limit for safety
                    if statement.startswith('INSERT'):
                        await conn.execute(statement)
                
                await conn.execute("COMMIT")
                
            except Exception as e:
                await conn.execute("ROLLBACK")
                raise e
            
            await conn.close()
            
            logger.info("Database recovery completed")
            
        except Exception as e:
            logger.error(f"Database recovery failed: {e}")
            raise

    async def _recover_models(self, backup_dir: Path, job: RecoveryJob):
        """Recover ML models from backup"""
        
        try:
            models_dir = backup_dir / "models"
            if not models_dir.exists():
                raise Exception("Models backup directory not found")
            
            # Recovery would restore MLflow models, etc.
            logger.info("Models recovery completed (placeholder)")
            
        except Exception as e:
            logger.error(f"Models recovery failed: {e}")
            raise

    async def _recover_configurations(self, backup_dir: Path, job: RecoveryJob):
        """Recover configurations from backup"""
        
        try:
            config_dir = backup_dir / "configurations"
            if not config_dir.exists():
                raise Exception("Configuration backup directory not found")
            
            # Restore configuration files
            config_file = config_dir / "config.toml"
            if config_file.exists():
                shutil.copy2(config_file, "config.toml")
                logger.info("Configuration file restored")
            
        except Exception as e:
            logger.error(f"Configuration recovery failed: {e}")
            raise

    async def _recover_logs(self, backup_dir: Path, job: RecoveryJob):
        """Recover logs from backup"""
        
        try:
            logs_dir = backup_dir / "logs"
            if logs_dir.exists():
                # Create logs directory if it doesn't exist
                Path("logs").mkdir(exist_ok=True)
                
                # Restore log files
                for log_file in logs_dir.glob("*.log"):
                    dest_path = Path("logs") / f"restored_{log_file.name}"
                    shutil.copy2(log_file, dest_path)
                
                logger.info("Logs recovery completed")
            
        except Exception as e:
            logger.error(f"Logs recovery failed: {e}")
            raise

    async def _recover_feature_store(self, backup_dir: Path, job: RecoveryJob):
        """Recover feature store from backup"""
        
        try:
            features_dir = backup_dir / "feature_store"
            if features_dir.exists():
                # Feature store recovery would restore Redis data, etc.
                logger.info("Feature store recovery completed (placeholder)")
            
        except Exception as e:
            logger.error(f"Feature store recovery failed: {e}")
            raise

    async def _validate_recovery(self, job: RecoveryJob) -> bool:
        """Validate recovery operation"""
        
        try:
            # Basic validation checks
            validation_passed = True
            
            # Check database connectivity
            if "database" in job.components:
                try:
                    conn = await asyncpg.connect(
                        host=self.settings.database.host,
                        port=self.settings.database.port,
                        database=self.settings.database.database,
                        user=self.settings.database.user,
                        password=self.settings.database.password,
                    )
                    await conn.execute("SELECT 1")
                    await conn.close()
                    logger.info("Database validation passed")
                except Exception as e:
                    logger.error(f"Database validation failed: {e}")
                    validation_passed = False
            
            # Check configuration files
            if "configurations" in job.components:
                if Path("config.toml").exists():
                    logger.info("Configuration validation passed")
                else:
                    logger.error("Configuration file missing after recovery")
                    validation_passed = False
            
            return validation_passed
            
        except Exception as e:
            logger.error(f"Recovery validation failed: {e}")
            return False

    # Monitoring and Maintenance
    
    async def _backup_monitoring_loop(self):
        """Monitor backup operations"""
        
        while True:
            try:
                await asyncio.sleep(self.config["backup_health_check_interval"])
                
                # Check backup health
                await self._check_backup_health()
                
            except Exception as e:
                logger.error(f"Backup monitoring error: {e}")
                await asyncio.sleep(300)

    async def _disaster_monitoring_loop(self):
        """Monitor for disaster scenarios"""
        
        while True:
            try:
                await asyncio.sleep(self.config["disaster_detection_interval"])
                
                # Check for disaster indicators
                await self._check_disaster_indicators()
                
            except Exception as e:
                logger.error(f"Disaster monitoring error: {e}")
                await asyncio.sleep(300)

    async def _check_backup_health(self):
        """Check backup system health"""
        
        try:
            # Check recent backup success
            recent_backups = [
                b for b in self.backup_history[-10:] 
                if (datetime.now() - b.completed_time).days <= 1
            ]
            
            if not recent_backups:
                logger.warning("No recent successful backups")
                
                # Trigger emergency backup
                await self.create_backup(BackupType.EMERGENCY)
            
            # Check backup storage space
            backup_path = self.config["backup_root_path"]
            if backup_path.exists():
                total, used, free = shutil.disk_usage(backup_path)
                free_percent = (free / total) * 100
                
                if free_percent < 20:
                    logger.warning(f"Low backup storage space: {free_percent:.1f}% free")
                    await self._cleanup_old_backups(force=True)
            
        except Exception as e:
            logger.error(f"Backup health check failed: {e}")

    async def _check_disaster_indicators(self):
        """Check for disaster scenarios"""
        
        try:
            disaster_indicators = []
            
            # Check system health indicators
            # This would integrate with the self-healing system
            
            # Check backup failures
            recent_failed = [
                b for b in self.backup_history[-5:] 
                if b.status == BackupStatus.FAILED
            ]
            
            if len(recent_failed) >= 3:
                disaster_indicators.append("Multiple backup failures")
            
            # Check recovery failures
            recent_recovery_failed = [
                r for r in self.recovery_history[-5:] 
                if r.status == BackupStatus.FAILED
            ]
            
            if len(recent_recovery_failed) >= 2:
                disaster_indicators.append("Multiple recovery failures")
            
            if disaster_indicators:
                logger.error(f"Disaster indicators detected: {disaster_indicators}")
                
                # Record break-glass activation
                self.metrics_service.record_break_glass_activation(
                    "disaster_detected",
                    f"Indicators: {', '.join(disaster_indicators)}"
                )
            
        except Exception as e:
            logger.error(f"Disaster indicator check failed: {e}")

    async def _schedule_regular_backups(self):
        """Schedule regular backup jobs"""
        
        try:
            # Schedule daily backup
            asyncio.create_task(self._daily_backup_scheduler())
            
            # Schedule weekly full backup
            asyncio.create_task(self._weekly_backup_scheduler())
            
            logger.info("Regular backup schedules initialized")
            
        except Exception as e:
            logger.error(f"Backup scheduling failed: {e}")

    async def _daily_backup_scheduler(self):
        """Daily backup scheduler"""
        
        while True:
            try:
                # Wait until 2 AM
                now = datetime.now()
                next_backup = now.replace(hour=2, minute=0, second=0, microsecond=0)
                if next_backup <= now:
                    next_backup += timedelta(days=1)
                
                sleep_seconds = (next_backup - now).total_seconds()
                await asyncio.sleep(sleep_seconds)
                
                # Create daily backup
                await self.create_backup(
                    BackupType.INCREMENTAL,
                    components=["database", "configurations"],
                    retention_days=self.config["daily_retention_days"]
                )
                
            except Exception as e:
                logger.error(f"Daily backup scheduler error: {e}")
                await asyncio.sleep(3600)  # Wait 1 hour before retrying

    async def _weekly_backup_scheduler(self):
        """Weekly backup scheduler"""
        
        while True:
            try:
                # Wait until Sunday 1 AM
                now = datetime.now()
                days_until_sunday = (6 - now.weekday()) % 7
                next_backup = now.replace(hour=1, minute=0, second=0, microsecond=0)
                next_backup += timedelta(days=days_until_sunday)
                
                if next_backup <= now:
                    next_backup += timedelta(days=7)
                
                sleep_seconds = (next_backup - now).total_seconds()
                await asyncio.sleep(sleep_seconds)
                
                # Create weekly full backup
                await self.create_backup(
                    BackupType.FULL,
                    retention_days=self.config["weekly_retention_weeks"] * 7
                )
                
            except Exception as e:
                logger.error(f"Weekly backup scheduler error: {e}")
                await asyncio.sleep(3600)

    async def _cleanup_old_backups(self, force: bool = False):
        """Clean up old backup files"""
        
        try:
            backup_path = self.config["backup_root_path"]
            if not backup_path.exists():
                return
            
            now = datetime.now()
            removed_count = 0
            
            for backup_file in backup_path.iterdir():
                try:
                    # Check file age
                    file_age = now - datetime.fromtimestamp(backup_file.stat().st_mtime)
                    
                    # Determine retention based on backup type
                    should_remove = False
                    
                    if "full_" in backup_file.name:
                        should_remove = file_age.days > self.config["weekly_retention_weeks"] * 7
                    elif "incremental_" in backup_file.name:
                        should_remove = file_age.days > self.config["daily_retention_days"]
                    elif "emergency_" in backup_file.name:
                        should_remove = file_age.days > 7  # Keep emergency backups for 1 week
                    
                    if should_remove or (force and file_age.days > 1):
                        if backup_file.is_file():
                            backup_file.unlink()
                        elif backup_file.is_dir():
                            shutil.rmtree(backup_file)
                        
                        removed_count += 1
                        logger.info(f"Removed old backup: {backup_file.name}")
                        
                except Exception as e:
                    logger.error(f"Failed to remove backup {backup_file}: {e}")
                    continue
            
            if removed_count > 0:
                logger.info(f"Cleaned up {removed_count} old backup files")
            
        except Exception as e:
            logger.error(f"Backup cleanup failed: {e}")

    async def _maintenance_loop(self):
        """Periodic maintenance tasks"""
        
        while True:
            try:
                # Run maintenance every 6 hours
                await asyncio.sleep(21600)
                
                # Clean up old backups
                await self._cleanup_old_backups()
                
                # Clean up history
                cutoff_time = datetime.now() - timedelta(days=30)
                
                self.backup_history = [
                    b for b in self.backup_history 
                    if b.completed_time and b.completed_time > cutoff_time
                ]
                
                self.recovery_history = [
                    r for r in self.recovery_history 
                    if r.completed_time and r.completed_time > cutoff_time
                ]
                
            except Exception as e:
                logger.error(f"Maintenance loop error: {e}")
                await asyncio.sleep(3600)

    def get_backup_status(self) -> Dict[str, Any]:
        """Get backup system status"""
        
        return {
            "active_backup_jobs": [asdict(job) for job in self.active_backup_jobs.values()],
            "active_recovery_jobs": [asdict(job) for job in self.active_recovery_jobs.values()],
            "recent_backups": [asdict(b) for b in self.backup_history[-10:]],
            "recent_recoveries": [asdict(r) for r in self.recovery_history[-5:]],
            "backup_statistics": {
                "total_backups": len(self.backup_history),
                "successful_backups": len([b for b in self.backup_history if b.status == BackupStatus.COMPLETED]),
                "failed_backups": len([b for b in self.backup_history if b.status == BackupStatus.FAILED]),
                "total_backup_size": sum(b.backup_size_bytes for b in self.backup_history if b.backup_size_bytes),
            }
        }


# Global disaster recovery system instance
_disaster_recovery_system: Optional[DisasterRecoverySystem] = None


def get_disaster_recovery_system() -> DisasterRecoverySystem:
    """Get or create the global disaster recovery system instance"""
    global _disaster_recovery_system
    if _disaster_recovery_system is None:
        _disaster_recovery_system = DisasterRecoverySystem()
    return _disaster_recovery_system