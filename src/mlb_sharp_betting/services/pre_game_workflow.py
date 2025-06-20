#!/usr/bin/env python3
"""
Pre-Game Automated Workflow Service

Implements a three-stage automated workflow that triggers 5 minutes before each MLB game:

Stage 1: Pre-Game Data Collection
- Triggers the main entrypoint program for current betting data collection
- Includes proper error handling and retry logic

Stage 2: Betting Analysis  
- Executes master betting detector with 5-minute analysis window
- Only runs if Stage 1 completes successfully
- Captures full output for email transmission

Stage 3: Automated Notification
- Sends email containing betting detector results
- Includes both text output and analysis files as attachments
- Formatted for mobile reading with HTML and plain text versions

Technical Features:
- Separate from existing scheduler system
- 3 retry attempts for failed stages with exponential backoff
- Comprehensive error handling and logging
- Timeout handling for hanging processes
- Email notifications for both success and failure cases
- Integration with existing alert service infrastructure
"""

import asyncio
import subprocess
import smtplib
import tempfile
import zipfile
from datetime import datetime, timezone, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, asdict
from enum import Enum
import structlog
import pytz

from .mlb_api_service import MLBStatsAPIService, MLBGameInfo
from .alert_service import AlertService, AlertSeverity
from ..core.logging import get_logger
from ..core.config import get_settings

logger = get_logger(__name__)


class WorkflowStage(Enum):
    """Workflow stage identifiers."""
    DATA_COLLECTION = "data_collection"
    BETTING_ANALYSIS = "betting_analysis"
    EMAIL_NOTIFICATION = "email_notification"


class StageStatus(Enum):
    """Stage execution status."""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    RETRYING = "retrying"
    SKIPPED = "skipped"


class EmailType(Enum):
    """Email type for tracking success vs failure notifications."""
    SUCCESS = "success"
    FAILURE = "failure"


@dataclass
class StageResult:
    """Result of a workflow stage execution."""
    stage: WorkflowStage
    status: StageStatus
    start_time: datetime
    end_time: Optional[datetime] = None
    execution_time_seconds: float = 0.0
    stdout: str = ""
    stderr: str = ""
    return_code: Optional[int] = None
    error_message: str = ""
    retry_count: int = 0
    output_files: List[Path] = None
    
    def __post_init__(self):
        if self.output_files is None:
            self.output_files = []


@dataclass
class WorkflowResult:
    """Complete workflow execution result."""
    game: MLBGameInfo
    workflow_id: str
    start_time: datetime
    end_time: Optional[datetime] = None
    total_execution_time: float = 0.0
    stages: Dict[WorkflowStage, StageResult] = None
    overall_status: StageStatus = StageStatus.PENDING
    email_sent: bool = False
    
    def __post_init__(self):
        if self.stages is None:
            self.stages = {}


class EmailConfig:
    """Email configuration for Gmail SMTP."""
    
    def __init__(self):
        self.settings = get_settings()
        
        # Gmail SMTP settings
        self.smtp_server = "smtp.gmail.com"
        self.smtp_port = 587
        
        # Email credentials from environment or settings
        self.from_email = getattr(self.settings, 'email_from_address', None)
        self.app_password = getattr(self.settings, 'email_app_password', None)
        self.to_emails = self.settings.get_email_list()
        
        # Default recipients if not configured
        if not self.to_emails:
            self.to_emails = ["your-betting-alerts@gmail.com"]  # User should update this
        
        self.enabled = bool(self.from_email and self.app_password)
    
    def is_configured(self) -> bool:
        """Check if email is properly configured."""
        return self.enabled and bool(self.to_emails)


class PreGameWorkflowService:
    """Service for executing pre-game automated workflows."""
    
    def __init__(self, 
                 project_root: Optional[Path] = None,
                 max_retries: int = 3,
                 stage_timeout_seconds: int = 90,  # 90 seconds per stage (total ~4.5 min max)
                 retry_delay_base: float = 2.0):
        """
        Initialize the pre-game workflow service.
        
        Args:
            project_root: Path to project root directory
            max_retries: Maximum retry attempts per stage
            stage_timeout_seconds: Timeout for each stage execution
            retry_delay_base: Base delay for exponential backoff (seconds)
        """
        self.project_root = project_root or Path(__file__).parent.parent.parent.parent
        self.max_retries = max_retries
        self.stage_timeout = stage_timeout_seconds
        self.retry_delay_base = retry_delay_base
        
        # Services
        self.mlb_api = MLBStatsAPIService()
        self.alert_service = AlertService()
        self.email_config = EmailConfig()
        
        # Timezone setup
        self.est = pytz.timezone('US/Eastern')
        
        # Workflow tracking
        self.active_workflows: Dict[str, WorkflowResult] = {}
        self.workflow_history: List[WorkflowResult] = []
        
        # Email deduplication tracking with type awareness
        self.emails_sent_today: Dict[str, Tuple[datetime, EmailType]] = {}  # game_pk -> (timestamp, email_type)
        
        # Metrics
        self.metrics = {
            "workflows_executed": 0,
            "workflows_successful": 0,
            "workflows_failed": 0,
            "stage_failures": 0,
            "emails_sent": 0,
            "success_emails_sent": 0,
            "failure_emails_sent": 0,
            "emails_deduplicated": 0,
            "status_change_emails_sent": 0,  # Success after failure or failure after success
            "total_retries": 0
        }
        
        self.logger = logger.bind(service="pre_game_workflow")
        
        # Validate configuration on startup
        if not self.email_config.is_configured():
            self.logger.warning("Email not configured - notifications will be logged only",
                              has_from_email=bool(self.email_config.from_email),
                              has_app_password=bool(self.email_config.app_password),
                              has_recipients=bool(self.email_config.to_emails))
    
    def _should_send_email(self, game: MLBGameInfo, email_type: EmailType) -> bool:
        """
        Check if we should send an email for this game.
        Allows status change emails (success after failure, failure after success)
        but prevents duplicate emails of the same type.
        
        Args:
            game: MLB game information
            email_type: Type of email being sent (SUCCESS or FAILURE)
            
        Returns:
            True if email should be sent, False if duplicate of same type
        """
        game_key = str(game.game_pk)
        today = datetime.now(self.est).date()
        
        # Clean up old entries (older than today)
        self.emails_sent_today = {
            k: v for k, v in self.emails_sent_today.items()
            if v[0].date() >= today  # v[0] is the timestamp
        }
        
        # Check if we already sent an email for this game today
        if game_key in self.emails_sent_today:
            last_sent_time, last_email_type = self.emails_sent_today[game_key]
            if last_sent_time.date() == today:
                if last_email_type == email_type:
                    # Same type of email already sent today - block duplicate
                    self.logger.info("Email of same type already sent for this game today",
                                   game=f"{game.away_team} @ {game.home_team}",
                                   email_type=email_type.value,
                                   last_sent=last_sent_time.isoformat())
                    self.metrics["emails_deduplicated"] += 1
                    return False
                else:
                    # Different type of email - this is a status change, allow it
                    self.logger.info("Status change email allowed",
                                   game=f"{game.away_team} @ {game.home_team}",
                                   previous_type=last_email_type.value,
                                   current_type=email_type.value,
                                   last_sent=last_sent_time.isoformat())
                    self.metrics["status_change_emails_sent"] += 1
                    return True
        
        return True
    
    def _mark_email_sent(self, game: MLBGameInfo, email_type: EmailType):
        """Mark that an email has been sent for this game."""
        game_key = str(game.game_pk)
        self.emails_sent_today[game_key] = (datetime.now(self.est), email_type)
    
    async def execute_pre_game_workflow(self, game: MLBGameInfo) -> WorkflowResult:
        """
        Execute the complete three-stage pre-game workflow for a specific game.
        
        Args:
            game: MLB game information
            
        Returns:
            Complete workflow execution result
        """
        workflow_id = f"pregame_{game.game_pk}_{datetime.now(timezone.utc).timestamp()}"
        game_desc = f"{game.away_team} @ {game.home_team}"
        
        self.logger.info("Starting pre-game workflow", 
                        workflow_id=workflow_id,
                        game=game_desc,
                        game_time=game.game_date.isoformat())
        
        # Initialize workflow result
        workflow_result = WorkflowResult(
            game=game,
            workflow_id=workflow_id,
            start_time=datetime.now(timezone.utc)
        )
        
        self.active_workflows[workflow_id] = workflow_result
        
        try:
            # Stage 1: Data Collection
            data_result = await self._execute_stage_with_retry(
                WorkflowStage.DATA_COLLECTION,
                self._execute_data_collection,
                workflow_result,
                game_desc
            )
            workflow_result.stages[WorkflowStage.DATA_COLLECTION] = data_result
            
            # Stage 2: Betting Analysis (only if Stage 1 succeeded)
            if data_result.status == StageStatus.SUCCESS:
                analysis_result = await self._execute_stage_with_retry(
                    WorkflowStage.BETTING_ANALYSIS,
                    self._execute_betting_analysis,
                    workflow_result,
                    game_desc
                )
                workflow_result.stages[WorkflowStage.BETTING_ANALYSIS] = analysis_result
            else:
                # Skip Stage 2 if Stage 1 failed
                workflow_result.stages[WorkflowStage.BETTING_ANALYSIS] = StageResult(
                    stage=WorkflowStage.BETTING_ANALYSIS,
                    status=StageStatus.SKIPPED,
                    start_time=datetime.now(timezone.utc),
                    end_time=datetime.now(timezone.utc),
                    error_message="Skipped due to Stage 1 failure"
                )
            
            # Stage 3: Email Notification (always execute to report results)
            # Email stage should not retry to prevent multiple emails
            email_result = StageResult(
                stage=WorkflowStage.EMAIL_NOTIFICATION,
                status=StageStatus.PENDING,
                start_time=datetime.now(timezone.utc)
            )
            
            try:
                await self._execute_email_notification(email_result, workflow_result)
                if email_result.status != StageStatus.FAILED:
                    email_result.status = StageStatus.SUCCESS
            except Exception as e:
                email_result.status = StageStatus.FAILED
                email_result.error_message = str(e)
                self.logger.error("Email notification failed", error=str(e))
            
            email_result.end_time = datetime.now(timezone.utc)
            email_result.execution_time_seconds = (
                email_result.end_time - email_result.start_time
            ).total_seconds()
            workflow_result.stages[WorkflowStage.EMAIL_NOTIFICATION] = email_result
            workflow_result.email_sent = email_result.status == StageStatus.SUCCESS
            
            # Determine overall status
            if (data_result.status == StageStatus.SUCCESS and 
                workflow_result.stages[WorkflowStage.BETTING_ANALYSIS].status == StageStatus.SUCCESS):
                workflow_result.overall_status = StageStatus.SUCCESS
                self.metrics["workflows_successful"] += 1
            else:
                workflow_result.overall_status = StageStatus.FAILED
                self.metrics["workflows_failed"] += 1
            
        except Exception as e:
            self.logger.error("Workflow execution failed with exception",
                            workflow_id=workflow_id,
                            error=str(e))
            workflow_result.overall_status = StageStatus.FAILED
            workflow_result.stages[WorkflowStage.EMAIL_NOTIFICATION] = await self._send_error_notification(
                workflow_result, str(e)
            )
            self.metrics["workflows_failed"] += 1
        
        finally:
            # Finalize workflow
            workflow_result.end_time = datetime.now(timezone.utc)
            workflow_result.total_execution_time = (
                workflow_result.end_time - workflow_result.start_time
            ).total_seconds()
            
            # Move to history and clean up active tracking
            self.workflow_history.append(workflow_result)
            if workflow_id in self.active_workflows:
                del self.active_workflows[workflow_id]
            
            self.metrics["workflows_executed"] += 1
            
            self.logger.info("Pre-game workflow completed",
                           workflow_id=workflow_id,
                           overall_status=workflow_result.overall_status.value,
                           total_time=workflow_result.total_execution_time,
                           email_sent=workflow_result.email_sent)
        
        return workflow_result
    
    async def _execute_stage_with_retry(self,
                                      stage: WorkflowStage,
                                      stage_func: callable,
                                      workflow_result: WorkflowResult,
                                      game_desc: str) -> StageResult:
        """
        Execute a workflow stage with retry logic.
        
        Args:
            stage: Stage to execute
            stage_func: Function to execute for this stage
            workflow_result: Current workflow result
            game_desc: Game description for logging
            
        Returns:
            Stage execution result
        """
        stage_result = StageResult(
            stage=stage,
            status=StageStatus.PENDING,
            start_time=datetime.now(timezone.utc)
        )
        
        retry_count = 0
        
        while retry_count <= self.max_retries:
            try:
                if retry_count > 0:
                    # Exponential backoff delay
                    delay = self.retry_delay_base ** retry_count
                    self.logger.info(f"Retrying {stage.value} in {delay} seconds",
                                   retry_count=retry_count,
                                   game=game_desc)
                    await asyncio.sleep(delay)
                    stage_result.status = StageStatus.RETRYING
                    self.metrics["total_retries"] += 1
                
                stage_result.status = StageStatus.RUNNING
                stage_result.retry_count = retry_count
                
                # Execute stage function
                await stage_func(stage_result, workflow_result)
                
                if stage_result.status != StageStatus.FAILED:
                    stage_result.status = StageStatus.SUCCESS
                    break
                    
            except asyncio.TimeoutError:
                error_msg = f"Stage {stage.value} timed out after {self.stage_timeout} seconds"
                stage_result.error_message = error_msg
                stage_result.status = StageStatus.FAILED
                self.logger.error("Stage execution timed out",
                                stage=stage.value,
                                retry_count=retry_count,
                                timeout=self.stage_timeout)
                
            except Exception as e:
                error_msg = f"Stage {stage.value} failed: {str(e)}"
                stage_result.error_message = error_msg
                stage_result.status = StageStatus.FAILED
                self.logger.error("Stage execution failed",
                                stage=stage.value,
                                retry_count=retry_count,
                                error=str(e))
            
            retry_count += 1
            if retry_count > self.max_retries:
                self.logger.error(f"Stage {stage.value} failed after {self.max_retries} retries",
                                game=game_desc)
                self.metrics["stage_failures"] += 1
                break
        
        # Finalize stage result
        stage_result.end_time = datetime.now(timezone.utc)
        stage_result.execution_time_seconds = (
            stage_result.end_time - stage_result.start_time
        ).total_seconds()
        
        return stage_result
    
    async def _execute_data_collection(self, stage_result: StageResult, workflow_result: WorkflowResult):
        """Execute Stage 1: Data Collection using the main entrypoint."""
        self.logger.info("Executing Stage 1: Data Collection")
        
        try:
            # Build command for entrypoint execution
            entrypoint_path = self.project_root / "src" / "mlb_sharp_betting" / "entrypoint.py"
            cmd = ["uv", "run", str(entrypoint_path), "--verbose", "--sport", "mlb", "--sportsbook", "circa"]
            
            # Execute with timeout
            result = await asyncio.wait_for(
                self._run_subprocess(cmd, cwd=self.project_root),
                timeout=self.stage_timeout
            )
            
            stage_result.stdout = result["stdout"]
            stage_result.stderr = result["stderr"] 
            stage_result.return_code = result["returncode"]
            
            if result["returncode"] == 0:
                self.logger.info("Data collection completed successfully",
                               execution_time=stage_result.execution_time_seconds)
            else:
                stage_result.status = StageStatus.FAILED
                stage_result.error_message = f"Entrypoint failed with exit code {result['returncode']}"
                self.logger.error("Data collection failed",
                                return_code=result["returncode"],
                                stderr=result["stderr"])
                
        except Exception as e:
            stage_result.status = StageStatus.FAILED
            stage_result.error_message = str(e)
            raise
    
    async def _execute_betting_analysis(self, stage_result: StageResult, workflow_result: WorkflowResult):
        """Execute Stage 2: Betting Analysis using master betting detector."""
        self.logger.info("Executing Stage 2: Betting Analysis")
        
        try:
            # Build command for master betting detector
            detector_path = self.project_root / "analysis_scripts" / "master_betting_detector.py"
            cmd = ["uv", "run", str(detector_path), "--minutes", "5"]
            
            # Execute with timeout
            result = await asyncio.wait_for(
                self._run_subprocess(cmd, cwd=self.project_root),
                timeout=self.stage_timeout
            )
            
            stage_result.stdout = result["stdout"]
            stage_result.stderr = result["stderr"]
            stage_result.return_code = result["returncode"]
            
            if result["returncode"] == 0:
                # Look for any generated output files
                output_files = []
                
                # Check for common output file patterns
                analysis_dir = self.project_root / "analysis_results"
                if analysis_dir.exists():
                    # Look for recent files (created in last 10 minutes)
                    cutoff_time = datetime.now() - timedelta(minutes=10)
                    for file_path in analysis_dir.glob("*"):
                        if file_path.is_file():
                            file_time = datetime.fromtimestamp(file_path.stat().st_mtime)
                            if file_time > cutoff_time:
                                output_files.append(file_path)
                
                stage_result.output_files = output_files
                
                # Log betting recommendations for tracking
                await self._log_betting_recommendations(stage_result.stdout, workflow_result.game)
                
                self.logger.info("Betting analysis completed successfully",
                               execution_time=stage_result.execution_time_seconds,
                               output_files_count=len(output_files))
            else:
                stage_result.status = StageStatus.FAILED
                stage_result.error_message = f"Master detector failed with exit code {result['returncode']}"
                self.logger.error("Betting analysis failed",
                                return_code=result["returncode"],
                                stderr=result["stderr"])
                
        except Exception as e:
            stage_result.status = StageStatus.FAILED
            stage_result.error_message = str(e)
            raise
    
    async def _execute_email_notification(self, stage_result: StageResult, workflow_result: WorkflowResult):
        """Execute Stage 3: Email Notification with results."""
        self.logger.info("Executing Stage 3: Email Notification")
        
        try:
            if not self.email_config.is_configured():
                stage_result.status = StageStatus.FAILED
                stage_result.error_message = "Email not configured"
                self.logger.warning("Email notification skipped - not configured")
                return
            
            # Determine email type based on workflow status
            email_type = EmailType.SUCCESS if workflow_result.overall_status == StageStatus.SUCCESS else EmailType.FAILURE
            
            # Check if we should send email (type-aware deduplication)
            if not self._should_send_email(workflow_result.game, email_type):
                stage_result.status = StageStatus.SUCCESS
                stage_result.error_message = f"Email skipped - {email_type.value} email already sent today"
                self.logger.info("Email notification skipped - same type already sent for this game today",
                               email_type=email_type.value)
                return
            
            # Generate email content
            email_content = self._generate_email_content(workflow_result)
            
            # Send email
            await self._send_email(
                subject=email_content["subject"],
                plain_text=email_content["plain_text"],
                html_content=email_content["html"],
                attachments=email_content["attachments"]
            )
            
            # Mark email as sent for this game with type
            self._mark_email_sent(workflow_result.game, email_type)
            
            # Update metrics
            self.metrics["emails_sent"] += 1
            if email_type == EmailType.SUCCESS:
                self.metrics["success_emails_sent"] += 1
            else:
                self.metrics["failure_emails_sent"] += 1
                
            self.logger.info("Email notification sent successfully",
                           email_type=email_type.value)
            
        except Exception as e:
            stage_result.status = StageStatus.FAILED
            stage_result.error_message = str(e)
            raise
    
    def _generate_email_content(self, workflow_result: WorkflowResult) -> Dict[str, Any]:
        """Generate email content with both plain text and HTML versions."""
        game = workflow_result.game
        game_desc = f"{game.away_team} @ {game.home_team}"
        
        # Determine overall status emoji
        if workflow_result.overall_status == StageStatus.SUCCESS:
            status_emoji = "✅"
            status_text = "SUCCESS"
        else:
            status_emoji = "❌"
            status_text = "FAILED"
        
        # Game time in EST for mobile readability
        if game.game_date.tzinfo is None:
            game_time_est = self.est.localize(game.game_date)
        else:
            game_time_est = game.game_date.astimezone(self.est)
        
        game_time_str = game_time_est.strftime("%I:%M %p EST")
        
        # Check if analysis found betting opportunities for clearer subject
        analysis_stage = workflow_result.stages.get(WorkflowStage.BETTING_ANALYSIS)
        has_betting_opportunities = False
        
        if analysis_stage and analysis_stage.stdout:
            # Check if analysis found betting opportunities
            has_betting_opportunities = "VALIDATED BETTING SIGNALS" in analysis_stage.stdout
        
        # Generate clear subject line
        if workflow_result.overall_status == StageStatus.SUCCESS:
            if has_betting_opportunities:
                subject = f"🎯 BETTING OPPORTUNITIES: {game_desc} ({game_time_str})"
            else:
                subject = f"🚫 NO BETS RECOMMENDED: {game_desc} ({game_time_str})"
        else:
            subject = f"❌ Analysis Failed: {game_desc} ({game_time_str})"
        
        # Get analysis output (no truncation - send full output)
        analysis_stage = workflow_result.stages.get(WorkflowStage.BETTING_ANALYSIS)
        analysis_output = ""
        if analysis_stage and analysis_stage.stdout:
            # Send full output (no truncation)
            analysis_output = analysis_stage.stdout
        
        # Generate plain text version
        plain_text = f"""🏈 MLB PRE-GAME ANALYSIS REPORT

Game: {game_desc}
Time: {game_time_str}
Status: {status_emoji} {status_text}

WORKFLOW SUMMARY:
├─ Stage 1 (Data Collection): {self._get_stage_status_text(workflow_result.stages.get(WorkflowStage.DATA_COLLECTION))}
├─ Stage 2 (Betting Analysis): {self._get_stage_status_text(workflow_result.stages.get(WorkflowStage.BETTING_ANALYSIS))}
└─ Stage 3 (Email Notification): {self._get_stage_status_text(workflow_result.stages.get(WorkflowStage.EMAIL_NOTIFICATION))}

Total Execution Time: {workflow_result.total_execution_time:.1f}s

"""
        
        if analysis_output:
            plain_text += f"""BETTING ANALYSIS RESULTS:
{'-' * 40}
{analysis_output}

"""
        
        # Add error details if any stage failed
        for stage_name, stage_result in workflow_result.stages.items():
            if stage_result.status == StageStatus.FAILED:
                plain_text += f"""ERROR DETAILS ({stage_name.value.upper()}):
{stage_result.error_message}
"""
                if stage_result.stderr:
                    plain_text += f"Error Output: {stage_result.stderr[:500]}\n"
        
        plain_text += """
---
Generated by MLB Sharp Betting Analytics Platform
General Balls"""
        
        # Generate HTML version
        html_content = self._generate_html_email(workflow_result, game_desc, game_time_str, analysis_output)
        
        # Collect attachments
        attachments = []
        analysis_stage = workflow_result.stages.get(WorkflowStage.BETTING_ANALYSIS)
        if analysis_stage and analysis_stage.output_files:
            attachments.extend(analysis_stage.output_files)
        
        return {
            "subject": subject,
            "plain_text": plain_text,
            "html": html_content,
            "attachments": attachments
        }
    
    def _generate_html_email(self, workflow_result: WorkflowResult, game_desc: str, 
                           game_time_str: str, analysis_output: str) -> str:
        """Generate HTML email content optimized for mobile."""
        
        status_color = "#28a745" if workflow_result.overall_status == StageStatus.SUCCESS else "#dc3545"
        status_text = "SUCCESS" if workflow_result.overall_status == StageStatus.SUCCESS else "FAILED"
        
        html = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>MLB Pre-Game Analysis</title>
    <style>
        body {{ 
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Arial, sans-serif;
            line-height: 1.6;
            color: #333;
            max-width: 600px;
            margin: 0 auto;
            padding: 20px;
            background-color: #f8f9fa;
        }}
        .container {{
            background: white;
            border-radius: 8px;
            padding: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .header {{
            text-align: center;
            border-bottom: 2px solid #eee;
            padding-bottom: 15px;
            margin-bottom: 20px;
        }}
        .status {{
            color: {status_color};
            font-weight: bold;
            font-size: 1.2em;
        }}
        .game-info {{
            background: #f8f9fa;
            padding: 15px;
            border-radius: 6px;
            margin: 15px 0;
        }}
        .stage {{
            margin: 10px 0;
            padding: 8px;
            border-left: 4px solid #ddd;
        }}
        .stage.success {{ border-left-color: #28a745; }}
        .stage.failed {{ border-left-color: #dc3545; }}
        .stage.skipped {{ border-left-color: #ffc107; }}
        .analysis {{
            background: #f8f9fa;
            padding: 15px;
            border-radius: 6px;
            font-family: 'Courier New', monospace;
            font-size: 0.9em;
            white-space: pre-wrap;
            max-height: 400px;
            overflow-y: auto;
        }}
        .footer {{
            text-align: center;
            margin-top: 20px;
            padding-top: 15px;
            border-top: 1px solid #eee;
            font-size: 0.9em;
            color: #666;
        }}
        @media (max-width: 480px) {{
            body {{ padding: 10px; }}
            .container {{ padding: 15px; }}
            .analysis {{ font-size: 0.8em; }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🏈 MLB Pre-Game Analysis</h1>
            <div class="status">{status_text}</div>
        </div>
        
        <div class="game-info">
            <h2>{game_desc}</h2>
            <p><strong>Game Time:</strong> {game_time_str}</p>
            <p><strong>Total Execution:</strong> {workflow_result.total_execution_time:.1f} seconds</p>
        </div>
        
        <h3>Workflow Summary</h3>
"""
        
        # Add stage results
        for stage_name, stage_result in workflow_result.stages.items():
            stage_class = stage_result.status.value.lower()
            status_emoji = {"success": "✅", "failed": "❌", "skipped": "⏭️"}.get(stage_class, "❓")
            
            html += f"""
        <div class="stage {stage_class}">
            <strong>{status_emoji} Stage {stage_name.value.replace('_', ' ').title()}</strong>
            <br>Status: {stage_result.status.value.title()}
"""
            if stage_result.execution_time_seconds:
                html += f"<br>Time: {stage_result.execution_time_seconds:.1f}s"
            if stage_result.retry_count > 0:
                html += f"<br>Retries: {stage_result.retry_count}"
            if stage_result.error_message:
                html += f"<br><em>Error: {stage_result.error_message}</em>"
            html += "</div>"
        
        # Add analysis output if available
        if analysis_output:
            html += f"""
        <h3>Betting Analysis Results</h3>
        <div class="analysis">{analysis_output}</div>
"""
        
        html += """
        <div class="footer">
            <p>Generated by MLB Sharp Betting Analytics Platform</p>
            <p><em>General Balls</em></p>
        </div>
    </div>
</body>
</html>
"""
        return html
    
    async def _log_betting_recommendations(self, analysis_output: str, game: MLBGameInfo) -> None:
        """Log betting recommendations for performance tracking."""
        try:
            # Import the tracker (avoid circular imports)
            from .pre_game_recommendation_tracker import PreGameRecommendationTracker
            
            tracker = PreGameRecommendationTracker()
            
            # Parse recommendations from analysis output
            game_info = {
                'game_pk': game.game_pk,
                'home_team': game.home_team,
                'away_team': game.away_team,
                'game_datetime': game.game_date
            }
            
            recommendations = tracker.parse_pre_game_email_content(analysis_output, game_info)
            
            # Log them to database
            if recommendations:
                await tracker.log_pre_game_recommendations(recommendations)
                self.logger.info("Logged betting recommendations for tracking",
                               game=f"{game.away_team} @ {game.home_team}",
                               recommendations_count=len(recommendations))
            
        except Exception as e:
            self.logger.warning("Failed to log betting recommendations", 
                              game=f"{game.away_team} @ {game.home_team}",
                              error=str(e))
    
    def _get_stage_status_text(self, stage_result: Optional[StageResult]) -> str:
        """Get human-readable stage status text."""
        if not stage_result:
            return "❓ Not Executed"
        
        status_map = {
            StageStatus.SUCCESS: "✅ Success",
            StageStatus.FAILED: "❌ Failed", 
            StageStatus.SKIPPED: "⏭️ Skipped",
            StageStatus.RUNNING: "🔄 Running",
            StageStatus.RETRYING: "🔄 Retrying"
        }
        
        base_text = status_map.get(stage_result.status, f"❓ {stage_result.status.value}")
        
        if stage_result.execution_time_seconds:
            base_text += f" ({stage_result.execution_time_seconds:.1f}s)"
        
        if stage_result.retry_count > 0:
            base_text += f" [Retries: {stage_result.retry_count}]"
            
        return base_text
    
    async def _send_email(self, subject: str, plain_text: str, html_content: str, 
                         attachments: List[Path] = None):
        """Send email using Gmail SMTP."""
        if attachments is None:
            attachments = []
            
        msg = MIMEMultipart('alternative')
        msg['From'] = self.email_config.from_email
        msg['To'] = ", ".join(self.email_config.to_emails)
        msg['Subject'] = subject
        
        # Add plain text part
        text_part = MIMEText(plain_text, 'plain')
        msg.attach(text_part)
        
        # Add HTML part
        html_part = MIMEText(html_content, 'html')
        msg.attach(html_part)
        
        # Add attachments
        if attachments:
            for file_path in attachments:
                if file_path.exists():
                    try:
                        with open(file_path, 'rb') as f:
                            attachment = MIMEBase('application', 'octet-stream')
                            attachment.set_payload(f.read())
                            encoders.encode_base64(attachment)
                            attachment.add_header(
                                'Content-Disposition',
                                f'attachment; filename= {file_path.name}'
                            )
                            msg.attach(attachment)
                    except Exception as e:
                        self.logger.warning("Failed to attach file", 
                                          file_path=str(file_path), 
                                          error=str(e))
        
        # Send email
        server = smtplib.SMTP(self.email_config.smtp_server, self.email_config.smtp_port)
        server.starttls()
        server.login(self.email_config.from_email, self.email_config.app_password)
        server.send_message(msg)
        server.quit()
        
        self.logger.info("Email sent successfully",
                        recipients=len(self.email_config.to_emails),
                        attachments_count=len(attachments))
    
    async def _send_error_notification(self, workflow_result: WorkflowResult, error_message: str) -> StageResult:
        """Send error notification when workflow fails catastrophically."""
        stage_result = StageResult(
            stage=WorkflowStage.EMAIL_NOTIFICATION,
            status=StageStatus.RUNNING,
            start_time=datetime.now(timezone.utc)
        )
        
        try:
            if not self.email_config.is_configured():
                stage_result.status = StageStatus.FAILED
                stage_result.error_message = "Email not configured"
                return stage_result
            
            game_desc = f"{workflow_result.game.away_team} @ {workflow_result.game.home_team}"
            
            subject = f"❌ Pre-Game Workflow FAILED: {game_desc}"
            
            plain_text = f"""🚨 WORKFLOW FAILURE ALERT

Game: {game_desc}
Workflow ID: {workflow_result.workflow_id}
Error: {error_message}

The pre-game analysis workflow has failed unexpectedly.
Please check system logs and investigate the issue.

---
Generated by MLB Sharp Betting Analytics Platform
General Balls"""
            
            html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
</head>
<body style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto; padding: 20px;">
    <div style="background: #f8d7da; color: #721c24; padding: 20px; border-radius: 8px;">
        <h1>🚨 Workflow Failure Alert</h1>
        <p><strong>Game:</strong> {game_desc}</p>
        <p><strong>Error:</strong> {error_message}</p>
        <p>The pre-game analysis workflow has failed unexpectedly. Please investigate.</p>
    </div>
</body>
</html>"""
            
            await self._send_email(subject, plain_text, html_content)
            stage_result.status = StageStatus.SUCCESS
            
        except Exception as e:
            stage_result.status = StageStatus.FAILED
            stage_result.error_message = str(e)
            self.logger.error("Failed to send error notification", error=str(e))
        
        finally:
            stage_result.end_time = datetime.now(timezone.utc)
            stage_result.execution_time_seconds = (
                stage_result.end_time - stage_result.start_time
            ).total_seconds()
        
        return stage_result
    
    async def _run_subprocess(self, cmd: List[str], cwd: Path) -> Dict[str, Any]:
        """Run subprocess with proper async handling."""
        process = await asyncio.create_subprocess_exec(
            *cmd,
            cwd=cwd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        stdout, stderr = await process.communicate()
        
        return {
            "returncode": process.returncode,
            "stdout": stdout.decode('utf-8', errors='ignore'),
            "stderr": stderr.decode('utf-8', errors='ignore')
        }
    
    def get_workflow_status(self, workflow_id: str) -> Optional[WorkflowResult]:
        """Get status of a specific workflow."""
        # Check active workflows first
        if workflow_id in self.active_workflows:
            return self.active_workflows[workflow_id]
        
        # Check history
        for workflow in self.workflow_history:
            if workflow.workflow_id == workflow_id:
                return workflow
        
        return None
    
    def get_recent_workflows(self, limit: int = 10) -> List[WorkflowResult]:
        """Get recent workflow results."""
        return sorted(self.workflow_history, 
                     key=lambda w: w.start_time, 
                     reverse=True)[:limit]
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get workflow service metrics."""
        return {
            **self.metrics,
            "active_workflows": len(self.active_workflows),
            "workflow_history_count": len(self.workflow_history),
            "email_configured": self.email_config.is_configured()
        } 