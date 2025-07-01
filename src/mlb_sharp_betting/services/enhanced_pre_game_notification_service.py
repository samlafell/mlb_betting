"""
Enhanced Pre-Game Notification Service

This service implements precise 5-minute pre-game email notifications as required
by the enhanced backtesting methodology. Key features:

1. Precise timing control - notifications sent exactly 5 minutes before first pitch
2. Automatic database tracking integration with tracking.pre_game_recommendations
3. Enhanced email content with complete betting recommendations
4. Robust error handling and retry mechanisms
5. Integration with existing pre-game workflow and recommendation tracker

Addresses backtesting methodology requirements:
- Ensures all pre-game recommendations are properly logged to database
- Provides consistent timing for recommendation delivery
- Links email notifications with backtesting performance tracking
"""

import asyncio
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, asdict
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import structlog
import json

from ..core.config import get_settings
from ..core.logging import get_logger
from ..db.connection import get_db_manager
from .pre_game_recommendation_tracker import PreGameRecommendationTracker, PreGameRecommendation
from .pre_game_workflow import PreGameWorkflow, WorkflowResult
from .mlb_api_service import MLBStatsAPIService
from .alert_service import AlertService


@dataclass
class GameNotificationSchedule:
    """Schedule entry for a game notification."""
    game_pk: int
    home_team: str
    away_team: str
    game_start_time: datetime
    notification_time: datetime
    email_addresses: List[str]
    
    # Status tracking
    scheduled: bool = False
    notification_sent: bool = False
    recommendations_generated: bool = False
    database_logged: bool = False
    
    # Error tracking
    last_error: Optional[str] = None
    retry_count: int = 0
    max_retries: int = 3
    
    # Results
    workflow_result: Optional[WorkflowResult] = None
    recommendations: List[PreGameRecommendation] = None
    
    def __post_init__(self):
        if self.recommendations is None:
            self.recommendations = []


@dataclass
class NotificationMetrics:
    """Metrics for notification service performance."""
    total_games_scheduled: int
    notifications_sent: int
    notifications_failed: int
    recommendations_generated: int
    database_records_created: int
    average_processing_time_seconds: float
    timing_accuracy_seconds: float  # How close to 5-minute target
    
    # Error breakdown
    timing_errors: int = 0
    email_errors: int = 0
    database_errors: int = 0
    workflow_errors: int = 0


class EnhancedPreGameNotificationService:
    """
    Enhanced service for precise 5-minute pre-game notifications with full tracking.
    
    This service ensures that all pre-game recommendations are properly tracked
    in the database and aligned with backtesting methodology requirements.
    """
    
    def __init__(self):
        self.logger = get_logger(__name__)
        self.settings = get_settings()
        self.db_manager = get_db_manager()
        
        # Initialize components
        self.recommendation_tracker = PreGameRecommendationTracker(self.db_manager)
        self.pre_game_workflow = PreGameWorkflow()
        self.mlb_api_service = MLBStatsAPIService()
        self.alert_service = AlertService()
        
        # Notification state
        self.scheduled_notifications: Dict[int, GameNotificationSchedule] = {}
        self.notification_metrics = NotificationMetrics(
            total_games_scheduled=0,
            notifications_sent=0,
            notifications_failed=0,
            recommendations_generated=0,
            database_records_created=0,
            average_processing_time_seconds=0.0,
            timing_accuracy_seconds=0.0
        )
        
        # Configuration
        self.email_config = self._load_email_configuration()
        self.notification_window_minutes = 5  # 5 minutes before game
        self.processing_buffer_seconds = 30  # Buffer for processing time
        
        self.logger.info("Enhanced pre-game notification service initialized")
    
    def _load_email_configuration(self) -> Dict[str, Any]:
        """Load email configuration for notifications."""
        return {
            'smtp_server': getattr(self.settings, 'email_smtp_server', None),
            'smtp_port': getattr(self.settings, 'email_smtp_port', 587),
            'smtp_username': getattr(self.settings, 'email_smtp_username', None),
            'smtp_password': getattr(self.settings, 'email_smtp_password', None),
            'from_address': getattr(self.settings, 'email_from_address', None),
            'to_addresses': getattr(self.settings, 'email_notification_recipients', '').split(',') if getattr(self.settings, 'email_notification_recipients', '') else [],
            'enabled': bool(getattr(self.settings, 'email_smtp_server', None) and getattr(self.settings, 'email_notification_recipients', None))
        }
    
    async def schedule_daily_notifications(self, target_date: Optional[datetime] = None) -> Dict[str, Any]:
        """
        Schedule 5-minute pre-game notifications for all games on target date.
        
        This is the main entry point for setting up notifications for a day's games.
        """
        
        if target_date is None:
            target_date = datetime.now(timezone.utc).date()
        
        self.logger.info("Scheduling daily pre-game notifications", target_date=target_date.isoformat())
        
        try:
            # Get games for the target date
            games = await self.mlb_api_service.get_games_for_date(target_date)
            
            scheduled_count = 0
            scheduling_errors = []
            
            for game in games:
                try:
                    schedule_entry = await self._create_notification_schedule(game)
                    if schedule_entry:
                        self.scheduled_notifications[game['game_pk']] = schedule_entry
                        scheduled_count += 1
                        
                        self.logger.info("Game notification scheduled",
                                       game_pk=game['game_pk'],
                                       teams=f"{schedule_entry.away_team} @ {schedule_entry.home_team}",
                                       notification_time=schedule_entry.notification_time.isoformat())
                    
                except Exception as e:
                    error_msg = f"Failed to schedule game {game.get('game_pk', 'unknown')}: {str(e)}"
                    scheduling_errors.append(error_msg)
                    self.logger.error("Game scheduling failed", 
                                    game_pk=game.get('game_pk'), 
                                    error=str(e))
            
            # Update metrics
            self.notification_metrics.total_games_scheduled += scheduled_count
            
            # Start notification monitoring
            if scheduled_count > 0:
                asyncio.create_task(self._monitor_notification_schedule())
            
            results = {
                'target_date': target_date.isoformat(),
                'total_games_found': len(games),
                'games_scheduled': scheduled_count,
                'scheduling_errors': scheduling_errors,
                'scheduled_games': [
                    {
                        'game_pk': entry.game_pk,
                        'teams': f"{entry.away_team} @ {entry.home_team}",
                        'game_time': entry.game_start_time.isoformat(),
                        'notification_time': entry.notification_time.isoformat()
                    }
                    for entry in self.scheduled_notifications.values()
                    if entry.game_start_time.date() == target_date
                ]
            }
            
            self.logger.info("Daily notification scheduling completed",
                           games_scheduled=scheduled_count,
                           total_games=len(games))
            
            return results
            
        except Exception as e:
            self.logger.error("Failed to schedule daily notifications", error=str(e))
            raise
    
    async def _create_notification_schedule(self, game: Dict[str, Any]) -> Optional[GameNotificationSchedule]:
        """Create a notification schedule for a single game."""
        
        try:
            game_pk = game['game_pk']
            game_start_time = datetime.fromisoformat(game['game_datetime_utc']).replace(tzinfo=timezone.utc)
            
            # Convert to EST for user display (but keep UTC for calculations)
            game_start_est = game_start_time.astimezone(timezone(timedelta(hours=-5)))
            
            # Calculate notification time (5 minutes before game start)
            notification_time = game_start_time - timedelta(minutes=self.notification_window_minutes)
            
            # Add processing buffer
            processing_start_time = notification_time - timedelta(seconds=self.processing_buffer_seconds)
            
            # Skip if notification time has already passed
            if processing_start_time <= datetime.now(timezone.utc):
                self.logger.warning("Game notification time has passed, skipping",
                                  game_pk=game_pk,
                                  notification_time=notification_time.isoformat())
                return None
            
            # Create schedule entry
            schedule = GameNotificationSchedule(
                game_pk=game_pk,
                home_team=game['home_team'],
                away_team=game['away_team'],
                game_start_time=game_start_time,
                notification_time=processing_start_time,  # Use processing start time
                email_addresses=self.email_config['to_addresses']
            )
            
            self.logger.debug("Created notification schedule",
                            game_pk=game_pk,
                            notification_time=schedule.notification_time.isoformat(),
                            game_start_time=schedule.game_start_time.isoformat())
            
            return schedule
            
        except Exception as e:
            self.logger.error("Failed to create notification schedule", 
                            game=game, error=str(e))
            return None
    
    async def _monitor_notification_schedule(self):
        """Monitor scheduled notifications and trigger them at the appropriate time."""
        
        self.logger.info("Starting notification schedule monitoring")
        
        while self.scheduled_notifications:
            current_time = datetime.now(timezone.utc)
            notifications_to_process = []
            
            # Find notifications ready to be processed
            for game_pk, schedule in self.scheduled_notifications.items():
                if (not schedule.notification_sent and 
                    not schedule.scheduled and 
                    current_time >= schedule.notification_time):
                    
                    notifications_to_process.append(schedule)
            
            # Process ready notifications
            for schedule in notifications_to_process:
                try:
                    schedule.scheduled = True
                    await self._process_game_notification(schedule)
                    
                except Exception as e:
                    self.logger.error("Notification processing failed",
                                    game_pk=schedule.game_pk,
                                    error=str(e))
                    
                    schedule.last_error = str(e)
                    schedule.retry_count += 1
                    
                    # Retry logic
                    if schedule.retry_count < schedule.max_retries:
                        schedule.scheduled = False
                        schedule.notification_time = current_time + timedelta(minutes=1)  # Retry in 1 minute
                        self.logger.info("Scheduling notification retry",
                                       game_pk=schedule.game_pk,
                                       retry_count=schedule.retry_count,
                                       next_retry=schedule.notification_time.isoformat())
                    else:
                        self.notification_metrics.notifications_failed += 1
                        self.logger.error("Notification failed after max retries",
                                        game_pk=schedule.game_pk,
                                        retry_count=schedule.retry_count)
            
            # Clean up completed notifications (older than 2 hours)
            cleanup_cutoff = current_time - timedelta(hours=2)
            completed_games = [
                game_pk for game_pk, schedule in self.scheduled_notifications.items()
                if schedule.game_start_time < cleanup_cutoff
            ]
            
            for game_pk in completed_games:
                del self.scheduled_notifications[game_pk]
            
            # Wait before next check
            await asyncio.sleep(30)  # Check every 30 seconds
        
        self.logger.info("Notification schedule monitoring completed")
    
    async def _process_game_notification(self, schedule: GameNotificationSchedule):
        """Process a single game notification with full tracking."""
        
        processing_start = datetime.now(timezone.utc)
        self.logger.info("Processing game notification",
                        game_pk=schedule.game_pk,
                        teams=f"{schedule.away_team} @ {schedule.home_team}")
        
        try:
            # Step 1: Run pre-game workflow to generate recommendations
            game_info = {
                'game_pk': schedule.game_pk,
                'home_team': schedule.home_team,
                'away_team': schedule.away_team,
                'game_datetime': schedule.game_start_time
            }
            
            self.logger.info("Running pre-game workflow", game_pk=schedule.game_pk)
            workflow_result = await self.pre_game_workflow.execute_full_workflow(
                game_info=game_info,
                send_email=False  # We'll send our own enhanced email
            )
            
            schedule.workflow_result = workflow_result
            
            # Step 2: Extract recommendations from workflow result
            if workflow_result and getattr(workflow_result, 'recommendations', None):
                schedule.recommendations = workflow_result.recommendations
                schedule.recommendations_generated = True
                self.notification_metrics.recommendations_generated += len(schedule.recommendations)
                
                self.logger.info("Recommendations generated",
                               game_pk=schedule.game_pk,
                               recommendation_count=len(schedule.recommendations))
            else:
                self.logger.warning("No recommendations generated from workflow",
                                  game_pk=schedule.game_pk)
                schedule.recommendations = []
            
            # Step 3: Send enhanced email notification
            if self.email_config['enabled'] and schedule.email_addresses:
                await self._send_enhanced_email_notification(schedule)
                schedule.notification_sent = True
                self.notification_metrics.notifications_sent += 1
                
                self.logger.info("Email notification sent",
                               game_pk=schedule.game_pk,
                               recipient_count=len(schedule.email_addresses))
            else:
                self.logger.warning("Email not configured or no recipients",
                                  game_pk=schedule.game_pk,
                                  email_enabled=self.email_config['enabled'],
                                  recipient_count=len(schedule.email_addresses))
            
            # Step 4: Log to database using recommendation tracker
            if schedule.recommendations:
                await self.recommendation_tracker.log_pre_game_recommendations(schedule.recommendations)
                schedule.database_logged = True
                self.notification_metrics.database_records_created += len(schedule.recommendations)
                
                self.logger.info("Recommendations logged to database",
                               game_pk=schedule.game_pk,
                               record_count=len(schedule.recommendations))
            
            # Calculate timing accuracy
            processing_end = datetime.now(timezone.utc)
            processing_time = (processing_end - processing_start).total_seconds()
            
            # Update metrics
            self.notification_metrics.average_processing_time_seconds = (
                (self.notification_metrics.average_processing_time_seconds * 
                 self.notification_metrics.notifications_sent + processing_time) / 
                (self.notification_metrics.notifications_sent + 1)
            )
            
            # Calculate timing accuracy (how close to 5-minute target)
            intended_notification_time = schedule.game_start_time - timedelta(minutes=5)
            actual_notification_time = processing_end
            timing_difference = abs((actual_notification_time - intended_notification_time).total_seconds())
            
            self.notification_metrics.timing_accuracy_seconds = (
                (self.notification_metrics.timing_accuracy_seconds * 
                 self.notification_metrics.notifications_sent + timing_difference) / 
                (self.notification_metrics.notifications_sent + 1)
            )
            
            self.logger.info("Game notification processing completed",
                           game_pk=schedule.game_pk,
                           processing_time_seconds=processing_time,
                           timing_accuracy_seconds=timing_difference,
                           recommendations_count=len(schedule.recommendations))
            
        except Exception as e:
            self.logger.error("Game notification processing failed",
                            game_pk=schedule.game_pk,
                            error=str(e))
            schedule.last_error = str(e)
            raise
    
    async def _send_enhanced_email_notification(self, schedule: GameNotificationSchedule):
        """Send enhanced email notification with complete betting recommendations."""
        
        try:
            # Generate email content
            subject = f"🏈 MLB Betting Alert: {schedule.away_team} @ {schedule.home_team} (5-min warning)"
            
            html_content = self._generate_enhanced_email_html(schedule)
            text_content = self._generate_enhanced_email_text(schedule)
            
            # Create email message
            msg = MIMEMultipart('alternative')
            msg['Subject'] = subject
            msg['From'] = self.email_config['from_address']
            msg['To'] = ', '.join(schedule.email_addresses)
            
            # Add text and HTML parts
            text_part = MIMEText(text_content, 'plain')
            html_part = MIMEText(html_content, 'html')
            
            msg.attach(text_part)
            msg.attach(html_part)
            
            # Send email
            with smtplib.SMTP_SSL(self.email_config['smtp_server'], self.email_config['smtp_port']) as server:
                if self.email_config['smtp_username']:
                    server.login(self.email_config['smtp_username'], self.email_config['smtp_password'])
                
                server.send_message(msg)
            
            self.logger.info("Enhanced email notification sent successfully",
                           game_pk=schedule.game_pk,
                           recipient_count=len(schedule.email_addresses))
            
        except Exception as e:
            self.logger.error("Failed to send enhanced email notification",
                            game_pk=schedule.game_pk,
                            error=str(e))
            raise
    
    def _generate_enhanced_email_html(self, schedule: GameNotificationSchedule) -> str:
        """Generate enhanced HTML email content."""
        
        game_time_est = schedule.game_start_time.astimezone(timezone(timedelta(hours=-5)))
        
        html = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                .header {{ background-color: #1e3a8a; color: white; padding: 15px; border-radius: 5px; }}
                .game-info {{ background-color: #f3f4f6; padding: 15px; margin: 10px 0; border-radius: 5px; }}
                .recommendations {{ margin: 15px 0; }}
                .bet-item {{ background-color: #fef3c7; padding: 10px; margin: 5px 0; border-left: 4px solid #f59e0b; }}
                .high-confidence {{ background-color: #dcfce7; border-left-color: #16a34a; }}
                .moderate-confidence {{ background-color: #fef3c7; border-left-color: #f59e0b; }}
                .low-confidence {{ background-color: #fee2e2; border-left-color: #dc2626; }}
                .footer {{ margin-top: 20px; font-size: 12px; color: #6b7280; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h1>🏈 MLB Betting Alert - 5 Minute Warning!</h1>
                <h2>{schedule.away_team} @ {schedule.home_team}</h2>
            </div>
            
            <div class="game-info">
                <h3>⏰ Game Information</h3>
                <p><strong>Start Time:</strong> {game_time_est.strftime('%I:%M %p EST on %B %d, %Y')}</p>
                <p><strong>Game ID:</strong> {schedule.game_pk}</p>
                <p><strong>Time Until Start:</strong> ~5 minutes</p>
            </div>
            
            <div class="recommendations">
                <h3>💡 Betting Recommendations</h3>
        """
        
        if schedule.recommendations:
            for rec in schedule.recommendations:
                confidence_class = f"{rec.confidence_level.lower()}-confidence"
                
                html += f"""
                <div class="bet-item {confidence_class}">
                    <h4>{rec.recommendation}</h4>
                    <p><strong>Bet Type:</strong> {rec.bet_type.title()}</p>
                    <p><strong>Confidence:</strong> {rec.confidence_level}</p>
                    <p><strong>Signal Source:</strong> {rec.signal_source}</p>
                    <p><strong>Signal Strength:</strong> {rec.signal_strength:.2f}</p>
                </div>
                """
        else:
            html += """
            <div class="bet-item">
                <p>No specific recommendations generated for this game at this time.</p>
                <p>This may indicate insufficient signal strength or data quality issues.</p>
            </div>
            """
        
        html += f"""
            </div>
            
            <div class="footer">
                <p><strong>📊 Backtesting Integration:</strong> This notification is automatically tracked in the database for performance analysis.</p>
                <p><strong>⚠️ Disclaimer:</strong> These are algorithmic recommendations based on betting splits and sharp action indicators. Always bet responsibly.</p>
                <p><strong>🔧 System Info:</strong> Notification generated at {datetime.now(timezone.utc).isoformat()} UTC</p>
            </div>
        </body>
        </html>
        """
        
        return html
    
    def _generate_enhanced_email_text(self, schedule: GameNotificationSchedule) -> str:
        """Generate enhanced plain text email content."""
        
        game_time_est = schedule.game_start_time.astimezone(timezone(timedelta(hours=-5)))
        
        text = f"""
🏈 MLB BETTING ALERT - 5 MINUTE WARNING!

{schedule.away_team} @ {schedule.home_team}

⏰ GAME INFORMATION:
Start Time: {game_time_est.strftime('%I:%M %p EST on %B %d, %Y')}
Game ID: {schedule.game_pk}
Time Until Start: ~5 minutes

💡 BETTING RECOMMENDATIONS:
"""
        
        if schedule.recommendations:
            for i, rec in enumerate(schedule.recommendations, 1):
                text += f"""
{i}. {rec.recommendation}
   Bet Type: {rec.bet_type.title()}
   Confidence: {rec.confidence_level}
   Signal Source: {rec.signal_source}
   Signal Strength: {rec.signal_strength:.2f}

"""
        else:
            text += """
No specific recommendations generated for this game at this time.
This may indicate insufficient signal strength or data quality issues.

"""
        
        text += f"""
📊 BACKTESTING INTEGRATION:
This notification is automatically tracked in the database for performance analysis.

⚠️ DISCLAIMER:
These are algorithmic recommendations based on betting splits and sharp action indicators. Always bet responsibly.

🔧 SYSTEM INFO:
Notification generated at {datetime.now(timezone.utc).isoformat()} UTC
        """
        
        return text
    
    async def get_notification_status(self) -> Dict[str, Any]:
        """Get current status of notification service."""
        
        current_time = datetime.now(timezone.utc)
        
        # Count notifications by status
        status_counts = {
            'scheduled': 0,
            'sent': 0,
            'failed': 0,
            'pending': 0
        }
        
        upcoming_notifications = []
        failed_notifications = []
        
        for game_pk, schedule in self.scheduled_notifications.items():
            if schedule.notification_sent:
                status_counts['sent'] += 1
            elif schedule.last_error and schedule.retry_count >= schedule.max_retries:
                status_counts['failed'] += 1
                failed_notifications.append({
                    'game_pk': game_pk,
                    'teams': f"{schedule.away_team} @ {schedule.home_team}",
                    'error': schedule.last_error,
                    'retry_count': schedule.retry_count
                })
            elif schedule.notification_time > current_time:
                status_counts['pending'] += 1
                upcoming_notifications.append({
                    'game_pk': game_pk,
                    'teams': f"{schedule.away_team} @ {schedule.home_team}",
                    'notification_time': schedule.notification_time.isoformat(),
                    'minutes_until_notification': int((schedule.notification_time - current_time).total_seconds() / 60)
                })
            else:
                status_counts['scheduled'] += 1
        
        return {
            'service_status': 'active' if self.scheduled_notifications else 'idle',
            'current_time': current_time.isoformat(),
            'email_configured': self.email_config['enabled'],
            'notification_counts': status_counts,
            'upcoming_notifications': sorted(upcoming_notifications, key=lambda x: x['notification_time']),
            'failed_notifications': failed_notifications,
            'performance_metrics': asdict(self.notification_metrics)
        }
    
    async def send_test_notification(self, test_email: Optional[str] = None) -> Dict[str, Any]:
        """Send a test notification to verify email configuration."""
        
        test_recipients = [test_email] if test_email else self.email_config['to_addresses']
        
        if not test_recipients:
            return {'success': False, 'error': 'No test email addresses configured'}
        
        try:
            # Create test schedule
            test_schedule = GameNotificationSchedule(
                game_pk=999999,
                home_team='TEST HOME',
                away_team='TEST AWAY',
                game_start_time=datetime.now(timezone.utc) + timedelta(minutes=5),
                notification_time=datetime.now(timezone.utc),
                email_addresses=test_recipients
            )
            
            # Add test recommendations
            test_schedule.recommendations = [
                PreGameRecommendation(
                    recommendation_id='test_rec_1',
                    game_pk=999999,
                    home_team='TEST HOME',
                    away_team='TEST AWAY',
                    game_datetime=test_schedule.game_start_time,
                    recommendation='BET TEST HOME MONEYLINE',
                    bet_type='moneyline',
                    confidence_level='HIGH',
                    signal_source='TEST_SIGNAL',
                    signal_strength=0.85,
                    recommended_at=datetime.now(timezone.utc)
                )
            ]
            
            # Send test email
            await self._send_enhanced_email_notification(test_schedule)
            
            return {
                'success': True,
                'message': f'Test notification sent to {len(test_recipients)} recipients',
                'recipients': test_recipients
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }


# Convenience functions for integration
async def schedule_todays_notifications() -> Dict[str, Any]:
    """Convenience function to schedule today's notifications."""
    
    service = EnhancedPreGameNotificationService()
    return await service.schedule_daily_notifications()


async def get_notification_service_status() -> Dict[str, Any]:
    """Convenience function to get notification service status."""
    
    service = EnhancedPreGameNotificationService()
    return await service.get_notification_status()


if __name__ == "__main__":
    async def main():
        # Example usage
        print("🔔 Enhanced Pre-Game Notification Service")
        
        service = EnhancedPreGameNotificationService()
        
        # Schedule today's notifications
        results = await service.schedule_daily_notifications()
        print(f"\n📅 Scheduled {results['games_scheduled']} games for notifications")
        
        # Show status
        status = await service.get_notification_status()
        print(f"\n📊 Service Status: {status['service_status']}")
        print(f"📧 Email Configured: {status['email_configured']}")
        print(f"⏰ Upcoming Notifications: {len(status['upcoming_notifications'])}")
        
        # Send test notification
        test_result = await service.send_test_notification()
        if test_result['success']:
            print(f"\n✅ Test notification: {test_result['message']}")
        else:
            print(f"\n❌ Test notification failed: {test_result['error']}")
    
    asyncio.run(main()) 