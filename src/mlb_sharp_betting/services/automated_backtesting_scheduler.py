"""
Automated Backtesting Scheduler

Integrates with the existing MLBBettingScheduler to add:
1. Daily backtesting pipeline execution
2. Performance monitoring and alerting
3. Strategy validation and recommendations
4. Risk management and circuit breakers
"""

import asyncio
from datetime import datetime, timezone, timedelta, time
from pathlib import Path
from typing import Dict, List, Optional, Any
import structlog

from .scheduler import MLBBettingScheduler
from .backtesting_service import BacktestingService, BacktestingResults
from .alert_service import AlertService, AlertSeverity, AlertType
from .mlb_api_service import MLBStatsAPIService
from ..core.logging import get_logger


logger = get_logger(__name__)


class AutomatedBacktestingScheduler(MLBBettingScheduler):
    """
    Enhanced scheduler with automated backtesting and strategy validation.
    
    Extends the existing MLBBettingScheduler with:
    - Daily backtesting pipeline
    - Performance monitoring
    - Alert management
    - Risk controls
    """
    
    def __init__(self, 
                 project_root: Optional[Path] = None,
                 notifications_enabled: bool = True,
                 alert_minutes_before_game: int = 5,
                 backtesting_enabled: bool = True):
        """Initialize the enhanced scheduler."""
        
        # Initialize parent scheduler
        super().__init__(project_root, notifications_enabled, alert_minutes_before_game)
        
        # Initialize backtesting and alert services
        self.backtesting_enabled = backtesting_enabled
        self.backtesting_service = BacktestingService() if backtesting_enabled else None
        self.alert_service = AlertService()
        
        # Enhanced metrics
        self.metrics.update({
            'backtesting_runs': 0,
            'backtesting_failures': 0,
            'alerts_generated': 0,
            'strategy_recommendations': 0,
            'circuit_breaker_triggers': 0,
            'last_backtesting_run': None,
            'last_successful_backtest': None
        })
        
        # Circuit breaker state
        self.circuit_breaker = {
            'enabled': True,
            'consecutive_failures': 0,
            'max_consecutive_failures': 3,
            'cooldown_minutes': 60,
            'last_failure': None,
            'circuit_open': False
        }
        
        # Risk management
        self.risk_controls = {
            'max_daily_recommendations': 5,
            'min_time_between_changes': timedelta(hours=12),
            'last_threshold_change': None,
            'daily_recommendation_count': 0,
            'reset_daily_count_at': time(6, 0)  # 6 AM EST
        }
        
        self.enhanced_logger = logger.bind(service="automated_backtesting_scheduler")
    
    def start(self) -> None:
        """Start the enhanced scheduler with backtesting jobs."""
        # Start the parent scheduler first
        super().start()
        
        if self.backtesting_enabled:
            self._schedule_backtesting_jobs()
        
        self._schedule_alert_jobs()
        self.enhanced_logger.info("Enhanced scheduler started with backtesting and alerting")
    
    def _schedule_backtesting_jobs(self) -> None:
        """Schedule backtesting-related jobs."""
        
        # Daily backtesting pipeline - runs after games are likely complete
        # 2 AM EST to ensure all West Coast games are finished
        self.scheduler.add_job(
            func=self._daily_backtesting_handler,
            trigger='cron',
            hour=2,
            minute=0,
            timezone='US/Eastern',
            id='daily_backtesting',
            name='Daily Backtesting Pipeline',
            misfire_grace_time=300,  # 5 minute grace period
            max_instances=1
        )
        
        # Mid-day performance check - lighter analysis for early warning
        self.scheduler.add_job(
            func=self._midday_performance_check,
            trigger='cron',
            hour=14,  # 2 PM EST
            minute=0,
            timezone='US/Eastern',
            id='midday_performance_check',
            name='Mid-day Performance Check',
            misfire_grace_time=300,
            max_instances=1
        )
        
        # Weekly comprehensive analysis - Mondays at 6 AM EST
        self.scheduler.add_job(
            func=self._weekly_analysis_handler,
            trigger='cron',
            day_of_week='mon',
            hour=6,
            minute=0,
            timezone='US/Eastern',
            id='weekly_analysis',
            name='Weekly Comprehensive Analysis',
            misfire_grace_time=600,  # 10 minute grace period
            max_instances=1
        )
    
    def _schedule_alert_jobs(self) -> None:
        """Schedule alert and monitoring jobs."""
        
        # Daily alert summary - 8 AM EST
        self.scheduler.add_job(
            func=self._daily_alert_summary_handler,
            trigger='cron',
            hour=8,
            minute=0,
            timezone='US/Eastern',
            id='daily_alert_summary',
            name='Daily Alert Summary',
            misfire_grace_time=300,
            max_instances=1
        )
        
        # Alert cleanup - daily at midnight
        self.scheduler.add_job(
            func=self._alert_cleanup_handler,
            trigger='cron',
            hour=0,
            minute=0,
            timezone='US/Eastern',
            id='alert_cleanup',
            name='Alert Cleanup',
            misfire_grace_time=300,
            max_instances=1
        )
        
        # Circuit breaker check - every 30 minutes
        self.scheduler.add_job(
            func=self._circuit_breaker_check,
            trigger='interval',
            minutes=30,
            id='circuit_breaker_check',
            name='Circuit Breaker Check',
            max_instances=1
        )
    
    async def _daily_backtesting_handler(self) -> None:
        """Execute the daily backtesting pipeline."""
        start_time = datetime.now(timezone.utc)
        self.enhanced_logger.info("Starting daily backtesting pipeline")
        
        try:
            # Check circuit breaker
            if self._is_circuit_breaker_open():
                self.enhanced_logger.warning("Circuit breaker is open, skipping backtesting")
                await self._send_circuit_breaker_notification()
                return
            
            # Reset daily recommendation count if needed
            self._reset_daily_counts_if_needed()
            
            # Execute backtesting pipeline
            if not self.backtesting_service:
                raise RuntimeError("Backtesting service not initialized")
            
            results = await self.backtesting_service.run_daily_backtesting_pipeline()
            
            # Process results through alert service
            alerts = await self.alert_service.process_backtesting_results(results)
            
            # Update metrics
            self.metrics['backtesting_runs'] += 1
            self.metrics['last_backtesting_run'] = start_time
            self.metrics['last_successful_backtest'] = start_time
            self.metrics['alerts_generated'] += len(alerts)
            self.metrics['strategy_recommendations'] += len(results.threshold_recommendations)
            
            # Reset circuit breaker on success
            self._reset_circuit_breaker()
            
            # Generate and store daily report
            daily_report = await self.backtesting_service.generate_daily_report(results)
            await self._store_daily_report(daily_report, start_time.date())
            
            execution_time = (datetime.now(timezone.utc) - start_time).total_seconds()
            
            self.enhanced_logger.info("Daily backtesting completed successfully",
                                    execution_time=execution_time,
                                    strategies_analyzed=results.total_strategies_analyzed,
                                    recommendations=len(results.threshold_recommendations),
                                    alerts=len(alerts))
            
        except Exception as e:
            # Handle failure and update circuit breaker
            self._handle_backtesting_failure(e)
            self.enhanced_logger.error("Daily backtesting failed", error=str(e))
            
            # Send failure alert
            await self._send_backtesting_failure_alert(str(e))
    
    async def _midday_performance_check(self) -> None:
        """Perform a lighter performance check during the day."""
        self.enhanced_logger.info("Running mid-day performance check")
        
        try:
            # Quick performance check without full backtesting
            # Check for any critical alerts or threshold breaches
            active_alerts = await self.alert_service.get_active_alerts(
                severity_filter=[AlertSeverity.CRITICAL, AlertSeverity.HIGH]
            )
            
            if active_alerts:
                self.enhanced_logger.warning(f"Found {len(active_alerts)} high-priority active alerts")
                
                # Send consolidated alert
                await self._send_midday_alert_summary(active_alerts)
            
            # Basic data freshness check
            await self._check_data_freshness()
            
        except Exception as e:
            self.enhanced_logger.error("Mid-day performance check failed", error=str(e))
    
    async def _weekly_analysis_handler(self) -> None:
        """Execute weekly comprehensive analysis."""
        self.enhanced_logger.info("Starting weekly comprehensive analysis")
        
        try:
            # Generate weekly summary from alert service
            weekly_summary = await self.alert_service.generate_weekly_summary()
            
            # Store and potentially send weekly report
            await self._store_weekly_report(weekly_summary, datetime.now(timezone.utc).date())
            
            # Performance trend analysis (could be extended)
            await self._analyze_weekly_trends()
            
            self.enhanced_logger.info("Weekly analysis completed")
            
        except Exception as e:
            self.enhanced_logger.error("Weekly analysis failed", error=str(e))
    
    async def _daily_alert_summary_handler(self) -> None:
        """Send daily alert summary."""
        try:
            active_alerts = await self.alert_service.get_active_alerts()
            
            if active_alerts:
                high_priority = [a for a in active_alerts if a.severity in [AlertSeverity.HIGH, AlertSeverity.CRITICAL]]
                
                summary_message = f"""🌅 Daily Alert Summary
                
📊 Active Alerts: {len(active_alerts)}
🔥 High Priority: {len(high_priority)}

Recent Activity:
• {self.metrics['alerts_generated']} alerts generated today
• {self.metrics['strategy_recommendations']} recommendations pending"""
                
                await self.send_notification(summary_message, "daily_summary")
            
        except Exception as e:
            self.enhanced_logger.error("Failed to send daily alert summary", error=str(e))
    
    async def _alert_cleanup_handler(self) -> None:
        """Clean up old alerts and reset daily counters."""
        try:
            # Alert service handles its own cleanup
            self.enhanced_logger.info("Alert cleanup completed")
            
            # Reset daily recommendation counter
            self.risk_controls['daily_recommendation_count'] = 0
            
        except Exception as e:
            self.enhanced_logger.error("Alert cleanup failed", error=str(e))
    
    async def _circuit_breaker_check(self) -> None:
        """Check and manage circuit breaker state."""
        if not self.circuit_breaker['enabled']:
            return
        
        # Check if circuit breaker should be reset
        if self.circuit_breaker['circuit_open']:
            last_failure = self.circuit_breaker['last_failure']
            if last_failure:
                cooldown_period = timedelta(minutes=self.circuit_breaker['cooldown_minutes'])
                if datetime.now(timezone.utc) - last_failure > cooldown_period:
                    self._reset_circuit_breaker()
                    self.enhanced_logger.info("Circuit breaker reset after cooldown period")
    
    def _is_circuit_breaker_open(self) -> bool:
        """Check if circuit breaker is currently open."""
        return self.circuit_breaker.get('circuit_open', False)
    
    def _handle_backtesting_failure(self, error: Exception) -> None:
        """Handle backtesting failure and update circuit breaker."""
        self.metrics['backtesting_failures'] += 1
        self.circuit_breaker['consecutive_failures'] += 1
        self.circuit_breaker['last_failure'] = datetime.now(timezone.utc)
        
        # Check if circuit breaker should be triggered
        if (self.circuit_breaker['consecutive_failures'] >= 
            self.circuit_breaker['max_consecutive_failures']):
            
            self.circuit_breaker['circuit_open'] = True
            self.metrics['circuit_breaker_triggers'] += 1
            
            self.enhanced_logger.critical("Circuit breaker triggered after consecutive failures",
                                        consecutive_failures=self.circuit_breaker['consecutive_failures'])
    
    def _reset_circuit_breaker(self) -> None:
        """Reset circuit breaker state."""
        self.circuit_breaker['consecutive_failures'] = 0
        self.circuit_breaker['circuit_open'] = False
        self.circuit_breaker['last_failure'] = None
    
    def _reset_daily_counts_if_needed(self) -> None:
        """Reset daily counters if it's a new day."""
        now = datetime.now(timezone.utc)
        reset_time = self.risk_controls['reset_daily_count_at']
        
        # Convert to today's reset time in UTC
        today_reset = datetime.combine(now.date(), reset_time)
        today_reset = today_reset.replace(tzinfo=timezone.utc)
        
        # If we've passed today's reset time and haven't reset yet
        if now >= today_reset and self.risk_controls['daily_recommendation_count'] > 0:
            self.risk_controls['daily_recommendation_count'] = 0
            self.enhanced_logger.info("Daily counters reset")
    
    async def _send_circuit_breaker_notification(self) -> None:
        """Send notification about circuit breaker status."""
        message = f"""🔴 CIRCUIT BREAKER ACTIVE
        
The automated backtesting system has been suspended due to consecutive failures.

Status:
• Consecutive failures: {self.circuit_breaker['consecutive_failures']}
• Last failure: {self.circuit_breaker['last_failure'].strftime('%Y-%m-%d %H:%M UTC') if self.circuit_breaker['last_failure'] else 'Unknown'}
• Cooldown period: {self.circuit_breaker['cooldown_minutes']} minutes

The system will automatically retry after the cooldown period.
Manual intervention may be required if failures continue."""
        
        await self.send_notification(message, "circuit_breaker")
    
    async def _send_backtesting_failure_alert(self, error_message: str) -> None:
        """Send alert about backtesting failure."""
        message = f"""⚠️ BACKTESTING PIPELINE FAILURE
        
The daily backtesting pipeline has failed.

Error: {error_message}

Status:
• Consecutive failures: {self.circuit_breaker['consecutive_failures']}
• Total failures today: {self.metrics['backtesting_failures']}

Please check the system logs and resolve any issues."""
        
        await self.send_notification(message, "backtesting_failure")
    
    async def _send_midday_alert_summary(self, alerts: List) -> None:
        """Send mid-day alert summary."""
        critical_count = len([a for a in alerts if a.severity == AlertSeverity.CRITICAL])
        high_count = len([a for a in alerts if a.severity == AlertSeverity.HIGH])
        
        message = f"""🚨 MID-DAY ALERT SUMMARY
        
High priority alerts detected:
• Critical: {critical_count}
• High: {high_count}

Recent activity requires attention. Check the full alert dashboard for details."""
        
        await self.send_notification(message, "midday_summary")
    
    async def _check_data_freshness(self) -> None:
        """Check data freshness and alert if stale."""
        # Basic implementation - could be enhanced
        try:
            if self.backtesting_service:
                data_quality = await self.backtesting_service._validate_data_quality()
                
                if data_quality['freshness_hours'] > 12:  # 12 hour threshold
                    await self.send_notification(
                        f"🕐 Data freshness alert: {data_quality['freshness_hours']:.1f} hours old",
                        "data_freshness"
                    )
        except Exception as e:
            self.enhanced_logger.error("Data freshness check failed", error=str(e))
    
    async def _store_daily_report(self, report: str, date: datetime) -> None:
        """Store daily backtesting report."""
        try:
            reports_dir = self.project_root / "reports" / "daily"
            reports_dir.mkdir(parents=True, exist_ok=True)
            
            report_file = reports_dir / f"backtesting_report_{date.strftime('%Y%m%d')}.md"
            
            with open(report_file, 'w') as f:
                f.write(report)
            
            self.enhanced_logger.info("Daily report stored", file=str(report_file))
            
        except Exception as e:
            self.enhanced_logger.error("Failed to store daily report", error=str(e))
    
    async def _store_weekly_report(self, report: str, date: datetime) -> None:
        """Store weekly analysis report."""
        try:
            reports_dir = self.project_root / "reports" / "weekly"
            reports_dir.mkdir(parents=True, exist_ok=True)
            
            report_file = reports_dir / f"weekly_analysis_{date.strftime('%Y%m%d')}.md"
            
            with open(report_file, 'w') as f:
                f.write(report)
            
            self.enhanced_logger.info("Weekly report stored", file=str(report_file))
            
        except Exception as e:
            self.enhanced_logger.error("Failed to store weekly report", error=str(e))
    
    async def _analyze_weekly_trends(self) -> None:
        """Analyze weekly performance trends (placeholder for future enhancement)."""
        # This could include:
        # - Strategy performance trend analysis
        # - Market condition correlation
        # - Seasonal pattern detection
        # - ROI trend forecasting
        pass
    
    def get_enhanced_status(self) -> Dict[str, Any]:
        """Get enhanced status including backtesting and alert metrics."""
        base_status = self.get_status()
        
        enhanced_status = {
            **base_status,
            'backtesting': {
                'enabled': self.backtesting_enabled,
                'runs_completed': self.metrics['backtesting_runs'],
                'failures': self.metrics['backtesting_failures'],
                'last_run': self.metrics['last_backtesting_run'],
                'last_successful': self.metrics['last_successful_backtest']
            },
            'alerts': {
                'generated': self.metrics['alerts_generated'],
                'active_count': len(self.alert_service.active_alerts) if self.alert_service else 0,
                'recommendations_pending': self.metrics['strategy_recommendations']
            },
            'circuit_breaker': {
                'open': self.circuit_breaker['circuit_open'],
                'consecutive_failures': self.circuit_breaker['consecutive_failures'],
                'triggers': self.metrics['circuit_breaker_triggers']
            },
            'risk_controls': {
                'daily_recommendations_used': self.risk_controls['daily_recommendation_count'],
                'max_daily_recommendations': self.risk_controls['max_daily_recommendations']
            }
        }
        
        return enhanced_status


async def main():
    """Test the enhanced scheduler."""
    project_root = Path(__file__).parent.parent.parent.parent
    
    scheduler = AutomatedBacktestingScheduler(
        project_root=project_root,
        notifications_enabled=True,
        backtesting_enabled=True
    )
    
    print("🚀 Starting Enhanced MLB Betting Scheduler with Automated Backtesting")
    print("=" * 70)
    
    try:
        scheduler.start()
        
        # Show initial status
        status = scheduler.get_enhanced_status()
        print(f"\n📊 Enhanced Status:")
        print(f"   • Backtesting enabled: {status['backtesting']['enabled']}")
        print(f"   • Circuit breaker: {'OPEN' if status['circuit_breaker']['open'] else 'CLOSED'}")
        print(f"   • Active alerts: {status['alerts']['active_count']}")
        print(f"   • Daily recommendations: {status['risk_controls']['daily_recommendations_used']}/{status['risk_controls']['max_daily_recommendations']}")
        
        print("\n🎯 Scheduled Jobs:")
        for job in scheduler.scheduler.get_jobs():
            print(f"   • {job.name}: Next run at {job.next_run_time}")
        
        print("\nPress Ctrl+C to stop...")
        
        # Keep running
        while True:
            await asyncio.sleep(60)
            
    except KeyboardInterrupt:
        print("\n🛑 Shutting down scheduler...")
        scheduler.stop()
    except Exception as e:
        print(f"\n❌ Scheduler error: {e}")
        scheduler.stop()
        raise


if __name__ == "__main__":
    asyncio.run(main())