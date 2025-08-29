#!/usr/bin/env python3
"""
Integration Tests for Onboarding System

Tests the complete onboarding flow including:
- User progress tracking and persistence
- Achievement system functionality
- Tutorial system execution
- CLI command integration
- Context-sensitive help system

These tests ensure the onboarding experience works end-to-end.
"""

import json
import tempfile
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock
from click.testing import CliRunner

from src.interfaces.cli.commands.onboarding import (
    UserProgress, 
    InteractiveTutorial,
    onboarding_group
)
from src.interfaces.cli.commands.help_system import (
    ContextualHelpSystem,
    help_group
)


class TestUserProgress:
    """Test user progress tracking functionality."""
    
    def test_progress_initialization(self):
        """Test UserProgress initializes with correct defaults."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir)
            progress = UserProgress(config_dir)
            
            assert progress.progress["current_level"] == "beginner"
            assert progress.progress["completed_steps"] == []
            assert progress.progress["started_at"] is None
            assert not progress.progress["beginner_completed"]
    
    def test_progress_persistence(self):
        """Test progress saves and loads correctly."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir)
            
            # Create and modify progress
            progress1 = UserProgress(config_dir)
            progress1.mark_step_completed("environment_validated", "beginner")
            progress1.unlock_achievement("test_achievement", "Test Achievement", 10)
            
            # Create new instance and verify persistence
            progress2 = UserProgress(config_dir)
            assert "environment_validated" in progress2.progress["completed_steps"]
            assert "test_achievement" in progress2.achievements["unlocked"]
            assert progress2.achievements["total_points"] == 10
    
    def test_level_completion_detection(self):
        """Test automatic level completion detection."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir)
            progress = UserProgress(config_dir)
            
            # Complete beginner level steps
            beginner_steps = ["environment_validated", "database_setup", "first_collection", "first_status_check"]
            for step in beginner_steps:
                progress.mark_step_completed(step)
            
            assert progress.progress["beginner_completed"]
            assert "beginner_complete" in progress.achievements["unlocked"]
            assert "beginner" in progress.achievements["level_badges"]
    
    def test_completion_percentage_calculation(self):
        """Test completion percentage calculation."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir)
            progress = UserProgress(config_dir)
            
            # Initially 0%
            assert progress.get_completion_percentage() == 0
            
            # Complete some steps
            for i in range(8):  # Complete half of 16 total steps
                progress.mark_step_completed(f"step_{i}")
            
            completion = progress.get_completion_percentage()
            assert 40 <= completion <= 60  # Should be around 50%
    
    def test_current_level_progress(self):
        """Test current level progress tracking."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir)
            progress = UserProgress(config_dir)
            
            # Complete partial beginner level
            progress.mark_step_completed("environment_validated")
            progress.mark_step_completed("database_setup")
            
            level_progress = progress.get_current_level_progress()
            assert level_progress["level"] == "beginner"
            assert level_progress["completed_steps"] == 2
            assert level_progress["total_steps"] == 4
            assert level_progress["progress_percentage"] == 50


class TestInteractiveTutorial:
    """Test interactive tutorial functionality."""
    
    def test_tutorial_initialization(self):
        """Test InteractiveTutorial initializes correctly."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir)
            progress = UserProgress(config_dir)
            tutorial = InteractiveTutorial(progress)
            
            assert tutorial.progress == progress
            assert tutorial.project_root  # Should find project root
    
    def test_user_confirmation_skipping(self):
        """Test user confirmation can be skipped via preferences."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir)
            progress = UserProgress(config_dir)
            progress.preferences["skip_confirmations"] = True
            tutorial = InteractiveTutorial(progress)
            
            # Should return True without user input when skipping enabled
            result = tutorial.wait_for_user_confirmation("Test confirmation?")
            assert result is True
    
    def test_tip_display_control(self):
        """Test tip display can be controlled via preferences."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir)
            progress = UserProgress(config_dir)
            tutorial = InteractiveTutorial(progress)
            
            # Tips enabled by default
            assert progress.preferences.get("show_tips", True)
            
            # Disable tips
            progress.preferences["show_tips"] = False
            # show_tip should not raise exception even when disabled
            tutorial.show_tip("Test tip")
    
    def test_performance_benchmarking(self):
        """Test performance benchmarking functionality."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir)
            progress = UserProgress(config_dir)
            tutorial = InteractiveTutorial(progress)
            
            benchmark = tutorial.benchmark_performance("test_operation")
            
            assert benchmark["operation"] == "test_operation"
            assert "elapsed_time" in benchmark
            assert "success" in benchmark
            assert "performance_score" in benchmark
            assert isinstance(benchmark["recommendations"], list)


class TestContextualHelpSystem:
    """Test context-sensitive help system."""
    
    def test_help_system_initialization(self):
        """Test ContextualHelpSystem initializes correctly."""
        help_system = ContextualHelpSystem()
        
        assert hasattr(help_system, 'progress')
        assert hasattr(help_system, 'error_patterns')
        assert hasattr(help_system, 'command_help')
    
    def test_contextual_help_generation(self):
        """Test contextual help adapts to user progress."""
        help_system = ContextualHelpSystem()
        
        # Mock progress for different levels
        help_system.progress = {"current_level": "beginner", "completed_steps": []}
        help_info = help_system.get_contextual_help()
        
        assert help_info["user_level"] == "beginner"
        assert len(help_info["recommended_commands"]) > 0
        assert any("onboarding start" in cmd["command"] for cmd in help_info["recommended_commands"])
    
    def test_error_diagnosis(self):
        """Test error diagnosis functionality."""
        help_system = ContextualHelpSystem()
        
        # Test database connection error
        error_text = "connection refused to database"
        diagnosis = help_system.diagnose_error(error_text)
        
        assert diagnosis["error_type"] == "database_connection"
        assert diagnosis["confidence"] > 0.0
        assert len(diagnosis["solutions"]) > 0
    
    def test_command_suggestions(self):
        """Test command suggestion functionality."""
        help_system = ContextualHelpSystem()
        help_system.progress = {"current_level": "beginner", "completed_steps": []}
        
        # Test general suggestions
        suggestions = help_system.get_command_suggestions("")
        assert len(suggestions) > 0
        assert all("command" in s and "description" in s for s in suggestions)
        
        # Test filtered suggestions
        filtered = help_system.get_command_suggestions("prediction")
        prediction_suggestions = [s for s in filtered if "prediction" in s["command"].lower()]
        assert len(prediction_suggestions) > 0


class TestOnboardingCLICommands:
    """Test onboarding CLI command integration."""
    
    def test_onboarding_start_command(self):
        """Test onboarding start command executes without errors."""
        runner = CliRunner()
        
        with patch('src.interfaces.cli.commands.onboarding.UserProgress') as mock_progress:
            with patch('src.interfaces.cli.commands.onboarding.InteractiveTutorial') as mock_tutorial:
                # Mock progress instance
                mock_progress_instance = MagicMock()
                mock_progress_instance.progress = {
                    "started_at": None,
                    "session_count": 0,
                    "beginner_completed": False,
                    "current_level": "beginner"
                }
                mock_progress.return_value = mock_progress_instance
                
                # Mock tutorial instance
                mock_tutorial_instance = MagicMock()
                mock_tutorial_instance.wait_for_user_confirmation.return_value = False  # Skip actual tutorial
                mock_tutorial.return_value = mock_tutorial_instance
                
                result = runner.invoke(onboarding_group, ['start', '--skip-intro'])
                assert result.exit_code == 0
    
    def test_onboarding_status_command(self):
        """Test onboarding status command shows progress correctly."""
        runner = CliRunner()
        
        with patch('src.interfaces.cli.commands.onboarding.UserProgress') as mock_progress:
            # Mock progress with some completion
            mock_progress_instance = MagicMock()
            mock_progress_instance.progress = {
                "started_at": "2024-01-01T00:00:00",
                "session_count": 3,
                "completed_steps": ["environment_validated", "database_setup"],
                "current_level": "beginner"
            }
            mock_progress_instance.achievements = {
                "total_points": 25,
                "level_badges": ["beginner"],
                "last_achievement": {"name": "Test Achievement", "points": 10}
            }
            mock_progress_instance.get_completion_percentage.return_value = 25
            mock_progress_instance.get_current_level_progress.return_value = {
                "level": "beginner",
                "completed_steps": 2,
                "total_steps": 4,
                "progress_percentage": 50,
                "remaining_steps": ["first_collection", "first_status_check"]
            }
            mock_progress.return_value = mock_progress_instance
            
            result = runner.invoke(onboarding_group, ['status'])
            assert result.exit_code == 0
            assert "25%" in result.output  # Completion percentage
            assert "beginner" in result.output.lower()  # Current level
    
    def test_onboarding_validate_command(self):
        """Test onboarding validate command executes validation."""
        runner = CliRunner()
        
        with patch('src.interfaces.cli.commands.onboarding.UserProgress') as mock_progress:
            with patch('src.interfaces.cli.commands.onboarding.InteractiveTutorial') as mock_tutorial:
                with patch('src.interfaces.cli.commands.onboarding._validate_environment_comprehensive') as mock_validate:
                    mock_validate.return_value = {"status": "ok", "message": "Environment validated"}
                    
                    result = runner.invoke(onboarding_group, ['validate'])
                    assert result.exit_code == 0


class TestHelpSystemCLICommands:
    """Test help system CLI command integration."""
    
    def test_help_context_command(self):
        """Test help context command provides contextual assistance."""
        runner = CliRunner()
        
        with patch('src.interfaces.cli.commands.help_system.ContextualHelpSystem') as mock_help:
            mock_help_instance = MagicMock()
            mock_help_instance.get_contextual_help.return_value = {
                "user_level": "beginner",
                "completion_status": 2,
                "recommended_commands": [
                    {"command": "test command", "description": "test description", "priority": "high"}
                ],
                "tips": ["Test tip"]
            }
            mock_help.return_value = mock_help_instance
            
            result = runner.invoke(help_group, ['context'])
            assert result.exit_code == 0
            assert "beginner" in result.output.lower()
    
    def test_help_troubleshoot_command(self):
        """Test help troubleshoot command provides error assistance."""
        runner = CliRunner()
        
        result = runner.invoke(help_group, ['troubleshoot', '--issue', 'database-connection'])
        assert result.exit_code == 0
        assert "database" in result.output.lower()
    
    def test_help_tips_command(self):
        """Test help tips command shows level-appropriate tips."""
        runner = CliRunner()
        
        with patch('src.interfaces.cli.commands.help_system.ContextualHelpSystem') as mock_help:
            mock_help_instance = MagicMock()
            mock_help_instance.progress = {"current_level": "beginner", "completed_steps": []}
            mock_help.return_value = mock_help_instance
            
            result = runner.invoke(help_group, ['tips'])
            assert result.exit_code == 0
    
    def test_help_suggest_command(self):
        """Test help suggest command provides command suggestions."""
        runner = CliRunner()
        
        with patch('src.interfaces.cli.commands.help_system.ContextualHelpSystem') as mock_help:
            mock_help_instance = MagicMock()
            mock_help_instance.get_command_suggestions.return_value = [
                {"command": "predictions today", "description": "Get predictions"}
            ]
            mock_help.return_value = mock_help_instance
            
            result = runner.invoke(help_group, ['suggest'])
            assert result.exit_code == 0


class TestOnboardingIntegration:
    """Test end-to-end onboarding integration."""
    
    def test_achievement_unlocking_flow(self):
        """Test that achievements unlock correctly during onboarding."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir)
            progress = UserProgress(config_dir)
            
            # Simulate beginner onboarding flow
            progress.mark_step_completed("environment_validated")
            assert "env_ready" not in progress.achievements["unlocked"]  # Achievement not auto-unlocked
            
            progress.unlock_achievement("env_ready", "Environment Ready", 5)
            assert "env_ready" in progress.achievements["unlocked"]
            assert progress.achievements["total_points"] == 5
    
    def test_level_progression_flow(self):
        """Test progression from beginner to intermediate level."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir)
            progress = UserProgress(config_dir)
            
            # Complete beginner level
            beginner_steps = ["environment_validated", "database_setup", "first_collection", "first_status_check"]
            for step in beginner_steps:
                progress.mark_step_completed(step)
            
            assert progress.progress["beginner_completed"]
            assert progress.progress["current_level"] == "beginner"  # Level doesn't auto-advance
            
            # Manually advance to intermediate (would be done by tutorial)
            progress.progress["current_level"] = "intermediate"
            progress.save_progress()
            
            level_progress = progress.get_current_level_progress()
            assert level_progress["level"] == "intermediate"
            assert level_progress["completed_steps"] == 0  # Fresh start on new level
    
    def test_help_system_context_awareness(self):
        """Test help system adapts to onboarding progress."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create progress file with specific state
            config_dir = Path(temp_dir)
            progress_file = config_dir / "onboarding_progress.json"
            config_dir.mkdir(exist_ok=True)
            
            progress_data = {
                "current_level": "intermediate",
                "completed_steps": ["environment_validated", "database_setup", "first_collection", "first_status_check"]
            }
            
            with open(progress_file, 'w') as f:
                json.dump(progress_data, f)
            
            # Create help system and verify it loads progress
            help_system = ContextualHelpSystem()
            help_system.config_dir = config_dir
            help_system.progress_file = progress_file
            help_system.progress = help_system._load_progress()
            
            help_info = help_system.get_contextual_help()
            assert help_info["user_level"] == "intermediate"
            assert help_info["completion_status"] == 4
    
    def test_preference_persistence(self):
        """Test user preferences persist correctly."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir)
            progress = UserProgress(config_dir)
            
            # Modify preferences
            progress.preferences["confidence_threshold"] = 0.8
            progress.preferences["skip_confirmations"] = True
            progress.save_preferences()
            
            # Create new instance and verify persistence
            progress2 = UserProgress(config_dir)
            assert progress2.preferences["confidence_threshold"] == 0.8
            assert progress2.preferences["skip_confirmations"] is True


@pytest.mark.integration
class TestOnboardingSystemIntegration:
    """Integration tests requiring real system components."""
    
    def test_project_root_detection(self):
        """Test project root detection works correctly."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir)
            progress = UserProgress(config_dir)
            tutorial = InteractiveTutorial(progress)
            
            # Should detect project root without errors
            project_root = tutorial._get_project_root()
            assert project_root
            assert Path(project_root).exists()
    
    @pytest.mark.slow
    def test_command_execution_simulation(self):
        """Test command execution simulation (without actual execution)."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir)
            progress = UserProgress(config_dir)
            tutorial = InteractiveTutorial(progress)
            
            # Mock subprocess to avoid actual command execution
            with patch('subprocess.run') as mock_run:
                mock_run.return_value = MagicMock(returncode=0, stdout="success", stderr="")
                
                result = tutorial.run_command_with_feedback(
                    ["echo", "test"], 
                    "Test command",
                    ["success"]
                )
                
                assert result is True
                mock_run.assert_called_once()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])