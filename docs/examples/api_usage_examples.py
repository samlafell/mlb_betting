#!/usr/bin/env python3
"""
MLB Betting API Usage Examples

Working Python scripts demonstrating how to interact with the MLB betting system API.
These examples show real-world usage patterns for getting betting recommendations.

Requirements:
    pip install requests pandas  # or: uv add requests pandas

Usage:
    python api_usage_examples.py
"""

import json
import requests
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import polars as pl


class MLBBettingAPI:
    """Simple wrapper for MLB Betting API interactions."""
    
    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url.rstrip('/')
        self.session = requests.Session()
        
    def _request(self, method: str, endpoint: str, **kwargs) -> requests.Response:
        """Make HTTP request with error handling."""
        url = f"{self.base_url}{endpoint}"
        try:
            response = self.session.request(method, url, **kwargs)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            print(f"API Error: {e}")
            raise
    
    def health_check(self) -> Dict:
        """Check if API is healthy."""
        response = self._request("GET", "/health")
        return response.json()
    
    def get_todays_predictions(self, min_confidence: float = 0.7, model_name: str = None) -> List[Dict]:
        """Get today's betting recommendations."""
        params = {}
        if min_confidence:
            params['min_confidence'] = min_confidence
        if model_name:
            params['model_name'] = model_name
            
        response = self._request("GET", "/api/v1/predictions/today", params=params)
        return response.json()
    
    def get_active_models(self) -> List[Dict]:
        """Get list of active models."""
        response = self._request("GET", "/api/v1/models/active")
        return response.json()
    
    def get_model_leaderboard(self, metric: str = "roi_percentage", days: int = 30, limit: int = 10) -> List[Dict]:
        """Get model performance leaderboard."""
        params = {
            'metric': metric,
            'days': days,
            'limit': limit
        }
        response = self._request("GET", "/api/v1/models/leaderboard", params=params)
        return response.json()
    
    def get_model_performance(self, model_name: str, days: int = 30) -> List[Dict]:
        """Get detailed model performance metrics."""
        params = {'days': days}
        response = self._request("GET", f"/api/v1/models/{model_name}/performance", params=params)
        return response.json()
    
    def predict_single_game(self, game_id: str, model_name: str = None, include_explanation: bool = False) -> Dict:
        """Get prediction for a single game."""
        data = {
            'game_id': game_id,
            'include_explanation': include_explanation
        }
        if model_name:
            data['model_name'] = model_name
            
        response = self._request("POST", "/api/v1/predict", json=data)
        return response.json()
    
    def predict_batch_games(self, game_ids: List[str], model_name: str = None) -> List[Dict]:
        """Get predictions for multiple games."""
        data = {
            'game_ids': game_ids[:50],  # API limit is 50
            'include_explanation': False
        }
        if model_name:
            data['model_name'] = model_name
            
        response = self._request("POST", "/api/v1/predict/batch", json=data)
        return response.json()


def example_1_basic_health_check():
    """Example 1: Basic system health check."""
    print("=" * 60)
    print("EXAMPLE 1: Basic Health Check")
    print("=" * 60)
    
    api = MLBBettingAPI()
    
    try:
        health = api.health_check()
        print(f"✅ System Status: {health.get('status', 'unknown')}")
        print(f"📡 Service: {health.get('service', 'unknown')}")
        
        # Check individual components
        checks = health.get('checks', {})
        for component, status in checks.items():
            icon = "✅" if status.get('status') == 'healthy' else "❌"
            print(f"{icon} {component.title()}: {status.get('message', 'unknown')}")
            
    except Exception as e:
        print(f"❌ System appears to be down: {e}")
        print("💡 Make sure the API is running: docker-compose up -d")


def example_2_todays_predictions():
    """Example 2: Get today's betting recommendations."""
    print("\n" + "=" * 60)
    print("EXAMPLE 2: Today's Betting Recommendations")
    print("=" * 60)
    
    api = MLBBettingAPI()
    
    try:
        # Get high-confidence predictions only
        predictions = api.get_todays_predictions(min_confidence=0.7)
        
        if not predictions:
            print("📅 No high-confidence predictions available for today")
            print("💡 Try lowering the confidence threshold or check if models are active")
            return
            
        print(f"🎯 Found {len(predictions)} high-confidence predictions for today:")
        print()
        
        for pred in predictions:
            game_id = pred['game_id']
            model = pred['model_name']
            
            # Total predictions
            total_prob = pred.get('total_over_probability', 0)
            total_bet = "OVER" if pred.get('total_over_binary') == 1 else "UNDER"
            total_conf = pred.get('total_over_confidence', 0)
            
            # Moneyline predictions  
            ml_prob = pred.get('home_ml_probability', 0)
            ml_bet = "HOME" if pred.get('home_ml_binary') == 1 else "AWAY"
            ml_conf = pred.get('home_ml_confidence', 0)
            
            print(f"🏟️  Game: {game_id}")
            print(f"🤖 Model: {model}")
            print(f"🎲 Total: {total_bet} ({total_prob:.3f} prob, {total_conf:.3f} conf)")
            print(f"💰 Moneyline: {ml_bet} ({ml_prob:.3f} prob, {ml_conf:.3f} conf)")
            
            # Show betting recommendations if available
            recommendations = pred.get('betting_recommendations', {})
            if recommendations:
                rec_bets = recommendations.get('recommended_bets', [])
                confidence_level = recommendations.get('confidence_level', 'unknown')
                risk_level = recommendations.get('risk_level', 'unknown')
                
                print(f"📋 Recommended: {', '.join(rec_bets)}")
                print(f"📊 Confidence: {confidence_level}, Risk: {risk_level}")
            
            print("-" * 40)
            
    except Exception as e:
        print(f"❌ Error getting predictions: {e}")


def example_3_model_leaderboard():
    """Example 3: Find the best performing models."""
    print("\n" + "=" * 60)  
    print("EXAMPLE 3: Model Performance Leaderboard")
    print("=" * 60)
    
    api = MLBBettingAPI()
    
    try:
        # Get top models by ROI
        models = api.get_model_leaderboard(metric="roi_percentage", days=30, limit=5)
        
        if not models:
            print("📊 No model performance data available")
            return
            
        print("🏆 Top 5 Models by ROI (Last 30 Days):")
        print()
        
        for i, model in enumerate(models, 1):
            name = model.get('model_name', 'unknown')
            version = model.get('model_version', 'unknown')
            roi = model.get('roi_percentage', 0)
            win_rate = model.get('win_rate', 0)
            predictions = model.get('total_predictions', 0)
            accuracy = model.get('accuracy', 0)
            
            print(f"{i}. 🥇 {name} (v{version})")
            print(f"   💹 ROI: {roi:.1f}%")  
            print(f"   🎯 Win Rate: {win_rate:.1f}%")
            print(f"   📊 Accuracy: {accuracy:.3f}")
            print(f"   📈 Predictions: {predictions}")
            print()
            
        # Also show by accuracy for technical comparison
        print("\n📐 Same Models Ranked by Accuracy:")
        accuracy_models = api.get_model_leaderboard(metric="accuracy", days=30, limit=3)
        
        for i, model in enumerate(accuracy_models, 1):
            name = model.get('model_name', 'unknown')
            accuracy = model.get('accuracy', 0)
            roi = model.get('roi_percentage', 0)
            print(f"{i}. {name}: {accuracy:.3f} accuracy ({roi:.1f}% ROI)")
            
    except Exception as e:
        print(f"❌ Error getting leaderboard: {e}")


def example_4_detailed_model_analysis():
    """Example 4: Deep dive into a specific model's performance."""
    print("\n" + "=" * 60)
    print("EXAMPLE 4: Detailed Model Analysis") 
    print("=" * 60)
    
    api = MLBBettingAPI()
    
    try:
        # First, get the best model
        models = api.get_model_leaderboard(metric="roi_percentage", days=30, limit=1)
        if not models:
            print("❌ No models available for analysis")
            return
            
        best_model = models[0]['model_name']
        print(f"🔍 Analyzing top model: {best_model}")
        print()
        
        # Get detailed performance
        performance = api.get_model_performance(best_model, days=30)
        
        if not performance:
            print(f"📊 No performance data available for {best_model}")
            return
            
        for perf in performance:
            pred_type = perf.get('prediction_type', 'unknown')
            period_start = perf.get('evaluation_period_start', 'unknown')
            period_end = perf.get('evaluation_period_end', 'unknown')
            
            print(f"📅 Period: {period_start} to {period_end}")
            print(f"🎯 Prediction Type: {pred_type}")
            print()
            
            # Technical metrics
            print("🔬 Technical Metrics:")
            print(f"   Accuracy: {perf.get('accuracy', 0):.3f}")
            print(f"   Precision: {perf.get('precision_score', 0):.3f}")
            print(f"   Recall: {perf.get('recall_score', 0):.3f}")
            print(f"   F1 Score: {perf.get('f1_score', 0):.3f}")
            print(f"   ROC AUC: {perf.get('roc_auc', 0):.3f}")
            print()
            
            # Business metrics
            print("💰 Business Metrics:")
            total_bets = perf.get('total_bets_made', 0)
            winning_bets = perf.get('winning_bets', 0)
            hit_rate = perf.get('hit_rate', 0)
            roi = perf.get('roi_percentage', 0)
            
            print(f"   Total Bets: {total_bets}")
            print(f"   Winning Bets: {winning_bets}")
            print(f"   Hit Rate: {hit_rate:.1f}%")
            print(f"   ROI: {roi:.1f}%")
            print()
            
            # Risk metrics
            print("⚖️ Risk Metrics:")
            sharpe = perf.get('sharpe_ratio', 0)
            max_drawdown = perf.get('max_drawdown_pct', 0)
            print(f"   Sharpe Ratio: {sharpe:.2f}")
            print(f"   Max Drawdown: {max_drawdown:.1f}%")
            print()
            
            # Performance assessment
            print("📈 Assessment:")
            if roi > 15:
                print("   🔥 Excellent ROI - Strong performer")
            elif roi > 8:
                print("   ✅ Good ROI - Solid performer") 
            elif roi > 0:
                print("   ⚠️  Marginal ROI - Consider alternatives")
            else:
                print("   ❌ Negative ROI - Avoid this model")
                
            if hit_rate > 55:
                print("   🎯 High hit rate - Consistent winner")
            elif hit_rate > 52:
                print("   👍 Decent hit rate - Above breakeven")
            else:
                print("   👎 Low hit rate - Below expectations")
            
            print("-" * 50)
            
    except Exception as e:
        print(f"❌ Error analyzing model: {e}")


def example_5_single_game_prediction():
    """Example 5: Get detailed prediction for a specific game."""
    print("\n" + "=" * 60)
    print("EXAMPLE 5: Single Game Prediction")
    print("=" * 60)
    
    api = MLBBettingAPI()
    
    # For demonstration, we'll try to predict a sample game
    # In practice, you'd get real game IDs from today's predictions
    sample_game_id = "2025-01-31-NYY-BOS"  # This would be a real game ID
    
    try:
        # Get prediction with explanation
        prediction = api.predict_single_game(
            game_id=sample_game_id, 
            include_explanation=True
        )
        
        print(f"🏟️  Game Analysis: {prediction['game_id']}")
        print(f"🤖 Model: {prediction['model_name']} v{prediction['model_version']}")
        print(f"⏰ Prediction Time: {prediction['prediction_timestamp']}")
        print()
        
        # Detailed breakdown
        print("🎲 Prediction Breakdown:")
        
        # Total Over/Under
        total_prob = prediction.get('total_over_probability', 0)
        total_binary = prediction.get('total_over_binary', 0)
        total_conf = prediction.get('total_over_confidence', 0)
        total_bet = "OVER" if total_binary == 1 else "UNDER"
        
        print(f"   Total: {total_bet}")
        print(f"   Probability: {total_prob:.3f} ({total_prob*100:.1f}%)")
        print(f"   Confidence: {total_conf:.3f}")
        
        # Moneyline
        ml_prob = prediction.get('home_ml_probability', 0)
        ml_binary = prediction.get('home_ml_binary', 0)
        ml_conf = prediction.get('home_ml_confidence', 0)
        ml_bet = "HOME" if ml_binary == 1 else "AWAY"
        
        print(f"   Moneyline: {ml_bet}")
        print(f"   Probability: {ml_prob:.3f} ({ml_prob*100:.1f}%)")
        print(f"   Confidence: {ml_conf:.3f}")
        
        # Spread if available
        if prediction.get('home_spread_probability'):
            spread_prob = prediction.get('home_spread_probability', 0)
            spread_binary = prediction.get('home_spread_binary', 0)
            spread_conf = prediction.get('home_spread_confidence', 0)
            spread_bet = "HOME +spread" if spread_binary == 1 else "AWAY +spread"
            
            print(f"   Spread: {spread_bet}")
            print(f"   Probability: {spread_prob:.3f} ({spread_prob*100:.1f}%)")
            print(f"   Confidence: {spread_conf:.3f}")
        
        print()
        
        # Betting recommendations
        recommendations = prediction.get('betting_recommendations', {})
        if recommendations:
            print("💡 Betting Recommendations:")
            rec_bets = recommendations.get('recommended_bets', [])
            confidence_level = recommendations.get('confidence_level', 'unknown')
            risk_level = recommendations.get('risk_level', 'unknown')
            
            print(f"   Recommended Bets: {', '.join(rec_bets)}")
            print(f"   Confidence Level: {confidence_level}") 
            print(f"   Risk Level: {risk_level}")
            print()
            
        # Model explanation if available
        explanation = prediction.get('explanation', {})
        if explanation:
            print("🧠 Model Explanation:")
            for key, value in explanation.items():
                print(f"   {key}: {value}")
            
    except requests.exceptions.HTTPException as e:
        if e.response.status_code == 404:
            print(f"🔍 Game {sample_game_id} not found")
            print("💡 Try getting today's predictions first to get real game IDs")
        else:
            print(f"❌ Error getting prediction: {e}")
    except Exception as e:
        print(f"❌ Error getting prediction: {e}")


def example_6_batch_predictions():
    """Example 6: Get predictions for multiple games efficiently."""
    print("\n" + "=" * 60)
    print("EXAMPLE 6: Batch Predictions")
    print("=" * 60)
    
    api = MLBBettingAPI()
    
    try:
        # First get today's games to have real game IDs
        todays_games = api.get_todays_predictions(min_confidence=0.0)  # Get all games
        
        if not todays_games:
            print("📅 No games available for batch prediction demo")
            print("💡 Using sample game IDs for demonstration")
            game_ids = ["2025-01-31-NYY-BOS", "2025-01-31-LAD-SF", "2025-01-31-HOU-TEX"]
        else:
            # Use first few real game IDs
            game_ids = [game['game_id'] for game in todays_games[:3]]
            
        print(f"🎯 Getting batch predictions for {len(game_ids)} games:")
        for game_id in game_ids:
            print(f"   📍 {game_id}")
        print()
        
        # Get batch predictions
        predictions = api.predict_batch_games(game_ids)
        
        print("📊 Batch Prediction Results:")
        print()
        
        # Create summary table
        results = []
        for pred in predictions:
            game_id = pred['game_id']
            
            # Extract key predictions
            total_bet = "OVER" if pred.get('total_over_binary') == 1 else "UNDER"
            total_conf = pred.get('total_over_confidence', 0)
            
            ml_bet = "HOME" if pred.get('home_ml_binary') == 1 else "AWAY"
            ml_conf = pred.get('home_ml_confidence', 0)
            
            overall_conf = max(total_conf, ml_conf)  # Highest confidence prediction
            
            results.append({
                'Game': game_id,
                'Total': f"{total_bet} ({total_conf:.2f})",
                'ML': f"{ml_bet} ({ml_conf:.2f})",
                'Best_Confidence': overall_conf,
                'Model': pred['model_name']
            })
            
        # Display as formatted table
        if results:
            df = pl.DataFrame(results)
            print(df.to_pandas().to_string(index=False))  # Convert to pandas for formatted output
            print()
            
            # Summary statistics
            high_conf_count = len([r for r in results if r['Best_Confidence'] > 0.7])
            avg_conf = sum(r['Best_Confidence'] for r in results) / len(results)
            
            print(f"📈 Summary:")
            print(f"   High confidence predictions (>0.7): {high_conf_count}/{len(results)}")
            print(f"   Average confidence: {avg_conf:.3f}")
            print(f"   Models used: {', '.join(set(r['Model'] for r in results))}")
            
    except Exception as e:
        print(f"❌ Error getting batch predictions: {e}")


def example_7_automated_betting_strategy():
    """Example 7: Automated betting decision workflow."""
    print("\n" + "=" * 60)
    print("EXAMPLE 7: Automated Betting Strategy")
    print("=" * 60)
    
    api = MLBBettingAPI()
    
    # Configuration for betting strategy
    MIN_CONFIDENCE = 0.75  # Only bet on high-confidence predictions
    MIN_ROI_MODEL = 8.0    # Only use models with >8% ROI
    MAX_DAILY_BETS = 5     # Limit daily bets for risk management
    
    print(f"🎯 Betting Strategy Configuration:")
    print(f"   Minimum Confidence: {MIN_CONFIDENCE}")
    print(f"   Minimum Model ROI: {MIN_ROI_MODEL}%")
    print(f"   Maximum Daily Bets: {MAX_DAILY_BETS}")
    print()
    
    try:
        # Step 1: Find profitable models
        print("📊 Step 1: Evaluating model performance...")
        leaderboard = api.get_model_leaderboard(metric="roi_percentage", days=30)
        
        profitable_models = [
            model for model in leaderboard 
            if model.get('roi_percentage', 0) >= MIN_ROI_MODEL
        ]
        
        if not profitable_models:
            print(f"❌ No models meet ROI threshold of {MIN_ROI_MODEL}%")
            return
            
        print(f"✅ Found {len(profitable_models)} profitable models:")
        for model in profitable_models:
            name = model.get('model_name', 'unknown')
            roi = model.get('roi_percentage', 0)
            print(f"   🏆 {name}: {roi:.1f}% ROI")
        print()
        
        # Step 2: Get today's predictions from profitable models
        print("🔍 Step 2: Finding high-confidence predictions...")
        all_predictions = []
        
        for model in profitable_models:
            model_name = model['model_name']
            try:
                predictions = api.get_todays_predictions(
                    min_confidence=MIN_CONFIDENCE, 
                    model_name=model_name
                )
                all_predictions.extend(predictions)
            except Exception as e:
                print(f"⚠️  Warning: Could not get predictions from {model_name}: {e}")
                
        if not all_predictions:
            print(f"📅 No high-confidence predictions available today")
            print(f"💡 Try lowering confidence threshold or checking data freshness")
            return
            
        print(f"🎯 Found {len(all_predictions)} high-confidence predictions")
        print()
        
        # Step 3: Rank and select best bets
        print("🏅 Step 3: Ranking predictions by confidence...")
        
        betting_opportunities = []
        for pred in all_predictions:
            game_id = pred['game_id']
            model = pred['model_name']
            
            # Find the highest confidence prediction type for this game
            predictions_data = []
            
            # Total Over/Under
            if pred.get('total_over_confidence', 0) >= MIN_CONFIDENCE:
                bet_type = "OVER" if pred.get('total_over_binary') == 1 else "UNDER"
                predictions_data.append({
                    'game_id': game_id,
                    'model': model,
                    'bet_type': f"Total {bet_type}",
                    'confidence': pred.get('total_over_confidence', 0),
                    'probability': pred.get('total_over_probability', 0)
                })
                
            # Moneyline
            if pred.get('home_ml_confidence', 0) >= MIN_CONFIDENCE:
                bet_type = "HOME" if pred.get('home_ml_binary') == 1 else "AWAY"
                predictions_data.append({
                    'game_id': game_id,
                    'model': model,
                    'bet_type': f"ML {bet_type}",
                    'confidence': pred.get('home_ml_confidence', 0),
                    'probability': pred.get('home_ml_probability', 0)
                })
                
            # Spread
            if pred.get('home_spread_confidence', 0) >= MIN_CONFIDENCE:
                bet_type = "HOME" if pred.get('home_spread_binary') == 1 else "AWAY"
                predictions_data.append({
                    'game_id': game_id,
                    'model': model,
                    'bet_type': f"Spread {bet_type}",
                    'confidence': pred.get('home_spread_confidence', 0),
                    'probability': pred.get('home_spread_probability', 0)
                })
                
            betting_opportunities.extend(predictions_data)
        
        # Sort by confidence and take top bets
        betting_opportunities.sort(key=lambda x: x['confidence'], reverse=True)
        top_bets = betting_opportunities[:MAX_DAILY_BETS]
        
        print(f"🎪 Top {len(top_bets)} Betting Opportunities for Today:")
        print()
        
        for i, bet in enumerate(top_bets, 1):
            print(f"{i}. 🎯 {bet['game_id']}")
            print(f"   Model: {bet['model']}")  
            print(f"   Bet: {bet['bet_type']}")
            print(f"   Confidence: {bet['confidence']:.3f}")
            print(f"   Probability: {bet['probability']:.3f} ({bet['probability']*100:.1f}%)")
            print()
            
        # Step 4: Final recommendations
        print("💡 Final Recommendations:")
        if len(top_bets) > 0:
            avg_confidence = sum(bet['confidence'] for bet in top_bets) / len(top_bets)
            print(f"   ✅ {len(top_bets)} betting opportunities identified")
            print(f"   📊 Average confidence: {avg_confidence:.3f}")
            print(f"   🎯 All bets from models with >{MIN_ROI_MODEL}% ROI")
            print(f"   ⚖️  Risk management: Limited to {MAX_DAILY_BETS} bets max")
        else:
            print(f"   ⚠️  No bets meet all criteria today")
            print(f"   💡 Consider lowering confidence threshold or checking model performance")
            
    except Exception as e:
        print(f"❌ Error in automated betting strategy: {e}")


def main():
    """Run all examples."""
    print("🏟️  MLB Betting System - API Usage Examples")
    print("=" * 60)
    print()
    print("This script demonstrates how to interact with the MLB betting system API")
    print("to get model performance data and betting recommendations.")
    print()
    
    # Run all examples
    example_1_basic_health_check()
    example_2_todays_predictions()
    example_3_model_leaderboard()
    example_4_detailed_model_analysis()
    example_5_single_game_prediction()
    example_6_batch_predictions()
    example_7_automated_betting_strategy()
    
    print("\n" + "=" * 60)
    print("🎉 All examples completed!")
    print()
    print("💡 Next steps:")
    print("   1. Adapt these examples for your specific use case")
    print("   2. Set up automated scripts for daily predictions")
    print("   3. Build your own risk management rules")
    print("   4. Track your betting performance vs predictions")
    print()
    print("📚 More resources:")
    print("   • Web Dashboard: http://localhost:8000/dashboard")
    print("   • API Documentation: http://localhost:8000/docs")
    print("   • User Guide: docs/ML_USER_GUIDE.md")


if __name__ == "__main__":
    main()