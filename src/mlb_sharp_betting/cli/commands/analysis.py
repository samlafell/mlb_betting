#!/usr/bin/env python3
"""
Comprehensive Sports Betting Strategy Analysis Runner
Executes all SQL analysis scripts and generates detailed reports with ROI calculations

Usage:
    uv run -m mlb_sharp_betting.cli.commands.analysis
"""

import duckdb
import pandas as pd
import sys
from pathlib import Path
import datetime
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('analysis_results.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class BettingAnalyzer:
    def __init__(self, db_path: str = "data/raw/mlb_betting.duckdb"):
        """Initialize the betting analyzer with database connection"""
        self.db_path = db_path
        self.conn = None
        self.results = {}
        
    def connect_db(self):
        """Connect to the DuckDB database"""
        try:
            self.conn = duckdb.connect(self.db_path)
            logger.info(f"Successfully connected to database: {self.db_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            return False
    
    def run_sql_file(self, sql_file_path: str, strategy_name: str):
        """Execute a SQL file and return results"""
        try:
            with open(sql_file_path, 'r') as f:
                query = f.read()
            
            logger.info(f"Executing {strategy_name} analysis...")
            result_df = self.conn.execute(query).fetchdf()
            
            if len(result_df) > 0:
                self.results[strategy_name] = result_df
                logger.info(f"✅ {strategy_name}: {len(result_df)} results found")
                return result_df
            else:
                logger.warning(f"⚠️  {strategy_name}: No results returned")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"❌ Error executing {strategy_name}: {e}")
            return pd.DataFrame()
    
    def print_strategy_summary(self, df: pd.DataFrame, strategy_name: str):
        """Print a summary of strategy results"""
        if df.empty:
            print(f"\n❌ {strategy_name}: No profitable opportunities found")
            return
            
        print(f"\n🎯 {strategy_name.upper()} RESULTS")
        print("=" * 60)
        
        if 'win_rate' in df.columns and 'total_bets' in df.columns:
            # Filter for meaningful results
            significant_results = df[df['total_bets'] >= 5].copy()
            
            if significant_results.empty:
                print("   No strategies with sufficient sample size (≥5 bets)")
                return
                
            # Sort by win rate for display
            significant_results = significant_results.sort_values('win_rate', ascending=False)
            
            print(f"   📊 Total Strategies Analyzed: {len(significant_results)}")
            print(f"   🏆 Best Win Rate: {significant_results['win_rate'].max():.1f}%")
            
            if 'roi_per_100_unit' in significant_results.columns:
                best_roi = significant_results['roi_per_100_unit'].max()
                print(f"   💰 Best ROI (per $100): ${best_roi:.2f}")
            
            # Show top 3 strategies
            print("\n   🥇 TOP STRATEGIES:")
            top_strategies = significant_results.head(3)
            
            for idx, row in top_strategies.iterrows():
                bet_info = ""
                if 'total_bets' in row:
                    bet_info = f" ({int(row['total_bets'])} bets)"
                    
                roi_info = ""
                if 'roi_per_100_unit' in row:
                    roi_info = f" | ROI: ${row['roi_per_100_unit']:.2f}"
                elif 'profit_per_bet' in row:
                    roi_info = f" | Profit/Bet: ${row['profit_per_bet']:.2f}"
                
                strategy_id = row.get('source_book_type', row.get('strategy_name', 'Unknown'))
                print(f"      • {strategy_id}{bet_info}: {row['win_rate']:.1f}%{roi_info}")
        
        # Special handling for executive summary
        elif strategy_name == "Executive Summary":
            print("   📋 KEY INSIGHTS:")
            for idx, row in df.iterrows():
                if pd.notna(row.get('insight')):
                    print(f"      • {row['metric']}: {row['insight']}")
    
    def generate_roi_summary(self):
        """Generate overall ROI summary across all strategies"""
        print("\n" + "="*80)
        print("🏦 COMPREHENSIVE ROI ANALYSIS FOR $100 UNIT BETS")
        print("="*80)
        
        all_profitable = []
        
        for strategy_name, df in self.results.items():
            if df.empty:
                continue
                
            # Look for profitable strategies
            if 'win_rate' in df.columns and 'total_bets' in df.columns:
                profitable = df[
                    (df['win_rate'] >= 52.4) & 
                    (df['total_bets'] >= 5)
                ].copy()
                
                if not profitable.empty:
                    profitable['strategy'] = strategy_name
                    all_profitable.append(profitable)
        
        if not all_profitable:
            print("❌ No profitable strategies identified with sufficient sample size")
            return
            
        # Combine all profitable strategies
        combined = pd.concat(all_profitable, ignore_index=True)
        
        # Sort by ROI if available, otherwise by win rate
        if 'roi_per_100_unit' in combined.columns:
            combined = combined.sort_values('roi_per_100_unit', ascending=False)
        else:
            combined = combined.sort_values('win_rate', ascending=False)
        
        print(f"✅ Found {len(combined)} profitable betting opportunities!")
        print("\n🎯 TOP 5 PROFIT OPPORTUNITIES:")
        
        top_5 = combined.head(5)
        for idx, row in top_5.iterrows():
            strategy_id = row.get('source_book_type', row.get('bet_recommendation', 'Unknown'))
            win_rate = row['win_rate']
            total_bets = int(row['total_bets'])
            
            roi_text = ""
            if 'roi_per_100_unit' in row:
                roi = row['roi_per_100_unit']
                roi_text = f"ROI: ${roi:.2f}"
            elif 'profit_per_bet' in row:
                profit = row['profit_per_bet']
                roi_text = f"Profit/Bet: ${profit:.2f}"
            
            confidence = "Low"
            if total_bets >= 20:
                confidence = "High"
            elif total_bets >= 10:
                confidence = "Medium"
            
            print(f"{idx+1:2d}. {row['strategy']} - {strategy_id}")
            print(f"    Win Rate: {win_rate:.1f}% | Sample: {total_bets} bets | {roi_text} | Confidence: {confidence}")
            print()
    
    def run_comprehensive_analysis(self):
        """Run all analysis scripts and generate comprehensive report"""
        logger.info("Starting comprehensive betting strategy analysis...")
        
        if not self.connect_db():
            logger.error("Cannot proceed without database connection")
            return False
        
        # Define analysis scripts in execution order
        analyses = [
            ("analysis_scripts/line_movement_strategy.sql", "Line Movement Strategy"),
            ("analysis_scripts/sharp_action_detector.sql", "Sharp Action Detection"),
            ("analysis_scripts/hybrid_line_sharp_strategy.sql", "Hybrid Line + Sharp Strategy"),
            ("analysis_scripts/timing_based_strategy.sql", "Timing-Based Strategy"),
            ("analysis_scripts/strategy_comparison_roi.sql", "Strategy ROI Comparison"),
            ("analysis_scripts/executive_summary_report.sql", "Executive Summary")
        ]
        
        successful_analyses = 0
        
        # Execute each analysis
        for sql_file, strategy_name in analyses:
            if Path(sql_file).exists():
                result_df = self.run_sql_file(sql_file, strategy_name)
                if not result_df.empty:
                    successful_analyses += 1
                    self.print_strategy_summary(result_df, strategy_name)
                else:
                    logger.warning(f"No results for {strategy_name}")
            else:
                logger.error(f"SQL file not found: {sql_file}")
        
        # Generate final ROI summary
        if successful_analyses > 0:
            self.generate_roi_summary()
            
            # Save results to files
            self.save_results()
            
            print(f"\n✅ Analysis complete! {successful_analyses}/{len(analyses)} strategies executed successfully.")
            print("📁 Results saved to analysis_results.log and individual CSV files.")
            
        else:
            logger.error("No successful analyses completed")
            return False
            
        return True
    
    def save_results(self):
        """Save all results to CSV files"""
        try:
            results_dir = Path("analysis_results")
            results_dir.mkdir(exist_ok=True)
            
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            
            for strategy_name, df in self.results.items():
                if not df.empty:
                    filename = f"{strategy_name.lower().replace(' ', '_')}_{timestamp}.csv"
                    filepath = results_dir / filename
                    df.to_csv(filepath, index=False)
                    logger.info(f"Saved {strategy_name} results to {filepath}")
                    
        except Exception as e:
            logger.error(f"Error saving results: {e}")

def main():
    """Main execution function"""
    print("🎲 MLB SHARP BETTING ANALYSIS")
    print("=" * 50)
    print("Analyzing betting splits data to identify profitable strategies...")
    print("Using line movement, sharp action, and hybrid approaches")
    print()
    
    analyzer = BettingAnalyzer()
    
    try:
        success = analyzer.run_comprehensive_analysis()
        if success:
            print("\n🎉 Analysis completed successfully!")
            print("💡 Check the logs and CSV files for detailed results.")
        else:
            print("\n❌ Analysis failed. Check the logs for details.")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n⚠️  Analysis interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        print(f"\n❌ Unexpected error occurred: {e}")
        sys.exit(1)
    finally:
        if analyzer.conn:
            analyzer.conn.close()
            logger.info("Database connection closed")

if __name__ == "__main__":
    main() 