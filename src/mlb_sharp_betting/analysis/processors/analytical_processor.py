import numpy as np
import pandas as pd

try:
    import plotly.express as px
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots

    PLOTLY_AVAILABLE = True
except ImportError:
    PLOTLY_AVAILABLE = False
    go = px = make_subplots = None
try:
    from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier
    from sklearn.metrics import classification_report, confusion_matrix, roc_auc_score
    from sklearn.model_selection import train_test_split
    from sklearn.preprocessing import StandardScaler

    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False
    train_test_split = RandomForestClassifier = GradientBoostingClassifier = None
    classification_report = confusion_matrix = roc_auc_score = StandardScaler = None
import warnings

warnings.filterwarnings("ignore")


class AnalyticalProcessor:
    """
    Comprehensive MLB Sharp Action Analysis Framework

    ML-based historical analysis with comprehensive feature extraction,
    model training, and predictive insights for sharp betting patterns.

    Migrated from analyzers/sharp_action_analyzer.py as part of Phase 2 refactoring.
    Handles JSON-formatted split_value data properly.
    """

    def __init__(self, db_path: str = "PostgreSQL database"):
        """Initialize analyzer with database connection"""
        self.db_path = db_path
        from ...db.connection import get_db_manager

        self.db_manager = get_db_manager()
        self.data = {}

    def extract_sharp_features(self) -> pd.DataFrame:
        """Extract sharp action features from database with proper JSON handling"""
        query = """
        WITH parsed_values AS (
            SELECT 
                *,
                CASE 
                    WHEN split_type = 'moneyline' AND split_value LIKE '{%}' THEN
                        CAST(split_value::json->>'$.home' AS REAL)
                    WHEN split_type IN ('spread', 'total') THEN
                        CAST(split_value AS REAL)
                    ELSE NULL
                END as home_line,
                CASE 
                    WHEN split_type = 'moneyline' AND split_value LIKE '{%}' THEN
                        CAST(split_value::json->>'$.away' AS REAL)
                    WHEN split_type IN ('spread', 'total') THEN
                        CAST(split_value AS REAL)
                    ELSE NULL
                END as away_line,
                -- Extract the primary line value for movement analysis
                CASE 
                    WHEN split_type = 'moneyline' AND split_value LIKE '{%}' THEN
                        CAST(split_value::json->>'$.home' AS REAL)
                    WHEN split_type IN ('spread', 'total') THEN
                        CAST(split_value AS REAL)
                    ELSE NULL
                END as primary_line_value
            FROM splits.raw_mlb_betting_splits
            WHERE split_value IS NOT NULL
        ),
        line_movement AS (
            SELECT 
                game_id,
                split_type,
                book,
                home_team,
                away_team,
                game_datetime,
                -- Get opening and closing lines
                FIRST_VALUE(primary_line_value) OVER (
                    PARTITION BY game_id, split_type, book 
                    ORDER BY last_updated 
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) as opening_line,
                LAST_VALUE(primary_line_value) OVER (
                    PARTITION BY game_id, split_type, book 
                    ORDER BY last_updated 
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) as closing_line,
                -- Get final betting percentages
                LAST_VALUE(home_or_over_bets_percentage) OVER (
                    PARTITION BY game_id, split_type, book 
                    ORDER BY last_updated 
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) as final_bet_pct,
                LAST_VALUE(home_or_over_stake_percentage) OVER (
                    PARTITION BY game_id, split_type, book 
                    ORDER BY last_updated 
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) as final_stake_pct,
                -- Count line changes
                COUNT(*) OVER (PARTITION BY game_id, split_type, book) as line_changes,
                -- Row number for deduplication
                ROW_NUMBER() OVER (
                    PARTITION BY game_id, split_type, book 
                    ORDER BY last_updated DESC
                ) as rn
            FROM parsed_values
            WHERE primary_line_value IS NOT NULL
        ),
        sharp_indicators AS (
            SELECT 
                game_id,
                split_type,
                book,
                home_team,
                away_team,
                game_datetime,
                opening_line,
                closing_line,
                final_bet_pct,
                final_stake_pct,
                line_changes,
                -- Sharp money differential (key indicator)
                COALESCE(final_stake_pct, 0) - COALESCE(final_bet_pct, 0) as sharp_differential,
                -- Line movement amount (absolute value)
                CASE 
                    WHEN opening_line IS NOT NULL AND closing_line IS NOT NULL 
                    THEN ABS(closing_line - opening_line)
                    ELSE 0 
                END as line_movement_magnitude,
                -- Line movement direction
                CASE 
                    WHEN opening_line IS NOT NULL AND closing_line IS NOT NULL 
                    THEN closing_line - opening_line
                    ELSE 0 
                END as line_movement_direction,
                -- Reverse line movement detection (line moves against public betting)
                CASE 
                    WHEN final_bet_pct > 60 AND closing_line > opening_line THEN 1  -- Public on home/over, line moves up
                    WHEN final_bet_pct < 40 AND closing_line < opening_line THEN 1  -- Public on away/under, line moves down
                    ELSE 0
                END as reverse_line_movement,
                -- Consensus fade opportunity (heavy public betting)
                CASE WHEN final_bet_pct > 70 OR final_bet_pct < 30 THEN 1 ELSE 0 END as consensus_fade,
                -- Sharp money detected (stake % significantly higher than bet %)
                CASE WHEN COALESCE(final_stake_pct, 0) - COALESCE(final_bet_pct, 0) > 15 THEN 1 ELSE 0 END as sharp_money_detected
            FROM line_movement
            WHERE rn = 1  -- Get only the latest record per game/split_type/book
        ),
        game_features AS (
            SELECT 
                si.game_id,
                si.home_team,
                si.away_team,
                si.game_datetime,
                -- Aggregate sharp indicators across books and split types
                AVG(si.sharp_differential) as avg_sharp_differential,
                MAX(si.sharp_differential) as max_sharp_differential,
                MIN(si.sharp_differential) as min_sharp_differential,
                AVG(si.line_movement_magnitude) as avg_line_movement,
                MAX(si.line_movement_magnitude) as max_line_movement,
                AVG(si.line_movement_direction) as avg_line_direction,
                AVG(si.reverse_line_movement::FLOAT) as pct_reverse_movement,
                AVG(si.consensus_fade::FLOAT) as pct_consensus_fade,
                AVG(si.sharp_money_detected::FLOAT) as pct_sharp_money,
                AVG(si.line_changes) as avg_line_changes,
                COUNT(DISTINCT si.book) as num_books,
                COUNT(DISTINCT si.split_type) as num_split_types,
                -- Moneyline specific features
                AVG(CASE WHEN si.split_type = 'moneyline' THEN si.sharp_differential END) as ml_sharp_differential,
                AVG(CASE WHEN si.split_type = 'moneyline' THEN si.line_movement_magnitude END) as ml_line_movement,
                AVG(CASE WHEN si.split_type = 'moneyline' THEN si.reverse_line_movement::FLOAT END) as ml_reverse_movement,
                -- Spread specific features  
                AVG(CASE WHEN si.split_type = 'spread' THEN si.sharp_differential END) as spread_sharp_differential,
                AVG(CASE WHEN si.split_type = 'spread' THEN si.line_movement_magnitude END) as spread_line_movement,
                -- Total specific features
                AVG(CASE WHEN si.split_type = 'total' THEN si.sharp_differential END) as total_sharp_differential,
                AVG(CASE WHEN si.split_type = 'total' THEN si.line_movement_magnitude END) as total_line_movement
            FROM sharp_indicators si
            GROUP BY si.game_id, si.home_team, si.away_team, si.game_datetime
        )
        SELECT 
            gf.*,
            go.home_win,
            go.home_cover_spread,
            go.over,
            go.home_score,
            go.away_score,
            go.total_line,
            go.home_spread_line,
            go.game_date
        FROM game_features gf
        JOIN public.game_outcomes go ON gf.game_id = go.game_id
        WHERE go.home_win IS NOT NULL
        """

        try:
            df = self._execute_query_to_dataframe(query)
            print(f"Successfully extracted {len(df)} games with sharp action features")
            print(f"Date range: {df['game_date'].min()} to {df['game_date'].max()}")
            print(f"Average books per game: {df['num_books'].mean():.1f}")
            print(f"Average split types per game: {df['num_split_types'].mean():.1f}")
            return df
        except Exception as e:
            print(f"Error extracting features: {e}")
            return pd.DataFrame()

    def calculate_sharp_scores(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate comprehensive sharp action scores"""
        if df.empty:
            return df

        df = df.copy()

        # Fill NaN values with 0 for numerical operations
        numeric_cols = [
            "avg_sharp_differential",
            "max_line_movement",
            "pct_reverse_movement",
            "pct_sharp_money",
            "ml_sharp_differential",
            "spread_sharp_differential",
        ]
        df[numeric_cols] = df[numeric_cols].fillna(0)

        # Normalize features for scoring (using robust scaling to handle outliers)
        from sklearn.preprocessing import RobustScaler

        scaler = RobustScaler()

        features_to_scale = [
            "avg_sharp_differential",
            "max_line_movement",
            "pct_reverse_movement",
        ]
        df[features_to_scale] = scaler.fit_transform(df[features_to_scale])

        # Calculate composite sharp score with weighted components
        df["sharp_action_score"] = (
            df["avg_sharp_differential"] * 0.35  # Stake vs bet differential
            + df["pct_reverse_movement"] * 0.25  # Reverse line movement
            + df["max_line_movement"] * 0.20  # Line movement magnitude
            + df["pct_sharp_money"] * 0.20  # Sharp money detection rate
        )

        # Categorize sharp action strength using quantiles for better distribution
        df["sharp_strength"] = pd.qcut(
            df["sharp_action_score"],
            q=3,
            labels=["Weak", "Moderate", "Strong"],
            duplicates="drop",
        )

        # Calculate bet type specific scores
        df["moneyline_sharp_score"] = df["ml_sharp_differential"].fillna(0)
        df["spread_sharp_score"] = df["spread_sharp_differential"].fillna(0)

        return df

    def analyze_predictive_power(self, df: pd.DataFrame) -> dict:
        """Analyze predictive power of sharp action indicators"""
        if not SKLEARN_AVAILABLE:
            return {"error": "scikit-learn not available for ML analysis"}

        if df.empty:
            return {}

        results = {}

        # Enhanced feature set
        feature_cols = [
            "avg_sharp_differential",
            "max_sharp_differential",
            "min_sharp_differential",
            "avg_line_movement",
            "max_line_movement",
            "avg_line_direction",
            "pct_reverse_movement",
            "pct_consensus_fade",
            "pct_sharp_money",
            "avg_line_changes",
            "num_books",
            "sharp_action_score",
            "ml_sharp_differential",
            "spread_sharp_differential",
            "total_sharp_differential",
        ]

        # Only use features that exist in the dataframe
        feature_cols = [col for col in feature_cols if col in df.columns]
        X = df[feature_cols].fillna(0)

        # Predict different outcomes
        targets = {
            "home_win": df["home_win"],
            "home_cover_spread": df["home_cover_spread"].fillna(False),
            "over": df["over"],
        }

        for target_name, y in targets.items():
            if y.isna().sum() > len(y) * 0.5:  # Skip if too many missing values
                print(f"Skipping {target_name} - too many missing values")
                continue

            # Remove rows with missing target values
            mask = ~y.isna()
            X_clean = X[mask]
            y_clean = y[mask]

            if len(X_clean) < 50:  # Need minimum samples
                print(
                    f"Skipping {target_name} - insufficient data ({len(X_clean)} samples)"
                )
                continue

            try:
                # Split data
                X_train, X_test, y_train, y_test = train_test_split(
                    X_clean,
                    y_clean,
                    test_size=0.2,
                    random_state=42,
                    stratify=y_clean if len(y_clean.unique()) > 1 else None,
                )

                # Train models
                models = {
                    "RandomForest": RandomForestClassifier(
                        n_estimators=100,
                        max_depth=10,
                        min_samples_split=10,
                        random_state=42,
                    ),
                    "GradientBoosting": GradientBoostingClassifier(
                        n_estimators=100,
                        max_depth=6,
                        learning_rate=0.1,
                        random_state=42,
                    ),
                }

                target_results = {}
                for model_name, model in models.items():
                    model.fit(X_train, y_train)
                    y_pred = model.predict(X_test)
                    y_pred_proba = model.predict_proba(X_test)[:, 1]

                    target_results[model_name] = {
                        "accuracy": model.score(X_test, y_test),
                        "auc": roc_auc_score(y_test, y_pred_proba),
                        "classification_report": classification_report(y_test, y_pred),
                        "feature_importance": dict(
                            zip(feature_cols, model.feature_importances_, strict=False)
                        ),
                        "train_samples": len(X_train),
                        "test_samples": len(X_test),
                    }

                results[target_name] = target_results
                print(f"Successfully trained models for {target_name}")

            except Exception as e:
                print(f"Error training models for {target_name}: {e}")
                continue

        return results

    def create_visualizations(self, df: pd.DataFrame) -> None:
        """Create comprehensive visualizations"""
        if not PLOTLY_AVAILABLE:
            print("Plotly not available for visualizations")
            return

        if df.empty:
            print("No data available for visualizations")
            return

        try:
            # 1. Sharp Action Distribution
            fig1 = px.histogram(
                df,
                x="sharp_action_score",
                color="sharp_strength",
                title="Distribution of Sharp Action Scores",
                labels={
                    "sharp_action_score": "Sharp Action Score",
                    "count": "Number of Games",
                },
                nbins=30,
            )
            fig1.show()

            # 2. Sharp Action vs Win Rate Analysis
            win_rate_analysis = (
                df.groupby("sharp_strength")
                .agg(
                    {
                        "home_win": "mean",
                        "home_cover_spread": lambda x: x.fillna(False).mean(),
                        "over": "mean",
                        "game_id": "count",  # Sample size
                    }
                )
                .round(3)
            )

            win_rate_analysis.columns = [
                "Home Win Rate",
                "Spread Cover Rate",
                "Over Rate",
                "Games",
            ]

            fig2 = go.Figure()
            for outcome in ["Home Win Rate", "Spread Cover Rate", "Over Rate"]:
                fig2.add_trace(
                    go.Bar(
                        name=outcome,
                        x=win_rate_analysis.index,
                        y=win_rate_analysis[outcome],
                        text=[
                            f"{rate:.1%}<br>n={games}"
                            for rate, games in zip(
                                win_rate_analysis[outcome],
                                win_rate_analysis["Games"],
                                strict=False,
                            )
                        ],
                        textposition="auto",
                    )
                )

            fig2.update_layout(
                title="Win Rates by Sharp Action Strength",
                xaxis_title="Sharp Action Strength",
                yaxis_title="Win Rate",
                barmode="group",
            )
            fig2.show()

            # 3. Feature Correlation Heatmap
            feature_cols = [
                "avg_sharp_differential",
                "max_line_movement",
                "pct_reverse_movement",
                "pct_sharp_money",
                "sharp_action_score",
            ]
            outcome_cols = ["home_win"]

            # Only include columns that exist
            available_cols = [
                col for col in feature_cols + outcome_cols if col in df.columns
            ]

            if len(available_cols) > 2:
                corr_matrix = df[available_cols].corr()

                fig3 = px.imshow(
                    corr_matrix,
                    title="Feature Correlation Matrix",
                    color_continuous_scale="RdBu",
                    aspect="auto",
                    text_auto=True,
                )
                fig3.show()

            # 4. Line Movement Analysis
            if "avg_line_movement" in df.columns and "max_line_movement" in df.columns:
                fig4 = px.scatter(
                    df,
                    x="avg_line_movement",
                    y="sharp_action_score",
                    color="home_win",
                    title="Line Movement vs Sharp Action Score",
                    labels={
                        "avg_line_movement": "Average Line Movement",
                        "sharp_action_score": "Sharp Action Score",
                    },
                )
                fig4.show()

        except Exception as e:
            print(f"Error creating visualizations: {e}")

    def generate_insights(
        self, df: pd.DataFrame, model_results: dict
    ) -> dict[str, str]:
        """Generate actionable insights from analysis"""
        if df.empty:
            return {"error": "No data available for analysis"}

        insights = {}

        try:
            # Sharp action effectiveness analysis
            if "sharp_strength" in df.columns:
                sharp_analysis = (
                    df.groupby("sharp_strength")
                    .agg(
                        {
                            "home_win": ["mean", "count"],
                            "home_cover_spread": lambda x: x.fillna(False).mean(),
                            "over": "mean",
                        }
                    )
                    .round(3)
                )

                insights["sharp_effectiveness"] = f"""
Sharp Action Effectiveness Analysis:
- Total games analyzed: {len(df)}
- Games by sharp strength: {dict(df["sharp_strength"].value_counts())}
- Win rates by strength: {dict(sharp_analysis["home_win"]["mean"])}
- Sample sizes: {dict(sharp_analysis["home_win"]["count"])}

Key Finding: {self._interpret_sharp_effectiveness(sharp_analysis)}
                """

            # Model performance summary
            if model_results:
                best_models = {}
                for target, models in model_results.items():
                    if models:  # Check if models dict is not empty
                        best_model = max(models.items(), key=lambda x: x[1]["auc"])
                        best_models[target] = best_model

                if best_models:
                    insights["model_performance"] = f"""
Best Predictive Models:
{chr(10).join([f"- {target}: {model[0]} (AUC: {model[1]['auc']:.3f}, Accuracy: {model[1]['accuracy']:.3f})" for target, model in best_models.items()])}

Model Reliability: {self._assess_model_reliability(best_models)}
                    """

            # Feature importance analysis
            if model_results and "home_win" in model_results:
                rf_results = model_results["home_win"].get("RandomForest")
                if rf_results:
                    rf_importance = rf_results["feature_importance"]
                    top_features = sorted(
                        rf_importance.items(), key=lambda x: x[1], reverse=True
                    )[:5]

                    insights["key_features"] = f"""
Most Predictive Features for Game Outcomes:
{chr(10).join([f"- {feature.replace('_', ' ').title()}: {importance:.3f}" for feature, importance in top_features])}

Feature Interpretation: {self._interpret_top_features(top_features)}
                    """

            # Data quality and coverage
            insights["data_summary"] = f"""
Data Quality Summary:
- Date range: {df["game_date"].min()} to {df["game_date"].max()}
- Average books per game: {df["num_books"].mean():.1f}
- Games with sharp action detected: {(df["pct_sharp_money"] > 0.5).sum()} ({(df["pct_sharp_money"] > 0.5).mean() * 100:.1f}%)
- Games with reverse line movement: {(df["pct_reverse_movement"] > 0.5).sum()} ({(df["pct_reverse_movement"] > 0.5).mean() * 100:.1f}%)
            """

        except Exception as e:
            insights["error"] = f"Error generating insights: {str(e)}"

        return insights

    def _interpret_sharp_effectiveness(self, sharp_analysis) -> str:
        """Interpret sharp action effectiveness results"""
        try:
            strong_win_rate = sharp_analysis["home_win"]["mean"].get("Strong", 0.5)
            weak_win_rate = sharp_analysis["home_win"]["mean"].get("Weak", 0.5)

            if strong_win_rate > weak_win_rate + 0.05:
                return f"Strong sharp action shows {(strong_win_rate - weak_win_rate) * 100:.1f}% better win rate"
            elif weak_win_rate > strong_win_rate + 0.05:
                return f"Weak sharp action outperforms by {(weak_win_rate - strong_win_rate) * 100:.1f}%"
            else:
                return "Sharp action strength shows minimal impact on outcomes"
        except:
            return "Unable to determine sharp action effectiveness"

    def _assess_model_reliability(self, best_models) -> str:
        """Assess overall model reliability"""
        try:
            avg_auc = np.mean([model[1]["auc"] for model in best_models.values()])
            if avg_auc > 0.7:
                return "High - Models show strong predictive power"
            elif avg_auc > 0.6:
                return "Moderate - Models show decent predictive ability"
            else:
                return "Low - Models show limited predictive power"
        except:
            return "Unable to assess model reliability"

    def _interpret_top_features(self, top_features) -> str:
        """Interpret the most important features"""
        if not top_features:
            return "No feature importance data available"

        top_feature = top_features[0][0]
        if "sharp_differential" in top_feature:
            return "Stake vs bet percentage differential is the strongest predictor"
        elif "line_movement" in top_feature:
            return "Line movement patterns are the primary predictive factor"
        elif "reverse" in top_feature:
            return "Reverse line movement is the key indicator"
        else:
            return f"The {top_feature.replace('_', ' ')} metric is most predictive"

    def generate_betting_strategy(self, df: pd.DataFrame) -> dict[str, str]:
        """Generate actionable betting strategies based on analysis"""
        if df.empty:
            return {"error": "No data available for strategy generation"}

        strategies = {}

        try:
            baseline_success = df["home_win"].mean()

            # Strategy 1: Sharp Money Following
            sharp_games = df[
                df["pct_sharp_money"] > 0.7
            ]  # Games with strong sharp money
            if len(sharp_games) > 10:
                sharp_success = sharp_games["home_win"].mean()

                strategies["sharp_money_following"] = f"""
Sharp Money Following Strategy:
- Target games with >70% sharp money indicators
- Historical success rate: {sharp_success:.1%} vs baseline {baseline_success:.1%}
- Edge: {(sharp_success - baseline_success) * 100:+.1f} percentage points
- Qualified games: {len(sharp_games)} out of {len(df)} ({len(sharp_games) / len(df) * 100:.1f}%)
- Recommendation: {"VIABLE" if sharp_success > baseline_success + 0.03 else "MARGINAL"}
                """

            # Strategy 2: Reverse Line Movement
            reverse_games = df[df["pct_reverse_movement"] > 0.5]
            if len(reverse_games) > 10:
                reverse_success = reverse_games["home_win"].mean()

                strategies["reverse_line_movement"] = f"""
Reverse Line Movement Strategy:
- Target games where line moves against public betting
- Historical success rate: {reverse_success:.1%}
- Edge vs baseline: {(reverse_success - baseline_success) * 100:+.1f} percentage points
- Qualified games: {len(reverse_games)} ({len(reverse_games) / len(df) * 100:.1f}%)
- Recommendation: {"VIABLE" if reverse_success > baseline_success + 0.03 else "MARGINAL"}
                """

            # Strategy 3: Consensus Fade
            consensus_games = df[df["pct_consensus_fade"] > 0.5]
            if len(consensus_games) > 10:
                fade_success = consensus_games["home_win"].mean()

                strategies["consensus_fade"] = f"""
Consensus Fade Strategy:
- Target games with heavy public betting (>70% one side)
- Historical success rate: {fade_success:.1%}
- Edge vs baseline: {(fade_success - baseline_success) * 100:+.1f} percentage points
- Qualified games: {len(consensus_games)} ({len(consensus_games) / len(df) * 100:.1f}%)
- Recommendation: {"VIABLE" if fade_success > baseline_success + 0.03 else "MARGINAL"}
                """

        except Exception as e:
            strategies["error"] = f"Error generating strategies: {str(e)}"

        return strategies

    def run_full_analysis(self) -> dict:
        """Run complete sharp action analysis"""
        print("=" * 60)
        print("STARTING COMPREHENSIVE SHARP ACTION ANALYSIS")
        print("=" * 60)

        try:
            # Extract data
            print("\n1. Extracting sharp action features...")
            df = self.extract_sharp_features()

            if df.empty:
                return {"error": "No data extracted from database"}

            # Calculate sharp scores
            print("\n2. Calculating sharp action scores...")
            df = self.calculate_sharp_scores(df)

            # Analyze predictive power
            print("\n3. Analyzing predictive power...")
            model_results = self.analyze_predictive_power(df)

            # Create visualizations
            print("\n4. Creating visualizations...")
            self.create_visualizations(df)

            # Generate insights
            print("\n5. Generating insights...")
            insights = self.generate_insights(df, model_results)

            # Generate betting strategies
            print("\n6. Generating betting strategies...")
            strategies = self.generate_betting_strategy(df)

            # Store results
            self.data["features"] = df
            self.data["model_results"] = model_results
            self.data["insights"] = insights
            self.data["strategies"] = strategies

            print("\n" + "=" * 60)
            print("ANALYSIS COMPLETE!")
            print("=" * 60)

            return {
                "summary_stats": df.describe(),
                "model_results": model_results,
                "insights": insights,
                "strategies": strategies,
                "data_shape": df.shape,
            }

        except Exception as e:
            error_msg = f"Error in analysis: {str(e)}"
            print(f"\nERROR: {error_msg}")
            return {"error": error_msg}


# Usage Example and Testing
if __name__ == "__main__":
    print("Initializing Sharp Action Analyzer...")

    # Initialize analyzer
    analyzer = AnalyticalProcessor()

    # Run analysis
    results = analyzer.run_full_analysis()

    # Print results
    if "error" in results:
        print(f"Analysis failed: {results['error']}")
    else:
        print("\nDATA SUMMARY:")
        print(f"- Games analyzed: {results['data_shape'][0]}")
        print(f"- Features extracted: {results['data_shape'][1]}")

        # Print insights
        if "insights" in results:
            for insight_type, insight_text in results["insights"].items():
                print(f"\n{insight_type.upper().replace('_', ' ')}:")
                print(insight_text)
                print("-" * 50)

        # Print strategies
        if "strategies" in results:
            print("\nBETTING STRATEGIES:")
            for strategy_name, strategy_text in results["strategies"].items():
                print(f"\n{strategy_name.upper().replace('_', ' ')}:")
                print(strategy_text)
                print("-" * 50)
