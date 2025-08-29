"""
Results Analysis and Performance Comparison

Analyzes optimization results to identify the best parameters and provides
comprehensive performance comparisons and insights.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
import numpy as np
import json
from pathlib import Path

from src.core.logging import LogComponent, get_logger
from .job import OptimizationResult, OptimizationJob


@dataclass
class ParameterImportance:
    """Importance analysis for a parameter"""
    parameter_name: str
    importance_score: float  # 0-1, higher = more important
    correlation_with_objective: float  # -1 to 1
    optimal_value: Any
    value_range: Tuple[Any, Any]
    sensitivity_score: float  # How much objective changes with parameter


@dataclass 
class ComparisonResult:
    """Result of comparing parameter configurations"""
    baseline_parameters: Dict[str, Any]
    optimized_parameters: Dict[str, Any]
    
    baseline_performance: float
    optimized_performance: float
    improvement_percentage: float
    
    statistical_significance: bool
    confidence_level: float
    
    parameter_changes: Dict[str, Dict[str, Any]]  # parameter -> {old, new, change}


class ResultsAnalyzer:
    """
    Analyzes hyperparameter optimization results to provide insights
    and recommendations for strategy improvement.
    """
    
    def __init__(self):
        self.logger = get_logger(__name__, LogComponent.OPTIMIZATION)
    
    def analyze_optimization_job(self, job: OptimizationJob) -> Dict[str, Any]:
        """
        Comprehensive analysis of an optimization job.
        
        Args:
            job: Completed optimization job
            
        Returns:
            Comprehensive analysis results
        """
        if not job.all_results:
            return {"error": "No results to analyze"}
        
        valid_results = [r for r in job.all_results if r.error_message is None]
        
        if not valid_results:
            return {"error": "No valid results to analyze"}
        
        analysis = {
            "job_summary": self._analyze_job_summary(job),
            "parameter_importance": self._analyze_parameter_importance(valid_results),
            "performance_distribution": self._analyze_performance_distribution(valid_results),
            "convergence_analysis": self._analyze_convergence(valid_results),
            "best_configuration": self._analyze_best_configuration(job.best_result),
            "recommendations": self._generate_recommendations(job, valid_results)
        }
        
        return analysis
    
    def compare_configurations(
        self,
        baseline_results: List[OptimizationResult],
        optimized_results: List[OptimizationResult],
        confidence_level: float = 0.95
    ) -> ComparisonResult:
        """
        Compare baseline vs optimized parameter configurations.
        
        Args:
            baseline_results: Results from baseline configuration
            optimized_results: Results from optimized configuration
            confidence_level: Statistical confidence level
            
        Returns:
            Detailed comparison results
        """
        # Calculate performance statistics
        baseline_perf = np.mean([r.objective_value for r in baseline_results])
        optimized_perf = np.mean([r.objective_value for r in optimized_results])
        
        improvement_pct = ((optimized_perf - baseline_perf) / baseline_perf) * 100 if baseline_perf != 0 else 0
        
        # Statistical significance test (simplified)
        is_significant = self._test_significance(
            [r.objective_value for r in baseline_results],
            [r.objective_value for r in optimized_results],
            confidence_level
        )
        
        # Analyze parameter changes
        baseline_params = baseline_results[0].parameters if baseline_results else {}
        optimized_params = optimized_results[0].parameters if optimized_results else {}
        
        parameter_changes = {}
        all_params = set(baseline_params.keys()) | set(optimized_params.keys())
        
        for param in all_params:
            old_val = baseline_params.get(param, "N/A")
            new_val = optimized_params.get(param, "N/A")
            
            if old_val != "N/A" and new_val != "N/A":
                try:
                    if isinstance(old_val, (int, float)) and isinstance(new_val, (int, float)):
                        change = ((new_val - old_val) / old_val) * 100 if old_val != 0 else 0
                    else:
                        change = "changed" if old_val != new_val else "unchanged"
                except:
                    change = "changed" if old_val != new_val else "unchanged"
            else:
                change = "N/A"
            
            parameter_changes[param] = {
                "old_value": old_val,
                "new_value": new_val,
                "change": change
            }
        
        return ComparisonResult(
            baseline_parameters=baseline_params,
            optimized_parameters=optimized_params,
            baseline_performance=baseline_perf,
            optimized_performance=optimized_perf,
            improvement_percentage=improvement_pct,
            statistical_significance=is_significant,
            confidence_level=confidence_level,
            parameter_changes=parameter_changes
        )
    
    def generate_optimization_report(
        self,
        job: OptimizationJob,
        output_path: Optional[str] = None
    ) -> str:
        """
        Generate a comprehensive optimization report.
        
        Args:
            job: Optimization job to report on
            output_path: Optional path for report file
            
        Returns:
            Path to generated report
        """
        analysis = self.analyze_optimization_job(job)
        
        # Generate report content
        report_content = {
            "optimization_report": {
                "job_id": job.job_id,
                "strategy_name": job.config.strategy_name,
                "algorithm": job.config.algorithm.value,
                "generation_time": datetime.now().isoformat(),
                "analysis": analysis
            }
        }
        
        # Determine output path
        if output_path is None:
            output_path = f"optimization_report_{job.job_id}.json"
        
        # Write report
        with open(output_path, 'w') as f:
            json.dump(report_content, f, indent=2, default=str)
        
        self.logger.info(f"Generated optimization report: {output_path}")
        return output_path
    
    def _analyze_job_summary(self, job: OptimizationJob) -> Dict[str, Any]:
        """Analyze overall job performance"""
        
        summary = job.get_results_summary()
        progress = job.get_progress_info()
        
        return {
            "job_id": job.job_id,
            "strategy_name": job.config.strategy_name,
            "algorithm": job.config.algorithm.value,
            "status": progress["status"],
            "total_evaluations": summary["total_evaluations"],
            "valid_evaluations": summary["valid_evaluations"],
            "success_rate": summary.get("success_rate", 0),
            "best_performance": summary.get("best_performance", 0),
            "mean_performance": summary.get("mean_performance", 0),
            "performance_std": summary.get("std_performance", 0),
            "execution_time": progress.get("elapsed_time_seconds", 0),
            "evaluations_per_hour": summary["valid_evaluations"] / (progress.get("elapsed_time_seconds", 1) / 3600) if progress.get("elapsed_time_seconds", 0) > 0 else 0
        }
    
    def _analyze_parameter_importance(self, results: List[OptimizationResult]) -> List[ParameterImportance]:
        """Analyze the importance of each parameter"""
        
        if len(results) < 10:
            return []
        
        # Get all parameter names
        all_params = set()
        for result in results:
            all_params.update(result.parameters.keys())
        
        param_importance = []
        
        for param_name in all_params:
            # Extract parameter values and corresponding objectives
            param_values = []
            objective_values = []
            
            for result in results:
                if param_name in result.parameters:
                    param_val = result.parameters[param_name]
                    
                    # Convert to numeric if possible
                    if isinstance(param_val, (int, float)):
                        param_values.append(param_val)
                        objective_values.append(result.objective_value)
                    elif isinstance(param_val, bool):
                        param_values.append(1.0 if param_val else 0.0)
                        objective_values.append(result.objective_value)
                    elif isinstance(param_val, str):
                        # For categorical parameters, use one-hot encoding
                        unique_values = list(set(r.parameters.get(param_name) for r in results if param_name in r.parameters))
                        if param_val in unique_values:
                            param_values.append(float(unique_values.index(param_val)))
                            objective_values.append(result.objective_value)
            
            if len(param_values) < 5:  # Need minimum samples
                continue
            
            # Calculate correlation
            correlation = np.corrcoef(param_values, objective_values)[0, 1] if len(param_values) > 1 else 0.0
            if np.isnan(correlation):
                correlation = 0.0
            
            # Calculate importance score (simplified)
            importance_score = abs(correlation)
            
            # Calculate sensitivity (how much objective changes with parameter)
            param_range = max(param_values) - min(param_values)
            obj_range = max(objective_values) - min(objective_values)
            sensitivity = (obj_range / param_range) if param_range > 0 else 0.0
            
            # Find optimal value (value associated with best performance)
            best_idx = objective_values.index(max(objective_values))
            optimal_value = param_values[best_idx]
            
            param_importance.append(ParameterImportance(
                parameter_name=param_name,
                importance_score=importance_score,
                correlation_with_objective=correlation,
                optimal_value=optimal_value,
                value_range=(min(param_values), max(param_values)),
                sensitivity_score=sensitivity
            ))
        
        # Sort by importance
        param_importance.sort(key=lambda x: x.importance_score, reverse=True)
        
        return param_importance
    
    def _analyze_performance_distribution(self, results: List[OptimizationResult]) -> Dict[str, Any]:
        """Analyze the distribution of performance results"""
        
        objective_values = [r.objective_value for r in results]
        
        return {
            "count": len(objective_values),
            "mean": np.mean(objective_values),
            "median": np.median(objective_values),
            "std": np.std(objective_values),
            "min": min(objective_values),
            "max": max(objective_values),
            "q25": np.percentile(objective_values, 25),
            "q75": np.percentile(objective_values, 75),
            "skewness": self._calculate_skewness(objective_values),
            "top_10_percent_threshold": np.percentile(objective_values, 90),
            "bottom_10_percent_threshold": np.percentile(objective_values, 10)
        }
    
    def _analyze_convergence(self, results: List[OptimizationResult]) -> Dict[str, Any]:
        """Analyze optimization convergence"""
        
        # Sort by evaluation time
        sorted_results = sorted(results, key=lambda r: r.evaluation_timestamp)
        
        # Calculate rolling best
        rolling_best = []
        current_best = sorted_results[0].objective_value
        
        for result in sorted_results:
            if result.objective_value > current_best:
                current_best = result.objective_value
            rolling_best.append(current_best)
        
        # Find improvement points
        improvement_points = []
        for i in range(1, len(rolling_best)):
            if rolling_best[i] > rolling_best[i-1]:
                improvement_points.append(i)
        
        # Calculate convergence metrics
        total_improvement = rolling_best[-1] - rolling_best[0]
        early_improvement = rolling_best[len(rolling_best)//4] - rolling_best[0] if len(rolling_best) > 4 else 0
        late_improvement = rolling_best[-1] - rolling_best[3*len(rolling_best)//4] if len(rolling_best) > 4 else 0
        
        return {
            "total_improvement": total_improvement,
            "early_improvement_rate": early_improvement / total_improvement if total_improvement > 0 else 0,
            "late_improvement_rate": late_improvement / total_improvement if total_improvement > 0 else 0,
            "improvement_points": len(improvement_points),
            "convergence_rate": len(improvement_points) / len(results),
            "plateau_length": len(results) - (improvement_points[-1] if improvement_points else 0),
            "converged": len(results) - (improvement_points[-1] if improvement_points else 0) > 20
        }
    
    def _analyze_best_configuration(self, best_result: Optional[OptimizationResult]) -> Dict[str, Any]:
        """Analyze the best parameter configuration found"""
        
        if best_result is None:
            return {"error": "No best result available"}
        
        return {
            "parameters": best_result.parameters,
            "objective_value": best_result.objective_value,
            "roi_percentage": best_result.roi_percentage,
            "win_rate": best_result.win_rate,
            "profit_factor": best_result.profit_factor,
            "total_bets": best_result.total_bets,
            "max_drawdown": best_result.max_drawdown,
            "evaluation_id": best_result.evaluation_id,
            "validation_period": {
                "start": best_result.validation_period_start.isoformat() if best_result.validation_period_start else None,
                "end": best_result.validation_period_end.isoformat() if best_result.validation_period_end else None
            }
        }
    
    def _generate_recommendations(
        self,
        job: OptimizationJob,
        results: List[OptimizationResult]
    ) -> Dict[str, Any]:
        """Generate optimization recommendations"""
        
        recommendations = {
            "parameter_tuning": [],
            "algorithm_suggestions": [],
            "validation_recommendations": [],
            "next_steps": []
        }
        
        # Parameter-specific recommendations
        param_importance = self._analyze_parameter_importance(results)
        
        for param in param_importance[:5]:  # Top 5 most important parameters
            if param.importance_score > 0.3:  # High importance
                recommendations["parameter_tuning"].append({
                    "parameter": param.parameter_name,
                    "importance": param.importance_score,
                    "recommendation": f"Focus on optimizing {param.parameter_name} (correlation: {param.correlation_with_objective:.3f})",
                    "optimal_value": param.optimal_value
                })
        
        # Algorithm suggestions
        convergence = self._analyze_convergence(results)
        
        if convergence["converged"]:
            recommendations["algorithm_suggestions"].append(
                "Optimization has converged. Consider using a more focused search around the best parameters."
            )
        else:
            recommendations["algorithm_suggestions"].append(
                "Optimization has not fully converged. Consider running more evaluations or trying a different algorithm."
            )
        
        # Validation recommendations
        if job.best_result and job.best_result.total_bets < 50:
            recommendations["validation_recommendations"].append(
                "Best configuration has low sample size. Validate with longer time period or different validation set."
            )
        
        if len([r for r in results if r.objective_value > 0]) / len(results) < 0.3:
            recommendations["validation_recommendations"].append(
                "Many configurations show poor performance. Consider adjusting parameter ranges or strategy logic."
            )
        
        # Next steps
        recommendations["next_steps"].append("Deploy best configuration with monitoring")
        recommendations["next_steps"].append("Set up A/B testing against baseline")
        recommendations["next_steps"].append("Schedule regular re-optimization")
        
        return recommendations
    
    def _test_significance(
        self,
        baseline_values: List[float],
        optimized_values: List[float],
        confidence_level: float
    ) -> bool:
        """Simple statistical significance test"""
        
        if len(baseline_values) < 2 or len(optimized_values) < 2:
            return False
        
        baseline_mean = np.mean(baseline_values)
        optimized_mean = np.mean(optimized_values)
        
        # Simple heuristic: significant if optimized mean is substantially better
        improvement = (optimized_mean - baseline_mean) / baseline_mean if baseline_mean != 0 else 0
        
        return improvement > 0.05  # 5% improvement threshold
    
    def _calculate_skewness(self, values: List[float]) -> float:
        """Calculate skewness of distribution"""
        if len(values) < 3:
            return 0.0
        
        mean = np.mean(values)
        std = np.std(values)
        
        if std == 0:
            return 0.0
        
        skewness = np.mean([((x - mean) / std) ** 3 for x in values])
        return skewness
    
    def export_analysis_results(
        self,
        analysis: Dict[str, Any],
        output_path: str
    ) -> None:
        """Export analysis results to file"""
        
        with open(output_path, 'w') as f:
            json.dump(analysis, f, indent=2, default=str)
        
        self.logger.info(f"Exported analysis results to {output_path}")
    
    def create_summary_table(self, jobs: List[OptimizationJob]) -> str:
        """Create a summary table of optimization jobs"""
        
        if not jobs:
            return "No optimization jobs to summarize"
        
        headers = ["Job ID", "Strategy", "Algorithm", "Status", "Best ROI", "Evaluations", "Runtime"]
        rows = []
        
        for job in jobs:
            progress = job.get_progress_info()
            summary = job.get_results_summary()
            
            rows.append([
                job.job_id[:12] + "...",  # Truncated job ID
                job.config.strategy_name,
                job.config.algorithm.value,
                progress["status"],
                f"{summary.get('best_performance', 0):.2f}%",
                f"{summary['valid_evaluations']}/{summary['total_evaluations']}",
                f"{progress.get('elapsed_time_seconds', 0)/3600:.1f}h"
            ])
        
        # Format as simple table
        col_widths = [max(len(str(item)) for item in col) for col in zip(headers, *rows)]
        
        table_lines = []
        
        # Header
        header_line = " | ".join(headers[i].ljust(col_widths[i]) for i in range(len(headers)))
        table_lines.append(header_line)
        table_lines.append("-" * len(header_line))
        
        # Rows
        for row in rows:
            row_line = " | ".join(str(row[i]).ljust(col_widths[i]) for i in range(len(row)))
            table_lines.append(row_line)
        
        return "\n".join(table_lines)