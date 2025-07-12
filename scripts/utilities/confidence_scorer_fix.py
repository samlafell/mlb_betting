"""
FIXED ConfidenceScorer with adjusted weights and thresholds
"""

# In __init__, adjust weights:
self.weights = {
    'signal_strength': 0.50,      # INCREASED from 0.40 - More emphasis on signal
    'source_reliability': 0.25,   # DECREASED from 0.30
    'strategy_performance': 0.15, # DECREASED from 0.20
    'data_quality': 0.05,         # Same
    'market_context': 0.05        # Same
}

# In _calculate_signal_strength_score(), adjust thresholds:
# OLD scoring was too conservative, NEW scoring:
if abs_diff >= 25:
    return min(100, 90 + (abs_diff - 25) * 0.8)  # 90-100 points (was 30+)
elif abs_diff >= 18:  # LOWERED from 22
    return 80 + (abs_diff - 18) * 2.5  # 80-89 points
elif abs_diff >= 12:  # LOWERED from 15  
    return 65 + (abs_diff - 12) * 2.5  # 65-79 points
elif abs_diff >= 8:   # LOWERED from 10
    return 50 + (abs_diff - 8) * 3.75  # 50-64 points
elif abs_diff >= 5:   # Same
    return 25 + (abs_diff - 5) * 8.33  # 25-49 points
elif abs_diff >= 3:   # LOWERED from 2
    return 10 + (abs_diff - 3) * 7.5   # 10-24 points
else:
    return max(0, abs_diff * 3.33)      # 0-9 points for <3%

print("✅ ConfidenceScorer thresholds lowered and weights adjusted")
