"""
FIXED BookConflictProcessor with lowered thresholds
"""

# In process() method, change:
# if conflict_strength < 8.0:  # OLD THRESHOLD - TOO HIGH
if conflict_strength < 5.5:  # NEW THRESHOLD - Based on 75th percentile

# Also add debug logging:
self.logger.debug(f"Conflict strength: {conflict_strength:.2f}% (threshold: 5.5%)")

# In _is_significant_conflict(), change:
# return conflict_analysis.get('weighted_sharp_variance', 0) >= 8.0  # OLD
return conflict_analysis.get('weighted_sharp_variance', 0) >= 5.5  # NEW

print("✅ BookConflictProcessor threshold lowered from 8.0% to 5.5%")
