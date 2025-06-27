"""
FIXED PublicFadeProcessor with lowered thresholds
"""

# In process() method, change:
# if consensus_strength < 65.0:  # OLD THRESHOLD - TOO HIGH
if consensus_strength < 58.0:  # NEW THRESHOLD - More realistic

# Also add debug logging:
self.logger.debug(f"Public consensus: {consensus_strength:.1f}% (threshold: 58.0%)")

# In _is_significant_public_consensus(), adjust criteria:
# OLD: if (avg_money_pct >= 80 or max_money_pct >= 85) and num_books >= 1:
if (avg_money_pct >= 70 or max_money_pct >= 78) and num_books >= 1:  # NEW - More lenient

print("✅ PublicFadeProcessor threshold lowered from 65.0% to 58.0%")
