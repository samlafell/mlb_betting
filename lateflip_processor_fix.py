"""
FIXED LateFlipProcessor with lowered thresholds
"""

# In process() method, change:
# if flip_strength < 12.0:  # OLD THRESHOLD - TOO HIGH
if flip_strength < 8.5:  # NEW THRESHOLD - Based on 80th percentile

# Also change sharp threshold:
# abs(row.get('differential', 0)) >= 8  # OLD SHARP THRESHOLD
abs(row.get('differential', 0)) >= 6  # NEW SHARP THRESHOLD - More inclusive

# Add debug logging:
self.logger.debug(f"Flip strength: {flip_strength:.2f}% (threshold: 8.5%)")

print("✅ LateFlipProcessor threshold lowered from 12.0% to 8.5%")
