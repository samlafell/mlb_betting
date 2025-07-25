# Testing Documentation

This directory contains testing-related documentation, sample data, and test results.

## Structure

- **`sample_data/`** - Sample data files for testing and development
  - `raw_data.txt` - Full sample dataset from SBD API
  - `raw_data_sampled.txt` - Smaller sample for quick testing
- **`test_results/`** - Test execution results and reports

## Sample Data

The sample data files contain real betting data structure examples that can be used for:
- Testing data parsing logic
- Developing new collectors
- Debugging data transformation issues
- Performance testing with known datasets

## Usage

Sample data can be used in tests:
```python
# Load sample data for testing
with open('docs/testing/sample_data/raw_data_sampled.txt', 'r') as f:
    sample_data = f.read()
```

## Integration

These files support the testing framework in `tests/` and are referenced by various development utilities.