# Comprehensive Unmapped Columns Analysis Test

This test automatically analyzes all datasets in the `data` folder and generates detailed reports on unmapped columns in STTM files. It's designed to be scalable and will automatically detect new datasets without manual configuration.

## Features

- **Automatic Dataset Discovery**: Automatically finds all datasets in the data directory
- **Comprehensive Analysis**: Analyzes column mapping coverage across all datasets
- **Priority Categorization**: Categorizes unmapped columns by business importance
- **Detailed Reporting**: Generates comprehensive reports with statistics
- **Export Capabilities**: Supports JSON, CSV, and HTML export formats
- **Quality Assertions**: Includes quality checks and warnings
- **Scalable**: Automatically recognizes new datasets without manual configuration

## Usage

### Running as a Test

```bash
# Run the test using pytest
pytest tests/test_unmapped_columns_analysis.py -v

# Run specific test functions
pytest tests/test_unmapped_columns_analysis.py::test_comprehensive_analysis -v
pytest tests/test_unmapped_columns_analysis.py::test_dataset_discovery -v
```

### Running as a Standalone Script

```bash
# Run the complete analysis
python tests/test_unmapped_columns_analysis.py

# This will run all test functions and display the comprehensive report
```

### Using the Analyzer Class Directly

```python
from tests.test_unmapped_columns_analysis import UnmappedColumnsAnalyzer

# Initialize analyzer
analyzer = UnmappedColumnsAnalyzer(data_dir="data", output_dir="output")

# Run comprehensive analysis
results = analyzer.analyze_all_datasets()

# Generate summary report
summary = analyzer.generate_summary_report()
print(summary)

# Export results
analyzer.export_results('json', 'my_analysis')
analyzer.export_results('csv', 'my_analysis')
analyzer.export_results('html', 'my_analysis')
```

## Test Functions

### 1. `test_dataset_discovery()`
- Tests automatic discovery of datasets in the data directory
- Verifies that discovered items are directories with CSV files

### 2. `test_source_columns_extraction()`
- Tests extraction of source columns from CSV files
- Verifies that columns can be read from multiple datasets

### 3. `test_sttm_file_detection()`
- Tests detection and reading of STTM files
- Verifies that mapped columns can be extracted from STTM JSON files

### 4. `test_column_categorization()`
- Tests categorization of columns by business priority
- Verifies that columns are properly classified as high/medium/low priority

### 5. `test_comprehensive_analysis()`
- Tests the complete analysis workflow
- Generates and displays the comprehensive report
- Verifies minimum coverage expectations

### 6. `test_export_functionality()`
- Tests export capabilities (JSON, CSV, HTML)
- Verifies that exported files are created correctly

### 7. `test_coverage_quality_assertions()`
- Tests quality assertions for coverage
- Provides warnings for high priority unmapped columns

## Column Priority Categories

### High Priority
- Pricing data: `price`, `cost`, `amount`, `value`, `revenue`, `sales`
- Customer data: `customer`, `user`, `email`, `phone`, `premium`, `member`
- Product data: `rating`, `review`, `brand`, `category`, `sku`, `product`
- Temporal data: `date`, `time`, `created`, `updated`, `modified`

### Medium Priority
- Location data: `address`, `city`, `state`, `country`, `zip`, `postal`
- Content data: `description`, `content`, `title`, `name`, `link`, `url`
- Media data: `image`, `img`, `photo`, `picture`
- Business data: `unit`, `quantity`, `discount`, `percentage`, `tax`, `fee`, `charge`

### Low Priority
- Administrative: `unnamed`, `index`, `id_`, `_id`, `order`, `number`
- System data: `sequence`, `counter`, `flag`, `status`, `type`, `code`
- Classification: `class`, `category`, `group`, `level`, `tier`, `rank`

## Output Formats

### JSON Export
```json
{
  "analysis_metadata": {
    "timestamp": "2024-01-01T12:00:00",
    "data_directory": "data",
    "output_directory": "output",
    "total_datasets_analyzed": 7
  },
  "overall_statistics": {
    "total_columns": 144,
    "total_mapped": 106,
    "total_unmapped": 38,
    "overall_coverage_percentage": 73.6,
    "perfect_coverage_datasets": 3,
    "moderate_coverage_datasets": 2,
    "low_coverage_datasets": 2
  },
  "dataset_analyses": {
    "dataset_name": {
      "dataset_name": "ecommerce",
      "total_columns": 31,
      "mapped_columns": ["column1", "column2"],
      "unmapped_columns": ["column3", "column4"],
      "mapped_count": 18,
      "unmapped_count": 13,
      "coverage_percentage": 58.1,
      "categorized_unmapped": {
        "high_priority": ["price", "email"],
        "medium_priority": ["address"],
        "low_priority": ["order_number"],
        "unknown": []
      }
    }
  }
}
```

### CSV Export
Contains a summary table with dataset-level statistics including:
- Dataset name
- Total columns
- Mapped columns count
- Unmapped columns count
- Coverage percentage
- High/medium/low priority unmapped counts

### HTML Export
Generates a styled HTML report with:
- Executive summary
- Overall statistics
- Detailed dataset analysis
- Color-coded coverage indicators
- Unmapped columns by priority

## Adding New Datasets

The test automatically detects new datasets without any configuration changes:

1. **Add Dataset**: Place your dataset folder in the `data` directory
2. **Include CSV Files**: Ensure the dataset folder contains `.csv` files
3. **Generate STTM**: Run the STTM generation for the new dataset
4. **Run Test**: The test will automatically include the new dataset in analysis

## Example Output

```
🚀 Running Comprehensive Unmapped Columns Analysis Test
================================================================================

🧪 Running test_dataset_discovery...
✅ Discovered 7 datasets: Amazon-Sales-Data, Nyc-property-sales-data, Pharma, ecommerce, pharma, property, stock
✅ test_dataset_discovery passed

🧪 Running test_comprehensive_analysis...
🚀 Starting comprehensive unmapped columns analysis...
📁 Data directory: data
📁 Output directory: output

📊 Found 7 datasets: Amazon-Sales-Data, Nyc-property-sales-data, Pharma, ecommerce, pharma, property, stock

🔍 Analyzing dataset: Amazon-Sales-Data
🔍 Analyzing dataset: Nyc-property-sales-data
🔍 Analyzing dataset: Pharma
🔍 Analyzing dataset: ecommerce
🔍 Analyzing dataset: pharma
🔍 Analyzing dataset: property
🔍 Analyzing dataset: stock

================================================================================
                    COMPREHENSIVE UNMAPPED COLUMNS ANALYSIS REPORT
================================================================================

📊 EXECUTIVE SUMMARY
================================================================================
Analysis Timestamp: 2024-01-01T12:00:00
Data Directory: data
Output Directory: output
Total Datasets Analyzed: 7

📈 OVERALL STATISTICS
================================================================================
Total Columns: 144
Total Mapped: 106
Total Unmapped: 38
Overall Coverage: 73.6%

📋 COVERAGE BREAKDOWN
================================================================================
Perfect Coverage (100%): 3 datasets
Moderate Coverage (60-99%): 2 datasets
Low Coverage (<60%): 2 datasets

✅ Comprehensive analysis completed successfully!
📊 Overall coverage: 73.6%
📁 Datasets analyzed: 7
📋 Total columns: 144
✅ test_comprehensive_analysis passed

================================================================================
📊 Test Results: 7 passed, 0 failed
🎉 All tests passed!
```

## Quality Metrics

The test includes several quality assertions:

- **Minimum Coverage**: Overall coverage should be ≥ 50%
- **Perfect Coverage**: At least one dataset should have 100% coverage
- **High Priority Warning**: Warns if >10 high priority columns are unmapped
- **Data Integrity**: Ensures not all columns are unmapped

## Troubleshooting

### Common Issues

1. **No datasets found**: Ensure the `data` directory exists and contains dataset folders with CSV files
2. **No STTM files found**: Run STTM generation first to create output files
3. **Import errors**: Ensure pandas and pytest are installed
4. **Permission errors**: Check file permissions for data and output directories

### Debug Mode

To run with more verbose output:

```bash
pytest tests/test_unmapped_columns_analysis.py -v -s
```

The `-s` flag allows print statements to be displayed during test execution.

## Integration

This test can be integrated into:

- **CI/CD Pipelines**: Run automatically on code changes
- **Quality Gates**: Use coverage metrics as quality thresholds
- **Development Workflow**: Run before committing changes
- **Monitoring**: Regular execution to track coverage trends

## Future Enhancements

Potential improvements:

1. **Custom Priority Rules**: Allow configuration of priority patterns
2. **Industry-Specific Patterns**: Add domain-specific categorization
3. **Trend Analysis**: Track coverage changes over time
4. **Automated Fixes**: Suggest column mappings for unmapped columns
5. **Performance Optimization**: Parallel processing for large datasets 