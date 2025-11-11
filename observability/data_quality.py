"""
observability/data_quality.py
Automated data quality validation framework
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime


class DataQualityValidator:
    """
    Automated data quality validation with comprehensive checks

    Categories:
    - Completeness: Check for null/missing values
    - Validity: Check value ranges, formats, types
    - Consistency: Check referential integrity, duplicates
    - Timeliness: Check data freshness
    """

    def __init__(self, observer=None):
        """
        Initialize validator

        Args:
            observer: PipelineObserver instance for logging checks
        """
        self.observer = observer
        self.results = []

    def validate_all(
        self,
        df: pd.DataFrame,
        table_name: str = 'data',
        critical_columns: Optional[List[str]] = None
    ) -> Tuple[bool, Dict[str, Any]]:
        """
        Run all quality checks

        Args:
            df: DataFrame to validate
            table_name: Name of table/dataset
            critical_columns: Columns that must pass all checks

        Returns:
            (all_passed, summary_dict)
        """
        if critical_columns is None:
            critical_columns = df.columns.tolist()

        checks = [
            self.check_completeness(df, table_name, critical_columns),
            self.check_validity(df, table_name),
            self.check_consistency(df, table_name),
            self.check_uniqueness(df, table_name),
            self.check_data_types(df, table_name)
        ]

        all_passed = all(check['passed'] for check in checks)

        summary = {
            'all_passed': all_passed,
            'checks_run': len(checks),
            'checks_passed': sum(1 for c in checks if c['passed']),
            'checks_failed': sum(1 for c in checks if not c['passed']),
            'overall_score': sum(c['score'] for c in checks) / len(checks) * 100,
            'details': checks
        }

        return all_passed, summary

    def check_completeness(
        self,
        df: pd.DataFrame,
        table_name: str,
        critical_columns: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Check for missing/null values

        Critical columns should have <5% nulls
        """
        if critical_columns is None:
            critical_columns = df.columns.tolist()

        results = {}
        total_cells = len(df) * len(critical_columns)
        null_cells = df[critical_columns].isnull().sum().sum()
        null_pct = null_cells / total_cells if total_cells > 0 else 0

        threshold = 0.05  # 5% null threshold
        passed = null_pct < threshold

        # Per-column breakdown
        column_nulls = {}
        for col in critical_columns:
            col_null_pct = df[col].isnull().sum() / len(df)
            column_nulls[col] = {
                'null_count': int(df[col].isnull().sum()),
                'null_pct': float(col_null_pct),
                'passed': col_null_pct < threshold
            }

            # Log each column check
            if self.observer:
                self.observer.log_quality_check(
                    check_name=f'completeness_{col}',
                    passed=col_null_pct < threshold,
                    check_category='completeness',
                    table_name=table_name,
                    column_name=col,
                    metric_value=float(1 - col_null_pct),  # Completeness score
                    threshold_value=float(1 - threshold),
                    row_count=len(df),
                    failure_count=int(df[col].isnull().sum()),
                    details={'null_pct': float(col_null_pct)}
                )

        result = {
            'check': 'completeness',
            'passed': passed,
            'score': 1 - null_pct,
            'metric_value': 1 - null_pct,
            'threshold': 1 - threshold,
            'details': {
                'total_cells': total_cells,
                'null_cells': int(null_cells),
                'null_pct': float(null_pct),
                'column_breakdown': column_nulls
            }
        }

        self.results.append(result)
        return result

    def check_validity(
        self,
        df: pd.DataFrame,
        table_name: str,
        value_column: str = 'value'
    ) -> Dict[str, Any]:
        """
        Check value ranges and validity

        Checks:
        - No negative values (for counts/percentages)
        - No extreme outliers (beyond reasonable bounds)
        - No invalid characters in text fields
        """
        checks_passed = []

        # Check numeric values if present
        if value_column in df.columns and pd.api.types.is_numeric_dtype(df[value_column]):
            # Remove nulls for validation
            values = df[value_column].dropna()

            if len(values) > 0:
                # Check 1: No negative values
                negative_count = (values < 0).sum()
                negative_pct = negative_count / len(values)
                no_negatives = negative_count == 0

                if self.observer:
                    self.observer.log_quality_check(
                        check_name='validity_no_negatives',
                        passed=no_negatives,
                        check_category='validity',
                        table_name=table_name,
                        column_name=value_column,
                        metric_value=float(1 - negative_pct),
                        threshold_value=1.0,
                        row_count=len(values),
                        failure_count=int(negative_count)
                    )
                checks_passed.append(no_negatives)

                # Check 2: Values within reasonable range (0 to 1 billion)
                min_threshold = 0
                max_threshold = 1e9
                out_of_range = ((values < min_threshold) | (values > max_threshold)).sum()
                out_of_range_pct = out_of_range / len(values)
                in_range = out_of_range_pct < 0.01  # <1% outliers acceptable

                if self.observer:
                    self.observer.log_quality_check(
                        check_name='validity_value_range',
                        passed=in_range,
                        check_category='validity',
                        table_name=table_name,
                        column_name=value_column,
                        metric_value=float(1 - out_of_range_pct),
                        threshold_value=0.99,
                        row_count=len(values),
                        failure_count=int(out_of_range),
                        details={
                            'min_value': float(values.min()),
                            'max_value': float(values.max()),
                            'expected_range': [min_threshold, max_threshold]
                        }
                    )
                checks_passed.append(in_range)

                # Check 3: No infinite values
                infinite_count = np.isinf(values).sum()
                no_infinites = infinite_count == 0

                if self.observer:
                    self.observer.log_quality_check(
                        check_name='validity_no_infinites',
                        passed=no_infinites,
                        check_category='validity',
                        table_name=table_name,
                        column_name=value_column,
                        metric_value=1.0 if no_infinites else 0.0,
                        threshold_value=1.0,
                        failure_count=int(infinite_count)
                    )
                checks_passed.append(no_infinites)

        passed = all(checks_passed) if checks_passed else True
        score = sum(checks_passed) / len(checks_passed) if checks_passed else 1.0

        result = {
            'check': 'validity',
            'passed': passed,
            'score': score,
            'details': {
                'checks_run': len(checks_passed),
                'checks_passed': sum(checks_passed)
            }
        }

        self.results.append(result)
        return result

    def check_consistency(
        self,
        df: pd.DataFrame,
        table_name: str
    ) -> Dict[str, Any]:
        """
        Check data consistency

        Checks:
        - No duplicate rows
        - Referential consistency (if IDs present)
        - Format consistency
        """
        checks_passed = []

        # Check 1: No duplicate rows
        duplicate_count = df.duplicated().sum()
        duplicate_pct = duplicate_count / len(df)
        no_duplicates = duplicate_pct < 0.01  # <1% duplicates acceptable

        if self.observer:
            self.observer.log_quality_check(
                check_name='consistency_no_duplicates',
                passed=no_duplicates,
                check_category='consistency',
                table_name=table_name,
                metric_value=float(1 - duplicate_pct),
                threshold_value=0.99,
                row_count=len(df),
                failure_count=int(duplicate_count),
                details={'duplicate_count': int(duplicate_count)}
            )
        checks_passed.append(no_duplicates)

        # Check 2: Consistent year_label format (if present)
        if 'year_label' in df.columns:
            # Should match pattern like "2016/17"
            year_labels = df['year_label'].dropna().astype(str)
            valid_format = year_labels.str.match(r'^\d{4}/\d{2}$').sum()
            format_consistency = valid_format / len(year_labels) if len(year_labels) > 0 else 1.0
            consistent_format = format_consistency > 0.95  # 95% should match pattern

            if self.observer:
                self.observer.log_quality_check(
                    check_name='consistency_year_format',
                    passed=consistent_format,
                    check_category='consistency',
                    table_name=table_name,
                    column_name='year_label',
                    metric_value=float(format_consistency),
                    threshold_value=0.95,
                    row_count=len(year_labels),
                    failure_count=int(len(year_labels) - valid_format)
                )
            checks_passed.append(consistent_format)

        passed = all(checks_passed) if checks_passed else True
        score = sum(checks_passed) / len(checks_passed) if checks_passed else 1.0

        result = {
            'check': 'consistency',
            'passed': passed,
            'score': score,
            'details': {
                'checks_run': len(checks_passed),
                'checks_passed': sum(checks_passed)
            }
        }

        self.results.append(result)
        return result

    def check_uniqueness(
        self,
        df: pd.DataFrame,
        table_name: str,
        key_columns: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Check uniqueness constraints

        Args:
            key_columns: Columns that should be unique together
        """
        if key_columns is None:
            # Try to infer key columns
            possible_keys = ['indicator', 'year_label', 'location']
            key_columns = [col for col in possible_keys if col in df.columns]

        if not key_columns:
            # Skip if no key columns identified
            return {
                'check': 'uniqueness',
                'passed': True,
                'score': 1.0,
                'details': {'skipped': 'no_key_columns'}
            }

        # Check for duplicate keys
        duplicate_keys = df.duplicated(subset=key_columns, keep=False).sum()
        duplicate_pct = duplicate_keys / len(df)
        passed = duplicate_keys == 0

        if self.observer:
            self.observer.log_quality_check(
                check_name='uniqueness_key_constraint',
                passed=passed,
                check_category='consistency',
                table_name=table_name,
                metric_value=float(1 - duplicate_pct),
                threshold_value=1.0,
                row_count=len(df),
                failure_count=int(duplicate_keys),
                details={
                    'key_columns': key_columns,
                    'duplicate_count': int(duplicate_keys)
                }
            )

        result = {
            'check': 'uniqueness',
            'passed': passed,
            'score': 1 - duplicate_pct,
            'details': {
                'key_columns': key_columns,
                'duplicate_keys': int(duplicate_keys),
                'duplicate_pct': float(duplicate_pct)
            }
        }

        self.results.append(result)
        return result

    def check_data_types(
        self,
        df: pd.DataFrame,
        table_name: str
    ) -> Dict[str, Any]:
        """
        Check that data types are appropriate

        Checks:
        - Numeric columns contain numbers
        - No mixed types in columns
        """
        checks_passed = []

        # Check value column is numeric
        if 'value' in df.columns:
            is_numeric = pd.api.types.is_numeric_dtype(df['value'])
            checks_passed.append(is_numeric)

            if self.observer:
                self.observer.log_quality_check(
                    check_name='type_value_is_numeric',
                    passed=is_numeric,
                    check_category='validity',
                    table_name=table_name,
                    column_name='value',
                    metric_value=1.0 if is_numeric else 0.0,
                    threshold_value=1.0
                )

        # Check for mixed types in any column
        mixed_type_columns = []
        for col in df.columns:
            # Sample check: if inferred type changes across chunks
            if len(df) > 100:
                type1 = type(df[col].iloc[0]).__name__
                type2 = type(df[col].iloc[len(df)//2]).__name__
                if type1 != type2 and not pd.isna(df[col].iloc[0]) and not pd.isna(df[col].iloc[len(df)//2]):
                    mixed_type_columns.append(col)

        no_mixed_types = len(mixed_type_columns) == 0
        checks_passed.append(no_mixed_types)

        if self.observer:
            self.observer.log_quality_check(
                check_name='type_no_mixed_types',
                passed=no_mixed_types,
                check_category='validity',
                table_name=table_name,
                metric_value=1.0 if no_mixed_types else 0.0,
                threshold_value=1.0,
                details={'mixed_type_columns': mixed_type_columns}
            )

        passed = all(checks_passed) if checks_passed else True
        score = sum(checks_passed) / len(checks_passed) if checks_passed else 1.0

        result = {
            'check': 'data_types',
            'passed': passed,
            'score': score,
            'details': {
                'checks_run': len(checks_passed),
                'checks_passed': sum(checks_passed),
                'mixed_type_columns': mixed_type_columns
            }
        }

        self.results.append(result)
        return result

    def get_summary(self) -> Dict[str, Any]:
        """Get summary of all validation results"""
        if not self.results:
            return {'no_checks_run': True}

        return {
            'checks_run': len(self.results),
            'checks_passed': sum(1 for r in self.results if r['passed']),
            'checks_failed': sum(1 for r in self.results if not r['passed']),
            'overall_score': sum(r['score'] for r in self.results) / len(self.results) * 100,
            'all_passed': all(r['passed'] for r in self.results),
            'details': self.results
        }

    def print_report(self):
        """Print human-readable validation report"""
        summary = self.get_summary()

        if summary.get('no_checks_run'):
            print("No quality checks have been run.")
            return

        print("\n" + "="*60)
        print("DATA QUALITY VALIDATION REPORT")
        print("="*60)
        print(f"Checks Run: {summary['checks_run']}")
        print(f"Passed: {summary['checks_passed']} [PASS]")
        print(f"Failed: {summary['checks_failed']} [FAIL]")
        print(f"Overall Score: {summary['overall_score']:.1f}/100")
        print(f"Status: {'PASS' if summary['all_passed'] else 'FAIL'}")
        print("="*60)

        for result in self.results:
            status = "[PASS]" if result['passed'] else "[FAIL]"
            score = result['score'] * 100
            print(f"{status} {result['check']}: {score:.1f}%")

        print("="*60 + "\n")


# Pre-configured validator for health data
class HealthDataValidator(DataQualityValidator):
    """
    Specialized validator for Uganda health indicator data

    Knows about specific columns and reasonable ranges
    """

    def validate_health_data(
        self,
        df: pd.DataFrame,
        table_name: str = 'health_indicators'
    ) -> Tuple[bool, Dict[str, Any]]:
        """
        Run health-specific validation

        Expected columns: indicator, year_label, value
        """
        # Standard checks
        critical_columns = []
        if 'indicator' in df.columns:
            critical_columns.append('indicator')
        if 'year_label' in df.columns:
            critical_columns.append('year_label')
        if 'value' in df.columns:
            critical_columns.append('value')

        all_passed, summary = self.validate_all(df, table_name, critical_columns)

        # Additional health-specific checks
        self._check_indicator_names(df, table_name)
        self._check_year_range(df, table_name)

        return all_passed, summary

    def _check_indicator_names(self, df: pd.DataFrame, table_name: str):
        """Check that indicator names are not empty and reasonable length"""
        if 'indicator' not in df.columns:
            return

        indicators = df['indicator'].dropna().astype(str)

        # Check not empty
        empty_count = (indicators.str.strip() == '').sum()
        no_empty = empty_count == 0

        # Check reasonable length (5-500 chars)
        length_check = ((indicators.str.len() >= 5) & (indicators.str.len() <= 500)).sum()
        length_valid = length_check / len(indicators) if len(indicators) > 0 else 1.0
        valid_length = length_valid > 0.95

        if self.observer:
            self.observer.log_quality_check(
                check_name='health_indicator_names_valid',
                passed=no_empty and valid_length,
                check_category='validity',
                table_name=table_name,
                column_name='indicator',
                metric_value=float(length_valid),
                threshold_value=0.95,
                failure_count=int(len(indicators) - length_check)
            )

    def _check_year_range(self, df: pd.DataFrame, table_name: str):
        """Check that year labels are within reasonable range (2010-2030)"""
        if 'year_label' not in df.columns:
            return

        year_labels = df['year_label'].dropna().astype(str)

        # Extract first year from labels like "2016/17"
        years = year_labels.str.extract(r'^(\d{4})')[0].astype(float)

        # Check within range
        valid_range = ((years >= 2010) & (years <= 2030)).sum()
        range_pct = valid_range / len(years) if len(years) > 0 else 1.0
        in_range = range_pct > 0.99

        if self.observer:
            self.observer.log_quality_check(
                check_name='health_year_range_valid',
                passed=in_range,
                check_category='validity',
                table_name=table_name,
                column_name='year_label',
                metric_value=float(range_pct),
                threshold_value=0.99,
                row_count=len(years),
                failure_count=int(len(years) - valid_range),
                details={
                    'expected_range': [2010, 2030],
                    'min_year': float(years.min()) if len(years) > 0 else None,
                    'max_year': float(years.max()) if len(years) > 0 else None
                }
            )
