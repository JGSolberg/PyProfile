import unittest
from dataprofiler import detect_column_type, calculate_descriptive_stats

class TestDataProfiler(unittest.TestCase):
    def test_detect_column_type(self):
        # Test numeric column detection
        numeric_series = pd.Series([1, 2, 3, 4, 5])
        self.assertEqual(detect_column_type(numeric_series), 'number')

        # Test string column detection
        string_series = pd.Series(['apple', 'banana', 'orange'])
        self.assertEqual(detect_column_type(string_series), 'string')

        # Test datetime column detection
        datetime_series = pd.to_datetime(['2023-01-01', '2023-01-02', '2023-01-03'])
        self.assertEqual(detect_column_type(datetime_series), 'datetime')

        # Test unknown column detection
        unknown_series = pd.Series([True, False, None])
        self.assertEqual(detect_column_type(unknown_series), 'unknown')

    def test_calculate_descriptive_stats(self):
        # Test numeric column statistics
        numeric_series = pd.Series([1, 2, 3, 4, 5])
        expected_stats = {'min': 1, 'max': 5}
        self.assertEqual(calculate_descriptive_stats(numeric_series), expected_stats)

        # Test string column statistics
        string_series = pd.Series(['apple', 'banana', 'orange'])
        expected_stats = {'min_length': 5, 'max_length': 6}
        self.assertEqual(calculate_descriptive_stats(string_series), expected_stats)

        # Test datetime column statistics
        datetime_series = pd.to_datetime(['2023-01-01', '2023-01-02', '2023-01-03'])
        expected_stats = {'min_date': pd.Timestamp('2023-01-01 00:00:00'), 'max_date': pd.Timestamp('2023-01-03 00:00:00')}
        self.assertEqual(calculate_descriptive_stats(datetime_series), expected_stats)

if __name__ == "__main__":
    unittest.main()
