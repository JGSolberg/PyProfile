import unittest
from Py_Profiler.db_connect import MultiDBConnector

class TestDBConnect(unittest.TestCase):
    def test_db_connection(self):
        # Test if the database connection is successful
        db_connector = MultiDBConnector('postgres', 'localhost', '5432', 'your_database', 'your_username', 'your_password')
        db_connector.connect()
        self.assertIsNotNone(db_connector.conn)
        self.assertIsNotNone(db_connector.cursor)
        db_connector.close()

    def test_incorrect_credentials(self):
        # Test incorrect database credentials
        db_connector = MultiDBConnector('postgres', 'localhost', '5432', 'your_database', 'wrong_username', 'wrong_password')
        with self.assertRaises(ValueError):
            db_connector.connect()

if __name__ == "__main__":
    unittest.main()
