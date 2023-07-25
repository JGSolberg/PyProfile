import ibm_db
import cx_Oracle
import psycopg2

class MultiDBConnector:
    def __init__(self, db_type, host, port, database, username, password):
        self.db_type = db_type
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password
        self.conn = None
        self.cursor = None

    def connect(self):
        if self.db_type == 'db2':
            conn_string = f"DATABASE={self.database};HOSTNAME={self.host};PORT={self.port};PROTOCOL=TCPIP;UID={self.username};PWD={self.password};"
            self.conn = ibm_db.connect(conn_string, "", "")
        elif self.db_type == 'oracle':
            dsn = cx_Oracle.makedsn(self.host, self.port, service_name=self.database)
            self.conn = cx_Oracle.connect(self.username, self.password, dsn)
        elif self.db_type == 'postgres':
            conn_string = f"host={self.host} port={self.port} dbname={self.database} user={self.username} password={self.password}"
            self.conn = psycopg2.connect(conn_string)
        else:
            raise ValueError("Unsupported database type")

        self.cursor = self.conn.cursor()

    def execute_query(self, query):
        if not self.conn or not self.cursor:
            raise ValueError("Connection not established. Call connect() method first.")
        
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

if __name__ == "__main__":
    # Example usage:
    db_type = 'postgres'  # Change this to 'db2' or 'oracle' for different databases
    host = 'localhost'
    port = '5432'  # Change this to the appropriate port for different databases
    database = 'your_database_name'
    username = 'your_username'
    password = 'your_password'

    connector = MultiDBConnector(db_type, host, port, database, username, password)
    connector.connect()

    query = "SELECT * FROM your_table;"
    result = connector.execute_query(query)
    print(result)

    connector.close()
