import mysql.connector

class Storage:
    def __init__(self, host, user, password, database):
        self.db = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        self.cursor = self.db.cursor()

    def create_table(self):
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS kv_store (
                key_hash VARCHAR(255) PRIMARY KEY,
                key_data TEXT,
                value TEXT,
                version INT DEFAULT 0,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        self.db.commit()

    def insert_or_update(self, key_hash, key_data, value, version=0):
        # Insert new or update existing record
        self.cursor.execute("""
            INSERT INTO kv_store (key_hash, key_data, value, version)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                value = VALUES(value),
                version = version + 1
        """, (key_hash, key_data, value, version))
        self.db.commit()

    def get_value(self, key_hash):
        self.cursor.execute("SELECT value, version FROM kv_store WHERE key_hash = %s", (key_hash,))
        result = self.cursor.fetchone()
        return result if result else (None, None)

    def close(self):
        self.cursor.close()
        self.db.close()
