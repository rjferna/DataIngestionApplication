import psycopg2
from psycopg2.extras import RealDictCursor, RealDictRow


class PostgresDB:
    def __init__(self, host, database, user, password):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.conn = None
        self.cur = None

    def open_connection(self):
        try:
            self.conn = psycopg2.connect(
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password,
            )
            self.cur = self.conn.cursor(cursor_factory=RealDictCursor)
            print("Connection opened successfully.")
        except (Exception, psycopg2.Error) as error:
            print("Error while connecting to PostgreSQL", error)

    def execute_query(self, query):
        try:
            self.cur.execute(query)
            self.conn.commit()  # Commit transaction if they are insert, update, delete, create statements
            if query.strip().upper().startswith("SELECT"):
                return dict(self.cur.fetchone())
            return "SUCCESS"
        except (Exception, psycopg2.Error) as error:
            print("Error executing query", error)
            self.conn.rollback()  # Rollback in case of error
            return f"Error: {error}"

    def close_connection(self):
        try:
            if self.cur:
                self.cur.close()
            if self.conn:
                self.conn.close()
            print("PostgreSQL connection is closed.")
        except (Exception, psycopg2.Error) as error:
            print("Error while closing the connection", error)


if __name__ == "__main__":
    pass
