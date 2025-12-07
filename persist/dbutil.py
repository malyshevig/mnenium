import logging
import psycopg2
from psycopg2 import pool


host = "localhost"
port = 5432

user = "postgres"
password = "begemot"


class DbUtil:
    def __init__(self, dbname:str):
        self.dbname = dbname

        self. pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=1,  # Минимальное количество соединений
            maxconn=5,   # Максимальное количество соединений
            host = host, port = port, dbname=self.dbname, user = user, password=password
            )

    def __del__(self):
        if self.pool:
            self.pool.closeall()

    def get_connections_info(self):
        pool = self.pool
        return pool.maxconn


    def query_template(self, callback):
        cur = None

        with self.pool.getconn() as conn:
            #logging.info(f"get connection {conn} {th.current_thread()}")
            if not conn:
                raise Exception("Cant get connection")

            try:
                cur = conn.cursor()
                r = callback(conn, cur)

                return r
            finally:
                if cur:
                    cur.close()
                #logging.info(f"put connection {conn} {th.current_thread()}")
                self.pool.putconn(conn)

    def execute_query_update(self, query) -> int:

        logging.debug(f"Execute query update: {query}")
        def call (conn, cur):
            cur.execute(query)
            c = cur.rowcount
            conn.commit()
            return c

        return self.query_template (call)

    def execute_query_select (self, query, limit=1):

        logging.debug(f"Execute query select: {query}")
        def call (_, cur):
            cur.execute(query)
            data = cur.fetchall ()
            return data[:limit] if limit else data

        return self.query_template(call)

    def execute_query_update_and_select(self, query, limit=1):

        logging.debug(f"Execute query select: {query}")

        def call(con, cur):
            cur.execute(query)
            data = cur.fetchall()
            if con:
                con.commit()
            return data[:limit] if limit else data

        return self.query_template(call)

    def execute_query_select_dict (self, query, limit=1):
        logging.debug(f"Execute query select: {query}")
        def call (_, cur):
            cur.execute(query)
            cols = [desc[0] for desc in cur.description]
            data = cur.fetchall ()

            result = [dict(zip(cols, row)) for row in data]
            return result[:limit] if limit else result

        return self.query_template(call)
