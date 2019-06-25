import psycopg2 as pg2
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    conn = pg2.connect(database = '', user = '', password = '')
    conn.set_session(autocommit = True)
    cur = conn.cursor()
    cur.execute("DROP DATABASE IF EXISTS governmentpayroll")
    cur.execute("CREATE DATABASE governmentpayroll WITH ENCODING 'utf8' TEMPLATE template0")
    conn.close()
    conn = pg2.connect(database = 'governmentpayroll', user = '', password = '')
    cur = conn.cursor()

    return cur, conn

def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    cur, conn = create_database()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)
    conn.close()
    
if __name__ == "__main__":
    main()