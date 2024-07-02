import configparser
import psycopg2
from sql_queries import drop_tables, create_tables

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect(
        f"host={config['CLUSTER']['HOST']} dbname={config['CLUSTER']['DB_NAME']} user={config['CLUSTER']['DB_USER']} password={config['CLUSTER']['DB_PASSWORD']} port={config['CLUSTER']['DB_PORT']}"
    )
    cur = conn.cursor()

    for query in drop_tables:
        cur.execute(query)
        conn.commit()

    for query in create_tables:
        cur.execute(query)
        conn.commit()

    conn.close()

if __name__ == "__main__":
    main()
