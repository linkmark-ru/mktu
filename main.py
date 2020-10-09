import psycopg2
import json
from config import keys
import hashlib
from datetime import datetime


def main():
    def _hash(kind, doc_num):
        return hashlib.md5(kind.encode('utf-8') + doc_num.encode('utf-8')).hexdigest()

    try:
        connect_db = psycopg2.connect(
            dbname=f"{keys['dbname']}",
            user=f"{keys['user']}",
            password=f"{keys['password']}",
            host=f"{keys['host']}",
            port=f"{keys['port']}"
        )

        connect_db.autocommit = True

        db_cursor = connect_db.cursor()

        db_cursor.execute("""CREATE TABLE IF NOT EXISTS icgs
                                (id SERIAL,
                                 icgs_item VARCHAR,
                                 icgs_code INTEGER,
                                 kind VARCHAR(4),
                                 doc_num VARCHAR(15))""")

        db_cursor.execute(f"SELECT * FROM {keys['table_name']};")

        for index, value in enumerate(db_cursor.fetchall()):
            if 'kind' and 'doc_num' in value:
                continue

            data = json.loads(value[2])

            for item in data:
                SQL = """INSERT INTO icgs (icgs_item, icgs_code, kind, doc_num) VALUES(%s, %s, %s, %s);"""

                try:
                    icgs_items = item['ru'].split(';')

                    for icgs_item in icgs_items:
                        data = (icgs_item, item['code'], value[0], value[1])
                        db_cursor.execute(SQL, data)

                except Exception as error:
                    with open('errors.log', encoding='utf-8') as file:

                        hash_name = _hash(value[0], value[1])

                        file_read = file.read()

                        if file_read.find(hash_name) == -1:
                            with open('errors.log', 'a', encoding='utf-8') as file:
                                log = f"{datetime.now()}, kind:{value[0]}, doc_num:{value[1]}, ru:{item['ru']}, code:{item['code']}, error:{error}, hash: {hash_name}\n"
                                file.write(log)

        print("Records done successfully")
    finally:
        connect_db.close()


if __name__ == "__main__":
    main()
