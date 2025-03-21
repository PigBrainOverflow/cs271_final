import sqlite3
import argparse


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--i", type=int, help="index of the server")
    args = parser.parse_args()

    conn = sqlite3.connect(f"server{args.i}.db")
    cur = conn.execute("SELECT * FROM log ORDER BY index_;")
    for row in cur:
        print(row)