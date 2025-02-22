import dataclasses
import sqlite3
import json


@dataclasses.dataclass(frozen=True)
class Endpoint:
    ip: str
    port: int


class PersistentStorage:
    """
    NOTE: Log index starts from 1.
    """

    _db_conn: sqlite3.Connection
    CREATE_CURRENT_TERM = """
        CREATE TABLE IF NOT EXISTS current_term (
            id INTEGER PRIMARY KEY CHECK (id = 0),
            value INTEGER NOT NULL
        );
    """
    CREATE_VOTED_FOR = """
        CREATE TABLE IF NOT EXISTS voted_for (
            id INTEGER PRIMARY KEY CHECK (id = 0),
            value INTEGER
        );
    """
    CREATE_LOG = """
        CREATE TABLE IF NOT EXISTS log (
            index INTEGER PRIMARY KEY,
            term INTEGER,
            command JSON,
            committed BOOLEAN DEFAULT FALSE
        );
    """

    def __init__(self, db_name: str):
        self._db_conn = sqlite3.connect(db_name + ".db")


    def create_tables(self):
        self._db_conn.execute(self.CREATE_CURRENT_TERM)
        self._db_conn.execute(self.CREATE_VOTED_FOR)
        self._db_conn.execute(self.CREATE_LOG)
        self._db_conn.commit()


    def init_tables(self):
        self._db_conn.execute("INSERT OR IGNORE INTO current_term (id, value) VALUES (0, 0)")
        self._db_conn.execute("INSERT OR IGNORE INTO voted_for (id, value) VALUES (0, NULL)")
        self._db_conn.commit()


    @property
    def current_term(self) -> int:
        cursor = self._db_conn.execute("SELECT value FROM current_term WHERE id = 0")
        return cursor.fetchone()[0]


    @current_term.setter
    def current_term(self, value: int):
        self._db_conn.execute("UPDATE current_term SET value = ? WHERE id = 0", (value,))
        self._db_conn.commit()


    @property
    def voted_for(self) -> int | None:
        cursor = self._db_conn.execute("SELECT value FROM voted_for WHERE id = 0")
        return cursor.fetchone()[0]


    @voted_for.setter
    def voted_for(self, value: int):
        self._db_conn.execute("UPDATE voted_for SET value = ? WHERE id = 0", (value,))
        self._db_conn.commit()


    def append(self, index: int, term: int, command: dict, committed: bool = False):
        self._db_conn.execute("INSERT INTO log (index, term, command, committed) VALUES (?, ?, ?, ?)", (index, term, json.dumps(command), committed))
        self._db_conn.commit()


    def __getitem__(self, index: int) -> tuple[int, dict, bool] | None:
        cursor = self._db_conn.execute("SELECT term, command, committed FROM log WHERE index = ?", (index,))
        row = cursor.fetchone()
        return None if row is None else (row[0], json.loads(row[1]), row[2])


    def __setitem__(self, index: int, term: int, command: dict, committed: bool = False):
        self._db_conn.execute("UPDATE log SET term = ?, command = ?, committed = ? WHERE index = ?", (term, json.dumps(command), committed, index))
        self._db_conn.commit()


    def __len__(self) -> int:
        cursor = self._db_conn.execute("SELECT COUNT(*) FROM log")
        return cursor.fetchone()[0]


    def remove_back(self, index: int):
        # remove all logs with index >= index
        self._db_conn.execute("DELETE FROM log WHERE index >= ?", (index,))
        self._db_conn.commit()