import os
import sqlite3

from .constants import CHORD_URL, CHORD_REGISTRY_URL, DB_PATH


db_exists = os.path.exists(DB_PATH)
peer_db = sqlite3.connect(DB_PATH, detect_types=sqlite3.PARSE_DECLTYPES)
peer_db.row_factory = sqlite3.Row


def check_peer_exists(c, url) -> bool:
    c.execute("SELECT 1 FROM peers WHERE url = ?", (url,))
    return c.fetchone() is not None


def insert_or_ignore_peer(c, n):
    c.execute("INSERT OR IGNORE INTO peers VALUES (?)", (n,))


def insert_or_ignore_fixed_nodes(c):
    insert_or_ignore_peer(c, CHORD_URL)
    insert_or_ignore_peer(c, CHORD_REGISTRY_URL)

    peer_db.commit()


def init_db():
    with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "schema.sql"), "r") as sf:
        peer_db.executescript(sf.read())

    insert_or_ignore_fixed_nodes(peer_db.cursor())


def update_db():
    c = peer_db.cursor()

    c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='peers'")
    if c.fetchone() is None:
        init_db()
        return

    insert_or_ignore_fixed_nodes(c)

    # TODO


# noinspection SqlWithoutWhere
def clear_db_and_insert_fixed_nodes():
    # TODO: Maybe this should be called at startup? Unclear
    c = peer_db.cursor()
    c.execute("DELETE FROM peers")
    insert_or_ignore_fixed_nodes(c)


if not db_exists:
    init_db()
else:
    update_db()
