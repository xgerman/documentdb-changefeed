import io
import random
import struct

import psycopg2


def connect():
    connection = psycopg2.connect(dbname="postgres", user="postgres")
    connection.autocommit = True
    return connection


def query(connection, sql, parameters=None):
    with connection.cursor() as cursor:
        cursor.execute(sql, parameters)
        return cursor.fetchall() if cursor.description else None


def bson(payload, revision=0):
    fields = (
        b"\x05payload\x00"
        + struct.pack("<i", len(payload))
        + b"\x00"
        + payload
        + b"\x10revision\x00"
        + struct.pack("<i", revision)
    )
    return struct.pack("<i", len(fields) + 5) + fields + b"\x00"


def boundary(connection):
    query(connection, "SELECT pg_logical_emit_message(false, 'cf_fixture', 'boundary')")
    # A separate, synchronous commit flushes the nontransactional message too.
    query(connection, "INSERT INTO flush_marker DEFAULT VALUES")
    return query(connection, "SELECT pg_current_wal_flush_lsn()")[0][0]


def decode(connection, start, end, memory=128, table="fixture"):
    rows = query(
        connection,
        "SELECT * FROM decode_fixture(%s::regclass, %s::pg_lsn, %s::pg_lsn, %s)",
        (table, start, end, memory),
    )
    return [
        tuple(bytes(value) if isinstance(value, memoryview) else value for value in row)
        for row in rows
    ]


def expect_error(code, function):
    try:
        function()
    except psycopg2.Error as error:
        assert error.pgcode == code, (code, error.pgcode, str(error))
    else:
        raise AssertionError("Expected SQLSTATE " + code)


def verify(rows, expected):
    actual = [(row[4], row[5], row[6]) for row in rows]
    assert len(actual) == len(expected), (len(actual), len(expected))
    for index, (received, wanted) in enumerate(zip(actual, expected)):
        assert received == wanted, (
            "BSON image mismatch at event",
            index,
            received[0],
            wanted[0],
            [len(value) if value is not None else None for value in received[1:]],
            [len(value) if value is not None else None for value in wanted[1:]],
        )
    identities = [(row[0], row[1], row[2]) for row in rows]
    assert len(set(identities)) == len(identities), "Duplicate physical identity"
    assert identities == sorted(
        identities, key=lambda value: (lsn(value[0]), lsn(value[1]), value[2])
    ), "Events not in commit/record/ordinal order"


def lsn(value):
    high, low = value.split("/")
    return (int(high, 16) << 32) + int(low, 16)


def install_decoder(connection):
    query(
        connection,
        """
        CREATE FUNCTION decode_fixture(regclass, pg_lsn, pg_lsn, integer)
        RETURNS TABLE (
            commit_lsn pg_lsn, event_lsn pg_lsn, row_ordinal integer,
            transaction_id bigint, operation text,
            old_document bytea, new_document bytea
        )
        AS '$libdir/pg_documentdb_changefeed', 'documentdb_changefeed_decode_test'
        LANGUAGE C STRICT;
        REVOKE ALL ON FUNCTION decode_fixture(regclass, pg_lsn, pg_lsn, integer)
            FROM PUBLIC;
        """,
    )


def main():
    connection = connect()
    expect_error("42704", lambda: query(connection, "CREATE EXTENSION documentdb_changefeed"))
    query(
        connection,
        """
        CREATE FUNCTION fixture_version() RETURNS text
        AS '$libdir/pg_documentdb_changefeed', 'documentdb_changefeed_version'
        LANGUAGE C STRICT;
        """,
    )
    assert query(connection, "SELECT fixture_version()")[0][0] == "0.1.0-dev"
    install_decoder(connection)
    query(
        connection,
        """
        CREATE TABLE fixture (
            id bigint PRIMARY KEY, document bytea NOT NULL, marker integer DEFAULT 0
        );
        ALTER TABLE fixture REPLICA IDENTITY FULL;
        CREATE TABLE flush_marker (id bigint GENERATED ALWAYS AS IDENTITY);
        SELECT pg_create_physical_replication_slot('cf_fixture', true);
        """,
    )
    start = query(connection, "SELECT pg_current_wal_insert_lsn()")[0][0]
    expected = []
    first = connect()
    second = connect()
    first.autocommit = second.autocommit = False
    documents = [bson(str(number).encode()) for number in range(40)]
    try:
        query(first, "INSERT INTO fixture (id, document) VALUES (1, %s)", (documents[1],))
        query(second, "INSERT INTO fixture (id, document) VALUES (2, %s)", (documents[2],))
        second.commit()
        query(first, "INSERT INTO fixture (id, document) VALUES (3, %s)", (documents[3],))
        first.commit()
        expected.extend(("insert", None, documents[number]) for number in (2, 1, 3))

        query(first, "INSERT INTO fixture (id, document) VALUES (4, %s)", (documents[4],))
        first.rollback()
        query(first, "SAVEPOINT kept")
        query(first, "INSERT INTO fixture (id, document) VALUES (5, %s)", (documents[5],))
        query(first, "RELEASE SAVEPOINT kept")
        query(first, "SAVEPOINT discarded")
        query(first, "INSERT INTO fixture (id, document) VALUES (6, %s)", (documents[6],))
        query(first, "ROLLBACK TO SAVEPOINT discarded")
        query(first, "INSERT INTO fixture (id, document) VALUES (7, %s)", (documents[7],))
        first.commit()
        expected.extend(("insert", None, documents[number]) for number in (5, 7))

        with first.cursor() as cursor:
            data = "".join(
                f"{100 + number},\\x{documents[number].hex()}\n"
                for number in range(10, 40)
            )
            cursor.copy_expert(
                "COPY fixture (id, document) FROM STDIN WITH (FORMAT csv)", io.StringIO(data)
            )
        first.commit()
        expected.extend(("insert", None, documents[number]) for number in range(10, 40))

        for key in (1001, 1002):
            query(first, "INSERT INTO fixture (id, document) VALUES (%s, %s)", (key, documents[1]))
        first.commit()
        expected.extend([("insert", None, documents[1])] * 2)

        generator = random.Random(734)
        compressed = bson(b"x" * 1024 * 1024, 1)
        random_images = [bson(generator.randbytes(180000), revision) for revision in range(2, 5)]
        query(first, "INSERT INTO fixture (id, document) VALUES (2000, %s)", (compressed,))
        first.commit()
        expected.append(("insert", None, compressed))
        physical_size = query(connection, "SELECT pg_column_size(document) FROM fixture WHERE id=2000")[0][0]
        assert 2000 < physical_size < len(compressed) // 10, "Fixture must exercise compressed external TOAST"
        query(first, "UPDATE fixture SET marker=1 WHERE id=2000")
        first.commit()
        expected.append(("update", compressed, compressed))
        previous = compressed
        for image in random_images:
            query(first, "UPDATE fixture SET document=%s WHERE id=2000", (image,))
            expected.append(("update", previous, image))
            previous = image
        query(first, "DELETE FROM fixture WHERE id=2000")
        first.commit()
        expected.append(("delete", previous, None))

        child_image = bson(generator.randbytes(120000), 10)
        query(first, "SAVEPOINT child_toast")
        query(first, "INSERT INTO fixture (id, document) VALUES (3000, %s)", (child_image,))
        query(first, "RELEASE SAVEPOINT child_toast")
        query(first, "UPDATE fixture SET marker=2 WHERE id=3000")
        query(first, "SAVEPOINT aborted_toast")
        query(first, "UPDATE fixture SET document=%s WHERE id=3000", (random_images[0],))
        query(first, "ROLLBACK TO SAVEPOINT aborted_toast")
        first.commit()
        expected.extend([("insert", None, child_image), ("update", child_image, child_image)])

        query(first, "INSERT INTO fixture (id, document) VALUES (9000, %s)", (documents[9],))
        end = boundary(connection)
        rows = decode(connection, start, end)
        verify(rows, expected)
        assert lsn(rows[0][0]) < lsn(rows[1][0]), "Commit order not exercised"
        assert lsn(rows[0][1]) > lsn(rows[1][1]), "WAL/start order must differ from commit order"
        copy_rows = rows[5:35]
        assert len({row[1] for row in copy_rows}) < 30, "COPY did not exercise multi-insert WAL"
        assert any(row[2] > 0 for row in copy_rows), "Missing multi-insert ordinal coverage"
        assert decode(connection, start, end) == rows, "Replaying WAL changed event identities/images"
        first.commit()
        expected.append(("insert", None, documents[9]))
        final_end = boundary(connection)
        verify(decode(connection, start, final_end), expected)
        verify(decode(second, start, final_end), expected)
        second.commit()

        expect_error("54000", lambda: decode(connection, start, final_end, memory=1))
        expect_error("22023", lambda: decode(connection, "0/0", final_end))
        expect_error("22023", lambda: decode(connection, final_end, start))
        expect_error("22023", lambda: decode(connection, start, final_end, memory=0))
        expect_error("22023", lambda: decode(connection, start, final_end, memory=257))
        past_flush = query(connection, "SELECT %s::pg_lsn + 268435456", (final_end,))[0][0]
        expect_error("22023", lambda: decode(connection, final_end, past_flush))
        partial = query(connection, "SELECT %s::pg_lsn - 1", (final_end,))[0][0]
        expect_error("22023", lambda: decode(connection, start, partial))
        expect_error("58P01", lambda: decode(connection, "0/1", final_end))
        child_insert = next(
            row for row in rows if row[4] == "insert" and row[6] == child_image
        )
        # The live TOAST value still exists, but must not fill missing WAL history.
        expect_error("XX001", lambda: decode(connection, child_insert[1], final_end))
        query(connection, "CREATE ROLE cf_reader; GRANT EXECUTE ON FUNCTION decode_fixture(regclass, pg_lsn, pg_lsn, integer) TO cf_reader")
        query(connection, "SET ROLE cf_reader")
        try:
            expect_error("42501", lambda: decode(connection, start, final_end))
        finally:
            query(connection, "RESET ROLE")
        query(connection, "CREATE TABLE bad_fixture (id bigint, document bytea)")
        expect_error("22023", lambda: decode(connection, start, final_end, table="bad_fixture"))
        query(connection, "CREATE UNLOGGED TABLE unlogged_fixture (id bigint, document bytea); ALTER TABLE unlogged_fixture REPLICA IDENTITY FULL")
        expect_error("22023", lambda: decode(connection, start, final_end, table="unlogged_fixture"))
        query(connection, "CREATE TABLE speculative_fixture (LIKE fixture INCLUDING ALL); ALTER TABLE speculative_fixture REPLICA IDENTITY FULL")
        speculative_start = query(connection, "SELECT pg_current_wal_insert_lsn()")[0][0]
        query(connection, "INSERT INTO speculative_fixture (id, document) VALUES (1, %s) ON CONFLICT DO NOTHING", (documents[1],))
        speculative_end = boundary(connection)
        expect_error("0A000", lambda: decode(connection, speculative_start, speculative_end, table="speculative_fixture"))
        query(connection, "TRUNCATE fixture")
        truncated_end = boundary(connection)
        expect_error("0A000", lambda: decode(connection, start, truncated_end))
        query(connection, "SELECT pg_drop_replication_slot('cf_fixture')")
        version = query(connection, "SHOW server_version")[0][0]
        print(f"PASS PostgreSQL {version}: {len(expected)} exact-image events, commit ordering, rollback/savepoints, multi-insert, TOAST, replay, permissions and bounds")
    finally:
        first.close()
        second.close()
        connection.close()


if __name__ == "__main__":
    main()
