"""Differential (oracle) tests for the Spanner -> Arrow type mapping.

The highest-signal way to catch bugs in ``src/conversion.rs`` is to run the same
query through two independent clients against the *same* emulator database and
assert the results agree after normalization:

* our ADBC driver (``adbc_driver_spanner``), which returns Apache Arrow, and
* the official ``google-cloud-spanner`` Python client, which returns native
  Python objects and acts as the oracle for "what the value really is".

We seed a table with a curated set of type-mapping-sensitive values (i64 min/max,
float NaN/+-Inf/-0.0, unicode/empty strings, empty/non-empty bytes, epoch/far
dates, sub-second timestamps, high-precision/negative NUMERIC, assorted JSON,
empty/populated/NULL arrays, and an all-NULL row), read it back through both
clients, and compare column by column.

Both clients honor ``SPANNER_EMULATOR_HOST`` (the official client picks up the
emulator + anonymous credentials automatically), so no real GCP credentials are
needed. The whole module self-skips without the emulator, and ``importorskip``
keeps a bare ``pytest`` green when ``pyarrow`` / ``google-cloud-spanner`` are
absent.
"""

import base64
import datetime
import json
import math

import pytest

pa = pytest.importorskip("pyarrow")
pc = pytest.importorskip("pyarrow.compute")
spanner = pytest.importorskip("google.cloud.spanner")

import adbc_driver_spanner.dbapi as adbc_spanner

from conftest import DATABASE, INSTANCE, PROJECT

TABLE = "AdbcOracle"

# Column name + logical kind, in the order the SELECT returns them. The kind
# drives normalization/comparison below.
COLUMNS = [
    ("Id", "int"),
    ("IntCol", "int"),
    ("FloatCol", "float"),
    ("BoolCol", "bool"),
    ("StrCol", "str"),
    ("BytesCol", "bytes"),
    ("DateCol", "date"),
    ("TsCol", "ts"),
    ("NumCol", "num"),
    ("JsonCol", "json"),
    ("IntArr", "array_int"),
    ("StrArr", "array_str"),
]

# One INSERT per row so the type-sensitive literals stay readable. Min int64 is
# written as ``-9223372036854775807 - 1`` because ``9223372036854775808`` on its
# own overflows an INT64 literal (the unary minus is applied afterwards).
SEED_ROWS = [
    # Typical values.
    "(1, 42, 3.14, TRUE, 'hello', b'\\x00\\x01\\xff', DATE '2024-01-15', "
    "TIMESTAMP '2024-01-15T12:34:56.789012345Z', NUMERIC '123.456', "
    'JSON \'{"a":1,"b":[true,null],"c":"x"}\', [1, 2, 3], [\'a\', \'b\'])',
    # Maxima / +Inf / empty string+bytes / epoch date / min date / max NUMERIC /
    # empty arrays.
    "(2, 9223372036854775807, CAST('inf' AS FLOAT64), FALSE, '', b'', "
    "DATE '0001-01-01', TIMESTAMP '1970-01-01T00:00:00Z', "
    "NUMERIC '99999999999999999999999999999.999999999', JSON '[]', "
    "ARRAY<INT64>[], ARRAY<STRING>[])",
    # Minima / -Inf / unicode / max date / far in-range timestamp / tiny negative
    # NUMERIC.
    "(3, -9223372036854775807 - 1, CAST('-inf' AS FLOAT64), TRUE, "
    "'\\u65e5\\u672c\\u8a9e\\U0001f389', b'\\xde\\xad\\xbe\\xef', "
    "DATE '9999-12-31', TIMESTAMP '2200-01-01T00:00:00.123456789Z', "
    "NUMERIC '-0.000000001', JSON '{\"nested\":{\"x\":[1,2,3]}}', "
    "[100, -100], ['caf\\u00e9', ''])",
    # NaN.
    "(4, 0, CAST('NaN' AS FLOAT64), FALSE, 'x', b'x', DATE '1970-01-01', "
    "TIMESTAMP '1970-01-01T00:00:00Z', NUMERIC '0', "
    'JSON \'{"pi":3.14,"neg":-1,"big":12345678901234}\', [1], [\'x\'])',
    # Negative zero.
    "(5, 1, CAST('-0.0' AS FLOAT64), TRUE, 'y', b'\\x01', DATE '2000-02-29', "
    "TIMESTAMP '1999-12-31T23:59:59.999999999Z', NUMERIC '1000000000000000000', "
    'JSON \'{"unicode":"\\u65e5\\u672c\\u8a9e"}\', [0], [\'0\'])',
    # Every nullable column NULL.
    "(6, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)",
]


def _create_and_seed(cur):
    cur.execute(f"DROP TABLE IF EXISTS {TABLE}")
    cur.execute(
        f"CREATE TABLE {TABLE} ("
        "  Id INT64 NOT NULL,"
        "  IntCol INT64,"
        "  FloatCol FLOAT64,"
        "  BoolCol BOOL,"
        "  StrCol STRING(MAX),"
        "  BytesCol BYTES(MAX),"
        "  DateCol DATE,"
        "  TsCol TIMESTAMP,"
        "  NumCol NUMERIC,"
        "  JsonCol JSON,"
        "  IntArr ARRAY<INT64>,"
        "  StrArr ARRAY<STRING(MAX)>,"
        ") PRIMARY KEY (Id)"
    )
    cols = ", ".join(name for name, _ in COLUMNS)
    for values in SEED_ROWS:
        cur.execute(f"INSERT INTO {TABLE} ({cols}) VALUES {values}")


# --- normalization helpers ------------------------------------------------


def _to_jsonable(value):
    """Parse a JSON cell into plain Python for structural comparison.

    Our driver returns JSON as its raw text (a ``str``). The official client
    returns ``JsonObject`` (a ``dict`` subclass) whose ``str()`` is unreliable
    (Python ``repr`` for objects, and a ``dict`` that misleadingly reprs as
    ``[]`` for JSON arrays), so canonicalize it through ``.serialize()`` when
    available.
    """
    if value is None:
        return None
    if isinstance(value, str):
        return json.loads(value)
    serialize = getattr(value, "serialize", None)
    if callable(serialize):
        return json.loads(serialize())
    if isinstance(value, (dict, list)):
        return value
    return json.loads(str(value))


def _official_ts_to_nanos(dt):
    """Total nanoseconds since the Unix epoch for an official-client timestamp.

    The client returns ``DatetimeWithNanoseconds`` (a ``datetime`` subclass with
    a full ``nanosecond`` field); Python ``datetime`` itself only reaches
    microseconds, so we rebuild the whole-second instant from calendar fields and
    add the sub-second nanoseconds explicitly.
    """
    plain = datetime.datetime(
        dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second,
        tzinfo=datetime.timezone.utc,
    )
    secs = int(plain.timestamp())
    nanos = getattr(dt, "nanosecond", None)
    if nanos is None:
        nanos = dt.microsecond * 1000
    return secs * 1_000_000_000 + nanos


def _normalize_official(kind, value):
    if value is None:
        return None
    if kind == "ts":
        return _official_ts_to_nanos(value)
    if kind == "json":
        return _to_jsonable(value)
    if kind == "bytes":
        # The official client hands back BYTES still base64-encoded (as bytes);
        # decode so it lines up with the raw bytes our driver already decodes to.
        return base64.b64decode(value)
    return value


def _normalize_adbc(kind, value):
    if value is None:
        return None
    if kind == "json":
        return _to_jsonable(value)
    return value


def _floats_equal(a, b):
    if math.isnan(a) or math.isnan(b):
        return math.isnan(a) and math.isnan(b)
    # Distinguish +0.0 from -0.0 (== treats them equal) so a sign-dropping bug
    # would surface here.
    if a == 0.0 and b == 0.0:
        return math.copysign(1.0, a) == math.copysign(1.0, b)
    return a == b


def _values_equal(kind, a, b):
    if a is None or b is None:
        return a is None and b is None
    if kind == "float":
        return _floats_equal(a, b)
    if kind == "array_int":
        return len(a) == len(b) and all(_values_equal("int", x, y) for x, y in zip(a, b))
    if kind == "array_str":
        return len(a) == len(b) and all(_values_equal("str", x, y) for x, y in zip(a, b))
    return a == b


# --- the fixture(s) -------------------------------------------------------


@pytest.fixture(scope="module")
def official_database(emulator_database):
    """The official-client ``Database`` handle for the same emulator database."""
    client = spanner.Client(project=PROJECT)
    instance = client.instance(INSTANCE)
    return instance.database(DATABASE)


def _official_query(database, sql):
    with database.snapshot() as snapshot:
        return [list(row) for row in snapshot.execute_sql(sql)]


# --- tests ----------------------------------------------------------------


def test_scalar_and_array_types_match_oracle(emulator_database, official_database):
    conn = adbc_spanner.connect(uri=f"spanner:///{emulator_database}", emulator=True, autocommit=True)
    try:
        with conn.cursor() as cur:
            _create_and_seed(cur)

        cols = ", ".join(name for name, _ in COLUMNS)
        sql = f"SELECT {cols} FROM {TABLE} ORDER BY Id"

        with conn.cursor() as cur:
            cur.execute(sql)
            table = cur.fetch_arrow_table()
    finally:
        conn.close()

    official = _official_query(official_database, sql)

    assert table.num_rows == len(official) == len(SEED_ROWS), (
        f"row count mismatch: adbc={table.num_rows} official={len(official)}"
    )

    # Materialize each ADBC column as a normalized Python list. Timestamps are
    # read as raw int64 epoch-nanoseconds (pyarrow .to_pylist() would truncate the
    # nanosecond tail to microseconds), matching the official-side normalization.
    adbc_cols = {}
    for name, kind in COLUMNS:
        if kind == "ts":
            adbc_cols[name] = pc.cast(table.column(name), pa.int64()).to_pylist()
        else:
            adbc_cols[name] = [_normalize_adbc(kind, v) for v in table.column(name).to_pylist()]

    mismatches = []
    for row_idx in range(len(official)):
        for col_idx, (name, kind) in enumerate(COLUMNS):
            got = adbc_cols[name][row_idx]
            want = _normalize_official(kind, official[row_idx][col_idx])
            if not _values_equal(kind, got, want):
                mismatches.append(
                    f"column {name!r} ({kind}) row {row_idx}: "
                    f"adbc={got!r} official={want!r}"
                )

    assert not mismatches, "differential mismatches:\n" + "\n".join(mismatches)


def test_array_of_struct_matches_oracle(emulator_database, official_database):
    # Spanner only returns a STRUCT wrapped in an ARRAY (a top-level STRUCT is
    # UNIMPLEMENTED on real Spanner), so probe ARRAY<STRUCT<a,b,c>> via
    # ARRAY(SELECT AS STRUCT ...). Our driver maps it to List<Struct>; the
    # official client returns a list of structs, each a positional list.
    sql = (
        "SELECT ARRAY("
        "  SELECT AS STRUCT a, b, c FROM ("
        "    SELECT 0 AS n, 1 AS a, 'x' AS b, CAST(NULL AS INT64) AS c"
        "    UNION ALL SELECT 1 AS n, 2 AS a, 'y' AS b, 7 AS c"
        "  ) ORDER BY n"
        ") AS arr"
    )

    conn = adbc_spanner.connect(uri=f"spanner:///{emulator_database}", emulator=True, autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            table = cur.fetch_arrow_table()
    finally:
        conn.close()

    adbc_arr = table.column("arr").to_pylist()[0]  # list of dicts
    # Field order from the Arrow schema, so we can flatten each struct to a list.
    struct_type = table.schema.field("arr").type.value_type
    field_names = [struct_type.field(i).name for i in range(struct_type.num_fields)]
    adbc_structs = [[s[fn] for fn in field_names] for s in adbc_arr]

    official = _official_query(official_database, sql)[0][0]  # list of structs (lists)

    assert len(adbc_structs) == len(official), (
        f"struct-array length mismatch: adbc={len(adbc_structs)} official={len(official)}"
    )
    for i, (got_struct, want_struct) in enumerate(zip(adbc_structs, official)):
        assert list(got_struct) == list(want_struct), (
            f"struct element {i}: adbc={got_struct!r} official={want_struct!r}"
        )
