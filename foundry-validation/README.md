# Foundry validation

Runs the ADBC Driver Foundry type/feature suite against the shared library through the ADBC
driver manager. The [C++ harness](../adbc-validation/README.md) covers ADBC conformance;
[this harness](../scripts/run-foundry-validation.sh) adapts Foundry's query corpus to GoogleSQL.
See the [testing overview](../docs/testing.md) for other suites.

## Run

```sh
scripts/run-foundry-validation.sh
scripts/run-foundry-validation.sh -k ingest  # extra arguments go to pytest
```

Use Python 3.13+ with pip (the [pinned suite's requirement](https://github.com/adbc-drivers/validation/blob/744481372bd09d04125b977c30e2e178832d7ac6/pyproject.toml)),
Rust, git and curl, plus Docker when starting an emulator. `PYTHON` selects the interpreter.
The script builds the driver, installs the pinned suite if needed, creates the emulator
instance/database and runs pytest. The upstream revision is selected by `ADBC_VALIDATION_REF`
in the script; `ADBC_VALIDATION_REPO` can select another repository.

An existing `SPANNER_EMULATOR_HOST` takes precedence over `SPANNER_GCP_DATABASE`. With neither,
the script starts a disposable emulator. A real target must already exist and use ADC; the
current quirks expect database/catalog `adbc-test`, so adjust that expectation when using a
differently named test database.

The [workflow](../.github/workflows/foundry-validation.yml) runs on pushes to `main`, pull requests
and manual dispatch. Cases must pass or skip with a reason; there are no expected failures.

## Adaptations and coverage

- [`tests/spanner.py`](tests/spanner.py) defines capabilities, connection options, identifier
  quoting, `@pN` parameters and DDL overrides. Metadata expects catalog `adbc-test` and default
  schema `""`. Primary/foreign-key constraints and exact aggregate statistics are enabled;
  unique/check constraint cases are disabled in this harness.
- [`queries/spanner/`](queries/spanner/) overlays the upstream bind, select, literal and ingest
  cases with native type names, typed parameters, explicit INSERT column lists and deterministic
  readback ordering. The `.txtcase` files contain the exact expected schemas and skip reasons.
- [`tests/test_ingest.py`](tests/test_ingest.py) enables long-value tests for string, large string,
  string view, binary, large binary and binary view. Other test modules reuse upstream classes
  and fixtures through [`tests/conftest.py`](tests/conftest.py).

Timestamp cases use UTC-aware Arrow timestamps. Select cases `timestamp7tz` through `timestamp9tz`
use nanoseconds; `timestamp4tz` through `timestamp6tz` use the microsecond option for values outside
Arrow's nanosecond range. Bind cases similarly cover nanosecond and microsecond UTC timestamps.
Naive timestamps and second/millisecond readback units are skipped where schemas cannot match.

Some skipped cases reflect roundtrip schema differences rather than unsupported input: narrower
integers read back as `INT64`, fixed-size binary as variable-width binary, and `float16` as
`FLOAT32`. Decimal cases expect schemas other than Spanner's fixed `NUMERIC(38,9)` mapping. The
[skip inventory](skip_baseline.txt) records the current full set, including harness feature skips.

## Skip baseline

CI enables two guards in `tests/conftest.py`:

- `FOUNDRY_VALIDATION_REQUIRE_PASSES=1` rejects an otherwise successful run with no passing tests.
- `FOUNDRY_VALIDATION_CHECK_SKIP_BASELINE=1` compares skipped pytest node IDs with
  `skip_baseline.txt`, rejecting unlisted skips and entries that no longer skip. Trailing reason
  annotations are for readers and are not compared.

The baseline comparison is bypassed for `-k`/`-m` runs. Use a complete run for baseline checks or
updates. An explicit `.txtcase` skip prevents execution, so the guard cannot discover that a
still-skipped case would now pass; review skip reasons when driver capabilities change.

After investigating a legitimate change, regenerate and review the inventory:

```sh
FOUNDRY_UPDATE_SKIP_BASELINE=1 scripts/run-foundry-validation.sh
```
