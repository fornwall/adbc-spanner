# ADBC C++ validation

Runs Apache Arrow ADBC's driver-independent C++ validation suite against the built shared
library through the ADBC driver manager. This checks the driver's C ABI alongside the
[Rust and Python tests](../docs/testing.md).

## Run

```sh
scripts/run-adbc-validation.sh                 # emulator: normal CI checks
scripts/run-adbc-validation.sh --full          # every case, including known failures
scripts/run-adbc-validation.sh --check-drift   # build and check exclusions; no database

SPANNER_EMULATOR_HOST=localhost:9010 scripts/run-adbc-validation.sh
SPANNER_GCP_DATABASE=my-project.my-instance.my-test-db scripts/run-adbc-validation.sh
```

Requirements: Rust, Bash 4+, a C compiler, a C++20 compiler, CMake 3.20+, git, Python 3 and curl;
Docker when starting an emulator. The script builds the driver and harness and prepares the emulator
database. A real target uses ADC and must already exist; use a dedicated test database.
`SPANNER_EMULATOR_HOST` takes precedence over `SPANNER_GCP_DATABASE`.

[CMakeLists.txt](CMakeLists.txt) pins arrow-adbc with `ARROW_ADBC_TAG` and fetches/builds the C++
dependencies. Keep that revision aligned with the Rust arrow-adbc dependencies in `Cargo.toml`.
`ADBC_VALIDATION_BUILD_DIR` overrides the build directory.

`--full` uses CTest to run cases in separate processes. It prints failures but deliberately does
not propagate CTest's failure exit status; use the default mode for CI.

## Coverage and exclusions

[`SpannerQuirks`](spanner_validation.cc) declares capabilities, readback type conversions,
identifier/parameter syntax and GoogleSQL rewrites. Rewrites assert the expected upstream SQL
so changed queries require review when the pin advances.

The suite covers database/connection lifetimes, metadata, transaction options, queries, prepared
parameters, row counts, partitioning, cancellation, error/stream lifetimes and all four ingest
modes. It includes native, widened, large/view, dictionary and list Arrow representations where
supported. Exact cases come from the pinned suite, rather than a separately maintained count.

[`run-adbc-validation.sh`](../scripts/run-adbc-validation.sh) has one `EXCLUDED` list:

| Excluded case | Current limitation |
| --- | --- |
| `SpannerStatementTest.SqlIngestUInt64` | The driver cannot create a column for Arrow `UInt64`; its full range cannot fit `INT64`. |
| `SpannerStatementTest.SqlIngestDuration` | No ingest column mapping. |
| `SpannerStatementTest.SqlIngestInterval` | No ingest column mapping. |

The default run enforces three checks:

1. Every non-excluded case passes or self-skips, including cases added upstream.
2. Every excluded case still fails or skips; an unexpected pass requires removing the exclusion.
3. Every exclusion still names an available test; renamed or removed cases fail the drift check.

Quirk-controlled skips remain in the normal run: `Transactions` requires behavior incompatible
with immediate DDL and buffered manual writes; `SqlIngestPrimaryKey` expects ordered generated
keys; the `SqlIngestTemporary*` cases require temporary tables. These skips are not covered by the
unexpected-pass guard and should be reviewed when capabilities change.

## Sanitizers

The [CI workflow](../.github/workflows/adbc-validation.yml) runs the same checks in three modes:

| Mode | Instrumentation |
| --- | --- |
| `plain` | Normal Rust and C++ builds |
| `asan-ubsan` | C/C++ address and undefined-behavior sanitizers; Rust stays uninstrumented |
| `rust-asan` | Rust and its standard library built with nightly ASan; C++ built with clang ASan |

```sh
ADBC_VALIDATION_SANITIZE=address,undefined scripts/run-adbc-validation.sh

ADBC_VALIDATION_SANITIZE=address ADBC_VALIDATION_RUST_SANITIZE=address \
  scripts/run-adbc-validation.sh
```

The Rust mode needs nightly with `rust-src`, clang and clang++. It uses `-Zsanitizer=address`
and `-Zbuild-std`, defaults `CC`/`CXX` to clang, and loads the library from
`target/<host-triple>/debug/`. Keep the C++ and Rust ASan runtimes compatible when overriding
compilers. Rust mode supports `address`; UBSan coverage comes from the separate C++ mode.

C++ ASan can catch errors reaching its process-wide allocator/memory interceptors, but cannot
check every memory access inside uninstrumented Rust. Rust ASan covers those instrumented
accesses. Its positive-control [canary](asan_canary.cc) calls a test-only out-of-bounds Rust
function on a C++ allocation and requires an ASan heap-buffer-overflow report before the suite
runs. That symbol is enabled only by the script's `--cfg asan_canary` build.

Leak detection is disabled by default for process-lifetime runtime/client state; ASan memory
errors and UBSan reports remain fatal. Native TLS C/assembly is not instrumented by the Rust
flag. Plain, C++-sanitized and Rust-sanitized runs use separate build directories.
