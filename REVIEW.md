# Repo review — prioritized findings

*Review date: 2026-07-07, against main at f7364e8.*

Overall: the driver is in very good shape — the type mapping, dense-union metadata assembly,
transaction model, streaming reader, and test suite (property-based round-trips, FFI conformance,
C++ validation, Python cookbook tests) are all solid. The issues below are ranked by how likely
they are to bite a real user. (All P1 and P2 findings from the original review have been fixed.)

## P3 — improvements worth queuing

- **macOS deployment target**: the `macosx_10_12` tag is asserted, not enforced — export
  `MACOSX_DEPLOYMENT_TARGET` in the build (aws-lc's cmake defaults from the host).

## Verified non-issues

The PyPI trusted-publishing setup is correct and least-privilege; the tag-vs-Cargo.toml version
gate works; `deny.toml` correctly restricts git sources to the documented pins; and
`like_match`/`split_statements` are already adversarial-input-aware within the syntax they know.
