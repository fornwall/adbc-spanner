//! C ABI entrypoint that exports this crate as a loadable ADBC 1.1.0 driver.
//!
//! When built as a `cdylib` (the default), the resulting shared library
//! (`libadbc_spanner.so` / `libadbc_spanner.dylib` / `adbc_spanner.dll`) exports:
//!
//! - `AdbcSpannerInit` — the driver-specific init symbol, named per the ADBC convention
//!   (library `libadbc_spanner.so` → `AdbcSpannerInit`), and
//! - `AdbcDriverInit` — a fallback symbol the driver manager tries when no explicit entrypoint
//!   is given.
//!
//! Load it from any ADBC driver manager by pointing at the shared library path, e.g. with the
//! Python driver manager:
//!
//! ```python
//! import adbc_driver_manager
//! db = adbc_driver_manager.AdbcDatabase(
//!     driver="/path/to/libadbc_spanner.so",
//!     entrypoint="AdbcSpannerInit",
//!     uri="projects/p/instances/i/databases/d",
//! )
//! ```

adbc_ffi::export_driver!(AdbcSpannerInit, crate::SpannerDriver);
