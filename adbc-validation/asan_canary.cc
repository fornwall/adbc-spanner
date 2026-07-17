// Cross-boundary AddressSanitizer canary for the ADBC validation `rust-asan` leg.
//
// This tiny program is the positive control that proves the Rust cdylib is *actually*
// ASan-instrumented — not merely that a passing leg happens to be green. It is compiled by
// scripts/run-adbc-validation.sh with clang `-fsanitize=address` and run against the
// nightly-instrumented cdylib right after that cdylib is built (before any database work).
//
// The shape it exercises is the one the whole rust-asan leg is about: instrumented Rust writing
// out of bounds on **C++-allocated** memory. This program:
//   1. `dlopen`s the cdylib (exactly how the ADBC driver manager loads the driver),
//   2. `dlsym`s `adbc_spanner_asan_canary` (present only in an `--cfg asan_canary` build),
//   3. `new[]`-allocates a small heap buffer here on the C++ side (so the ASan redzone/poison
//      lives on the C++ allocation), and
//   4. calls into the instrumented Rust function, which writes one byte past the end.
//
// Because both sides share one compiler-rt ASan runtime and shadow memory, ASan reports a
// `heap-buffer-overflow` attributed to the Rust frame. If the cdylib were NOT instrumented the
// out-of-bounds store would go unnoticed and this program would exit 0 — which the run script
// treats as a disarmed leg and fails loudly.
//
// The script asserts on the outcome (non-zero exit + an `AddressSanitizer` / `heap-buffer-overflow`
// report), so this program does not need to self-diagnose; it just performs the overflow. With
// `ASAN_OPTIONS=...abort_on_error=1` ASan aborts the process on the finding, which the script
// captures and inspects.

#include <dlfcn.h>
#include <cstdio>
#include <cstdlib>

typedef void (*canary_fn)(unsigned char* ptr, size_t len);

int main(int argc, char** argv) {
  if (argc != 2) {
    std::fprintf(stderr, "usage: %s <path-to-libadbc_spanner.so>\n", argv[0]);
    return 2;
  }

  const char* lib_path = argv[1];
  void* handle = dlopen(lib_path, RTLD_NOW | RTLD_LOCAL);
  if (handle == nullptr) {
    std::fprintf(stderr, "canary: dlopen(%s) failed: %s\n", lib_path, dlerror());
    return 2;
  }

  dlerror();  // clear any stale error
  void* sym = dlsym(handle, "adbc_spanner_asan_canary");
  const char* dlsym_err = dlerror();
  if (sym == nullptr || dlsym_err != nullptr) {
    std::fprintf(stderr,
                 "canary: dlsym(adbc_spanner_asan_canary) failed: %s\n"
                 "canary: is the cdylib built with --cfg asan_canary?\n",
                 dlsym_err ? dlsym_err : "symbol not found");
    return 2;
  }
  canary_fn canary = reinterpret_cast<canary_fn>(sym);

  // Allocate the buffer HERE, on the C++ side, so the ASan redzone is placed by the C++ allocator.
  const size_t len = 16;
  unsigned char* buf = new unsigned char[len];

  std::fprintf(
      stderr,
      "canary: calling instrumented Rust adbc_spanner_asan_canary to write 1 byte past a "
      "%zu-byte C++ heap buffer\n",
      len);
  std::fflush(stderr);

  // Instrumented Rust writes buf[len] — one past the end. ASan should abort here.
  canary(buf, len);

  // If we reach this line, the out-of-bounds write went undetected: the cdylib is NOT ASan-armed.
  std::fprintf(
      stderr, "canary: RETURNED without an ASan report — the cdylib is not ASan-armed\n");
  delete[] buf;
  return 0;
}
