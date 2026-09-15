//! Panic containment and the small pointer/string conversions every entry point needs.
//!
//! Unwinding out of an `extern "C"` function is undefined behavior, so every exported entry point
//! runs its body inside [`catch`]. What distinguishes this from a plain `catch_unwind` is what
//! happens afterwards: see [`super::handle`], where the panic is confined to the one object whose
//! call panicked instead of disabling the driver process-wide.

use std::any::Any;
use std::ffi::{CStr, CString, c_char, c_void};
use std::panic::AssertUnwindSafe;

use adbc_core::error::{Error, Result, Status};

use crate::error::invalid_argument;
/// Run `f`, converting a panic into an `Internal` error rather than letting it cross the ABI.
pub(crate) fn catch<T>(f: impl FnOnce() -> Result<T>) -> std::result::Result<Result<T>, Error> {
    std::panic::catch_unwind(AssertUnwindSafe(f)).map_err(caught_panic)
}

// The payload formatting is independent of the callback and result types. Keep one copy
// instead of inlining it into every instantiation of `catch`.
#[cold]
#[inline(never)]
fn caught_panic(cause: Box<dyn Any + Send>) -> Error {
    let failure = crate::error::err(
        format!(
            "Uncaught panic in the Spanner driver: {}",
            panic_message(cause.as_ref())
        ),
        Status::Internal,
    );
    // Drop the owned payload here so its cleanup is shared by all callbacks too.
    drop(cause);
    failure
}

/// What a caught panic said, for the two boundaries that report one: entry points through
/// [`catch`], and the Arrow stream callbacks in [`super::stream`].
///
/// A payload that is neither of the two shapes `panic!` produces carries nothing renderable.
pub(crate) fn panic_message(cause: &(dyn Any + Send)) -> &str {
    cause
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| cause.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("unknown panic payload")
}

/// Renders text as a C string, replacing interior NULs rather than losing what follows them.
///
/// Driver messages are NUL-free by construction, but one that reached the driver from a vendor
/// library need not be, and no C string can carry a NUL. Truncating at it would silently drop the
/// rest of a diagnostic, so each one becomes `?`: a character that reads as "something was
/// replaced here", where a space would blend in and quietly turn one identifier into two words.
pub(crate) fn sanitized_cstring(text: &str) -> CString {
    let mut bytes = text.as_bytes().to_vec();
    for byte in &mut bytes {
        if *byte == 0 {
            *byte = b'?';
        }
    }
    CString::new(bytes).unwrap_or_default()
}

/// Borrow a required C string argument.
///
/// # Safety
/// `ptr` must be null or point to a NUL-terminated string that outlives `'a`.
pub(crate) unsafe fn required_str<'a>(ptr: *const c_char, name: &str) -> Result<&'a str> {
    if ptr.is_null() {
        return Err(missing_argument(name));
    }
    unsafe { CStr::from_ptr(ptr) }
        .to_str()
        .map_err(|error| invalid_argument(format!("{name} must be valid UTF-8: {error}")))
}

/// Borrow an optional C string argument, where null means "not specified".
///
/// # Safety
/// `ptr` must be null or point to a NUL-terminated string that outlives `'a`.
pub(crate) unsafe fn optional_str<'a>(ptr: *const c_char, name: &str) -> Result<Option<&'a str>> {
    if ptr.is_null() {
        return Ok(None);
    }
    unsafe { required_str(ptr, name) }.map(Some)
}

/// Borrow a NULL-terminated array of C strings, as `AdbcConnectionGetObjects` takes for
/// `table_types`.
///
/// # Safety
/// `ptr` must be null or point to a NULL-terminated array of NUL-terminated strings, all of which
/// outlive `'a`.
pub(crate) unsafe fn optional_str_list<'a>(
    ptr: *const *const c_char,
    name: &str,
) -> Result<Option<Vec<&'a str>>> {
    if ptr.is_null() {
        return Ok(None);
    }
    let mut items = Vec::new();
    let mut cursor = ptr;
    loop {
        let entry = unsafe { *cursor };
        if entry.is_null() {
            break;
        }
        items.push(unsafe { required_str(entry, name) }?);
        cursor = unsafe { cursor.add(1) };
    }
    Ok(Some(items))
}

/// Borrow a pointer/length byte argument, treating a null pointer as empty only when the length
/// agrees.
///
/// # Safety
/// `ptr` must be null or point to `len` initialized bytes that outlive `'a`.
pub(crate) unsafe fn byte_slice<'a>(ptr: *const u8, len: usize, name: &str) -> Result<&'a [u8]> {
    if ptr.is_null() {
        if len == 0 {
            return Ok(&[]);
        }
        return Err(invalid_argument(format!(
            "{name} must not be null for a length of {len}"
        )));
    }
    Ok(unsafe { std::slice::from_raw_parts(ptr, len) })
}

/// The space a GetOption caller says it has, or the rejection for a buffer it described
/// inconsistently.
///
/// adbc.h states the protocol as a postcondition on the caller's buffer -- "If output length <=
/// input length, value will contain a value with length bytes" (adbc.h:1185-1189) -- so a null
/// buffer advertised as having room would make the driver report a success it did not deliver.
/// A null buffer of length zero is the legitimate way to ask only for the required size. This is
/// the reference C++ framework's rule (`!out && *length > 0` in `base_driver.h`'s `CGet`).
///
/// # Safety
/// `length` must be null or point to a readable, writable `usize`.
// Takes the buffer as an untyped address: the two callers differ only in pointee type, and this
// check does not read through it.
unsafe fn available_space(dst: *const c_void, length: *mut usize) -> Result<usize> {
    if length.is_null() {
        return Err(missing_argument("length"));
    }
    let available = unsafe { *length };
    if dst.is_null() && available > 0 {
        return Err(invalid_argument(format!(
            "value must not be null for a buffer length of {available}; pass a length of 0 to ask \
             only for the size required"
        )));
    }
    Ok(available)
}

/// Write a string into the caller's buffer following the ADBC GetOption protocol: `length` is
/// in/out, always set to the space required (including the NUL), and the buffer is written only if
/// it is big enough. A caller that passed too little is expected to retry with the reported size.
///
/// # Safety
/// `length` must be non-null, and `dst` null or point to at least `*length` writable bytes.
pub(crate) unsafe fn write_string(value: &str, dst: *mut c_char, length: *mut usize) -> Result<()> {
    let available = unsafe { available_space(dst.cast::<c_void>(), length) }?;
    let bytes = value.as_bytes();
    if let Some(position) = bytes.iter().position(|&byte| byte == 0) {
        return Err(Error::with_message_and_status(
            format!("option value contains an interior NUL at byte {position}"),
            Status::Internal,
        ));
    }
    let required = bytes.len() + 1;
    // A buffer with room for the terminator is non-null after `available_space`.
    if required <= available {
        unsafe {
            std::ptr::copy_nonoverlapping(bytes.as_ptr().cast::<c_char>(), dst, bytes.len());
            dst.add(bytes.len()).write(0);
        }
    }
    unsafe { *length = required };
    Ok(())
}

/// The byte-valued counterpart of [`write_string`]. No NUL terminator is involved.
///
/// # Safety
/// `length` must be non-null, and `dst` null or point to at least `*length` writable bytes.
pub(crate) unsafe fn write_bytes(value: &[u8], dst: *mut u8, length: *mut usize) -> Result<()> {
    let available = unsafe { available_space(dst.cast::<c_void>(), length) }?;
    // The null check is load-bearing here, where [`write_string`] needs none: without a NUL to
    // make room for, an empty value fits the size probe's zero-length null buffer, and
    // `copy_nonoverlapping` demands a non-null destination even for zero bytes.
    if !dst.is_null() && value.len() <= available {
        unsafe { std::ptr::copy_nonoverlapping(value.as_ptr(), dst, value.len()) };
    }
    unsafe { *length = value.len() };
    Ok(())
}

/// Reject a missing output before performing work that the caller cannot collect.
pub(crate) fn required_output<T>(dst: *mut T, name: &str) -> Result<()> {
    if dst.is_null() {
        return Err(missing_argument(name));
    }
    Ok(())
}

/// The message does not depend on the pointee type, so it is built once rather than in each of
/// [`required_output`]'s and [`write_out`]'s instantiations.
#[cold]
#[inline(never)]
pub(crate) fn missing_argument(name: &str) -> Error {
    invalid_argument(format!("{name} must not be null"))
}

/// Write a value into a caller-provided out parameter, rejecting a null pointer.
///
/// # Safety
/// `dst` must be null or point to a writable, correctly aligned `T`.
pub(crate) unsafe fn write_out<T>(dst: *mut T, value: T, name: &str) -> Result<()> {
    required_output(dst, name)?;
    unsafe { std::ptr::write_unaligned(dst, value) };
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catches_a_panic_as_an_internal_error() {
        let error = catch(|| -> Result<()> { panic!("kaboom") }).unwrap_err();
        assert_eq!(error.status, Status::Internal);
        assert!(error.message.contains("kaboom"), "{}", error.message);
    }

    /// Both panic payload shapes `panic!` produces are rendered; anything else says so rather
    /// than reporting an empty message.
    #[test]
    fn renders_both_panic_payload_shapes() {
        assert_eq!(panic_message(&"borrowed"), "borrowed");
        assert_eq!(panic_message(&"owned".to_string()), "owned");
        assert_eq!(panic_message(&7_u8), "unknown panic payload");
    }

    /// An interior NUL becomes a visible `?` rather than truncating what follows it.
    #[test]
    fn sanitized_cstring_replaces_interior_nuls_instead_of_truncating() {
        assert_eq!(
            sanitized_cstring("before\0after").to_bytes(),
            b"before?after"
        );
        assert_eq!(sanitized_cstring("clean").to_bytes(), b"clean");
        assert_eq!(sanitized_cstring("").to_bytes(), b"");
    }

    #[test]
    fn passes_through_ordinary_results() {
        assert_eq!(catch(|| Ok(7)).unwrap().unwrap(), 7);
        let inner = catch(|| -> Result<()> {
            Err(Error::with_message_and_status("nope", Status::NotFound))
        })
        .unwrap()
        .unwrap_err();
        assert_eq!(inner.status, Status::NotFound);
    }

    #[test]
    fn write_string_and_bytes_report_the_required_size_without_writing() {
        let mut buffer = [0_i8; 2];
        let mut length = buffer.len();
        unsafe { write_string("hello", buffer.as_mut_ptr(), &raw mut length) }.unwrap();
        // "hello" plus NUL; nothing written because the buffer is too small.
        assert_eq!(length, 6);
        assert_eq!(buffer, [0, 0]);

        let mut buffer = [0_i8; 6];
        let mut length = buffer.len();
        unsafe { write_string("hello", buffer.as_mut_ptr(), &raw mut length) }.unwrap();
        assert_eq!(length, 6);
        assert_eq!(
            unsafe { CStr::from_ptr(buffer.as_ptr()) }.to_str().unwrap(),
            "hello"
        );

        // `write_bytes` follows the same protocol, without the trailing NUL.
        let mut buffer = [0_u8; 1];
        let mut length = buffer.len();
        unsafe { write_bytes(b"abc", buffer.as_mut_ptr(), &raw mut length) }.unwrap();
        assert_eq!(length, 3);
        assert_eq!(buffer, [0]);
    }

    #[test]
    fn write_string_preserves_utf8_and_terminates_empty_values() {
        let sentinel: c_char = 120;
        for value in ["", "smörgås"] {
            let mut buffer = [sentinel; 16];
            let mut length = value.len() + 1;
            unsafe { write_string(value, buffer.as_mut_ptr(), &raw mut length) }.unwrap();
            assert_eq!(length, value.len() + 1);
            assert_eq!(
                unsafe { CStr::from_ptr(buffer.as_ptr()) }.to_bytes(),
                value.as_bytes()
            );
            assert_eq!(buffer.get(length), Some(&sentinel));
        }

        let mut buffer = [sentinel; 16];
        let mut length = buffer.len();
        let error = unsafe { write_string("left\0right", buffer.as_mut_ptr(), &raw mut length) }
            .unwrap_err();
        assert_eq!(error.status, Status::Internal);
        assert!(error.message.contains("byte 4"), "{}", error.message);
        assert_eq!(length, buffer.len());
        assert_eq!(buffer, [sentinel; 16]);
    }

    /// adbc.h promises that a reported length no larger than the one passed in means the value
    /// was written, so a null buffer claiming room has to be refused rather than answered `OK`.
    /// A null buffer of length zero is the size probe, and stays legal.
    #[test]
    fn a_null_buffer_is_a_size_probe_only_when_it_says_it_holds_nothing() {
        let mut length = 64;
        let error =
            unsafe { write_string("hello", std::ptr::null_mut(), &raw mut length) }.unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
        assert!(error.message.contains("64"), "{}", error.message);
        assert_eq!(length, 64, "a rejected call reports no size");

        let mut length = 0;
        unsafe { write_string("hello", std::ptr::null_mut(), &raw mut length) }.unwrap();
        assert_eq!(length, 6);

        let mut length = 0;
        unsafe { write_bytes(b"abc", std::ptr::null_mut(), &raw mut length) }.unwrap();
        assert_eq!(length, 3);
        assert!(
            unsafe { write_bytes(b"", std::ptr::null_mut(), &raw mut length) }.is_err(),
            "the length is now 3, so the null buffer claims room it does not have"
        );
    }

    #[test]
    fn optional_str_maps_null_to_none_and_required_str_rejects_it() {
        assert_eq!(
            unsafe { optional_str(std::ptr::null(), "catalog") }.unwrap(),
            None
        );
        let value = CString::new("main").unwrap();
        assert_eq!(
            unsafe { optional_str(value.as_ptr(), "catalog") }.unwrap(),
            Some("main")
        );

        let error = unsafe { required_str(std::ptr::null(), "table_name") }.unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
        assert!(error.message.contains("table_name"), "{}", error.message);
    }

    #[test]
    fn optional_str_list_reads_until_the_null_terminator() {
        let first = CString::new("TABLE").unwrap();
        let second = CString::new("VIEW").unwrap();
        let array = [first.as_ptr(), second.as_ptr(), std::ptr::null()];
        assert_eq!(
            unsafe { optional_str_list(array.as_ptr(), "table_type") }.unwrap(),
            Some(vec!["TABLE", "VIEW"])
        );
        assert_eq!(
            unsafe { optional_str_list(std::ptr::null(), "table_type") }.unwrap(),
            None
        );
    }

    #[test]
    fn byte_slice_accepts_null_only_when_empty() {
        assert_eq!(
            unsafe { byte_slice(std::ptr::null(), 0, "plan") }.unwrap(),
            b""
        );
        assert!(unsafe { byte_slice(std::ptr::null(), 1, "plan") }.is_err());
        let data = [1_u8, 2, 3];
        assert_eq!(
            unsafe { byte_slice(data.as_ptr(), data.len(), "plan") }.unwrap(),
            &data
        );
    }
}
