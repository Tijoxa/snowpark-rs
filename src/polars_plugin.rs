//! The Polars 0.1 plugin ABI used by Python Polars 2.1. These entry points only
//! report an error. The planner and executor both provide an empty output.
use std::ffi::{c_char, c_void};

#[repr(C)]
struct ArrowArray {
    length: i64,
    null_count: i64,
    offset: i64,
    n_buffers: i64,
    n_children: i64,
    buffers: *mut *const c_void,
    children: *mut *mut Self,
    dictionary: *mut Self,
    release: Option<unsafe extern "C" fn(*mut Self)>,
    private_data: *mut c_void,
}

#[repr(C)]
struct SeriesExport {
    field: *mut c_void,
    arrays: *mut *mut ArrowArray,
    len: usize,
    release: Option<unsafe extern "C" fn(*mut Self)>,
    private_data: *mut c_void,
}

#[unsafe(no_mangle)]
extern "C" fn _polars_plugin_get_version() -> u32 {
    1 // Major 0, minor 1.
}

#[unsafe(no_mangle)]
extern "C" fn _polars_plugin_get_last_error_message() -> *const c_char {
    c"This Snowflake function requires a snowpark_rs.Table".as_ptr()
}

macro_rules! snowflake_only_plugin {
    ($name:ident) => {
        mod $name {
            use super::*;

            #[unsafe(export_name = concat!("_polars_plugin_field_", stringify!($name)))]
            extern "C" fn field(
                _fields: *const c_void,
                _len: usize,
                _result: *mut c_void,
                _kwargs: *const u8,
                _kwargs_len: usize,
            ) {
                // Leave the empty ArrowSchema untouched to fail local planning.
            }

            #[unsafe(export_name = concat!("_polars_plugin_", stringify!($name)))]
            unsafe extern "C" fn run(
                inputs: *mut SeriesExport,
                len: usize,
                _kwargs: *const u8,
                _kwargs_len: usize,
                _result: *mut SeriesExport,
                _context: *const c_void,
            ) {
                // SAFETY: Polars transfers ownership of its initialized exports.
                unsafe { release_inputs(inputs, len) };
            }
        }
    };
}

snowflake_only_plugin!(try_parse_json);
snowflake_only_plugin!(try_to_number);
snowflake_only_plugin!(type_of);
snowflake_only_plugin!(get_path);
snowflake_only_plugin!(is_null_value);
snowflake_only_plugin!(array_construct);
snowflake_only_plugin!(object_construct_keep_null);

unsafe fn release_inputs(inputs: *mut SeriesExport, len: usize) {
    // The 0.1 ABI transfers ownership of the exported arrays and series even
    // when the call fails. Normally field inference rejects the call first.
    for index in 0..len {
        // SAFETY: Polars passes len initialized, owned exports and valid callbacks.
        unsafe {
            let series = inputs.add(index);
            for chunk in 0..(*series).len {
                let array = *(*series).arrays.add(chunk);
                if let Some(release) = (*array).release {
                    release(array);
                }
            }
            if let Some(release) = (*series).release {
                release(series);
            }
        }
    }
}
