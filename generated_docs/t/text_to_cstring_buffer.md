# text_to_cstring_buffer

## Location
[src/backend/utils/adt/varlena.c:248-274](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L248-L274)

## Overview
Copies a PostgreSQL text value into a caller-supplied buffer with safe truncation and null-termination, supporting compressed and toasted text values.

## Definition
```c
void text_to_cstring_buffer(const text *src, char *dst, size_t dst_len)
```

## Detailed Description
The `text_to_cstring_buffer` function provides a safe way to convert PostgreSQL text data into a fixed-size C string buffer. Unlike `text_to_cstring` which allocates memory, this function copies the text content into a pre-allocated buffer provided by the caller. The function automatically handles detoasting of compressed or out-of-line text values and ensures encoding-safe truncation when the source text is longer than the destination buffer.

Key safety features include: automatic detoasting of compressed/toasted values, encoding-aware truncation using `pg_mbcliplen` to avoid cutting multibyte characters in the middle, guaranteed null-termination (unless `dst_len` is 0), and proper memory cleanup of temporary detoasted values.

## Parameters / Member Variables
- `src`: Pointer to the source PostgreSQL text value (may be compressed or toasted)
- `dst`: Destination buffer where the C string will be copied
- `dst_len`: Size of the destination buffer in bytes

## Dependencies
- Functions called/Symbols referenced:
  - `[pg_detoast_datum_packed](../p/pg_detoast_datum_packed.md)` - detoasts compressed or out-of-line values
  - `unconstify` - macro to cast away const qualifier
  - `VARSIZE_ANY_EXHDR` - macro to get data size excluding header
  - `VARDATA_ANY` - macro to get pointer to the actual data
  - `[pg_mbcliplen](../p/pg_mbcliplen.md)` - ensures encoding-safe truncation for multibyte characters
  - `memcpy` - standard C library function for memory copying
  - `[pfree](../p/pfree.md)` - PostgreSQL's memory deallocation function

- Called from (representative examples):
  - `[lo_import_internal](../l/lo_import_internal.md)` - large object import operations
  - `[be_lo_export](../b/be_lo_export.md)` - large object export operations
  - `[timetz_zone](timetz_zone.md)` - timezone conversion for time with timezone
  - `[parse_sane_timezone](../p/parse_sane_timezone.md)` - timezone parsing functions
  - `[timestamp_zone](timestamp_zone.md)` - timestamp timezone conversion
  - `[timestamptz_zone](timestamptz_zone.md)` - timestamptz timezone conversion

## Notes and Other Information
- Provides buffer overflow protection by respecting the `dst_len` parameter
- Uses encoding-aware truncation via `pg_mbcliplen` to prevent corruption of multibyte characters
- Automatically handles both regular and compressed/toasted text values
- The function is void-returning, with results written to the caller's buffer
- Reserves one byte for null termination, so effective copying length is `dst_len - 1`
- Particularly useful for interfacing with C APIs that require fixed-size string buffers
- Essential for cases where dynamic memory allocation is not desired or appropriate
- Located in `src/backend/utils/adt/varlena.c` as part of the variable-length data type utilities

## Simplified Source

```c
void text_to_cstring_buffer(const text *src, char *dst, size_t dst_len) {
    // Detoast the input text (handle compressed/out-of-line values)
    text *srcunpacked = pg_detoast_datum_packed(unconstify(text *, src));
    size_t src_len = VARSIZE_ANY_EXHDR(srcunpacked);

    if (dst_len > 0) {
        // Reserve space for null terminator
        dst_len--;

        // Calculate safe copy length
        if (dst_len >= src_len) {
            dst_len = src_len;
        } else {
            // Ensure encoding-safe truncation for multibyte characters
            dst_len = pg_mbcliplen(VARDATA_ANY(srcunpacked), src_len, dst_len);
        }

        // Copy data and null-terminate
        memcpy(dst, VARDATA_ANY(srcunpacked), dst_len);
        dst[dst_len] = '\0';
    }

    // Clean up detoasted copy if needed
    if (srcunpacked != src)
        pfree(srcunpacked);
}
```