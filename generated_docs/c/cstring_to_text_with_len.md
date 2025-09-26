# cstring_to_text_with_len

## Location
src/backend/utils/adt/varlena.c: 196 - 216

## Overview
Creates a PostgreSQL text data type value from a C string with an explicitly specified length, allowing conversion of strings that may not be null-terminated.

## Definition
```c
text *cstring_to_text_with_len(const char *s, int len)
```

## Detailed Description
The `cstring_to_text_with_len` function converts a C string of specified length into PostgreSQL's text data type. Unlike `cstring_to_text`, this function does not require the input string to be null-terminated, as the length is explicitly provided. This makes it particularly useful for processing string data from binary sources, substring operations, or when working with embedded null characters.

The function allocates memory using `palloc` to create a new text value with the PostgreSQL variable header (VARHDRSZ), sets the size using `SET_VARSIZE`, and copies the specified number of bytes from the source string using `memcpy`. This is the core implementation that `cstring_to_text` delegates to.

## Parameters / Member Variables
- `s`: Pointer to the source C string (need not be null-terminated)
- `len`: Number of bytes to copy from the source string into the text value

## Dependencies
- Functions called/Symbols referenced:
  - `palloc` - PostgreSQL's memory allocation function
  - `SET_VARSIZE` - macro to set the size field in the variable-length header
  - `VARDATA` - macro to get the data portion of a variable-length type
  - `memcpy` - standard C library function for memory copying
  - `VARHDRSZ` - constant defining the size of the variable-length header

- Called from (representative examples):
  - `cstring_to_text` - convenience wrapper for null-terminated strings
  - `textrecv` - text receive function for binary I/O
  - `json_recv` - JSON binary input processing
  - `replace_text` - text replacement operations
  - `split_text` - text splitting operations
  - `array_to_text_internal` - array to text conversion
  - `text_format` - text formatting function

## Notes and Other Information
- This is the fundamental implementation for creating text values from C strings in PostgreSQL
- The function handles arbitrary binary data and embedded null characters correctly
- Memory is allocated in the current PostgreSQL memory context
- The resulting text value includes the full variable-length header structure required by PostgreSQL's type system
- Performance advantage over `cstring_to_text` when the string length is already known, as it avoids the `strlen` call
- Used extensively throughout PostgreSQL for converting C string data to text, especially in JSON, XML, and string manipulation functions