# PQescapeBytea

## Location
[src/interfaces/libpq/fe-exec.c:4530-4537](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-exec.c#L4530-L4537)

## Overview
PQescapeBytea converts binary data to a format suitable for inclusion in SQL INSERT statements with bytea columns, using traditional octal escape encoding.

## Definition

```c
unsigned char *
PQescapeBytea(const unsigned char *from, size_t from_length, size_t *to_length)
```
## Detailed Description
PQescapeBytea is a libpq client library function that converts binary data (byte arrays) into an escaped string format that can be safely included in SQL statements for bytea columns. This function specifically uses the traditional escape encoding method, not hex encoding.

The function is a simple wrapper around PQescapeByteaInternal, passing static configuration values to disable hex encoding and use the global static_std_strings setting. The underlying implementation applies the following transformations in escape mode:
- '\0' (ASCII 0) becomes \000
- '\'' (ASCII 39) becomes '' (doubled single quote)
- '\' (ASCII 92) becomes \\ (doubled backslash)
- Any byte < 0x20 or > 0x7e becomes \ooo (octal representation)

If standard_conforming_strings is disabled, all backslashes in the output are doubled.

## Parameters / Member Variables
- `*from`: Pointer to the binary data to be escaped
- `from_length`: Length of the binary data in bytes
- `*to_length`: Output parameter - pointer to size_t where the length of the result string will be stored
## Dependencies
- Functions called/Symbols referenced:
  - [PQescapeByteaInternal](PQescapeByteaInternal.md) (internal implementation function)
  - static_std_strings (global variable for standard_conforming_strings setting)
- Called from (representative examples):
  - Referenced in libpq-fe.h header file declarations

## Notes and Other Information
- This function allocates memory for the result using malloc() - the caller is responsible for freeing the returned memory
- Returns NULL on memory allocation failure
- The function is part of the libpq client interface and is intended for use by client applications
- For new applications, consider using PQescapeByteaConn() which supports both hex and escape encoding formats
- The escaped result includes a null terminator and the total length includes this terminator
- This is the connection-independent version that uses default encoding settings

## Simplified Source

```c
unsigned char *
PQescapeBytea(const unsigned char *from, size_t from_length, size_t *to_length) {
    // Call internal escaping with default settings
    // No connection context, use global std_strings, traditional escape format
    return PQescapeByteaInternal(NULL, from, from_length, to_length,
                                static_std_strings,
                                false /* no hex encoding */);
}
```