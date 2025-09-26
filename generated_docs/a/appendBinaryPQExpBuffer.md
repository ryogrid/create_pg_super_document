# appendBinaryPQExpBuffer

## Location
[src/interfaces/libpq/pqexpbuffer.c:397-412](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/pqexpbuffer.c#L397-L412)

## Overview
Appends arbitrary binary data to a PQExpBuffer, handling memory allocation and ensuring proper buffer management for binary content.

## Definition
```c
void appendBinaryPQExpBuffer(PQExpBuffer str, const char *data, size_t datalen)
```

## Detailed Description
This function appends binary data of arbitrary length to a PQExpBuffer. Unlike text-oriented append functions, this function is specifically designed to handle binary data that may contain null bytes or other non-printable characters. The function automatically handles buffer expansion when needed and uses memcpy for efficient data copying.

The function maintains a trailing null terminator even for binary data, though this is typically not meaningful for binary content. This design choice ensures consistency with other PQExpBuffer functions and prevents issues if the buffer contents are later treated as a string.

## Parameters / Member Variables
- `str`: The PQExpBuffer to append the binary data to
- `data`: Pointer to the binary data to be appended
- `datalen`: The number of bytes to append from the data pointer

## Dependencies
- Functions called/Symbols referenced:
  - [enlargePQExpBuffer](../e/enlargePQExpBuffer.md)
  - memcpy (system function)
- Called from (representative examples):
  - [createViewAsClause](../c/createViewAsClause.md) (src/bin/pg_dump/pg_dump.c)
  - [dumpTableSchema](../d/dumpTableSchema.md) (src/bin/pg_dump/pg_dump.c)
  - [pg_GSS_error_int](../p/pg_GSS_error_int.md) (src/interfaces/libpq/fe-gssapi-common.c)
  - [pqGets_internal](../p/pqGets_internal.md) (src/interfaces/libpq/fe-misc.c)
  - [appendPQExpBufferStr](appendPQExpBufferStr.md) (src/interfaces/libpq/pqexpbuffer.c)
  - [test_gb18030_json](../t/test_gb18030_json.md) (src/test/modules/test_escape/test_escape.c)

## Notes and Other Information
- Designed specifically for binary data that may contain null bytes
- Uses memcpy for efficient copying of arbitrary data
- Maintains trailing null terminator for consistency with other PQExpBuffer functions
- Automatically handles buffer expansion when insufficient space is available
- The trailing null terminator is generally not meaningful for true binary data
- Used internally by other PQExpBuffer functions like appendPQExpBufferStr
- Located in src/interfaces/libpq/pqexpbuffer.c:397-412