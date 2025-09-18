# pgDataValue

## Location
[src/interfaces/libpq/libpq-int.h:306-309](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/libpq-int.h#L306-L309)

## Overview
pgDataValue represents a single data field value in a PostgreSQL result row, used for passing column data to row processors in libpq with support for both text and binary data formats.

## Definition
```c
typedef struct pgDataValue
{
    int         len;            /* data length in bytes, or <0 if NULL */
    const char *value;          /* data value, without zero-termination */
} PGdataValue;
```

## Detailed Description
The pgDataValue structure is designed to efficiently represent column values when processing query results through libpq's row processor interface. It handles both text and binary data formats without making assumptions about data encoding or zero-termination. This structure is particularly important for streaming result processing where each field value needs to be passed efficiently to user-defined row processing functions. The design allows for direct memory references without copying data, making it suitable for high-performance applications. SQL NULL values are represented using a negative length, allowing the value pointer to remain valid while indicating no actual data is present.

## Parameters / Member Variables
- `len`: Length of the data in bytes. Positive values indicate valid data length, negative values (< 0) represent SQL NULL values
- `value`: Pointer to the actual data bytes. For text data, this is not zero-terminated. For binary data, contains the raw bytes. Remains valid even when len < 0 (NULL case)

## Dependencies
- Functions called/Symbols referenced: None (data structure only)
- Used by:
  - Row processor interface in pg_conn structure (libpq-int.h:540 as rowBuf array)
  - Memory allocation in fe-connect.c:4606 for connection setup
  - Row buffer management in fe-protocol3.c:767, 790-791
  - Row processing in fe-exec.c:1210 as columns array

## Notes and Other Information
- Part of libpq's row processor interface for efficient result set handling
- Text data is explicitly not zero-terminated, requiring length-based processing
- Supports both text and binary data formats transparently
- NULL SQL values are represented by len < 0, not by a NULL value pointer
- Used in arrays (rowBuf) to represent complete rows with multiple columns
- Memory-efficient design avoids data copying by using direct pointers to result data
- The structure is allocated as part of the connection's rowBuf array and resized as needed based on query result width