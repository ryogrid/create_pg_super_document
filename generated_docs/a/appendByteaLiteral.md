# appendByteaLiteral

## Location
src/fe_utils/string_utils.c: 527 - 581

## Overview
Converts raw binary data (bytea) to a hexadecimal SQL string literal and appends it to a PQExpBuffer, accounting for different string escaping standards.

## Definition
```c
void appendByteaLiteral(PQExpBuffer buf, const unsigned char *str, size_t length, bool std_strings)
```

## Detailed Description
This function converts binary data into a PostgreSQL bytea literal using hexadecimal format. It handles the differences between standard-conforming strings and traditional PostgreSQL string escaping modes. The output is always in hexadecimal format (\\x followed by hex digits) regardless of server version, as the function cannot determine the target server's capabilities. The function is designed for situations where a database connection is not available, making PQescapeByteaConn unusable.

The implementation directly manipulates the PQExpBuffer's internal data for efficiency, building the hex representation byte by byte. It properly handles both standard_conforming_strings=on and standard_conforming_strings=off modes by adjusting the escaping format accordingly.

## Parameters / Member Variables
- `buf`: Target PQExpBuffer where the bytea literal will be appended
- `str`: Pointer to the raw binary data to be converted
- `length`: Number of bytes in the binary data
- `std_strings`: Boolean indicating whether standard-conforming string literals are enabled (affects escaping format)

## Dependencies
- Functions called/Symbols referenced:
  - [enlargePQExpBuffer](../e/enlargePQExpBuffer.md) (ensures sufficient buffer space for the hex output)
- Called from (representative examples):
  - appendByteaLiteralAHX (in pg_backup_archiver.h)

## Notes and Other Information
- Always produces hexadecimal format output (\\x...) for maximum compatibility
- Requires buffer space of approximately 2*length + 5 characters for the output
- When std_strings is false, uses double-backslash escaping (\\\\x) for compatibility with older PostgreSQL versions
- The hex conversion uses lowercase letters (a-f) for consistency
- More efficient than PQescapeByteaConn when no database connection is available
- Does not validate input parameters - assumes valid pointers and reasonable length values