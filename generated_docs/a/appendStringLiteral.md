# appendStringLiteral

## Location
src/fe_utils/string_utils.c: 351 - 445

## Overview
Converts a string value to a properly escaped SQL string literal and appends it to a PQExpBuffer, handling character encoding and SQL standard compliance without requiring a database connection.

## Definition
```c
void appendStringLiteral(PQExpBuffer buf, const char *str, int encoding, bool std_strings)
```

## Detailed Description
The `appendStringLiteral` function safely converts arbitrary string data into SQL string literals by applying appropriate escaping rules and wrapping the result in single quotes. It handles both ASCII and multibyte characters according to the specified encoding, and respects the `standard_conforming_strings` setting for PostgreSQL compatibility.

The function implements a two-path approach: a fast path for plain ASCII characters and a slower path for multibyte characters. It carefully validates multibyte character sequences and replaces invalid bytes with encoding-specific invalid sequences that will trigger server-side errors rather than potentially allowing SQL injection attacks.

This function is equivalent to libpq's `PQescapeStringInternal` but works with PQExpBuffer output and doesn't require a database connection, making it useful in contexts where connection information is unavailable.

## Parameters / Member Variables
- `buf`: Output PQExpBuffer to append the escaped string literal to
- `str`: Input string to be converted to SQL literal format
- `encoding`: Character encoding identifier for proper multibyte handling
- `std_strings`: Boolean indicating whether standard_conforming_strings is enabled

## Dependencies
- Functions called/Symbols referenced:
  - [enlargePQExpBuffer](../e/enlargePQExpBuffer.md) (buffer management)
  - `IS_HIGHBIT_SET` (ASCII detection macro)
  - `SQL_STR_DOUBLE` (character escaping decision macro)
  - [PQmblen](../P/PQmblen.md) (multibyte character length calculation)
  - [pg_encoding_verifymbchar](../p/pg_encoding_verifymbchar.md) (multibyte character validation)
  - [pg_encoding_set_invalid](../p/pg_encoding_set_invalid.md) (invalid sequence generation)
- Called from (representative examples):
  - `appendStringLiteralAH` (src/bin/pg_dump/pg_backup.h:337)
  - [appendStringLiteralConn](appendStringLiteralConn.md) (src/fe_utils/string_utils.c:460)
  - [appendReloptionsArray](appendReloptionsArray.md) (src/fe_utils/string_utils.c:1015)
  - [escape_append_literal](../e/escape_append_literal.md) (src/test/modules/test_escape/test_escape.c:383)

## Notes and Other Information
- Pre-allocates buffer space (2 * input_length + 2) to minimize reallocations
- Handles invalid multibyte sequences by replacing them with encoding-specific invalid sequences
- More secure than simple string escaping as it validates character encoding integrity
- Used when database connection is not available (prefer `appendStringLiteralConn` when connection exists)
- Critical for preventing SQL injection in dynamically constructed queries
- Maintains compatibility with different PostgreSQL string literal standards