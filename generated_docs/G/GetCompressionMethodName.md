# GetCompressionMethodName

## Location
[src/backend/access/common/toast_compression.c:304-316](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/toast_compression.c#L304-L316)

## Overview
Converts a compression method identifier to its corresponding string name, providing the reverse mapping of CompressionNameToMethod.

## Definition

```c
const char *
GetCompressionMethodName(char method)
```
## Detailed Description
This function takes a numeric compression method identifier and returns the corresponding string name. It uses a switch statement to map compression method IDs to their string representations. Currently supports two compression methods: TOAST_PGLZ_COMPRESSION (returns "pglz") and TOAST_LZ4_COMPRESSION (returns "lz4"). If an invalid or unrecognized method ID is provided, the function generates an error using elog(ERROR) and includes the invalid method character in the error message.

## Parameters / Member Variables
- : Character representing the compression method ID to convert to a name

## Dependencies
- Functions called/Symbols referenced:
  - TOAST_PGLZ_COMPRESSION
  - TOAST_LZ4_COMPRESSION
  - elog (for error reporting)
- Called from (representative examples):
  - [MergeAttributes](../M/MergeAttributes.md)
  - [transformTableLikeClause](../t/transformTableLikeClause.md)
  - CompressionMethodIsValid

## Notes and Other Information
- Located in src/backend/access/common/toast_compression.c:304-316
- Returns const char* pointing to a static string
- Generates an ERROR-level log message for invalid compression methods
- Part of PostgreSQL's TOAST compression infrastructure
- Complementary function to CompressionNameToMethod for bidirectional conversion
- The NULL return after elog(ERROR) is included to keep the compiler quiet, though it's unreachable code

## Simplified Source
```c
const char *GetCompressionMethodName(char method) {
    switch (method) {
        case TOAST_PGLZ_COMPRESSION:
            return "pglz";
        case TOAST_LZ4_COMPRESSION:
            return "lz4";
        default:
            // Invalid compression method - log error
            elog(ERROR, "invalid compression method %c", method);
            return NULL;  // Unreachable, keeps compiler quiet
    }
}
```