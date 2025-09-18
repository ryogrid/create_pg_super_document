# binary_decode

## Location
src/backend/utils/adt/encode.c: 96 - 161

## Overview
PostgreSQL SQL function that decodes text-encoded data back into binary format (bytea) using a specified decoding method.

## Definition


## Detailed Description
The `binary_decode` function is a SQL-callable function that converts encoded text data back into binary format (`bytea`). It serves as the inverse operation to `binary_encode`, supporting various decoding schemes like base64, hex, and escape formats. The function takes two arguments: the encoded text data to decode and the name of the decoding method to use.

Similar to its encoding counterpart, the function performs decoding length estimation to allocate appropriate memory, then performs the actual decoding operation. It includes overflow protection to prevent memory corruption and ensures the result size doesn't exceed PostgreSQL's maximum allocation limits.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - Argument 0: `text` - Encoded text data to be decoded
  - Argument 1: `text` - Name of the decoding method (e.g., "base64", "hex", "escape")

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_TEXT_PP` - Extract text argument from function call
  - `PG_GETARG_DATUM` - Extract datum argument from function call
  - `TextDatumGetCString` - Convert text datum to C string
  - `pg_find_encoding` - Find encoding structure by name
  - `[palloc](../p/palloc.md)` - PostgreSQL memory allocation
  - `SET_VARSIZE` - Set variable-length data size
  - `PG_RETURN_BYTEA_P` - Return bytea result from function
- Data structures used:
  - `pg_encoding` - Encoding method structure
  - `MaxAllocSize` - Maximum allocation size constant
- Called from (representative examples):
  - No direct references found (SQL-callable function)

## Notes and Other Information
- Complementary function to `binary_encode`, providing the reverse transformation
- Includes overflow protection similar to encoding function
- Uses uint64 for length calculations to handle large inputs safely
- Raises FATAL error if decoding estimate is insufficient, indicating memory corruption
- Supports the same encoding methods as `binary_encode` (base64, hex, escape)
- Part of PostgreSQL's binary data handling utilities in encode.c
- Input validation ensures only recognized encoding methods are accepted