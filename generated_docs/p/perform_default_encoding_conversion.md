# perform_default_encoding_conversion

## Location
[src/backend/utils/mb/mbutils.c:783-863](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/mbutils.c#L783-L863)

## Overview
A static function that performs encoding conversion between client and server encodings using cached conversion procedures, optimized for efficiency and safe for use outside transactions.

## Definition
```c
static char *perform_default_encoding_conversion(const char *src, int len, bool is_client_to_server)
```

## Detailed Description
This function provides optimized encoding conversion between the client and database encodings using pre-cached FmgrInfo structures. Key characteristics:

1. **Transaction-safe**: Does not access the database, making it safe to call outside transactions
2. **Cached conversion**: Uses pre-setup conversion functions (ToServerConvProc/ToClientConvProc) for performance
3. **Bidirectional**: Handles both client-to-server and server-to-client conversions based on the direction flag
4. **Memory management**: Carefully manages memory allocation with overflow protection and optimization for large strings
5. **Graceful fallback**: Returns unchanged input when no conversion function is available

The function allocates memory conservatively to handle worst-case conversion expansion, then optimizes by releasing excess memory for large strings.

## Parameters / Member Variables
- `src`: Source string to be converted
- `len`: Length of the source string in bytes
- `is_client_to_server`: Direction flag - true for client→server conversion, false for server→client

## Dependencies
- Functions called/Symbols referenced:
  - unconstify (type casting utility)
  - MaxAllocHugeSize/MAX_CONVERSION_GROWTH (memory allocation constants)
  - [MemoryContextAllocHuge](../M/MemoryContextAllocHuge.md) (large memory allocation)
  - FunctionCall6 (PostgreSQL function call interface)
  - [CStringGetDatum](../C/CStringGetDatum.md) (datum conversion)
  - [repalloc](../r/repalloc.md) (memory reallocation)
- Called from (representative examples):
  - [pg_any_to_server](pg_any_to_server.md) (general server encoding conversion)
  - [pg_server_to_any](pg_server_to_any.md) (general client encoding conversion)

## Notes and Other Information
- This is a static (internal) function within mbutils.c, not exposed in the public API
- Uses global conversion procedure caches (ToServerConvProc, ToClientConvProc) set up by SetClientEncoding()
- Implements careful memory management to avoid integer overflow in allocation calculations
- For strings over 1MB, it performs memory optimization by reallocating to the actual result size
- The function assumes that appropriate conversion procedures have been cached by prior setup
- Located in src/backend/utils/mb/mbutils.c:783-863