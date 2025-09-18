# PrepareClientEncoding

## Location
[src/backend/utils/mb/mbutils.c:110-207](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/mbutils.c#L110-L207)

## Overview
Prepares for a future call to SetClientEncoding by validating the encoding and ensuring conversion functions are available, guaranteeing that the subsequent SetClientEncoding call will succeed.

## Definition
```c
int PrepareClientEncoding(int encoding)
```

## Detailed Description
PrepareClientEncoding serves as a preparation step before actually setting the client encoding. It validates that the requested encoding is supported and ensures that the necessary conversion functions between the server encoding and client encoding are available. The function handles two main scenarios:

1. **During live transactions**: It performs catalog lookups to find conversion procedures and caches them in TopMemoryContext for future use.
2. **Outside transactions**: It relies on previously cached conversion information from the ConvProcList.

The function implements an optimization by checking for cases that don't require conversion (when server and client encodings are the same, or when either is SQL_ASCII). It also handles the special case during backend startup where full validation cannot be performed.

## Parameters / Member Variables
- `encoding`: The target client encoding ID to prepare for

## Dependencies
- Functions called/Symbols referenced:
  - PG_VALID_FE_ENCODING (macro to validate frontend encoding)
  - [GetDatabaseEncoding](../G/GetDatabaseEncoding.md) (gets current server encoding)
  - [IsTransactionState](../I/IsTransactionState.md) (checks if in a live transaction)
  - [FindDefaultConversionProc](../F/FindDefaultConversionProc.md) (finds conversion functions in catalogs)
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md) (allocates memory for conversion info)
  - [fmgr_info_cxt](../f/fmgr_info_cxt.md) (loads function manager info)
  - [lcons](../l/lcons.md) (adds to conversion procedure list)
- Called from:
  - [check_client_encoding](../c/check_client_encoding.md) (in variable.c:707)
  - [InitializeClientEncoding](../I/InitializeClientEncoding.md) (in mbutils.c:288)

## Notes and Other Information
- Returns 0 on success, -1 on failure (bad encoding or unsupported conversion)
- Success before backend_startup_complete does not guarantee success after startup completion
- The function caches conversion procedures in ConvProcList but doesn't immediately remove older entries for the same encoding pair
- During transaction rollback scenarios, it can only restore previous settings using the cache
- Conversion functions are loaded into TopMemoryContext to ensure they persist across memory context resets