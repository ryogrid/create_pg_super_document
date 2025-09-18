# SetClientEncoding

## Location
[src/backend/utils/mb/mbutils.c:208-280](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/mbutils.c#L208-L280)

## Overview
Sets the active client encoding and establishes conversion function pointers, relying on a previous call to PrepareClientEncoding to ensure the necessary conversion functions are available.

## Definition
```c
int SetClientEncoding(int encoding)
```

## Detailed Description
SetClientEncoding is the companion function to PrepareClientEncoding that actually activates a client encoding. It assumes that PrepareClientEncoding has been called previously for the specified encoding, which guarantees that the necessary conversion procedures are cached and available.

The function performs the following operations:
1. Validates the encoding using PG_VALID_FE_ENCODING
2. During startup, defers the operation by storing the encoding in pending_client_encoding
3. Checks for cases requiring no conversion (same encoding, or SQL_ASCII involved)
4. Searches the ConvProcList cache for previously prepared conversion functions
5. Sets up global variables (ClientEncoding, ToServerConvProc, ToClientConvProc)
6. Cleans up duplicate cache entries to prevent memory leaks

## Parameters / Member Variables
- `encoding`: The client encoding ID to activate

## Dependencies
- Functions called/Symbols referenced:
  - PG_VALID_FE_ENCODING (macro to validate frontend encoding)
  - [GetDatabaseEncoding](../G/GetDatabaseEncoding.md) (gets current server encoding)
  - foreach_delete_current (removes duplicate cache entries)
- Called from:
  - [ParallelWorkerMain](../P/ParallelWorkerMain.md) (in parallel.c:1437)
  - [assign_client_encoding](../a/assign_client_encoding.md) (in variable.c:785)
  - [InitializeClientEncoding](../I/InitializeClientEncoding.md) (in mbutils.c:289)

## Notes and Other Information
- Returns 0 on success, -1 on failure (bad encoding or conversion not cached)
- Must be preceded by a successful call to PrepareClientEncoding
- During backend startup, the encoding is stored in pending_client_encoding for later processing
- Sets global variables ClientEncoding, ToServerConvProc, and ToClientConvProc
- Automatically cleans up duplicate entries in ConvProcList to prevent memory leaks from repeated Prepare/Set cycles
- If no conversion is needed (same encodings or SQL_ASCII), sets conversion procedures to NULL