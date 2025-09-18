# DatumGetInetP

## Location
src/include/utils/inet.h: 139 - 142

## Overview
An obsolescent function that converts a Datum value to a modifiable inet pointer, performing full detoasting of packed data.

## Definition
```c
static inline inet *
DatumGetInetP(Datum X)
```

## Detailed Description
DatumGetInetP is an inline function that extracts a modifiable inet pointer from a Datum value. Unlike DatumGetInetPP, this function uses PG_DETOAST_DATUM which performs full detoasting, creating a copy of the data if it was packed/compressed. This ensures the returned pointer points to modifiable memory, but at the cost of potentially unnecessary copying.

The function is marked as "obsolescent" in the source code comments, indicating that DatumGetInetPP should be preferred for read-only access to avoid unnecessary detoasting overhead.

## Parameters / Member Variables
- `X`: A Datum value containing a packed or unpacked inet structure

## Dependencies
- Functions called/Symbols referenced:
  - PG_DETOAST_DATUM
  - inet
- Called from (representative examples):
  - PG_GETARG_INET_P (macro)

## Notes and Other Information
- This is an inline function defined in src/include/utils/inet.h
- Marked as "obsolescent" - [DatumGetInetPP](DatumGetInetPP.md) is preferred for read-only access
- Performs full detoasting, which may create unnecessary copies of data
- Returns a modifiable pointer, unlike the read-only pointer from DatumGetInetPP
- The PG_GETARG_INET_P macro provides the only remaining usage of this function
- Should only be used when modification of the inet data is required