# relmap_identify

## Location
[src/backend/access/rmgrdesc/relmapdesc.c:35-47](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/rmgrdesc/relmapdesc.c#L35-L47)

## Overview
Returns a human-readable string identifier for relation mapping (relmap) WAL record types.

## Definition

```c
const char *
relmap_identify(uint8 info)
```
## Detailed Description
The `relmap_identify` function is a WAL record identification function that converts numeric WAL record type codes into human-readable string identifiers for relation mapping operations. This function is part of PostgreSQL's Write-Ahead Logging (WAL) infrastructure and is used primarily for debugging, logging, and diagnostic purposes to help administrators and developers understand what type of relation mapping operation a particular WAL record represents.

The function takes the info field from a WAL record header and returns a corresponding string identifier. Currently, it only handles the `XLOG_RELMAP_UPDATE` record type, returning "UPDATE" for such records.

## Parameters / Member Variables
- `info`: An 8-bit unsigned integer containing the WAL record type information from the record header

## Dependencies
- Functions called/Symbols referenced:
  - `XLR_INFO_MASK`: Mask used to extract relevant info bits from the record type
  - `XLOG_RELMAP_UPDATE`: WAL record type constant for relmap updates

- Called from (representative examples):
  - WAL record identification infrastructure (referenced in src/include/utils/relmapper.h:71)

## Notes and Other Information
- This function is part of the rmgr (resource manager) identification interface for WAL records
- Returns NULL for unrecognized record types
- Currently only supports `XLOG_RELMAP_UPDATE` records, returning "UPDATE"
- Used primarily by WAL analysis tools, debugging utilities, and logging systems
- Part of PostgreSQL's relation mapping system which maintains the mapping between relation OIDs and their physical file locations
- The function masks out irrelevant bits from the info parameter before comparison

## Simplified Source

```c
const char *relmap_identify(uint8 info) {
    // Extract operation type from WAL record info
    switch (info & ~XLR_INFO_MASK) {
        case XLOG_RELMAP_UPDATE:
            return "UPDATE";
    }
    return NULL;  // Unknown operation type
}
```