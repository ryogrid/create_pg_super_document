# ginbeginscan

## Location
[src/backend/access/gin/ginscan.c:25-56](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginscan.c#L25-L56)

## Overview
Initializes and begins a new GIN (Generalized Inverted Index) index scan, setting up the necessary data structures and memory contexts for scanning operations.

## Definition

```c
IndexScanDesc
ginbeginscan(Relation rel, int nkeys, int norderbys)
```
## Detailed Description
The  function is the entry point for starting a GIN index scan. It creates and initializes an  structure with GIN-specific private data (GinScanOpaque). The function sets up two separate memory contexts: one for temporary data during scanning and another for scan key data. It also initializes the GIN state information that will be used throughout the scan process.

The function ensures that no ordering operators are specified (GIN indexes don't support ordered scans) and allocates the necessary private workspace for the scan operation.

## Parameters
- : The relation (index) being scanned
- : Number of scan keys that will be used
- : Number of order-by operators (must be 0 for GIN)

## Dependencies
- Functions called/Symbols referenced:
  - [RelationGetIndexScan](../R/RelationGetIndexScan.md)
  - [palloc](../p/palloc.md)
  - AllocSetContextCreate
  - [initGinState](../i/initGinState.md)
- Called from:
  - [ginhandler](ginhandler.md) (index access method handler)

## Notes and Other Information
- GIN indexes do not support ordered scans, so norderbys parameter must always be 0
- Creates two memory contexts: 'Gin scan temporary context' for temporary data and 'Gin scan key context' for scan keys
- The private workspace (GinScanOpaque) is attached to the scan descriptor's opaque field
- This function is part of the index access method interface for GIN indexes