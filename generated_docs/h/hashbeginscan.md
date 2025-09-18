# hashbeginscan

## Location
src/backend/access/hash/hash.c: 367 - 396

## Overview
Initializes and starts a scan on a hash index, setting up the necessary scan state and opaque data structures.

## Definition
```c
IndexScanDesc hashbeginscan(Relation rel, int nkeys, int norderbys)
```

## Detailed Description
The hashbeginscan function is responsible for initializing a new scan operation on a hash index. It creates and configures all the necessary data structures required for scanning the hash index, including the main IndexScanDesc structure and the hash-specific opaque data.

The function allocates a HashScanOpaque structure that maintains hash-specific scan state, including buffer references for the current bucket and any split bucket, flags indicating whether buckets have been populated or split, and structures for tracking killed (dead) tuples. The scan position is initially invalidated, meaning the first call to hashgettuple will need to call _hash_first to find the first qualifying tuple.

## Parameters / Member Variables
- `rel`: Relation structure representing the hash index to be scanned
- `nkeys`: Number of scan keys (search conditions) for this scan
- `norderbys`: Number of order by clauses (must be 0 for hash indexes)

## Dependencies
- Functions called/Symbols referenced:
  - RelationGetIndexScan
  - palloc
  - HashScanPosInvalidate
  - HashScanOpaqueData (structure)
  - InvalidBuffer (constant)
- Called from (representative examples):
  - hashhandler (hash access method handler)
  - Referenced in HASHNProcs (hash index procedure array)

## Notes and Other Information
- Hash indexes do not support ordered scans, so norderbys must always be 0
- Initializes buffer references to InvalidBuffer indicating no pages are currently pinned
- Sets up killed tuple tracking infrastructure for deferred cleanup optimization
- The scan position is initially invalid and will be established on the first tuple fetch
- Returns a fully initialized IndexScanDesc ready for tuple retrieval operations