# hashgettuple

## Location
[src/backend/access/hash/hash.c:283-334](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hash.c#L283-L334)

## Overview
Retrieves the next tuple in a hash index scan, handling both the initial tuple retrieval and subsequent tuple advancement in the specified scan direction.

## Definition
```c
bool hashgettuple(IndexScanDesc scan, ScanDirection dir)
```

## Detailed Description
The hashgettuple function is the core tuple retrieval mechanism for hash index access method in PostgreSQL. It implements a stateful scan that can move forward or backward through hash index pages. The function automatically sets the recheck flag since hash indexes are always lossy (they store only hash codes, not the actual key values). 

On the first call for a scan, it initializes the scan position using _hash_first. For subsequent calls, it advances the scan using _hash_next. The function also implements a "kill prior tuple" optimization where it tracks tuples that should be marked as dead, deferring the actual deletion until leaving the index page or ending the scan.

## Parameters / Member Variables
- `scan`: IndexScanDesc structure containing the scan state and configuration
- `dir`: ScanDirection indicating whether to scan forward or backward through the index

## Dependencies
- Functions called/Symbols referenced:
  - HashScanPosIsValid
  - [_hash_first](_hash_first.md)
  - [_hash_next](_hash_next.md)  
  - MaxIndexTuplesPerPage
  - [palloc](../p/palloc.md)
- Called from (representative examples):
  - [hashhandler](hashhandler.md) (hash access method handler)
  - Referenced in HASHNProcs (hash index procedure array)

## Notes and Other Information
- Always sets scan->xs_recheck = true because hash indexes are lossy
- Implements tuple killing optimization to defer dead tuple cleanup
- Maintains killedItems array to track tuples marked for deletion
- Can handle scan direction changes gracefully
- Returns boolean indicating whether a valid tuple was found