# smgrnblocks_cached

## Location
[src/backend/storage/smgr/smgr.c:679-700](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/smgr/smgr.c#L679-L700)

## Overview
Returns the cached number of blocks in the supplied storage manager relation, providing efficient access to relation size information during recovery operations.

## Definition

```c
BlockNumber
smgrnblocks_cached(SMgrRelation reln, ForkNumber forknum)
```
## Detailed Description
The  function retrieves the cached number of blocks for a specified fork of a storage manager relation. Currently, this function only returns cached values during recovery mode ( is true) due to the lack of a shared invalidation mechanism for changes in file size. Outside of recovery, it returns  to indicate that no cached value is available. This design prevents stale data issues in normal operation while still providing performance benefits during recovery when consistency requirements are different.

## Parameters / Member Variables
- : SMgrRelation pointer representing the storage manager relation
- : ForkNumber indicating which fork of the relation to query

## Dependencies
- Functions called/Symbols referenced:
  - SMgrRelation (type)
  - InRecovery (global variable)
  - InvalidBlockNumber (constant)
- Called from (representative examples):
  - DropRelationBuffers
  - DropRelationsAllBuffers
  - smgrnblocks
  - SmgrIsTemp

## Notes and Other Information
- Currently limited to recovery mode due to lack of shared invalidation mechanism for file size changes
- Returns InvalidBlockNumber when not in recovery or when no cached value exists
- The cached value is stored in 
- Code elsewhere in PostgreSQL is designed to cope with potentially stale cached data
- This function provides a performance optimization by avoiding repeated disk operations during recovery
- Located in src/backend/storage/smgr/smgr.c:679-700