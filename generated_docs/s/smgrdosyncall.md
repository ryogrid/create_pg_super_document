# smgrdosyncall

## Location
[src/backend/storage/smgr/smgr.c:426-461](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/smgr/smgr.c#L426-L461)

## Overview
Immediately synchronizes all forks of multiple relations to persistent storage, providing an optimized bulk sync operation.

## Definition


## Detailed Description
This function performs an immediate synchronization of all forks for multiple relations to persistent storage. It represents an optimized bulk operation that is significantly faster than performing individual FlushRelationBuffers() and smgrimmedsync() calls for each relation separately.

The function operates in two phases: first, it flushes all dirty buffers for all the specified relations using FlushRelationsAllBuffers(). Then, it iterates through each relation and each fork, checking if the fork exists using the storage manager's exists method, and if so, performs an immediate sync using the storage manager's immedsync method.

This bulk approach is more efficient because it can optimize I/O operations across multiple relations and takes advantage of the storage system's ability to batch synchronization operations.

## Parameters / Member Variables
- : Array of SMgrRelation pointers representing the relations to sync
- : Integer count of the number of relations in the rels array

## Dependencies
- Functions called/Symbols referenced:
  - [FlushRelationsAllBuffers](../F/FlushRelationsAllBuffers.md) (flushes dirty buffers for all relations)
  - smgrsw (storage manager switch table for exists and immedsync operations)
  - MAX_FORKNUM (maximum fork number constant for iteration)
  - SMgrRelation (relation structure type)
  - [ForkNumber](../F/ForkNumber.md) (fork identifier type)
- Called from (representative examples):
  - [smgrDoPendingSyncs](smgrDoPendingSyncs.md) (performs pending sync operations)
  - SmgrIsTemp (via header inclusion)

## Notes and Other Information
- Optimized for bulk synchronization operations, significantly faster than individual relation syncing
- Essential for ensuring data durability, especially during checkpoint operations
- Handles all fork types (main, FSM, VM, init) for each relation
- Only syncs forks that actually exist, avoiding unnecessary I/O for non-existent forks  
- Part of PostgreSQL's write-ahead logging and checkpoint mechanism for crash recovery
- Located in src/backend/storage/smgr/smgr.c:426-461