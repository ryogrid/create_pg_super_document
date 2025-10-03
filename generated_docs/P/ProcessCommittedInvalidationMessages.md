# ProcessCommittedInvalidationMessages

## Location
[src/backend/utils/cache/inval.c:962-1025](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/inval.c#L962-L1025)

## Overview
Processes invalidation messages during transaction replay in recovery mode, handling both shared invalidation messages and relation cache initialization file invalidation.

## Definition

```c
void
ProcessCommittedInvalidationMessages(SharedInvalidationMessage *msgs,
									 int nmsgs, bool RelcacheInitFileInval,
									 Oid dbid, Oid tsid)
```
## Detailed Description
This function is executed by xact_redo_commit() or standby_redo() to process invalidation messages during WAL replay. It handles the processing of shared invalidation messages that were recorded during a committed transaction, ensuring that cache invalidations are properly applied during recovery.

The function follows a specific sequence for relcache init file invalidation: it performs pre-invalidation processing, sends the shared invalidation messages, and then performs post-invalidation processing. This ordering is critical for maintaining cache consistency during recovery.

When processing relcache init file invalidation for a specific database, the function temporarily sets DatabasePath to allow proper invalidation processing, then cleans it up afterward. This is necessary because SetDatabasePath is intended for use only once by normal backends, not during recovery.

## Parameters / Member Variables
- `*msgs`: Array of SharedInvalidationMessage structures containing the invalidation messages to process
- `nmsgs`: Number of messages in the msgs array
- `RelcacheInitFileInval`: Boolean flag indicating whether relation cache initialization files should be invalidated
- `dbid`: Database OID for which invalidation is being processed (used for relcache file invalidation)
- `tsid`: Tablespace OID associated with the database
## Dependencies
- Functions called/Symbols referenced:
  - [GetDatabasePath](../G/GetDatabasePath.md)
  - [RelationCacheInitFilePreInvalidate](../R/RelationCacheInitFilePreInvalidate.md)
  - [SendSharedInvalidMessages](../S/SendSharedInvalidMessages.md)
  - [RelationCacheInitFilePostInvalidate](../R/RelationCacheInitFilePostInvalidate.md)
- Called from (representative examples):
  - [xact_redo_commit](../x/xact_redo_commit.md)
  - [standby_redo](../s/standby_redo.md)

## Notes and Other Information
- Only processes messages if nmsgs > 0, returning early for empty message sets
- Uses DEBUG4 logging level to trace replay operations
- Temporarily manipulates DatabasePath global variable during recovery, which is a hack necessitated by recovery context limitations
- The function is specifically designed for recovery scenarios and should not be used in normal backend operation
- Relcache init file invalidation requires careful ordering of pre- and post-invalidation steps around message sending