# ReindexIndexCallbackState

## Location
[src/backend/commands/indexcmds.c:123-131](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/indexcmds.c#L123-L131)

## Overview
A callback state structure used to pass context information to the  function during index reindexing operations.

## Definition

```c
struct ReindexIndexCallbackState
{
	ReindexParams params;		/* options from statement */
	Oid			locked_table_oid;	/* tracks previously locked table */
};
```
## Detailed Description
The  structure serves as a callback argument for , which is responsible for checking permissions and acquiring proper locks during index reindexing operations. This structure maintains state across multiple callback invocations, particularly tracking which tables have been locked to avoid deadlocks and ensure proper lock management during the reindexing process.

The structure is designed to support both concurrent and non-concurrent reindexing operations by carrying the reindex parameters and maintaining lock state information.

## Parameters / Member Variables
- `params`: A  structure containing reindex options (bitmask of REINDEXOPT_* flags) and optional tablespace OID for relocating indexes
- `locked_table_oid`: Tracks the OID of a previously locked table to enable proper lock cleanup when the same callback is used for multiple indexes

## Dependencies
- Functions called/Symbols referenced:
  - [ReindexParams](ReindexParams.md)
- Called from (representative examples):
  - [RangeVarCallbackForReindexIndex](RangeVarCallbackForReindexIndex.md)
  - [ReindexIndex](ReindexIndex.md)

## Notes and Other Information
- This structure is specifically designed for the callback mechanism used during range variable resolution for index reindexing
- The  member is crucial for preventing deadlocks by ensuring proper lock ordering (heap before index) and cleanup of stale locks
- Used internally within the index reindexing subsystem and not exposed to external callers