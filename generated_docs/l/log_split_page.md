# log_split_page

## Location
[src/backend/access/hash/hashpage.c:1474-1500](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hashpage.c#L1474-L1500)

## Overview
Logs a hash table bucket split operation to the Write-Ahead Log (WAL) when a new bucket page becomes full during the split process.

## Definition

```c
static void
log_split_page(Relation rel, Buffer buf)
```
## Detailed Description
This function creates a WAL record for a hash table split operation. It is called when a new bucket page becomes full during a split operation, requiring the entire page to be logged for crash recovery purposes. The function only performs logging if the relation requires WAL (Write-Ahead Logging).

The logging process involves:
1. Checking if the relation needs WAL logging
2. Beginning a new WAL insert operation
3. Registering the buffer with force image and standard flags
4. Inserting the WAL record with the XLOG_HASH_SPLIT_PAGE operation type
5. Setting the LSN (Log Sequence Number) on the page

This ensures that the split operation can be properly replayed during crash recovery or on standby servers.

## Parameters / Member Variables
- `rel`: The hash index relation for which the split is being logged
- `buf`: Buffer containing the page to be logged (must be locked by caller)
## Dependencies
- Functions called/Symbols referenced:
  - RelationNeedsWAL (checks if relation requires WAL logging)
  - [XLogBeginInsert](../X/XLogBeginInsert.md) (starts a new WAL insert operation)
  - [XLogRegisterBuffer](../X/XLogRegisterBuffer.md) (registers the buffer for WAL logging)
  - [XLogInsert](../X/XLogInsert.md) (inserts the WAL record)
  - [PageSetLSN](../P/PageSetLSN.md) (sets the LSN on the page)
  - REGBUF_FORCE_IMAGE (flag to force full page image)
  - REGBUF_STANDARD (standard buffer registration flag)
  - XLOG_HASH_SPLIT_PAGE (WAL record type for hash split operations)
- Called from (representative examples):
  - [_hash_splitbucket](../h/_hash_splitbucket.md) (during bucket split operations when pages become full)

## Notes and Other Information
- This is a static function, only used within the hashpage.c module
- The function is responsible for WAL logging only - the caller must handle buffer locking and unlocking
- The REGBUF_FORCE_IMAGE flag ensures the entire page is logged, which is necessary for split operations
- The function is part of PostgreSQL's crash recovery mechanism for hash indexes
- Only logs if RelationNeedsWAL returns true, allowing for optimization when WAL is not required
- The LSN is set on the page to maintain consistency between the page and the WAL record