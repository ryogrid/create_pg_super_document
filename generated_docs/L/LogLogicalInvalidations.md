# LogLogicalInvalidations

## Location
[src/backend/utils/cache/inval.c:1607-1637](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/inval.c#L1607-L1637)

## Overview
LogLogicalInvalidations emits WAL (Write-Ahead Log) records for cache invalidation messages caused by the current command, ensuring that logical replication subscribers receive the necessary invalidation information to maintain cache consistency.

## Definition


## Detailed Description
LogLogicalInvalidations is responsible for writing invalidation messages to the Write-Ahead Log for logical replication purposes. This function is called at command end or commit time when there are pending cache invalidation messages that need to be replicated to logical subscribers.

The function examines the current command's invalidation messages stored in  and, if any messages exist, constructs a WAL record containing these invalidations. The invalidation messages are processed in two categories: catalog cache messages (CatCacheMsgs) and relation cache messages (RelCacheMsgs).

The function uses the standard WAL insertion pattern: it calls XLogBeginInsert() to start record construction, registers the invalidation data using XLogRegisterData(), and finally inserts the completed record with XLogInsert() using the XLOG_XACT_INVALIDATIONS record type.

## Parameters / Member Variables
This function takes no parameters as it operates on global transaction state.

## Dependencies
- Functions called/Symbols referenced:
  - [XLogBeginInsert](../X/XLogBeginInsert.md)
  - [XLogRegisterData](../X/XLogRegisterData.md)
  - [XLogInsert](../X/XLogInsert.md)
  - NumMessagesInGroup
  - ProcessMessageSubGroupMulti
- Data structures used:
  - xl_xact_invals
  - [InvalidationMsgsGroup](../I/InvalidationMsgsGroup.md)
  - SharedInvalidationMessage
  - transInvalInfo (global variable)
- Constants used:
  - MinSizeOfXactInvals
  - RM_XACT_ID
  - XLOG_XACT_INVALIDATIONS
  - CatCacheMsgs
  - RelCacheMsgs
- Called from:
  - [RecordTransactionCommit](../R/RecordTransactionCommit.md) (src/backend/access/transam/xact.c:1328)
  - [CommandEndInvalidationMessages](../C/CommandEndInvalidationMessages.md) (src/backend/utils/cache/inval.c:1188)

## Notes and Other Information
- This function performs a quick exit if  is NULL, indicating no invalidation activity has occurred in the current transaction
- The function only logs invalidations if there are actually messages to process (nmsgs > 0)
- The WAL record uses the xl_xact_invals structure format and is tagged with XLOG_XACT_INVALIDATIONS type
- Invalidation messages are categorized and processed separately for catalog cache and relation cache invalidations
- This is part of PostgreSQL's logical replication infrastructure, ensuring that cache invalidations are properly replicated to maintain consistency across logical subscribers
- The function is declared in src/include/utils/inval.h and is part of the cache invalidation subsystem