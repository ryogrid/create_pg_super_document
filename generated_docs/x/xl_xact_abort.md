# xl_xact_abort

## Location
[src/include/access/xact.h:330-343](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/xact.h#L330-L343)

## Overview
Structure representing the transaction abort record in PostgreSQL's Write-Ahead Log (WAL), containing abort timestamp and variable-length additional information for rollback operations.

## Definition
```c
typedef struct xl_xact_abort
{
    TimestampTz xact_time;      /* time of abort */

    /* xl_xact_xinfo follows if XLOG_XACT_HAS_INFO */
    /* xl_xact_dbinfo follows if XINFO_HAS_DBINFO */
    /* xl_xact_subxacts follows if XINFO_HAS_SUBXACT */
    /* xl_xact_relfilelocators follows if XINFO_HAS_RELFILELOCATORS */
    /* xl_xact_stats_items follows if XINFO_HAS_DROPPED_STATS */
    /* No invalidation messages needed. */
    /* xl_xact_twophase follows if XINFO_HAS_TWOPHASE */
    /* twophase_gid follows if XINFO_HAS_GID. As a null-terminated string. */
    /* xl_xact_origin follows if XINFO_HAS_ORIGIN, stored unaligned! */
} xl_xact_abort;
```

## Detailed Description
The xl_xact_abort structure is the main WAL record format for transaction aborts in PostgreSQL. Similar to xl_xact_commit, it uses a variable-length format with the abort timestamp followed by optional sections based on transaction characteristics. The structure is designed to record all necessary information for properly rolling back a transaction during recovery, including sub-transactions, dropped relations, and statistics cleanup. Note that unlike commit records, abort records do not include invalidation messages since aborted transactions don't affect cached data.

## Parameters / Member Variables
- `xact_time`: The timestamp when the transaction was aborted, used for point-in-time recovery and transaction ordering

## Dependencies
- Functions called/Symbols referenced:
  - TimestampTz (data type)
- Called from (representative examples):
  - [ParseAbortRecord](../P/ParseAbortRecord.md) (in xactdesc.c)
  - [xact_desc_abort](xact_desc_abort.md) (in xactdesc.c)
  - [XactLogAbortRecord](../X/XactLogAbortRecord.md) (in xact.c)
  - [xact_redo](xact_redo.md) (in xact.c)
  - [xact_decode](xact_decode.md) (in decode.c)
  - [getRecordTimestamp](../g/getRecordTimestamp.md) (in xlogrecovery.c)
  - MinSizeOfXactAbort (minimum size calculation)

## Notes and Other Information
The xl_xact_abort structure is crucial for transaction rollback during both normal operation and crash recovery. It shares the same variable-length design as commit records but omits invalidation messages since aborted transactions don't commit changes that would affect cached data. During recovery, these records ensure that all resources allocated by the aborted transaction (including sub-transactions and temporary files) are properly cleaned up. The structure supports complex abort scenarios including two-phase transactions and replicated transaction aborts.