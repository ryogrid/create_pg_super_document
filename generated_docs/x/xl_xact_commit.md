# xl_xact_commit

## Location
src/include/access/xact.h: 314 - 327

## Overview
Structure representing the core transaction commit record in PostgreSQL's Write-Ahead Log (WAL), containing commit timestamp and variable-length additional information.

## Definition
```c
typedef struct xl_xact_commit
{
    TimestampTz xact_time;      /* time of commit */

    /* xl_xact_xinfo follows if XLOG_XACT_HAS_INFO */
    /* xl_xact_dbinfo follows if XINFO_HAS_DBINFO */
    /* xl_xact_subxacts follows if XINFO_HAS_SUBXACT */
    /* xl_xact_relfilelocators follows if XINFO_HAS_RELFILELOCATORS */
    /* xl_xact_stats_items follows if XINFO_HAS_DROPPED_STATS */
    /* xl_xact_invals follows if XINFO_HAS_INVALS */
    /* xl_xact_twophase follows if XINFO_HAS_TWOPHASE */
    /* twophase_gid follows if XINFO_HAS_GID. As a null-terminated string. */
    /* xl_xact_origin follows if XINFO_HAS_ORIGIN, stored unaligned! */
} xl_xact_commit;
```

## Detailed Description
The xl_xact_commit structure is the main WAL record format for transaction commits in PostgreSQL. It contains the essential commit timestamp followed by variable-length optional sections that provide additional transaction information. The structure uses a flexible format where different optional sections are appended based on transaction characteristics, controlled by flags in the XLOG_XACT_HAS_INFO section. This design allows for efficient storage while accommodating various transaction types including simple commits, two-phase commits, and replicated transactions.

## Parameters / Member Variables
- `xact_time`: The timestamp when the transaction was committed, used for point-in-time recovery and transaction ordering

## Dependencies
- Functions called/Symbols referenced:
  - TimestampTz (data type)
- Called from (representative examples):
  - ParseCommitRecord (in xactdesc.c)
  - xact_desc_commit (in xactdesc.c) 
  - XactLogCommitRecord (in xact.c)
  - xact_redo (in xact.c)
  - xact_decode (in decode.c)
  - getRecordTimestamp (in xlogrecovery.c)
  - MinSizeOfXactCommit (minimum size calculation)

## Notes and Other Information
This structure is the foundation of PostgreSQL's transaction commit logging. The variable-length design allows it to efficiently handle different commit scenarios - from simple single-statement transactions to complex distributed transactions with sub-transactions, invalidations, and replication origin tracking. The optional sections are parsed based on info flags, making the format both compact and extensible. During recovery, these records are used to recreate the exact state of committed transactions.