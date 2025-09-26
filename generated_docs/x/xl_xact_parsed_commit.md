# xl_xact_parsed_commit

## Location
[src/include/access/xact.h:371-400](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/xact.h#L371-L400)

## Overview
Parsed and deconstructed representation of transaction commit records, providing convenient access to all commit-related information in structured form.

## Definition
```c
typedef struct xl_xact_parsed_commit
{
    TimestampTz xact_time;
    uint32      xinfo;

    Oid         dbId;           /* MyDatabaseId */
    Oid         tsId;           /* MyDatabaseTableSpace */

    int         nsubxacts;
    TransactionId *subxacts;

    int         nrels;
    RelFileLocator *xlocators;

    int         nstats;
    xl_xact_stats_item *stats;

    int         nmsgs;
    SharedInvalidationMessage *msgs;

    TransactionId twophase_xid; /* only for 2PC */
    char        twophase_gid[GIDSIZE];  /* only for 2PC */
    int         nabortrels;     /* only for 2PC */
    RelFileLocator *abortlocators;  /* only for 2PC */
    int         nabortstats;    /* only for 2PC */
    xl_xact_stats_item *abortstats; /* only for 2PC */

    XLogRecPtr  origin_lsn;
    TimestampTz origin_timestamp;
} xl_xact_parsed_commit;
```

## Detailed Description
The xl_xact_parsed_commit structure is a deconstructed representation of the xl_xact_commit WAL record, created by ParseCommitRecord() for easier consumption by various PostgreSQL components. Instead of requiring each consumer to parse the variable-length xl_xact_commit format, this structure provides direct access to all commit information through clearly typed fields and arrays. This design significantly simplifies the processing of commit records during WAL replay, logical decoding, and other operations that need to examine transaction commit details.

## Parameters / Member Variables
- `xact_time`: Timestamp when the transaction was committed
- `xinfo`: Extended information flags indicating which optional sections are present
- `dbId`: Database OID where the transaction was executed (MyDatabaseId)
- `tsId`: Tablespace OID of the database (MyDatabaseTableSpace)  
- `nsubxacts`: Number of sub-transaction IDs
- `subxacts`: Array of sub-transaction IDs
- `nrels`: Number of relation file locators for relations to delete on commit
- `xlocators`: Array of RelFileLocator for relations to delete on commit
- `nstats`: Number of statistics items to drop on commit
- `stats`: Array of xl_xact_stats_item structures for statistics cleanup
- `nmsgs`: Number of shared invalidation messages
- `msgs`: Array of SharedInvalidationMessage for cache invalidation
- `twophase_xid`: Transaction ID for two-phase commit transactions
- `twophase_gid`: Global Identifier string for two-phase commit (fixed size GIDSIZE)
- `nabortrels`: Number of relations to delete on abort (two-phase only)
- `abortlocators`: Array of RelFileLocator for relations to delete on abort (two-phase only)
- `nabortstats`: Number of statistics items to drop on abort (two-phase only)
- `abortstats`: Array of xl_xact_stats_item for abort statistics cleanup (two-phase only)
- `origin_lsn`: LSN of the record at the origin node (for replication)
- `origin_timestamp`: Timestamp at the origin node (for replication)

## Dependencies
- Functions called/Symbols referenced:
  - TimestampTz (data type)
  - TransactionId (data type)
  - [RelFileLocator](../R/RelFileLocator.md) (data type)
  - [xl_xact_stats_item](xl_xact_stats_item.md) (structure)
  - [SharedInvalidationMessage](../S/SharedInvalidationMessage.md) (data type)
  - GIDSIZE (constant)
  - XLogRecPtr (data type)
- Called from (representative examples):
  - [ParseCommitRecord](../P/ParseCommitRecord.md) (in xactdesc.c) - creates this structure
  - [xact_desc_commit](xact_desc_commit.md) (in xactdesc.c)
  - [xact_redo_commit](xact_redo_commit.md) (in xact.c)
  - [DecodeCommit](../D/DecodeCommit.md) (in decode.c)
  - [recoveryStopsBefore](../r/recoveryStopsBefore.md)/After (in xlogrecovery.c)

## Notes and Other Information
This structure greatly simplifies WAL processing by providing a parsed view of commit records. The parsing is done once by ParseCommitRecord(), and then various subsystems can access the structured data without needing to understand the original variable-length format. The structure includes both regular commit data and two-phase commit specific fields, making it suitable for all commit scenarios. The arrays (subxacts, xlocators, stats, msgs, etc.) point to the actual data within the parsed record, providing efficient access without unnecessary copying.