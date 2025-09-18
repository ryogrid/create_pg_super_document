# xl_xact_prepare

## Location
src/include/access/xact.h: 346 - 364

## Overview
Structure representing the two-phase commit prepare record in PostgreSQL's Write-Ahead Log (WAL), containing comprehensive transaction state information needed for later commit or rollback.

## Definition
```c
typedef struct xl_xact_prepare
{
    uint32      magic;          /* format identifier */
    uint32      total_len;      /* actual file length */
    TransactionId xid;          /* original transaction XID */
    Oid         database;       /* OID of database it was in */
    TimestampTz prepared_at;    /* time of preparation */
    Oid         owner;          /* user running the transaction */
    int32       nsubxacts;      /* number of following subxact XIDs */
    int32       ncommitrels;    /* number of delete-on-commit rels */
    int32       nabortrels;     /* number of delete-on-abort rels */
    int32       ncommitstats;   /* number of stats to drop on commit */
    int32       nabortstats;    /* number of stats to drop on abort */
    int32       ninvalmsgs;     /* number of cache invalidation messages */
    bool        initfileinval;  /* does relcache init file need invalidation? */
    uint16      gidlen;         /* length of the GID - GID follows the header */
    XLogRecPtr  origin_lsn;     /* lsn of this record at origin node */
    TimestampTz origin_timestamp; /* time of prepare at origin node */
} xl_xact_prepare;
```

## Detailed Description
The xl_xact_prepare structure is the WAL record format for the PREPARE phase of two-phase commit transactions. This structure contains all the information necessary to later commit or abort the transaction, including metadata about sub-transactions, relations to be deleted, statistics to be dropped, and cache invalidation requirements. It's designed to preserve the complete transaction state so that the transaction can be completed even after system restart or in distributed transaction scenarios.

## Parameters / Member Variables
- `magic`: Format identifier for validation and version compatibility
- `total_len`: Total length of the prepare record including variable-length data
- `xid`: The transaction ID of the prepared transaction
- `database`: OID of the database where the transaction was executed
- `prepared_at`: Timestamp when the transaction was prepared
- `owner`: OID of the user who executed the transaction
- `nsubxacts`: Count of sub-transaction XIDs that follow this header
- `ncommitrels`: Number of relations to be deleted if transaction commits
- `nabortrels`: Number of relations to be deleted if transaction aborts
- `ncommitstats`: Number of statistics objects to drop on commit
- `nabortstats`: Number of statistics objects to drop on abort  
- `ninvalmsgs`: Number of cache invalidation messages to process
- `initfileinval`: Whether the relation cache initialization file needs invalidation
- `gidlen`: Length of the Global Transaction Identifier that follows
- `origin_lsn`: LSN of this record at the origin node (for replication)
- `origin_timestamp`: Timestamp of preparation at the origin node (for replication)

## Dependencies
- Functions called/Symbols referenced:
  - TransactionId (data type)
  - Oid (data type)
  - TimestampTz (data type)
  - XLogRecPtr (data type)
- Called from (representative examples):
  - ParsePrepareRecord (in xactdesc.c)
  - xact_desc_prepare (in xactdesc.c)
  - TwoPhaseFileHeader (in twophase.c)
  - xact_decode (in decode.c)

## Notes and Other Information
This structure is central to PostgreSQL's two-phase commit implementation, allowing transactions to be prepared on multiple nodes and later committed or aborted atomically. The comprehensive metadata ensures that all transaction effects (including sub-transactions, relation cleanup, and cache invalidation) can be properly applied or rolled back even after system restarts. The origin tracking fields support distributed transaction scenarios in logical replication setups.