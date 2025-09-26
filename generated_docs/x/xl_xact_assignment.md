# xl_xact_assignment

## Location
[src/include/access/xact.h:218-223](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/xact.h#L218-L223)

## Overview
WAL record structure used to log transaction ID assignments for subtransactions to limit shared memory requirements on hot standby servers.

## Definition
```c
typedef struct xl_xact_assignment
{
    TransactionId xtop;         /* assigned XID's top-level XID */
    int           nsubxacts;    /* number of subtransaction XIDs */
    TransactionId xsub[FLEXIBLE_ARRAY_MEMBER]; /* assigned subxids */
} xl_xact_assignment;
```

## Detailed Description
xl_xact_assignment is a WAL record structure that tracks the assignment of transaction IDs to subtransactions. This structure is critical for hot standby functionality, as it limits the amount of shared memory required on standby servers to track in-progress transaction IDs.

When subtransactions are created and assigned XIDs, PostgreSQL needs to communicate these assignments to standby servers. Rather than tracking every individual assignment, the system batches them and writes xl_xact_assignment records to the WAL. This occurs when either PGPROC_MAX_CACHED_SUBXIDS subtransaction IDs have been assigned, or when logical replication requires logging an unknown top-level transaction.

The structure contains the top-level transaction ID and an array of subtransaction IDs that have been assigned but not yet reported to standby servers. This design ensures that standby servers can maintain consistent views of transaction hierarchies without excessive memory overhead.

## Parameters / Member Variables
- `xtop`: The top-level transaction ID that all subtransactions in this record belong to
- `nsubxacts`: The number of subtransaction IDs included in the xsub array
- `xsub`: A flexible array containing the actual subtransaction IDs that were assigned

## Dependencies
- Functions called/Symbols referenced:
  - FLEXIBLE_ARRAY_MEMBER
  - TransactionId (type)
- Called from (representative examples):
  - [AssignTransactionId](../A/AssignTransactionId.md) (creates and logs these records)
  - [xact_desc_assignment](xact_desc_assignment.md) (describes the record for debugging)
  - [xact_redo](xact_redo.md) (processes the record during recovery)
  - MinSizeOfXactAssignment (macro for calculating minimum size)

## Notes and Other Information
- Located in src/include/access/xact.h:218-223
- Used specifically for hot standby support to limit memory usage on standby servers
- Records are written to WAL with XLOG_XACT_ASSIGNMENT resource manager ID
- The structure uses a flexible array member to accommodate variable numbers of subtransaction IDs
- MinSizeOfXactAssignment macro calculates the base size excluding the flexible array
- Critical for maintaining transaction consistency during recovery and replication scenarios
- Only subtransaction assignments are logged; the parent-child relationships are inferred during recovery