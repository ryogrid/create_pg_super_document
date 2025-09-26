# logicalrep_read_rollback_prepared

## Location
[src/backend/replication/logical/proto.c:336-363](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/proto.c#L336-L363)

## Overview
Reads a ROLLBACK PREPARED message from the logical replication stream and populates the provided LogicalRepRollbackPreparedTxnData structure with the rollback information.

## Definition
```c
void logicalrep_read_rollback_prepared(StringInfo in, LogicalRepRollbackPreparedTxnData *rollback_data)
```

## Detailed Description
This function parses a ROLLBACK PREPARED message from the logical replication protocol stream, extracting the rollback information for a two-phase transaction that has been rolled back. It reads the binary message format and populates the provided data structure with prepare end LSN, rollback end LSN, prepare timestamp, rollback timestamp, transaction ID, and global identifier (GID). The function includes validation to ensure required LSN fields are properly set and flags are recognized. The prepare information is particularly important for downstream nodes to determine whether they have received the original prepared transaction.

## Parameters / Member Variables
- `in`: StringInfo buffer containing the incoming binary message data from the replication stream
- `rollback_data`: Pointer to LogicalRepRollbackPreparedTxnData structure that will be populated with the parsed rollback prepared transaction information

## Dependencies
- Functions called/Symbols referenced:
  - pq_getmsgbyte
  - pq_getmsgint64
  - pq_getmsgint
  - pq_getmsgstring
  - strlcpy
- Called from (representative examples):
  - apply_handle_rollback_prepared

## Notes and Other Information
- This function is part of PostgreSQL's logical replication protocol implementation for two-phase commit support
- It validates that flags are zero (currently no flags are defined for rollback prepared messages)
- Performs validation checks to ensure prepare_end_lsn and rollback_end_lsn are valid (not InvalidXLogRecPtr)
- The prepare information (prepare_end_lsn and prepare_time) allows downstream nodes to determine if they received the original prepared transaction
- The GID alone is insufficient for identification since downstream nodes can have prepared transactions with the same identifier
- The GID is copied into a pre-allocated buffer with size checking via strlcpy
- Located in src/backend/replication/logical/proto.c:336-363
- Used by logical replication workers to process rollback prepared messages during two-phase commit operations