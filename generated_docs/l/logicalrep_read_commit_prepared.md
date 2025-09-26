# logicalrep_read_commit_prepared

## Location
src/backend/replication/logical/proto.c: 278 - 303

## Overview
Reads a COMMIT PREPARED message from the logical replication stream and populates the provided LogicalRepCommitPreparedTxnData structure with the commit information.

## Definition
```c
void logicalrep_read_commit_prepared(StringInfo in, LogicalRepCommitPreparedTxnData *prepare_data)
```

## Detailed Description
This function parses a COMMIT PREPARED message from the logical replication protocol stream, extracting the commit information for a two-phase transaction that has been committed. It reads the binary message format and populates the provided data structure with commit LSN, end LSN, commit timestamp, transaction ID, and global identifier (GID). The function includes validation to ensure required fields are properly set and flags are recognized.

## Parameters / Member Variables
- `in`: StringInfo buffer containing the incoming binary message data from the replication stream
- `prepare_data`: Pointer to LogicalRepCommitPreparedTxnData structure that will be populated with the parsed commit prepared transaction information

## Dependencies
- Functions called/Symbols referenced:
  - pq_getmsgbyte
  - pq_getmsgint64
  - pq_getmsgint
  - pq_getmsgstring
  - strlcpy
- Called from (representative examples):
  - apply_handle_commit_prepared

## Notes and Other Information
- This function is part of PostgreSQL's logical replication protocol implementation for two-phase commit support
- It validates that flags are zero (currently no flags are defined for commit prepared messages)
- Performs validation checks to ensure commit_lsn and end_lsn are valid (not InvalidXLogRecPtr)
- The GID is copied into a pre-allocated buffer with size checking via strlcpy
- Located in src/backend/replication/logical/proto.c:278-303
- Used by logical replication workers to process commit prepared messages during two-phase commit operations