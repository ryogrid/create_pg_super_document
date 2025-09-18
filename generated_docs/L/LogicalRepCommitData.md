# LogicalRepCommitData

## Location
[src/include/replication/logicalproto.h:134-139](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/replication/logicalproto.h#L134-L139)

## Overview
LogicalRepCommitData is a structure that contains transaction commit information for logical replication, providing essential metadata needed to complete transaction processing.

## Definition


## Detailed Description
This structure encapsulates the critical information needed to complete a transaction in logical replication. It serves as a transaction footer that provides the subscriber with essential metadata about the transaction that has just been replicated. The structure contains LSN (Log Sequence Number) information that marks both the commit point and the end of the transaction in the WAL, along with the commit timestamp.

The commit_lsn represents the exact point where the transaction was committed, while end_lsn marks the end of the transaction's WAL records. This distinction is important for proper WAL processing and ensures that all transaction data has been fully processed. The commit timestamp provides temporal information that can be used for ordering, conflict resolution, and debugging purposes.

## Parameters / Member Variables
- : XLogRecPtr representing the LSN where this transaction was committed in the WAL
- : XLogRecPtr representing the LSN marking the end of this transaction's WAL records
- : TimestampTz indicating the timestamp when the transaction was committed

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecPtr (PostgreSQL log sequence number type)
  - TimestampTz (PostgreSQL timestamp with timezone type)
- Called from (representative examples):
  - [logicalrep_read_commit](../l/logicalrep_read_commit.md)
  - [logicalrep_read_stream_commit](../l/logicalrep_read_stream_commit.md)
  - [apply_handle_commit](../a/apply_handle_commit.md)
  - [apply_handle_stream_commit](../a/apply_handle_stream_commit.md)
  - [apply_handle_commit_internal](../a/apply_handle_commit_internal.md)

## Notes and Other Information
- This structure marks the end of a transaction in the logical replication stream
- The distinction between commit_lsn and end_lsn is important for proper WAL processing
- The commit timestamp can be used for temporal ordering and conflict resolution
- This structure is used for both regular commits and streaming transaction commits
- Proper handling of this structure is essential for maintaining transaction consistency
- Located in src/include/replication/logicalproto.h:134-139