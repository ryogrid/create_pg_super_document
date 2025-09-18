# logicalrep_read_stream_commit

## Location
src/backend/replication/logical/proto.c: 1137 - 1165

## Overview
Reads and parses a stream commit message from the logical replication input stream to extract transaction commit information.

## Definition
TransactionId logicalrep_read_stream_commit(StringInfo in, LogicalRepCommitData *commit_data)

## Detailed Description
This function is the counterpart to logicalrep_write_stream_commit and is used by logical replication subscribers to parse LOGICAL_REP_MSG_STREAM_COMMIT messages. It extracts the transaction ID and commit metadata from the incoming replication stream and populates the provided LogicalRepCommitData structure.

The function performs validation by checking that the flags field contains only recognized values (currently must be 0). If unrecognized flags are encountered, it raises an error to ensure protocol compatibility.

## Parameters / Member Variables
- `in`: StringInfo buffer containing the incoming stream commit message to parse
- `commit_data`: LogicalRepCommitData structure to be populated with commit LSN, end LSN, and commit timestamp

## Dependencies
- Functions called/Symbols referenced:
  - pq_getmsgint
  - pq_getmsgbyte
  - pq_getmsgint64
  - elog (for error reporting)
  - LogicalRepCommitData (data structure)
- Called from (representative examples):
  - apply_handle_stream_commit

## Notes and Other Information
- Returns the transaction ID extracted from the message
- Validates that flags field is 0, raising an ERROR for unrecognized flag values
- Populates commit_data with commit_lsn, end_lsn, and committime fields
- Part of the logical replication subscriber-side message parsing
- Used to reconstruct transaction commit information on the subscriber side
- Corresponds to the data written by logicalrep_write_stream_commit on the publisher side
- Located in src/backend/replication/logical/proto.c:1137-1165