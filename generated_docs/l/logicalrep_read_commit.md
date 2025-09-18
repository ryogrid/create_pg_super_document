# logicalrep_read_commit

## Location
[src/backend/replication/logical/proto.c:109-126](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/proto.c#L109-L126)

## Overview
Reads and parses a COMMIT message from the logical replication input stream to extract transaction completion information.

## Definition


## Detailed Description
This function deserializes a COMMIT message from the logical replication protocol stream. It extracts transaction completion metadata including the commit LSN, end LSN, and commit timestamp from the input buffer and populates a LogicalRepCommitData structure. The function includes validation for the flags field, ensuring that no unrecognized flags are present (currently all flags should be 0).

This is the counterpart to logicalrep_write_commit and is used by logical replication subscribers to process transaction commit messages received from publishers.

## Parameters / Member Variables
- : StringInfo buffer containing the serialized COMMIT message data to be read
- : LogicalRepCommitData structure that will be populated with the extracted transaction completion information

## Dependencies
- Functions called/Symbols referenced:
  - [pq_getmsgbyte](../p/pq_getmsgbyte.md) (reads a single byte from the input buffer)
  - [pq_getmsgint64](../p/pq_getmsgint64.md) (reads a 64-bit integer from the input buffer)
  - [LogicalRepCommitData](../L/LogicalRepCommitData.md) (structure type for storing COMMIT message data)
  - elog (logging/error reporting function)
- Called from (representative examples):
  - [apply_handle_commit](../a/apply_handle_commit.md) (in the logical replication worker for processing COMMIT messages)

## Notes and Other Information
- This function is part of the logical replication protocol implementation for subscribers
- Includes validation to ensure no unrecognized flags are present, throwing an error if any non-zero flags are encountered
- The function expects the input buffer to contain data in the exact format written by logicalrep_write_commit
- Used by logical replication workers to process transaction commit boundaries from publishers
- Located in src/backend/replication/logical/proto.c as part of the protocol decoding functions
- Throws an ERROR if unrecognized flags are present, ensuring protocol compatibility
- The flags field is currently unused but reserved for future protocol extensions