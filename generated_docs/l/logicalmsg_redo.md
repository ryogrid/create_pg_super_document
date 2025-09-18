# logicalmsg_redo

## Location
[src/backend/replication/logical/message.c:87-95](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/message.c#L87-L95)

## Overview
logicalmsg_redo is a WAL redo function that handles replay of logical message records during PostgreSQL recovery, though it performs minimal operations since logical messages are primarily processed during logical decoding.

## Definition


## Detailed Description
This function is called during WAL replay to handle XLOG_LOGICAL_MESSAGE records. It serves as a no-operation redo function because logical messages don't modify the database state directly - they are primarily consumed by logical decoding plugins during logical replication. The function validates that the WAL record has the correct operation code (XLOG_LOGICAL_MESSAGE) and panics if an unknown operation code is encountered, but otherwise performs no actual replay work since the logical messages are handled separately by the logical decoding infrastructure.

## Parameters / Member Variables
- `record`: Pointer to XLogReaderState containing the WAL record being replayed

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetInfo
  - XLR_INFO_MASK
  - XLOG_LOGICAL_MESSAGE
  - PANIC
- Called from (representative examples):
  - WAL replay infrastructure (indirectly through XLOG_LOGICAL_MESSAGE resource manager)

## Notes and Other Information
- This is essentially a no-op function for WAL replay purposes
- The actual processing of logical messages happens in logical decoding (see decode.c)
- The function validates the operation code to ensure WAL record integrity
- Logical messages are stored in WAL but don't affect physical database state during recovery
- Part of the logical message resource manager (RM_LOGICALMSG_ID) in the WAL replay system