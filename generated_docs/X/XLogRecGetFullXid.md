# XLogRecGetFullXid

## Location
[src/backend/access/transam/xlogreader.c:2177-2189](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogreader.c#L2177-L2189)

## Overview
Extracts the full 64-bit transaction ID from a WAL record, extending the 32-bit XID stored in the record to its complete form.

## Definition
```c
FullTransactionId XLogRecGetFullXid(XLogReaderState *record)
```

## Detailed Description
XLogRecGetFullXid is a utility function that constructs a complete 64-bit FullTransactionId from the 32-bit transaction ID stored in a WAL record. This is necessary because WAL records only store the lower 32 bits of transaction IDs to save space, but the full 64-bit value is needed for proper transaction processing during replay.

The function uses the current replay state (TransamVariables->nextXid) to determine the correct epoch (upper 32 bits) that should be combined with the 32-bit XID from the record. This reconstruction is only safe during WAL replay when the transaction state is being maintained properly.

The function includes safety assertions to ensure it is only called during startup recovery or in processes that are not under the postmaster, as the transaction state required for correct operation is only available in these contexts.

## Parameters / Member Variables
- `record`: Pointer to XLogReaderState containing the WAL record from which to extract the transaction ID

## Dependencies
- Functions called/Symbols referenced:
  - AmStartupProcess (process identification function)
  - [FullTransactionIdFromAllowableAt](../F/FullTransactionIdFromAllowableAt.md) (transaction ID reconstruction function)
  - XLogRecGetXid (function to extract 32-bit XID from record)
  - TransamVariables->nextXid (global transaction state)
- Called from (representative examples):
  - XLogRecHasBlockData (indirect usage in header)

## Notes and Other Information
- Only safe to call during WAL replay when proper transaction state is maintained
- Critical for maintaining transaction ID continuity across the 32-bit wraparound boundary
- The assertion ensures the function is not misused in contexts where transaction state is unreliable
- Essential component of PostgreSQL XID handling that prevents transaction ID confusion during recovery
- The function bridges the gap between space-efficient WAL storage and complete transaction identification