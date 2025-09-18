# CommandEndInvalidationMessages

## Location
src/backend/utils/cache/inval.c: 1173 - 1206

## Overview
Processes queued invalidation messages at the end of one command within a transaction, performing local cache invalidation and preparing messages for potential commit.

## Definition


## Detailed Description
This function is called during CommandCounterIncrement(), after the command ID has been advanced, to handle invalidation messages generated during the just-completed command. Unlike transaction-end processing, this function does not send messages to the shared invalidation queue since the transaction outcome is still unknown.

The function performs local processing of CurrentCmdInvalidMsgs to flush caches of entries that were outdated during the current command. This ensures that subsequent commands within the same transaction see consistent cache state. After local processing, it moves the current command's invalidation messages to the prior-commands list, where they will accumulate until transaction end.

For logical replication (when wal_level=logical), the function also logs the invalidation messages to WAL via LogLogicalInvalidations(), ensuring that logical replication consumers can properly maintain their cache consistency.

The function includes safeguards to handle calls outside transactions, which can occur during bootstrap operations or when ABORT is issued outside a transaction context.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - ProcessInvalidationMessages
  - LocalExecuteInvalidationMessage
  - XLogLogicalInfoActive
  - LogLogicalInvalidations
  - AppendInvalidationMessages
- Called from (representative examples):
  - AtCCI_LocalCache
  - AtEOSubXact_Inval

## Notes and Other Information
- Called specifically during CommandCounterIncrement() after command ID advancement
- Does not send messages to shared invalidation queue since transaction outcome is unknown
- Handles calls outside transaction context gracefully (for bootstrap and error cases)
- Includes WAL logging for logical replication when wal_level=logical is active
- Moves processed messages from CurrentCmdInvalidMsgs to PriorCmdInvalidMsgs for later transaction-end processing
- Essential for maintaining cache consistency within multi-command transactions
- Part of the PostgreSQL command counter mechanism that tracks command boundaries within transactions