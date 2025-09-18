# ProcessNotifyInterrupt

## Location
[src/backend/commands/async.c:1834-1850](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/async.c#L1834-L1850)

## Overview
Processes pending NOTIFY interrupts by checking transaction state and calling ProcessIncomingNotify() when the backend is truly idle outside of any transaction block.

## Definition
```c
void ProcessNotifyInterrupt(bool flush)
```

## Detailed Description
This function handles the actual processing of NOTIFY interrupts that were flagged by HandleNotifyInterrupt(). It serves as the safe, non-signal-handler context where the real work of processing incoming notifications can be performed.

The function implements a critical safety check: it only processes notifications when the backend is truly idle, meaning it's not inside any transaction or transaction block. This prevents notifications from interfering with ongoing transaction processing and ensures consistent behavior.

When notifications are processed, the function uses a loop to handle cases where additional signals may arrive while messages are being sent to the frontend. This ensures all pending notifications are processed in a single call.

The function is called in two main scenarios:
1. At the end of a frontend command, just before transmitting ReadyForQuery
2. When a notify signal interrupts reading from the frontend (via HandleNotifyInterrupt)

## Parameters / Member Variables
- `flush`: If true, forces immediate transmission of frontend messages. Can be false when called at the end of a frontend command since ReadyForQuery will trigger a flush.

## Dependencies
- Functions called/Symbols referenced:
  - [IsTransactionOrTransactionBlock](../I/IsTransactionOrTransactionBlock.md)
  - [ProcessIncomingNotify](ProcessIncomingNotify.md)
- Called from (representative examples):
  - [ProcessClientReadInterrupt](ProcessClientReadInterrupt.md)
  - [PostgresMain](PostgresMain.md)

## Notes and Other Information
- **Transaction Safety**: Only processes notifications when outside transaction blocks to avoid interference with ongoing transactions
- **Loop Processing**: Uses a while loop to handle multiple pending notifications that may arrive during processing
- **Flush Control**: The flush parameter allows callers to control immediate message transmission based on their context
- Works in conjunction with HandleNotifyInterrupt() to provide signal-safe interrupt handling
- Part of PostgreSQL's asynchronous notification system for LISTEN/NOTIFY functionality
- Located in src/backend/commands/async.c:1834-1850