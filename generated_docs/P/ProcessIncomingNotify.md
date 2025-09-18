# ProcessIncomingNotify

## Location
src/backend/commands/async.c: 2183 - 2232

## Overview
Processes incoming notifications by scanning the shared queue and delivering relevant notifications to the frontend within a dedicated transaction context.

## Definition
```c
static void ProcessIncomingNotify(bool flush)
```

## Detailed Description
This function handles the processing of incoming notifications when a notification interrupt occurs. It operates outside of any existing transaction context and must create its own transaction to safely read from the notification queue. The function serves as a transaction wrapper around asyncQueueReadAllNotifications, ensuring proper error handling and memory context preservation.

The function first checks if the backend is actively listening on any channels before proceeding. If notifications are present, it creates a transaction context, reads all available notifications, commits the transaction, and optionally flushes messages to the frontend for immediate delivery.

## Parameters / Member Variables
- `flush`: Boolean flag indicating whether to immediately flush frontend messages after processing notifications

## Dependencies
- Functions called/Symbols referenced:
  - set_ps_display: Updates process status display
  - StartTransactionCommand: Initiates transaction for safe queue reading
  - asyncQueueReadAllNotifications: Core function that reads and processes notifications
  - CommitTransactionCommand: Commits the transaction after processing
  - MemoryContextIsValid/MemoryContextSwitchTo: Manages memory context preservation
  - pq_flush: Forces immediate frontend message delivery
  - elog: Debug logging functionality
- Called from:
  - ProcessNotifyInterrupt: Main interrupt handler for notification processing

## Notes and Other Information
- Must reset notifyInterruptPending flag to prevent repeated processing
- Returns early if no channels are being listened to (listenChannels == NIL)
- Preserves caller's memory context across transaction boundaries
- Uses dedicated transaction to ensure proper error handling during queue operations
- Updates process status to "notify interrupt" during processing and "idle" when complete
- Conditional flushing allows optimization for end-of-command vs. interrupt-driven scenarios
- Includes debug tracing support via Trace_notify flag