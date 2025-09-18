# Async_Notify

## Location
src/backend/commands/async.c: 591 - 689

## Overview
Executes the SQL NOTIFY command by adding notification messages to the list of pending notifications, with actual notification delivery occurring during transaction commit.

## Definition


## Detailed Description
Async_Notify is the core function that implements PostgreSQL's asynchronous notification system. When a SQL NOTIFY command is executed, this function validates the input parameters, creates a Notification structure, and adds it to the pending notifications list. The function manages notifications hierarchically across transaction nesting levels, ensuring proper cleanup and delivery semantics. It enforces length limits on channel names and payload data, prevents duplicate notifications, and handles memory context switching to ensure notifications persist until transaction end.

## Parameters / Member Variables
- : The notification channel name (must be non-empty and less than NAMEDATALEN characters)
- : Optional message payload (must be less than NOTIFY_PAYLOAD_MAX_LENGTH characters, can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - GetCurrentTransactionNestLevel
  - IsParallelWorker
  - MemoryContextSwitchTo
  - MemoryContextAlloc
  - AsyncExistsPendingNotify
  - AddEventToPendingNotifies
  - Notification (struct)
  - NotificationList (struct)
- Called from (representative examples):
  - pg_notify
  - standard_ProcessUtility

## Notes and Other Information
- Prevents execution from parallel workers due to shared memory constraints
- Uses CurTransactionContext for notification storage to ensure proper lifetime
- Implements deduplication to prevent duplicate notifications
- Creates hierarchical notification lists based on transaction nesting levels
- Validates channel name length against NAMEDATALEN limit
- Validates payload length against NOTIFY_PAYLOAD_MAX_LENGTH limit
- Actual notification delivery is deferred until transaction commit