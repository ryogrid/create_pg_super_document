# ExecAppendAsyncEventWait

## Location
src/backend/executor/nodeAppend.c: 1017 - 1126

## Overview
Waits or polls for file descriptor events and fires callbacks for asynchronous subplans in an Append node execution context.

## Definition
```c
static void ExecAppendAsyncEventWait(AppendState *node)
```

## Detailed Description
This function manages asynchronous event handling for Append node execution in PostgreSQL's executor. It creates a wait event set to monitor file descriptor events from async subplans and processes any events that occur. The function supports both blocking waits (when sync operations are done) and non-blocking polls (when sync operations are still active).

The function sets up a wait event set, allows each async subplan to register its events, then waits for events to occur. When events are detected, it dispatches appropriate callbacks to handle the asynchronous responses from subplans.

## Parameters / Member Variables
- `node`: AppendState structure containing the state of the Append node, including async subplan information, event sets, and callback management

## Dependencies
- Functions called/Symbols referenced:
  - [CreateWaitEventSet](../C/CreateWaitEventSet.md)
  - [AddWaitEventToSet](../A/AddWaitEventToSet.md)  
  - ExecAsyncConfigureWait
  - GetNumRegisteredWaitEvents
  - WaitEventSetWait
  - ExecAsyncNotify
  - [FreeWaitEventSet](../F/FreeWaitEventSet.md)
  - [bms_next_member](../b/bms_next_member.md)
  - [ResetLatch](../R/ResetLatch.md)
  - CHECK_FOR_INTERRUPTS
- Called from (representative examples):
  - [ExecAppend](ExecAppend.md)
  - [ExecAppendAsyncGetNext](ExecAppendAsyncGetNext.md)

## Notes and Other Information
- The function asserts that there are valid async subplans remaining before proceeding
- Uses a timeout value: -1 for blocking wait when sync operations are done, 0 for non-blocking poll otherwise
- Handles both socket events (WL_SOCKET_READABLE) and latch events (WL_LATCH_SET) for interrupt processing
- The process latch is added after subplan events for backward compatibility with postgres_fdw extension
- Returns early if no subplans configure any events to avoid unnecessary waiting
- Limited to EVENT_BUFFER_SIZE events per call to prevent buffer overflow