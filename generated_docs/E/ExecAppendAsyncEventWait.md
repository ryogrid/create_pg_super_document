# ExecAppendAsyncEventWait

## Location
[src/backend/executor/nodeAppend.c:1017-1126](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeAppend.c#L1017-L1126)

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
  - [ExecAsyncConfigureWait](ExecAsyncConfigureWait.md)
  - [GetNumRegisteredWaitEvents](../G/GetNumRegisteredWaitEvents.md)
  - [WaitEventSetWait](../W/WaitEventSetWait.md)
  - [ExecAsyncNotify](ExecAsyncNotify.md)
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

## Simplified Source

```c
static void
ExecAppendAsyncEventWait(AppendState *node)
{
    int nevents = node->as_nasyncplans + 2;
    long timeout = node->as_syncdone ? -1 : 0; // Block if sync done, poll otherwise
    WaitEvent occurred_event[EVENT_BUFFER_SIZE];
    int noccurred;

    Assert(node->as_nasyncremain > 0);

    // Create wait event set for all potential events
    Assert(node->as_eventset == NULL);
    node->as_eventset = CreateWaitEventSet(CurrentResourceOwner, nevents);
    AddWaitEventToSet(node->as_eventset, WL_EXIT_ON_PM_DEATH, PGINVALID_SOCKET, NULL, NULL);

    // Let each async subplan configure its events
    int i = -1;
    while ((i = bms_next_member(node->as_asyncplans, i)) >= 0)
    {
        AsyncRequest *areq = node->as_asyncrequests[i];
        if (areq->callback_pending)
            ExecAsyncConfigureWait(areq);
    }

    // Skip waiting if no events were configured
    if (GetNumRegisteredWaitEvents(node->as_eventset) == 1)
    {
        FreeWaitEventSet(node->as_eventset);
        node->as_eventset = NULL;
        return;
    }

    // Add process latch for interrupt handling (must be after subplan events)
    AddWaitEventToSet(node->as_eventset, WL_LATCH_SET, PGINVALID_SOCKET, MyLatch, NULL);

    // Limit events to buffer size
    if (nevents > EVENT_BUFFER_SIZE)
        nevents = EVENT_BUFFER_SIZE;

    // Wait for events (block or poll based on timeout)
    noccurred = WaitEventSetWait(node->as_eventset, timeout, occurred_event,
                                nevents, WAIT_EVENT_APPEND_READY);

    // Cleanup wait event set
    FreeWaitEventSet(node->as_eventset);
    node->as_eventset = NULL;

    if (noccurred == 0)
        return;

    // Process occurred events
    for (int i = 0; i < noccurred; i++)
    {
        WaitEvent *w = &occurred_event[i];

        // Handle socket readability events
        if ((w->events & WL_SOCKET_READABLE) != 0)
        {
            AsyncRequest *areq = (AsyncRequest *) w->user_data;
            if (areq->callback_pending)
            {
                areq->callback_pending = false;
                ExecAsyncNotify(areq); // Dispatch callback
            }
        }

        // Handle interrupt events
        if ((w->events & WL_LATCH_SET) != 0)
        {
            ResetLatch(MyLatch);
            CHECK_FOR_INTERRUPTS();
        }
    }
}
```