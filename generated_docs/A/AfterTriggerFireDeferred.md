# AfterTriggerFireDeferred

## Location
[src/backend/commands/trigger.c:5284-5339](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/trigger.c#L5284-L5339)

## Overview
Executes all pending deferred triggers at transaction commit time, ensuring proper snapshot management and handling recursive trigger queuing.

## Definition
```c
void AfterTriggerFireDeferred(void)
```

## Detailed Description
AfterTriggerFireDeferred is called just before transaction commit to execute all pending DEFERRED triggers. The function ensures proper snapshot management by establishing a transaction snapshot for trigger execution, then processes all deferred triggers in a loop to handle cases where triggers queue additional deferred triggers.

The function implements careful snapshot management since COMMIT operations don't automatically set an active snapshot. It runs triggers in a loop until all are processed, incrementing the firing counter for each batch to maintain proper trigger ordering and execution semantics.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - [AfterTriggerEventList](AfterTriggerEventList.md) (event list structure)
  - [GetTransactionSnapshot](../G/GetTransactionSnapshot.md) (obtains current transaction snapshot)
  - [PushActiveSnapshot](../P/PushActiveSnapshot.md) (establishes active snapshot)
  - [afterTriggerMarkEvents](../a/afterTriggerMarkEvents.md) (marks events for processing)
  - CommandId (type for firing counter)
  - [afterTriggerInvokeEvents](../a/afterTriggerInvokeEvents.md) (executes trigger events)
  - [PopActiveSnapshot](../P/PopActiveSnapshot.md) (removes active snapshot)
- Called from:
  - [CommitTransaction](../C/CommitTransaction.md) (in src/backend/access/transam/xact.c:2212)
  - [PrepareTransaction](../P/PrepareTransaction.md) (in src/backend/access/transam/xact.c:2490)

## Notes and Other Information
- Must not be called while inside a query (enforced by assertion checking query_depth == -1)
- May be called multiple times during transaction commit if other modules queue additional deferred triggers
- Manages snapshots carefully since COMMIT doesn't automatically establish an active snapshot
- Uses a loop to handle recursive trigger queuing where triggers create more deferred triggers
- Does not free the event list since it will be cleaned up more efficiently in AfterTriggerEndXact
- Critical component of PostgreSQL's deferred constraint checking and trigger execution system
- Called during both normal commit and two-phase commit preparation

## Simplified Source

```c
// Simplified version of AfterTriggerFireDeferred
void AfterTriggerFireDeferred(void) {
    AfterTriggerEventList *events;
    bool snap_pushed = false;

    // Must not be inside a query
    Assert(afterTriggers.query_depth == -1);

    // Set up snapshot for trigger execution if needed
    events = &afterTriggers.events;
    if (events->head != NULL) {
        PushActiveSnapshot(GetTransactionSnapshot());
        snap_pushed = true;
    }

    // Execute all deferred triggers in a loop
    // Loop handles cases where triggers queue more triggers
    while (afterTriggerMarkEvents(events, NULL, false)) {
        CommandId firing_id = afterTriggers.firing_counter++;

        if (afterTriggerInvokeEvents(events, firing_id, NULL, true))
            break;  // all fired
    }

    // Clean up snapshot if we pushed one
    if (snap_pushed)
        PopActiveSnapshot();
}
```

Key simplifications made:
- Preserved the core logic flow: snapshot setup, trigger loop, cleanup
- Kept essential assertions and safety checks
- Maintained the recursive trigger handling loop structure
- Focused on the main execution path without implementation details