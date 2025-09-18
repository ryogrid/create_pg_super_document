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
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - [AfterTriggerEventList](AfterTriggerEventList.md) (event list structure)
  - GetTransactionSnapshot (obtains current transaction snapshot)
  - PushActiveSnapshot (establishes active snapshot)
  - [afterTriggerMarkEvents](../a/afterTriggerMarkEvents.md) (marks events for processing)
  - CommandId (type for firing counter)
  - [afterTriggerInvokeEvents](../a/afterTriggerInvokeEvents.md) (executes trigger events)
  - PopActiveSnapshot (removes active snapshot)
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