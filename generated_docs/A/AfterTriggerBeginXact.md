# AfterTriggerBeginXact

## Location
[src/backend/commands/trigger.c:5073-5104](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/trigger.c#L5073-L5104)

## Overview
Initializes the after-trigger system state at the beginning of a transaction, setting up the necessary data structures for deferred trigger execution.

## Definition

```c
structure to empty
	 */
	afterTriggers.firing_counter = (CommandId) 1;
```
## Detailed Description
AfterTriggerBeginXact is called at transaction start (either explicit BEGIN or implicit for single statements outside transaction blocks) to initialize the after-trigger state structure. This function sets up the firing counter and query depth, and performs assertions to verify that no leftover state exists from previous transactions.

The function initializes the firing_counter to 1 (must not be 0) and sets the query_depth to -1. It also includes several assertions that verify the after-trigger system is in a clean state, checking that various components (state, query_stack, event_cxt, events.head, trans_stack) are NULL and depth counters are zero.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - CommandId (type used for firing_counter)
  - Assert (for state verification)
- Called from:
  - [StartTransaction](../S/StartTransaction.md) (in src/backend/access/transam/xact.c:2156)

## Notes and Other Information
- The firing_counter is initialized to 1 rather than 0 because 0 has special meaning in the trigger system
- The function includes comprehensive assertions to detect programming errors where the previous transaction didn't clean up properly via AfterTriggerEndXact
- This is part of PostgreSQL's deferred trigger mechanism that allows triggers to be executed at transaction commit time rather than immediately after the triggering event

## Simplified Source

```c
// Simplified version of AfterTriggerBeginXact
void AfterTriggerBeginXact(void) {
    // Initialize after-trigger state structure
    afterTriggers.firing_counter = (CommandId) 1;  // Must not be 0
    afterTriggers.query_depth = -1;

    // Verify clean state (detect cleanup bugs)
    Assert(afterTriggers.state == NULL);
    Assert(afterTriggers.query_stack == NULL);
    Assert(afterTriggers.maxquerydepth == 0);
    Assert(afterTriggers.event_cxt == NULL);
    Assert(afterTriggers.events.head == NULL);
    Assert(afterTriggers.trans_stack == NULL);
    Assert(afterTriggers.maxtransdepth == 0);
}
```

Key simplifications made:
- Function is already simple, just initializes trigger state
- Maintains comprehensive assertions for state validation
- Essential setup for deferred trigger execution system