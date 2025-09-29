# CallXactCallbacks

## Location
[src/backend/access/transam/xact.c:3787-3812](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xact.c#L3787-L3812)

## Overview
Internal function that invokes all registered transaction callbacks when transaction events occur.

## Definition
static void CallXactCallbacks(XactEvent event)

## Detailed Description
This static function iterates through the linked list of registered transaction callbacks and invokes each one with the specified transaction event. It is called internally by PostgreSQL transaction management functions during various transaction lifecycle events such as commit, abort, prepare, and pre-commit phases.

The function includes a safety mechanism to handle callbacks that may unregister themselves during execution. It stores the next pointer before calling each callback to prevent issues with list modification during iteration.

The function is responsible for propagating transaction events to all registered callbacks, allowing extensions and modules to respond appropriately to transaction state changes.

## Parameters / Member Variables
- event: XactEvent enum value indicating the type of transaction event (XACT_EVENT_COMMIT, XACT_EVENT_ABORT, XACT_EVENT_PREPARE, XACT_EVENT_PRE_COMMIT, XACT_EVENT_PARALLEL_COMMIT, XACT_EVENT_PARALLEL_ABORT, XACT_EVENT_PARALLEL_PRE_COMMIT, XACT_EVENT_PRE_PREPARE)

## Dependencies
- Functions called/Symbols referenced:
  - XactEvent (enum type for event classification)
  - [XactCallbackItem](../X/XactCallbackItem.md) (structure type for callback list items)
  - Xact_callbacks (global linked list head)
  - [callback](../c/callback.md) function (invoked on each registered callback)
- Called from (representative examples):
  - [CommitTransaction](CommitTransaction.md) (src/backend/access/transam/xact.c:2230, 2357)
  - [PrepareTransaction](../P/PrepareTransaction.md) (src/backend/access/transam/xact.c:2501, 2661)
  - [AbortTransaction](../A/AbortTransaction.md) (src/backend/access/transam/xact.c:2901, 2903)

## Notes and Other Information
- Function is static (internal to xact.c) and not directly accessible to external code
- Safely handles callbacks that unregister themselves during execution by caching the next pointer
- Called at different phases of transaction processing to notify registered callbacks
- Each callback receives both the event type and its registered argument pointer
- Multiple calls may occur during a single transaction for different event types
- Critical for allowing extensions to participate in transaction lifecycle management

## Simplified Source

```c
// Simplified version of CallXactCallbacks
static void CallXactCallbacks(XactEvent event) {
    XactCallbackItem *item;
    XactCallbackItem *next;

    // Iterate through all registered transaction callbacks
    for (item = Xact_callbacks; item; item = next) {
        // Cache next pointer before callback execution
        // (callbacks may unregister themselves)
        next = item->next;

        // Execute the callback with the event and its argument
        item->callback(event, item->arg);
    }
}
```

Key simplifications made:
- Added descriptive comments explaining the core logic
- Clarified the purpose of caching the next pointer
- Maintained the essential algorithm structure
- Preserved the safety mechanism for self-unregistering callbacks