# CallXactCallbacks

## Location
src/backend/access/transam/xact.c: 3787 - 3812

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