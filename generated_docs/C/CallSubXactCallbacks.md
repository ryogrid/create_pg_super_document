# CallSubXactCallbacks

## Location
[src/backend/access/transam/xact.c:3847-3872](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xact.c#L3847-L3872)

## Overview
CallSubXactCallbacks iterates through all registered subtransaction callback functions and invokes them with specified subtransaction event information, enabling modules to respond to subtransaction lifecycle events.

## Definition

```c
static void
CallSubXactCallbacks(SubXactEvent event,
					 SubTransactionId mySubid,
					 SubTransactionId parentSubid)
```
## Detailed Description
This internal function traverses the linked list of registered subtransaction callbacks (SubXact_callbacks) and calls each callback function with the provided event information. The function is designed to be safe against callbacks that unregister themselves during execution by capturing the next pointer before making each callback invocation. This ensures that the iteration continues correctly even if the current callback item is removed from the list during the callback execution. The function is called at key points in subtransaction lifecycle management to notify interested modules of subtransaction events.

## Parameters / Member Variables
- `event`: SubXactEvent enumeration value indicating the type of subtransaction event (START, COMMIT, ABORT, etc.)
- `mySubid`: SubTransactionId of the subtransaction for which the event is being reported
- `parentSubid`: SubTransactionId of the parent subtransaction

## Dependencies
- Functions called/Symbols referenced:
  - [SubXactEvent](../S/SubXactEvent.md) (enumeration type)
  - SubTransactionId (type definition)
  - [SubXactCallbackItem](../S/SubXactCallbackItem.md) (structure type)
  - SubXact_callbacks (global callback list head)
- Called from (representative examples):
  - [StartSubTransaction](../S/StartSubTransaction.md) (at src/backend/access/transam/xact.c:5035)
  - [CommitSubTransaction](CommitSubTransaction.md) (at src/backend/access/transam/xact.c:5060, 5099)
  - [AbortSubTransaction](../A/AbortSubTransaction.md) (at src/backend/access/transam/xact.c:5273)

## Notes and Other Information
- The function is declared static, limiting its scope to the xact.c file
- Thread-safe iteration design allows callbacks to safely unregister themselves during execution
- Called at critical subtransaction lifecycle points: start, commit, and abort
- Each callback receives the same event information, allowing modules to react appropriately to subtransaction state changes
- The iteration pattern (capturing next pointer before callback) is a common defensive programming technique for callback systems

## Simplified Source

```c
// Simplified version of CallSubXactCallbacks
static void CallSubXactCallbacks(SubXactEvent event,
                               SubTransactionId mySubid,
                               SubTransactionId parentSubid) {
    SubXactCallbackItem *current_callback;
    SubXactCallbackItem *next_callback;

    // Iterate through all registered subtransaction callbacks
    for (current_callback = SubXact_callbacks; current_callback; current_callback = next_callback) {
        // Save next pointer before callback (allows safe self-unregistration)
        next_callback = current_callback->next;

        // Invoke the callback with event details
        current_callback->callback(event, mySubid, parentSubid, current_callback->arg);
    }
}
```

Key simplifications made:
- Renamed variables from `item`/`next` to more descriptive `current_callback`/`next_callback`
- Added explanatory comments for the core logic steps
- Clarified the defensive programming pattern of saving the next pointer
- Maintained the essential callback iteration and invocation logic
- Preserved the function signature and return type