# printtup_destroy

## Location
[src/backend/access/common/printtup.c:413-422](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/printtup.c#L413-L422)

## Overview
The printtup_destroy function deallocates the memory used by a printtup DestReceiver structure.

## Definition

```c
static void
printtup_destroy(DestReceiver *self)
```
## Detailed Description
The printtup_destroy function is a simple cleanup function that deallocates the memory occupied by a DR_printtup DestReceiver structure. This function is called after printtup_shutdown has cleaned up the internal resources of the structure.

The function performs only one operation: freeing the memory allocated for the DestReceiver structure itself using PostgreSQL's pfree function. This is the final step in the lifecycle of a printtup DestReceiver, ensuring that all memory associated with it is returned to the system.

## Parameters / Member Variables
- : DestReceiver pointer to the DR_printtup structure to be destroyed

## Dependencies
- Functions called/Symbols referenced:
  - [pfree](pfree.md): PostgreSQL memory deallocation function
- Called from (representative examples):
  - [printtup_create_DR](printtup_create_DR.md): Sets this function as the destroy handler for the DestReceiver

## Notes and Other Information
- The function is marked as static, indicating it's only used within the printtup.c file
- This function should only be called after printtup_shutdown has been called to clean up internal resources
- The function assumes that the shutdown function has already been called and that it's safe to free the structure
- This is the final cleanup step in the DestReceiver lifecycle for printtup operations
- The function is very simple by design, as the actual resource cleanup is handled by printtup_shutdown

## Simplified Source

```c
// Simplified version of printtup_destroy
static void printtup_destroy(DestReceiver *self) {
    // Final cleanup: deallocate the receiver structure
    pfree(self);
}
```

Key simplifications made:
- Preserved essential memory deallocation
- Added clarifying comment about final cleanup purpose
- Maintained minimal interface for DestReceiver lifecycle