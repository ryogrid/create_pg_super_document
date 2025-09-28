# tstoreDestroyReceiver

## Location
[src/backend/executor/tstoreReceiver.c:229-237](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/tstoreReceiver.c#L229-L237)

## Overview
A cleanup function that destroys a tuplestore destination receiver by freeing its allocated memory when it is no longer needed.

## Definition
static void tstoreDestroyReceiver(DestReceiver *self)

## Detailed Description
This is a simple cleanup function that serves as the rDestroy callback for tuplestore destination receivers. It performs the final cleanup step in the lifecycle of a TStoreState object by deallocating the memory that was allocated for the receiver structure. The function is called when the executor or other components are finished using the tuplestore destination receiver and need to release its resources.

## Parameters / Member Variables
- self: A pointer to the DestReceiver structure to be destroyed. This is actually a TStoreState object cast to DestReceiver, but the function only needs to free the memory regardless of the specific type.

## Dependencies
- Functions called/Symbols referenced:
  - [pfree](../p/pfree.md) (PostgreSQL memory deallocation function)
- Called from (representative examples):
  - [CreateTuplestoreDestReceiver](../C/CreateTuplestoreDestReceiver.md) (sets this as the rDestroy callback)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the tstoreReceiver.c file
- The function assumes that all other cleanup (like closing the tuplestore itself) has already been handled by the rShutdown callback (tstoreShutdownReceiver)
- Part of the DestReceiver callback interface pattern used throughout PostgreSQL for handling query results
- The function is registered as the rDestroy callback in CreateTuplestoreDestReceiver
- Very simple implementation that only calls pfree - all the complex cleanup is handled elsewhere in the receiver lifecycle

## Simplified Source

```c
// Simplified version of tstoreDestroyReceiver
static void tstoreDestroyReceiver(DestReceiver *self) {
    // Final cleanup: free the receiver structure
    pfree(self);
}
```

Key simplifications made:
- Preserved essential memory deallocation
- Added clarifying comment about purpose
- Maintained minimal interface compliance