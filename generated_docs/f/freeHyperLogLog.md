# freeHyperLogLog

## Location
[src/backend/lib/hyperloglog.c:151-166](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/lib/hyperloglog.c#L151-L166)

## Overview
Releases allocated memory resources associated with a HyperLogLog state structure, specifically freeing the internal hash register array.

## Definition
```c
void freeHyperLogLog(hyperLogLogState *cState)
```

## Detailed Description
This function performs cleanup of a HyperLogLog state structure by releasing the dynamically allocated memory used for the hash register array (hashesArr). The function is designed to free only the internal allocations and not the state structure itself, allowing flexibility in how the state structure is managed (whether it was allocated via palloc or declared as a local/static variable).

The function includes an assertion to verify that the hashesArr pointer is not NULL before attempting to free it, providing defensive programming to catch potential double-free scenarios or use of uninitialized state structures.

## Parameters / Member Variables
- `cState`: Pointer to the hyperLogLogState structure whose internal resources should be freed

## Dependencies
- Functions called/Symbols referenced:
  - Assert (for debugging assertion)
  - [pfree](../p/pfree.md) (PostgreSQL memory deallocation function)
  - [hyperLogLogState](../h/hyperLogLogState.md) (structure type)
- Called from (representative examples):
  - [hashagg_spill_finish](../h/hashagg_spill_finish.md)

## Notes and Other Information
- Only frees the hashesArr field, not the state structure itself
- Allows the state structure to be stack-allocated or managed separately from its internal arrays
- Uses Assert() for debugging validation - will only check in debug builds
- Follows PostgreSQL memory management patterns using pfree() for cleanup
- Essential for preventing memory leaks when HyperLogLog operations are completed
- Typically called during cleanup phases of operations like hash aggregation spilling

## Simplified Source

```c
void
freeHyperLogLog(hyperLogLogState *cState)
{
    // Free the hash register array
    pfree(cState->hashesArr);
}
```