# systable_inplace_update_cancel

## Location
src/backend/access/index/genam.c: 892 - 903

## Overview
Cancels an in-place update operation that was initiated but not completed, releasing locks and cleaning up scan state without modifying the tuple.

## Definition
void systable_inplace_update_cancel(void *state)

## Detailed Description
This function provides a safe way to abandon an in-place update operation that was started with systable_inplace_update_begin() but should not be completed. It serves as an alternative to performing a no-op update when the caller determines that no actual changes need to be made to the tuple.

The function performs proper cleanup by releasing the exclusive locks that were acquired on the target tuple during the begin phase, using heap_inplace_unlock() to handle the low-level lock release. It then terminates the scan that was used to locate and lock the tuple, ensuring that all resources are properly freed.

This is particularly useful in scenarios where the decision to update a tuple depends on conditions that can only be evaluated after acquiring the exclusive lock, such as checking if the tuple's current values already match the intended updates.

## Parameters / Member Variables
- `state`: Opaque state pointer returned from systable_inplace_update_begin(), containing the SysScanDesc and lock information

## Dependencies
- Functions called/Symbols referenced:
  - heap_inplace_unlock
  - systable_endscan
  - SysScanDesc (type)
  - BufferHeapTupleTableSlot (type)
- Called from (representative examples):
  - index_update_stats
  - EventTriggerOnLogin
  - vac_update_relstats
  - vac_update_datfrozenxid

## Notes and Other Information
- Must be paired with a prior successful call to systable_inplace_update_begin()
- Should be used instead of systable_inplace_update_finish() when no changes are needed
- Properly releases exclusive locks without modifying the tuple data
- Automatically handles scan cleanup and resource deallocation
- The state parameter becomes invalid after this call and should not be reused
- Part of the three-function in-place update API: begin, finish, and cancel
- Preferred over performing a no-op update as it avoids unnecessary write operations and WAL logging