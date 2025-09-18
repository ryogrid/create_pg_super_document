# systable_inplace_update_finish

## Location
src/backend/access/index/genam.c: 873 - 891

## Overview
Completes an in-place tuple update operation by performing the actual overwrite of tuple data and properly cleaning up the scan state.

## Definition
void systable_inplace_update_finish(void *state, HeapTuple tuple)

## Detailed Description
This function represents the second and final phase of the in-place update process initiated by systable_inplace_update_begin(). It performs the actual tuple overwrite using the provided modified tuple data and handles proper cleanup of the scan state.

The function operates under strict constraints: the updated tuple cannot change size, which means its header fields and null bitmap remain unchanged. This size restriction is fundamental to the safety of in-place updates, as changing tuple size would require complex page reorganization that would violate the locking assumptions of concurrent readers.

The core update operation is delegated to heap_inplace_update_and_unlock(), which handles the low-level details of copying the new data into the existing tuple and releasing the exclusive locks acquired during the begin phase. After the update is complete, the function properly terminates the scan that was used to locate and lock the tuple.

## Parameters / Member Variables
- `state`: Opaque state pointer returned from systable_inplace_update_begin(), containing the SysScanDesc
- `tuple`: Modified tuple data to write over the existing tuple (must be same size as original)

## Dependencies
- Functions called/Symbols referenced:
  - heap_inplace_update_and_unlock
  - systable_endscan
  - SysScanDesc (type)
  - BufferHeapTupleTableSlot (type)
- Called from (representative examples):
  - index_update_stats
  - create_toast_table  
  - dropdb
  - EventTriggerOnLogin
  - vac_update_relstats
  - vac_update_datfrozenxid

## Notes and Other Information
- Must be paired with a prior successful call to systable_inplace_update_begin()
- The tuple parameter must have the same size as the original tuple found during begin phase
- Tuple header fields and null bitmap cannot be modified due to size constraints
- Automatically handles lock release and scan cleanup
- Should not be called if no actual changes were made to the tuple (use systable_inplace_update_cancel() instead)
- The state parameter becomes invalid after this call and should not be reused
- Part of the three-function in-place update API: begin, finish, and cancel