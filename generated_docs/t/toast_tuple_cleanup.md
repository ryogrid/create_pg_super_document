# toast_tuple_cleanup

## Location
src/backend/access/table/toast_helper.c: 275 - 317

## Overview
Performs cleanup operations after TOAST processing is complete, including freeing temporary memory allocations and deleting obsolete external values from the TOAST table.

## Definition


## Detailed Description
This function performs the final cleanup phase of TOAST processing after all compression and externalization operations have been completed. It handles two main cleanup tasks:

1. **Memory Management**: Frees any temporary memory allocations that were created during the TOAST process, such as compressed values or detoasted values that were processed but no longer needed.

2. **External Value Cleanup**: Deletes obsolete external values from the TOAST table that are no longer referenced by the updated tuple. This is particularly important during UPDATE operations where old external values need to be cleaned up to prevent storage leaks.

The function uses the flags set during the TOAST initialization and processing phases to determine what cleanup actions are necessary, ensuring efficient cleanup without unnecessary work.

## Parameters / Member Variables
- : ToastTupleContext containing the tuple data, metadata, and cleanup flags indicating what operations are needed

## Dependencies
- Functions called/Symbols referenced:
  - pfree
  - toast_delete_datum
  - DatumGetPointer
  - TOAST_NEEDS_FREE
  - TOAST_NEEDS_DELETE_OLD
  - TOASTCOL_NEEDS_FREE
  - TOASTCOL_NEEDS_DELETE_OLD
- Called from (representative examples):
  - heap_toast_insert_or_update

## Notes and Other Information
- This is the final step in the TOAST process, ensuring proper resource management
- Critical for preventing memory leaks during TOAST operations
- Prevents accumulation of obsolete external values in TOAST tables during updates
- Uses flags set during earlier phases to determine exactly what cleanup is needed
- The cleanup is conditional based on the flags to avoid unnecessary work when no cleanup is required
- Handles both the case of freeing temporary values created during processing and deleting old external values that are no longer needed