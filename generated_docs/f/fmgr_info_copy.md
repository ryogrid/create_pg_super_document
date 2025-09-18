# fmgr_info_copy

## Location
src/backend/utils/fmgr/fmgr.c: 580 - 594

## Overview
Creates a copy of an FmgrInfo structure while handling memory context management and resetting language-dependent subsidiary information.

## Definition


## Detailed Description
The  function performs a shallow copy of an FmgrInfo structure from source to destination. This function is necessary when FmgrInfo structures need to be duplicated across different memory contexts, such as when copying function call information from plan-time to execution-time contexts.

The function handles the inherent complexity of copying function manager information by taking a conservative approach: it copies all the basic function metadata but deliberately zeros out the  field. This is because  may contain language-specific subsidiary information that cannot be reliably duplicated across memory contexts. By resetting this field, the function ensures that any language-dependent state will be recomputed when needed in the new context.

## Parameters / Member Variables
- : FmgrInfo pointer to the destination structure where the copy will be stored
- : FmgrInfo pointer to the source structure to be copied
- : MemoryContext where the destination FmgrInfo will reside

## Dependencies
- Functions called/Symbols referenced:
  - memcpy (standard C library function)
- Called from (representative examples):
  - initGinState (GIN index operations)
  - initGISTstate (GiST index operations)
  - ScanKeyEntryInitializeWithInfo
  - fmgr_info_set_expr
  - Various BRIN index functions

## Notes and Other Information
- The function explicitly sets  to NULL, requiring subsidiary info to be recomputed
- This is a 'bogus' operation by design due to the complexity of duplicating language-dependent state
- The destination memory context is explicitly set to ensure proper memory management
- Widely used throughout PostgreSQL's index access methods and query execution
- Essential for copying function information between different execution phases