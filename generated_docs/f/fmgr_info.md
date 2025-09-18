# fmgr_info

## Location
[src/backend/utils/fmgr/fmgr.c:127-136](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/fmgr/fmgr.c#L127-L136)

## Overview
A convenience wrapper function that initializes a FmgrInfo struct for a given function Oid using the current memory context.

## Definition
```c
void fmgr_info(Oid functionId, FmgrInfo *finfo)
```

## Detailed Description
fmgr_info is a simplified interface to fmgr_info_cxt_security that provides commonly-used defaults for function information initialization. It fills a FmgrInfo structure with metadata about a PostgreSQL function identified by its Oid. The function uses the caller's CurrentMemoryContext as the memory context for the FmgrInfo struct and any subsidiary data.

This function is designed for typical use cases where the FmgrInfo struct is temporary (on the stack or in freshly-allocated space). For long-lived FmgrInfo structs that need to be stored in persistent tables, fmgr_info_cxt should be used instead to specify an appropriate memory context. The function sets security checking to false, meaning it doesn't perform additional security validation during function calls.

## Parameters / Member Variables
- `functionId`: The Oid of the function for which to initialize the FmgrInfo struct
- `finfo`: Pointer to a FmgrInfo struct to be filled with function metadata

## Dependencies
- Functions called/Symbols referenced:
  - [fmgr_info_cxt_security](fmgr_info_cxt_security.md) (the core function that performs the actual initialization)
  - CurrentMemoryContext (global variable representing the current memory context)
- Called from (representative examples):
  - [ScanKeyEntryInitialize](../S/ScanKeyEntryInitialize.md) (for initializing scan key comparison functions)
  - [ExecInitExprRec](../E/ExecInitExprRec.md) (during expression initialization in the executor)
  - [ExecInitFunc](../E/ExecInitFunc.md) (when setting up function calls in expressions)
  - [OidFunctionCall0Coll](../O/OidFunctionCall0Coll.md) through OidFunctionCall9Coll (convenience functions for direct function calls)
  - Various executor nodes (nodeAgg.c, nodeHash.c, nodeWindowAgg.c)
  - Statistics and selectivity estimation functions
  - Type input/output functions

## Notes and Other Information
- This is a public function available throughout PostgreSQL
- Part of PostgreSQL's Function Manager (fmgr) subsystem responsible for function call dispatch
- The caller must ensure that CurrentMemoryContext lives at least as long as the FmgrInfo struct
- For temporary function calls, this is typically the preferred interface over fmgr_info_cxt_security
- Does not perform security checking - use fmgr_info_cxt_security directly if security validation is needed
- Extensively used throughout PostgreSQL for initializing function call information in executors, operators, and built-in functions