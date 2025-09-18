# fmgr_info_cxt

## Location
src/backend/utils/fmgr/fmgr.c: 137 - 146

## Overview
A function that initializes a FmgrInfo struct with a specified memory context for managing subsidiary data allocation.

## Definition
```c
void fmgr_info_cxt(Oid functionId, FmgrInfo *finfo, MemoryContext mcxt)
```

## Detailed Description
fmgr_info_cxt provides more control over memory management compared to the simpler fmgr_info function. It allows the caller to explicitly specify which memory context should be used for allocating subsidiary data associated with the FmgrInfo struct. This is particularly important for long-lived FmgrInfo structs that need to be stored in persistent data structures, where using an appropriate long-lived memory context prevents memory leaks and ensures proper cleanup.

Like fmgr_info, this function is a wrapper around fmgr_info_cxt_security, but it provides explicit memory context control while still using the default security setting (false). This function is commonly used in caching scenarios, type system initialization, and other contexts where FmgrInfo structs need to persist beyond the current transaction or memory context.

## Parameters / Member Variables
- `functionId`: The Oid of the function for which to initialize the FmgrInfo struct
- `finfo`: Pointer to a FmgrInfo struct to be filled with function metadata
- `mcxt`: The memory context in which to allocate subsidiary data for this FmgrInfo

## Dependencies
- Functions called/Symbols referenced:
  - [fmgr_info_cxt_security](fmgr_info_cxt_security.md) (the core function that performs the actual initialization)
- Called from (representative examples):
  - [index_getprocinfo](../i/index_getprocinfo.md) (for setting up index access method procedures)
  - [lookup_type_cache](../l/lookup_type_cache.md) (when initializing type cache entries with function info)
  - [CatalogCacheInitializeCache](../C/CatalogCacheInitializeCache.md) (during catalog cache setup)
  - [array_in](../a/array_in.md), array_out, array_recv, array_send (for array type I/O functions)
  - Various procedural language handlers (plperl, plpython, pltcl)
  - Sort support and comparison function setup
  - BRIN index strategy functions

## Notes and Other Information
- This is a public function available throughout PostgreSQL
- Preferred over fmgr_info when the FmgrInfo struct needs to persist in long-lived memory contexts
- Part of PostgreSQL's Function Manager (fmgr) subsystem responsible for function call dispatch
- Extensively used in type system caching, index access methods, and procedural language implementations
- The specified memory context must be long-lived enough to support the intended lifetime of the FmgrInfo struct
- Does not perform security checking - use fmgr_info_cxt_security directly if security validation is needed
- Critical for preventing memory leaks in scenarios where FmgrInfo structs are cached or stored persistently