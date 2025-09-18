# fmgr_security_definer_cache

## Location
src/backend/utils/fmgr/fmgr.c: 611 - 631

## Overview
This structure caches information required for executing security-definer functions and functions with proconfig settings, supporting both features through a unified call handler for efficiency.

## Definition


## Detailed Description
The fmgr_security_definer_cache structure is used to cache metadata for PostgreSQL functions that require special execution contexts. It supports two main features: security-definer functions (which run with the privileges of their owner rather than caller) and functions with proconfig settings (which modify GUC parameters during execution). The structure is designed to avoid repeated lookups of function metadata and GUC configuration during function execution, improving performance for these special function types.

The cache is populated once per query when a security-definer or proconfig function is first called, and the cached information is reused for subsequent calls within the same query. This approach significantly reduces the overhead of privilege switching and configuration changes.

## Parameters / Member Variables
- : Function manager information for the target function, containing lookup details and call information
- : The user ID to switch to for security-definer functions, or InvalidOid if no user switch is needed
- : List of GUC (Grand Unified Configuration) parameter names that need to be set before function execution
- : Pre-resolved handles for the GUC parameters to avoid name lookup overhead during execution
- : List of values to set for the corresponding GUC parameters
- : Passthrough argument used by function manager plugin modules for custom handling

## Dependencies
- Functions called/Symbols referenced:
  - [FmgrInfo](../F/FmgrInfo.md) (structure type)
  - Oid (type)
  - [List](../L/List.md) (structure type) 
  - Datum (type)
- Called from (representative examples):
  - [fmgr_security_definer](fmgr_security_definer.md)

## Notes and Other Information
This structure is part of PostgreSQL's function manager (fmgr) subsystem and is specifically designed to optimize the execution of functions that require special security or configuration contexts. The caching mechanism is essential for performance, as switching user contexts and modifying GUC parameters can be expensive operations. The structure is allocated in the function's memory context to ensure proper lifetime management and is typically stored in the fn_extra field of the FmgrInfo structure.