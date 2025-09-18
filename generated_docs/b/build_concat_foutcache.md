# build_concat_foutcache

## Location
src/backend/utils/adt/varlena.c: 5384 - 5421

## Overview
Prepares a cache with function manager information for output functions of datatypes used in arguments of a concat-like function, optimizing repeated type conversions by caching the output function metadata.

## Definition


## Detailed Description
This static function builds and caches FmgrInfo structures for the output functions of data types starting from a specified argument index. The cache is stored in the function's memory context (fn_mcxt) to survive across multiple function calls, providing performance optimization for concatenation operations that need to convert various data types to their string representations. The function allocates memory for all arguments but only populates entries starting from the specified .

## Parameters / Member Variables
- : Function call information structure containing argument details and function metadata
- : Starting argument index from which to begin caching output function information (earlier slots are allocated but not filled)

## Dependencies
- Functions called/Symbols referenced:
  - MemoryContextAlloc
  - PG_NARGS
  - get_fn_expr_argtype
  - getTypeOutputInfo
  - fmgr_info_cxt
- Called from (representative examples):
  - concat_internal

## Notes and Other Information
- The function stores the cache in fcinfo->flinfo->fn_extra for persistence across calls
- Memory is allocated for all arguments but only populated from argidx onwards
- Error handling includes validation of argument data types with appropriate error messages
- The cache improves performance by avoiding repeated lookups of output function metadata during concatenation operations