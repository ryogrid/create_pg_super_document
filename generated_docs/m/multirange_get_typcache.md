# multirange_get_typcache

## Location
[src/backend/utils/adt/multirangetypes.c:548-568](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/multirangetypes.c#L548-L568)

## Overview
Retrieves cached type information for a multirange type, using the function call context's fn_extra field for efficient caching of type metadata.

## Definition


## Detailed Description
This utility function provides efficient access to cached type information for multirange types. It follows PostgreSQL's convention of using the fn_extra field in FunctionCallInfo to cache type metadata across function calls. The function first checks if valid cached information exists for the requested multirange type, and if not, retrieves it from the type cache system. This caching mechanism significantly improves performance for functions that repeatedly operate on the same multirange type.

The function validates that the requested type is indeed a multirange type by checking that the rngtype field is not NULL, throwing an error if an invalid type is requested.

## Parameters / Member Variables
- : FunctionCallInfo structure containing function call context, including the fn_extra field used for caching
- : OID of the multirange type for which to retrieve cached information

## Dependencies
- Functions called/Symbols referenced:
  - [lookup_type_cache](../l/lookup_type_cache.md) (to retrieve type information when not cached)
  - TYPECACHE_MULTIRANGE_INFO (flag specifying what multirange information to cache)
  - elog (for error reporting when type validation fails)
- Called from (representative examples):
  - [multirange_constructor2](multirange_constructor2.md)
  - [multirange_constructor1](multirange_constructor1.md)
  - [multirange_constructor0](multirange_constructor0.md)
  - [multirange_union](multirange_union.md)
  - [multirange_minus](multirange_minus.md)
  - [multirange_intersect](multirange_intersect.md)
  - [range_agg_finalfn](../r/range_agg_finalfn.md)
  - [multirange_agg_transfn](multirange_agg_transfn.md)
  - [multirange_intersect_agg_transfn](multirange_intersect_agg_transfn.md)
  - Various multirange comparison and operation functions

## Notes and Other Information
- This function is not exposed in pg_proc but serves as a utility for implementing multirange functions in C
- The caching strategy improves performance by avoiding repeated type cache lookups for the same multirange type
- Functions that need to cache additional information beyond what this function provides must implement their own caching mechanisms
- The function validates type correctness by ensuring the rngtype field is not NULL, providing early error detection for invalid multirange types
- This is a fundamental building block used by most multirange operations in PostgreSQL