# ExprEvalRowtypeCache

## Location
src/include/executor/execExpr.h: 45 - 55

## Overview
ExprEvalRowtypeCache is a struct used by ExprEvalSteps that need to cache a composite type's tuple descriptor for efficient access during expression evaluation.

## Definition

```c
typedef struct ExprEvalRowtypeCache
{
	/*
	 * cacheptr points to composite type's TypeCacheEntry if tupdesc_id is not
	 * 0; or for an anonymous RECORD type, it points directly at the cached
	 * tupdesc for the type, and tupdesc_id is 0.  (We'd use separate fields
	 * if space were not at a premium.)  Initial state is cacheptr == NULL.
	 */
	void	   *cacheptr;
	uint64		tupdesc_id;		/* last-seen tupdesc identifier, or 0 */
} ExprEvalRowtypeCache;
```
## Detailed Description
This structure provides a caching mechanism for composite type descriptors in PostgreSQL's expression evaluation system. It optimizes performance by avoiding repeated lookups of tuple descriptors for composite types during expression evaluation. The cache handles both regular composite types (which have TypeCacheEntry structures) and anonymous RECORD types (which cache the tuple descriptor directly).

The caching strategy uses a dual-purpose pointer system where  can point to either a TypeCacheEntry (for named composite types) or directly to a cached tuple descriptor (for anonymous RECORD types), with  serving as a validation mechanism to ensure cache consistency.

## Parameters / Member Variables
- : A void pointer that serves dual purposes - points to a composite type's TypeCacheEntry when tupdesc_id is non-zero, or points directly to the cached tuple descriptor for anonymous RECORD types when tupdesc_id is 0. Initial state is NULL.
- : A 64-bit identifier for the last-seen tuple descriptor, used for cache validation. Set to 0 for anonymous RECORD types or when the cache is invalid.

## Dependencies
- Functions called/Symbols referenced:
  - (None - this is a passive data structure)
- Called from (representative examples):
  - ExecInitExprRec (multiple locations in execExpr.c)
  - get_cached_rowtype (execExprInterp.c:2085)
  - EEO_JUMP (execExprInterp.c:151)
  - ExprEvalStep (used as member in various step types)

## Notes and Other Information
- This structure is designed to fit in-line within some ExprEvalStep types to minimize memory overhead, but can also be allocated out-of-line when necessary
- The dual-purpose nature of  is a space-saving optimization in a memory-constrained environment
- Cache invalidation is handled through the  field, which must match the current tuple descriptor identifier
- Used extensively in PostgreSQL's expression evaluation system for handling composite types efficiently